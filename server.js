const http = require('http');
const https = require('https');
const sqlite3 = require('sqlite3').verbose();

function requireEnv(name) {
  const value = process.env[name];
  if (!value) throw new Error(`Missing required env ${name}`);
  return value;
}

const PORT = process.env.PORT || 3000;
const BASE_URL = requireEnv('BASE_URL');
const API_KEY = requireEnv('API_KEY');
const MODEL_NAME = process.env.MODEL_NAME || 'bitrix/bitrixgpt-5.5';
const WEBHOOK_TOKEN = requireEnv('WEBHOOK_TOKEN');
const ELAPSED_NOTIFICATION_CHAT_ID = process.env.ELAPSED_NOTIFICATION_CHAT_ID || 'chat42358';
const BITRIX_PORTAL_URL = process.env.BITRIX_PORTAL_URL || 'https://elros.bitrix24.ru';
const DB_PATH = process.env.DB_PATH || './task_time_logs.db';
const TASK_TIME_LOOKBACK_DAYS = Number(process.env.TASK_TIME_LOOKBACK_DAYS || 180);
const TASK_TIME_CHECK_HOUR_MSK = Number(process.env.TASK_TIME_CHECK_HOUR_MSK || 20);
const TASK_TIME_CHECK_MINUTE_MSK = Number(process.env.TASK_TIME_CHECK_MINUTE_MSK || 0);
const TASK_TIME_CHECK_ENABLED = process.env.TASK_TIME_CHECK_ENABLED !== 'false';
const TASK_TIME_CHECK_RUN_ON_START = process.env.TASK_TIME_CHECK_RUN_ON_START === 'true';
const TASK_TIME_ALERT_RETENTION_DAYS = Number(process.env.TASK_TIME_ALERT_RETENTION_DAYS || 180);
const API_REQUEST_TIMEOUT_MS = Number(process.env.API_REQUEST_TIMEOUT_MS || 60 * 1000);
const MSK_UTC_OFFSET_HOURS = 3;
const GEMMA_EXCLUDED_GROUP_IDS = new Set(['12', '92', '376', '490']);
const GEMMA_COMMENT_AUTHOR_ID = String(process.env.GEMMA_COMMENT_AUTHOR_ID || 204);
const AI_IMAGE_PROCESSING_ENABLED = true;
const AI_MAX_IMAGES = 15;
const AI_MAX_IMAGE_BYTES = 20 * 1024 * 1024;
const AI_MAX_TOTAL_IMAGE_BYTES = 28 * 1024 * 1024;
const AI_SUPPORTED_IMAGE_MIME_TYPES = new Set(['image/png', 'image/jpeg', 'image/gif', 'image/webp']);
const AUDIO_TRANSCRIPTION_MODEL = 'bitrix/deepdml/faster-whisper-large-v3-turbo-ct2';
const AI_MAX_AUDIO_FILES = 10;
const AI_MAX_AUDIO_BYTES = 25 * 1024 * 1024;
const TASK_SUMMARY_FIELD_CODE = 'UF_TASK_TITLE';

let taskTimeCheckInProgress = false;
let taskTimeCheckStartedAt = null;
let taskTimeCheckStage = null;
const debugState = {
  lastRequest: null,
  lastTaskTimeCheckRequest: null,
  nextTaskTimeCheck: null,
  lastTaskTimeChatDecision: null,
  lastTaskTimeCheck: null,
  lastTaskImages: null,
  lastError: null,
};

const db = new sqlite3.Database(DB_PATH);
db.serialize(() => {
  db.run(`CREATE TABLE IF NOT EXISTS task_time_state (
    task_id TEXT PRIMARY KEY,
    time_spent_in_logs INTEGER NOT NULL DEFAULT 0,
    duration_fact INTEGER NOT NULL DEFAULT 0,
    status TEXT,
    closed_date TEXT,
    group_id TEXT,
    title TEXT,
    task_link TEXT,
    last_checked_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
  )`);

  db.run(`CREATE TABLE IF NOT EXISTS task_time_alerts (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    task_id TEXT NOT NULL,
    old_time_spent_in_logs INTEGER NOT NULL DEFAULT 0,
    new_time_spent_in_logs INTEGER NOT NULL DEFAULT 0,
    old_duration_fact INTEGER NOT NULL DEFAULT 0,
    new_duration_fact INTEGER NOT NULL DEFAULT 0,
    task_link TEXT,
    sent_at TEXT NOT NULL
  )`);

});

function log(message, data) {
  console.log(`[${new Date().toISOString()}] ${message}`, data ? JSON.stringify(data) : '');
}

function saveDebug(key, value) {
  debugState[key] = {
    received_at: new Date().toISOString(),
    ...value,
  };
}

function getSafeHeaders(headers) {
  const safeHeaders = { ...headers };
  delete safeHeaders.authorization;
  delete safeHeaders.cookie;
  return safeHeaders;
}

function readBody(req) {
  return new Promise((resolve, reject) => {
    let body = '';
    req.on('data', chunk => { body += chunk.toString(); });
    req.on('end', () => resolve(body));
    req.on('error', reject);
  });
}

function parseFormUrlEncoded(body) {
  const result = {};

  for (const [key, value] of new URLSearchParams(body)) {
    const parts = key.split(/[\[\]]/).filter(Boolean);
    let current = result;

    parts.forEach((part, index) => {
      if (index === parts.length - 1) {
        current[part] = value;
        return;
      }

      current[part] ||= {};
      current = current[part];
    });
  }

  return result;
}

function appendParams(params, value, prefix) {
  if (Array.isArray(value)) {
    value.forEach((item, index) => appendParams(params, item, `${prefix}[${index}]`));
    return;
  }

  if (value && typeof value === 'object') {
    Object.entries(value).forEach(([key, item]) => {
      appendParams(params, item, prefix ? `${prefix}[${key}]` : key);
    });
    return;
  }

  if (prefix) params.append(prefix, value == null ? '' : String(value));
}

function requestJson(endpoint, options = {}) {
  const client = endpoint.protocol === 'https:' ? https : http;

  return new Promise((resolve, reject) => {
    let settled = false;
    const req = client.request(endpoint, options, res => {
      let data = '';
      res.setEncoding('utf8');
      res.on('data', chunk => { data += chunk; });
      res.on('end', () => {
        if (settled) return;
        settled = true;
        let json = null;

        try {
          json = data ? JSON.parse(data) : null;
        } catch {
          reject(new Error(`Invalid JSON from ${endpoint.pathname}: ${data}`));
          return;
        }

        if (res.statusCode < 200 || res.statusCode >= 300) {
          reject(new Error(`HTTP ${res.statusCode} from ${endpoint.pathname}: ${data}`));
          return;
        }

        if (json?.error) {
          reject(new Error(json.error_description || json.error));
          return;
        }

        if (json?.success === false) {
          reject(new Error(json.error?.message || json.error?.code || 'API returned success=false'));
          return;
        }

        resolve(json);
      });
    });

    req.setTimeout(API_REQUEST_TIMEOUT_MS, () => {
      if (settled) return;
      settled = true;
      req.destroy(new Error(`Request timeout from ${endpoint.pathname}`));
    });

    req.on('error', error => {
      if (settled) return;
      settled = true;
      reject(error);
    });
    if (options.body) req.write(options.body);
    req.end();
  });
}

function requestBuffer(endpoint, options = {}, redirectCount = 0) {
  const client = endpoint.protocol === 'https:' ? https : http;
  const maxBytes = options.maxBytes || AI_MAX_IMAGE_BYTES;
  const sizeLabel = options.sizeLabel || 'File';
  const requestOptions = { ...options };
  delete requestOptions.maxBytes;
  delete requestOptions.sizeLabel;

  return new Promise((resolve, reject) => {
    let settled = false;
    const req = client.request(endpoint, requestOptions, res => {
      const location = res.headers.location;
      if ([301, 302, 303, 307, 308].includes(res.statusCode) && location && redirectCount < 3) {
        res.resume();
        const redirectUrl = new URL(location, endpoint);
        const redirectOptions = { ...options };
        if (redirectUrl.origin !== endpoint.origin && redirectOptions.headers?.['X-Api-Key']) {
          redirectOptions.headers = { ...redirectOptions.headers };
          delete redirectOptions.headers['X-Api-Key'];
        }
        requestBuffer(redirectUrl, redirectOptions, redirectCount + 1).then(resolve, reject);
        return;
      }

      const contentLength = Number.parseInt(res.headers['content-length'] || '0', 10);
      if (Number.isFinite(contentLength) && contentLength > maxBytes) {
        settled = true;
        res.resume();
        reject(new Error(`${sizeLabel} is too large: ${contentLength} bytes`));
        return;
      }

      const chunks = [];
      let total = 0;

      res.on('data', chunk => {
        if (settled) return;
        total += chunk.length;
        if (total > maxBytes) {
          settled = true;
          reject(new Error(`${sizeLabel} is too large: ${total} bytes`));
          req.destroy(new Error(`${sizeLabel} is too large: ${total} bytes`));
          return;
        }
        chunks.push(chunk);
      });

      res.on('end', () => {
        if (settled) return;
        settled = true;

        const buffer = Buffer.concat(chunks);
        if (res.statusCode < 200 || res.statusCode >= 300) {
          reject(new Error(`HTTP ${res.statusCode} from ${endpoint.pathname}: ${buffer.toString('utf8')}`));
          return;
        }

        resolve({
          buffer,
          contentType: String(res.headers['content-type'] || '').split(';')[0].trim().toLowerCase(),
        });
      });
    });

    req.setTimeout(API_REQUEST_TIMEOUT_MS, () => {
      if (settled) return;
      settled = true;
      req.destroy(new Error(`Request timeout from ${endpoint.pathname}`));
    });

    req.on('error', error => {
      if (settled) return;
      settled = true;
      reject(error);
    });
    if (requestOptions.body) req.write(requestOptions.body);
    req.end();
  });
}

function coworkRequest(method, path, body) {
  return requestJson(new URL(`/v1${path}`, BASE_URL), {
    method,
    headers: {
      'Content-Type': 'application/json',
      'X-Api-Key': API_KEY,
    },
    body: body ? JSON.stringify(body) : undefined,
  });
}

function coworkDownload(path) {
  return requestBuffer(new URL(`/v1${path}`, BASE_URL), {
    method: 'GET',
    headers: { 'X-Api-Key': API_KEY },
  });
}

function escapeMultipartName(value) {
  return String(value || '').replace(/"/g, '\\"');
}

function buildMultipartBody(fields, file) {
  const boundary = `----codex-${Date.now()}-${Math.random().toString(16).slice(2)}`;
  const chunks = [];

  for (const [name, value] of Object.entries(fields)) {
    chunks.push(Buffer.from(
      `--${boundary}\r\nContent-Disposition: form-data; name="${escapeMultipartName(name)}"\r\n\r\n${value}\r\n`
    ));
  }

  chunks.push(Buffer.from(
    `--${boundary}\r\nContent-Disposition: form-data; name="${escapeMultipartName(file.fieldName)}"; filename="${escapeMultipartName(file.filename)}"\r\nContent-Type: ${file.contentType || 'application/octet-stream'}\r\n\r\n`
  ));
  chunks.push(file.buffer);
  chunks.push(Buffer.from(`\r\n--${boundary}--\r\n`));

  return { boundary, body: Buffer.concat(chunks) };
}

function coworkMultipartRequest(path, fields, file) {
  const { boundary, body } = buildMultipartBody(fields, file);
  return requestJson(new URL(`/v1${path}`, BASE_URL), {
    method: 'POST',
    headers: {
      'X-Api-Key': API_KEY,
      'Content-Type': `multipart/form-data; boundary=${boundary}`,
      'Content-Length': body.length,
    },
    body,
  });
}

function queryDb(sql, params = []) {
  return new Promise((resolve, reject) => {
    db.all(sql, params, (err, rows) => {
      if (err) reject(err);
      else resolve(rows);
    });
  });
}

function runDb(sql, params = []) {
  return new Promise((resolve, reject) => {
    db.run(sql, params, function onRun(err) {
      if (err) reject(err);
      else resolve(this);
    });
  });
}

function unwrapData(response) {
  return response?.data || response?.result || response || null;
}

function normalizeTaskPayload(response) {
  const data = unwrapData(response);
  return data?.task || data?.Task || data;
}

function normalizeTaskListPayload(response) {
  const data = unwrapData(response);
  if (Array.isArray(data)) return data;
  if (Array.isArray(data?.tasks)) return data.tasks;
  if (Array.isArray(data?.items)) return data.items;
  if (Array.isArray(response?.tasks)) return response.tasks;
  return [];
}

function normalizeId(value) {
  const id = Number.parseInt(value, 10);
  return Number.isInteger(id) && id > 0 ? id : null;
}

function normalizeIds(value) {
  if (!value) return [];

  const raw = Array.isArray(value)
    ? value
    : typeof value === 'object'
      ? Object.values(value)
      : [value];

  return [...new Set(raw.map(normalizeId).filter(Boolean))];
}

function normalizeHistoryField(field) {
  return String(field || '').replace(/[^a-z0-9]/gi, '').toUpperCase();
}

function getTaskIdFromOutgoingWebhook(body) {
  return (
    body?.data?.FIELDS_AFTER?.ID ||
    body?.data?.FIELDS_BEFORE?.ID ||
    body?.data?.ID ||
    body?.data?.id ||
    body?.task_id ||
    body?.taskId ||
    null
  );
}

function getStatusFromTask(task) {
  const status = task?.status || task?.STATUS || task?.realStatus || task?.REAL_STATUS;
  return status ? String(status) : null;
}

function getGroupIdFromTask(task) {
  const groupId = task?.groupId || task?.groupID || task?.GroupID || task?.group_id || task?.GROUP_ID || task?.group?.id;
  return groupId ? String(groupId) : null;
}

function getParentIdFromTask(task) {
  return task?.parentId || task?.PARENT_ID ? String(task.parentId || task.PARENT_ID) : null;
}

function getResponsibleIdFromTask(task) {
  const responsibleId = (
    task?.responsibleId ||
    task?.RESPONSIBLE_ID ||
    task?.responsible?.id ||
    task?.assignee?.id
  );
  return responsibleId ? String(responsibleId).replace(/^user_/i, '') : null;
}

function getCreatorIdFromTask(task) {
  const creatorId = (
    task?.createdBy ||
    task?.creatorId ||
    task?.CREATED_BY ||
    task?.creator?.id
  );
  return creatorId ? String(creatorId).replace(/^user_/i, '') : null;
}

function getGroupNameFromTask(task) {
  return task?.groupName || task?.group?.name || task?.group?.title || null;
}

function getTaskChatId(task) {
  const chatId = task?.chatId || task?.CHAT_ID || task?.chat?.id;
  return chatId ? String(chatId).replace(/^chat/i, '') : null;
}

function isCollabGroupName(groupName) {
  return String(groupName || '').toLowerCase().includes('коллаба');
}

function isGemmaExcludedGroupId(groupId) {
  return GEMMA_EXCLUDED_GROUP_IDS.has(String(groupId || ''));
}

function getTaskTitle(task) {
  return task?.title || task?.TITLE || '';
}

function getTaskClosedDate(task) {
  return task?.closedDate || task?.CLOSED_DATE || null;
}

function getTaskTimeSpent(task) {
  const value = task?.timeSpentInLogs ?? task?.TIME_SPENT_IN_LOGS ?? 0;
  const number = Number.parseInt(value, 10);
  return Number.isFinite(number) ? number : 0;
}

function getTaskDurationFact(task) {
  const value = task?.durationFact ?? task?.DURATION_FACT ?? 0;
  const number = Number.parseInt(value, 10);
  return Number.isFinite(number) ? number : 0;
}

function isTaskClosed(task) {
  return getStatusFromTask(task) === '5';
}

function normalizeCommentsPayload(response) {
  const data = unwrapData(response);
  const comments = data?.comments || data?.Comments || data?.items || data?.list || data;
  return Array.isArray(comments) ? comments : [];
}

function normalizeChatMessagesPayload(response) {
  const data = unwrapData(response);
  const messages = data?.messages || data?.Messages || data?.items || data?.list || data;
  return Array.isArray(messages) ? messages : [];
}

function normalizeChatFilesPayload(response) {
  const data = unwrapData(response);
  const files = data?.files || data?.Files || [];
  return Array.isArray(files) ? files : [];
}

function isDeletedChatMessage(message) {
  return message?.params?.isDeleted === 'Y' || message?.PARAMS?.IS_DELETED === 'Y';
}

function isUserChatMessage(message) {
  return !message?.isSystem && String(message?.authorId || message?.AUTHOR_ID || '') !== '0' && !isDeletedChatMessage(message);
}

function normalizeChatComment(message, taskId) {
  return {
    id: message?.id || message?.ID || null,
    taskId,
    chatId: message?.chatId || message?.CHAT_ID || null,
    authorId: message?.authorId || message?.AUTHOR_ID || null,
    message: message?.text || message?.TEXT || '',
    createdAt: message?.date || message?.createdAt || message?.DATE_CREATE || null,
    params: message?.params || message?.PARAMS || {},
    source: 'chat',
  };
}

async function fetchTaskChatMessages(task) {
  const chatId = getTaskChatId(task);
  if (!chatId) return { chatId: null, messages: [], files: [] };

  try {
    const response = await coworkRequest('GET', `/chats/chat${chatId}/messages`);
    return {
      chatId,
      messages: normalizeChatMessagesPayload(response),
      files: normalizeChatFilesPayload(response),
    };
  } catch (error) {
    log('Task chat messages fetch failed', { chat_id: chatId, error: error.message });
    return { chatId, messages: [], files: [] };
  }
}

function getCommentAuthorId(comment) {
  const authorId = (
    comment?.authorId ??
    comment?.AUTHOR_ID
  );

  return authorId == null ? null : String(authorId).replace(/^user_/i, '');
}

function isGemmaComment(comment) {
  return getCommentAuthorId(comment) === GEMMA_COMMENT_AUTHOR_ID;
}

function filterGemmaComments(comments) {
  return comments.filter(comment => !isGemmaComment(comment));
}

function normalizeMimeType(value) {
  return String(value || '').split(';')[0].trim().toLowerCase();
}

function getMimeTypeFromName(name) {
  const lowerName = String(name || '').toLowerCase();
  if (lowerName.endsWith('.png')) return 'image/png';
  if (lowerName.endsWith('.jpg') || lowerName.endsWith('.jpeg')) return 'image/jpeg';
  if (lowerName.endsWith('.gif')) return 'image/gif';
  if (lowerName.endsWith('.webp')) return 'image/webp';
  return null;
}

function getAudioMimeTypeFromName(name) {
  const lowerName = String(name || '').toLowerCase();
  if (lowerName.endsWith('.m4a')) return 'audio/mp4';
  if (lowerName.endsWith('.mp3')) return 'audio/mpeg';
  if (lowerName.endsWith('.wav')) return 'audio/wav';
  if (lowerName.endsWith('.ogg') || lowerName.endsWith('.oga')) return 'audio/ogg';
  if (lowerName.endsWith('.webm')) return 'audio/webm';
  if (lowerName.endsWith('.mp4')) return 'audio/mp4';
  return null;
}

function isSupportedImageMimeType(mimeType) {
  return AI_SUPPORTED_IMAGE_MIME_TYPES.has(normalizeMimeType(mimeType));
}

function isAudioFileObject(file) {
  const type = String(file?.type || file?.TYPE || '').toLowerCase();
  const name = file?.name || file?.NAME || file?.fileName || file?.FILENAME;
  return type === 'audio' || file?.isTranscribable === true || file?.IS_TRANSCRIBABLE === true || Boolean(getAudioMimeTypeFromName(name));
}

function getKnownFileIds(value) {
  if (!value) return [];

  const rawItems = Array.isArray(value)
    ? value
    : typeof value === 'object'
      ? Object.values(value)
      : [value];

  return rawItems
    .map(item => {
      if (typeof item === 'object' && item) {
        return item.fileId ?? item.FILE_ID ?? item.id ?? item.ID ?? item.objectId ?? item.OBJECT_ID;
      }

      return item;
    })
    .map(item => String(item || '').replace(/^n/i, ''))
    .filter(item => /^\d+$/.test(item));
}

function getMessageFileIds(message) {
  return getKnownFileIds(
    message?.params?.fileId ??
    message?.PARAMS?.FILE_ID ??
    message?.files ??
    message?.FILES ??
    message?.attachments ??
    message?.ATTACHMENTS
  );
}

function extractFileIdsFromText(text) {
  const value = String(text || '');
  const ids = [];
  const patterns = [
    /\[(?:DISK\s+FILE|FILE)[^\]]*\bID\s*=\s*["']?n?(\d+)["']?[^\]]*\]/gi,
    /\b(?:fileId|FILE_ID|id)\s*[=:]\s*["']?n?(\d+)["']?/g,
    /\/(?:disk|files?)\/(?:download|file|showFile|open)\/n?(\d+)/gi,
  ];

  for (const pattern of patterns) {
    let match = pattern.exec(value);
    while (match) {
      ids.push(match[1]);
      match = pattern.exec(value);
    }
  }

  return [...new Set(ids)];
}

function extractHttpsUrlsFromText(text) {
  return [...String(text || '').matchAll(/https:\/\/[^\s"'<>[\]]+/gi)]
    .map(match => match[0].replace(/[),.;]+$/, ''));
}

function getAttachmentFields(object) {
  return [
    object?.UF_TASK_WEBDAV_FILES,
    object?.ufTaskWebdavFiles,
    object?.files,
    object?.FILES,
    object?.attachedFiles,
    object?.ATTACHED_FILES,
    object?.attachments,
    object?.ATTACHMENTS,
  ];
}

function getCommentMessage(comment) {
  return comment?.message || comment?.MESSAGE || comment?.text || comment?.TEXT || '';
}

function getTaskTextFields(task) {
  return [
    task?.description,
    task?.DESCRIPTION,
    task?.descriptionInBbcode,
    task?.DESCRIPTION_IN_BBCODE,
  ].filter(value => typeof value === 'string' && value.trim());
}

function getObjectImageCandidate(object, source) {
  if (!object || typeof object !== 'object') return null;

  const name = object.name || object.NAME || object.fileName || object.FILENAME || object.title || object.TITLE;
  const objectType = object.type || object.TYPE;
  const mimeType = normalizeMimeType(
    object.mimeType ||
    object.MIME_TYPE ||
    object.contentType ||
    object.CONTENT_TYPE ||
    (objectType === 'image' ? getMimeTypeFromName(name) : objectType) ||
    getMimeTypeFromName(name)
  );
  const url = object.downloadUrl || object.downloadURL || object.DOWNLOAD_URL || object.urlDownload || object.URL_DOWNLOAD || object.urlPreview || object.URL_PREVIEW || object.urlShow || object.URL_SHOW || object.url || object.URL || object.link || object.LINK;
  const dataUrl = typeof url === 'string' && url.startsWith('data:image/') ? url : null;

  if (!dataUrl && !isSupportedImageMimeType(mimeType) && !getMimeTypeFromName(name)) {
    return null;
  }

  return {
    source,
    name: name || null,
    mimeType: mimeType || getMimeTypeFromName(name),
    url: typeof url === 'string' ? url : null,
    dataUrl,
    fileId: getKnownFileIds(object)[0] || null,
  };
}

function collectImageCandidates(value, source, depth = 0) {
  if (!AI_IMAGE_PROCESSING_ENABLED || depth > 3 || value == null) return [];

  if (typeof value === 'string') {
    const trimmed = value.trim();
    const candidates = [];

    if (trimmed.startsWith('data:image/')) {
      return [{ source, name: null, mimeType: normalizeMimeType(trimmed.slice(5, trimmed.indexOf(';'))), dataUrl: trimmed }];
    }

    for (const fileId of extractFileIdsFromText(trimmed)) {
      candidates.push({ source, name: null, mimeType: null, fileId });
    }

    for (const url of extractHttpsUrlsFromText(trimmed)) {
      candidates.push({ source, name: url.split('/').pop() || null, mimeType: getMimeTypeFromName(url), url });
    }

    return candidates;
  }

  if (Array.isArray(value)) {
    return value.flatMap((item, index) => collectImageCandidates(item, `${source}[${index}]`, depth + 1));
  }

  if (typeof value !== 'object') return [];

  const candidates = [];
  const objectCandidate = getObjectImageCandidate(value, source);
  if (objectCandidate) candidates.push(objectCandidate);

  for (const attachmentField of getAttachmentFields(value)) {
    for (const fileId of getKnownFileIds(attachmentField)) {
      candidates.push({ source, name: null, mimeType: null, fileId });
    }
  }

  return candidates;
}

function dedupeImageCandidates(candidates) {
  const seen = new Set();
  return candidates.filter(candidate => {
    const key = candidate.dataUrl || candidate.url || candidate.fileId;
    if (!key || seen.has(key)) return false;
    seen.add(key);
    return true;
  });
}

async function downloadImageCandidate(candidate) {
  if (candidate.dataUrl) {
    if (!isSupportedImageMimeType(candidate.mimeType)) return null;
    const base64 = String(candidate.dataUrl).split(',')[1] || '';
    const bytes = Math.ceil(base64.length * 3 / 4);
    if (bytes > AI_MAX_IMAGE_BYTES) return null;
    return { ...candidate, bytes };
  }

  let downloaded = null;
  if (candidate.fileId) {
    downloaded = await coworkDownload(`/files/${candidate.fileId}/download`);
  } else if (candidate.url) {
    downloaded = await requestBuffer(new URL(candidate.url), { method: 'GET' });
  }

  if (!downloaded?.buffer?.length) return null;

  const mimeType = normalizeMimeType(downloaded.contentType || candidate.mimeType || getMimeTypeFromName(candidate.name));
  if (!isSupportedImageMimeType(mimeType)) return null;

  return {
    ...candidate,
    mimeType,
    bytes: downloaded.buffer.length,
    dataUrl: `data:${mimeType};base64,${downloaded.buffer.toString('base64')}`,
  };
}

function getFileDownloadUrl(file) {
  return file?.downloadUrl || file?.downloadURL || file?.DOWNLOAD_URL || file?.urlDownload || file?.URL_DOWNLOAD || file?.urlShow || file?.URL_SHOW || file?.viewerAttrs?.src || file?.VIEWER_ATTRS?.SRC || null;
}

async function downloadAudioCandidate(candidate) {
  let downloaded = null;
  if (candidate.fileId) {
    try {
      downloaded = await requestBuffer(new URL(`/v1/files/${candidate.fileId}/download`, BASE_URL), {
        method: 'GET',
        headers: { 'X-Api-Key': API_KEY },
        maxBytes: AI_MAX_AUDIO_BYTES,
        sizeLabel: 'Audio',
      });
    } catch (error) {
      if (!candidate.url) throw error;
      downloaded = await requestBuffer(new URL(candidate.url), {
        method: 'GET',
        maxBytes: AI_MAX_AUDIO_BYTES,
        sizeLabel: 'Audio',
      });
    }
  } else if (candidate.url) {
    downloaded = await requestBuffer(new URL(candidate.url), {
      method: 'GET',
      maxBytes: AI_MAX_AUDIO_BYTES,
      sizeLabel: 'Audio',
    });
  }

  if (!downloaded?.buffer?.length) return null;

  const mimeType = normalizeMimeType(downloaded.contentType || candidate.mimeType || getAudioMimeTypeFromName(candidate.name)) || 'application/octet-stream';
  return {
    ...candidate,
    mimeType,
    bytes: downloaded.buffer.length,
    buffer: downloaded.buffer,
  };
}

async function transcribeAudio(audio) {
  const response = await coworkMultipartRequest('/audio/transcriptions', {
    model: AUDIO_TRANSCRIPTION_MODEL,
    language: 'ru',
    response_format: 'json',
  }, {
    fieldName: 'file',
    filename: audio.name || `audio-${audio.fileId || Date.now()}.m4a`,
    contentType: audio.mimeType || 'application/octet-stream',
    buffer: audio.buffer,
  });

  return response?.text || response?.data?.text || null;
}

async function prepareTaskImages(task, comments, scope) {
  const candidates = dedupeImageCandidates([
    ...getTaskTextFields(task).flatMap((text, index) => collectImageCandidates(text, `${scope}.task.text:${index}`)),
    ...getAttachmentFields(task).flatMap(field => collectImageCandidates(field, `${scope}.task.attachments`)),
    ...comments.flatMap(comment => {
      const commentSource = `${scope}.comment:${comment?.id || comment?.ID || 'unknown'}`;
      return [
        ...collectImageCandidates(getCommentMessage(comment), `${commentSource}.message`),
        ...getAttachmentFields(comment).flatMap(field => collectImageCandidates(field, `${commentSource}.attachments`)),
      ];
    }),
  ]);

  const images = [];
  for (const candidate of candidates) {
    if (images.length >= AI_MAX_IMAGES) break;
    try {
      const image = await downloadImageCandidate(candidate);
      if (image?.dataUrl) images.push(image);
    } catch (error) {
      log('Image skipped', {
        source: candidate.source,
        file_id: candidate.fileId || null,
        url: candidate.url || null,
        error: error.message,
      });
    }
  }

  return {
    candidatesCount: candidates.length,
    candidates: candidates.map(candidate => ({
      source: candidate.source,
      name: candidate.name || null,
      mimeType: candidate.mimeType || null,
      fileId: candidate.fileId || null,
      url: candidate.url || null,
      hasDataUrl: Boolean(candidate.dataUrl),
    })),
    images,
  };
}

async function prepareTaskChatImages(task, scope) {
  const chat = await fetchTaskChatMessages(task);
  const humanMessages = chat.messages.filter(message => !message?.isSystem && !isGemmaComment(message));
  const messageFileIds = new Set(humanMessages.flatMap(getMessageFileIds));
  const fileCandidates = chat.files
    .filter(file => !messageFileIds.size || messageFileIds.has(String(file?.id || file?.ID)))
    .flatMap(file => collectImageCandidates(file, `${scope}.chat:${chat.chatId}.file:${file?.id || file?.ID || 'unknown'}`));

  const messageCandidates = humanMessages.flatMap(message => {
    const messageSource = `${scope}.chat:${chat.chatId}.message:${message?.id || message?.ID || 'unknown'}`;
    return [
      ...collectImageCandidates(getCommentMessage(message), `${messageSource}.text`),
    ];
  });

  const candidates = dedupeImageCandidates([...fileCandidates, ...messageCandidates]);
  const images = [];
  for (const candidate of candidates) {
    if (images.length >= AI_MAX_IMAGES) break;
    try {
      const image = await downloadImageCandidate(candidate);
      if (image?.dataUrl) images.push(image);
    } catch (error) {
      log('Chat image skipped', {
        source: candidate.source,
        file_id: candidate.fileId || null,
        url: candidate.url || null,
        error: error.message,
      });
    }
  }

  return {
    chatId: chat.chatId,
    candidatesCount: candidates.length,
    candidates: candidates.map(candidate => ({
      source: candidate.source,
      name: candidate.name || null,
      mimeType: candidate.mimeType || null,
      fileId: candidate.fileId || null,
      url: candidate.url || null,
      hasDataUrl: Boolean(candidate.dataUrl),
    })),
    images,
  };
}

async function prepareTaskChatAudioTranscripts(task, scope) {
  const chat = await fetchTaskChatMessages(task);
  const humanMessages = chat.messages.filter(isUserChatMessage).filter(message => !isGemmaComment(message));
  const messageFileIds = new Set(humanMessages.flatMap(getMessageFileIds));
  const files = chat.files
    .filter(isAudioFileObject)
    .filter(file => !messageFileIds.size || messageFileIds.has(String(file?.id || file?.ID)))
    .slice(0, AI_MAX_AUDIO_FILES);

  const transcripts = [];
  const fileCandidates = files.map(file => ({
    source: `${scope}.chat:${chat.chatId}.audio:${file?.id || file?.ID || 'unknown'}`,
    fileId: file?.id || file?.ID || null,
    name: file?.name || file?.NAME || null,
    mimeType: getAudioMimeTypeFromName(file?.name || file?.NAME) || null,
    url: getFileDownloadUrl(file),
    size: file?.size || file?.SIZE || null,
    isVoiceNote: Boolean(file?.isVoiceNote || file?.IS_VOICE_NOTE),
  }));
  const knownAudioFileIds = new Set(fileCandidates.map(candidate => String(candidate.fileId || '')).filter(Boolean));
  const messageCandidates = humanMessages.flatMap(message => {
    const messageSource = `${scope}.chat:${chat.chatId}.message:${message?.id || message?.ID || 'unknown'}`;
    return getMessageFileIds(message)
      .filter(fileId => !knownAudioFileIds.has(String(fileId)))
      .map(fileId => ({
        source: `${messageSource}.params.fileId`,
        fileId,
        name: null,
        mimeType: null,
        url: null,
        size: null,
        isVoiceNote: null,
      }));
  });
  const candidates = [...fileCandidates, ...messageCandidates].slice(0, AI_MAX_AUDIO_FILES);

  for (const candidate of candidates) {
    try {
      const audio = await downloadAudioCandidate(candidate);
      if (!audio) continue;
      const text = await transcribeAudio(audio);
      if (text) {
        transcripts.push({
          source: candidate.source,
          fileId: candidate.fileId,
          name: candidate.name,
          bytes: audio.bytes,
          text,
        });
      }
    } catch (error) {
      log('Audio transcription skipped', {
        source: candidate.source,
        file_id: candidate.fileId || null,
        error: error.message,
      });
    }
  }

  return {
    chatId: chat.chatId,
    candidatesCount: candidates.length,
    candidates,
    transcripts,
  };
}

function buildAiMessageContent(prompt, images) {
  if (!images.length) return prompt;

  let totalImageBytes = 0;
  const limitedImages = [];
  for (const image of images) {
    const base64 = String(image.dataUrl || '').split(',')[1] || '';
    const imageBytes = Math.ceil(base64.length * 3 / 4);
    if (totalImageBytes + imageBytes > AI_MAX_TOTAL_IMAGE_BYTES) break;
    totalImageBytes += imageBytes;
    limitedImages.push(image);
  }

  if (!limitedImages.length) return prompt;

  return [
    { type: 'text', text: prompt },
    ...limitedImages.map(image => ({
      type: 'image_url',
      image_url: { url: image.dataUrl },
    })),
  ];
}

function truncateDebugText(value, maxLength = 1500) {
  const text = String(value || '');
  return text.length > maxLength ? `${text.slice(0, maxLength)}...` : text;
}

async function extractImageFacts(images, scope) {
  if (!images.length) return null;

  try {
    const response = await coworkRequest('POST', '/chat/completions', {
      model: MODEL_NAME,
      messages: [{
        role: 'user',
        content: buildAiMessageContent(`Ты анализируешь изображения, приложенные к задачам технической поддержки 1С.
Твоя задача — извлечь из изображения максимум достоверной информации.

Сначала выполни OCR:
- перепиши весь читаемый текст максимально дословно;
- сохрани числа, проценты, названия систем, документов, ошибок, пунктов и подпунктов;
- не исправляй и не дополняй текст.

Затем сформируй полное описание изображения.
Включи все существенные видимые сведения: тексты ошибок, названия объектов, документов, форм, пользователей, организаций, даты, номера, суммы, статусы, настройки и результаты проверок.
Используй только сведения, которые действительно присутствуют на изображении.
Не придумывай отсутствующие данные.
Не объясняй причины.
Не предлагай решение.
Не делай выводов, которых нет на изображении.
Если данные невозможно определить, напиши: Не удалось определить.`, images),
      }],
    });

    return response?.choices?.[0]?.message?.content?.trim() || null;
  } catch (error) {
    log('Image facts extraction failed', { scope, error: error.message });
    return null;
  }
}

function normalizeTimePayload(response) {
  const data = unwrapData(response);
  return Array.isArray(data) ? data : [];
}

async function fetchTaskWithComments(taskId) {
  const taskResult = await coworkRequest('GET', `/tasks/${taskId}`);
  const task = normalizeTaskPayload(taskResult);
  const [commentsResult, chatResult] = await Promise.allSettled([
    coworkRequest('GET', `/tasks/${taskId}/comments`),
    fetchTaskChatMessages(task),
  ]);

  const fallbackComments = commentsResult.status === 'fulfilled'
    ? normalizeCommentsPayload(commentsResult.value)
    : [];
  const chatComments = chatResult.status === 'fulfilled'
    ? chatResult.value.messages
      .filter(isUserChatMessage)
      .map(message => normalizeChatComment(message, taskId))
    : [];

  return {
    task,
    comments: chatComments.length > 0 ? chatComments : fallbackComments,
    commentsSource: chatComments.length > 0 ? 'chat' : 'task_comments',
  };
}

async function fetchTaskTimeLogs(taskId) {
  const response = await coworkRequest('GET', `/tasks/${taskId}/time`);
  return normalizeTimePayload(response);
}

function getLookbackDate(days) {
  const date = new Date();
  date.setDate(date.getDate() - days);
  return date.toISOString();
}

function buildTaskListQuery(offset) {
  const params = new URLSearchParams();
  params.set('limit', '5000');
  params.set('offset', String(offset));
  params.set('sort', 'id');
  params.set('filter[status]', '5');
  params.set('filter[closedDate][$gte]', getLookbackDate(TASK_TIME_LOOKBACK_DAYS));
  params.set('select', [
    'id',
    'title',
    'status',
    'groupId',
    'closedDate',
    'timeSpentInLogs',
    'durationFact',
  ].join(','));
  return params.toString();
}

async function fetchClosedTasksForTimeCheck() {
  const tasks = [];

  for (let offset = 0; ; offset += 5000) {
    const path = `/tasks?${buildTaskListQuery(offset)}`;
    saveDebug('lastTaskTimeCheckRequest', {
      method: 'GET',
      path,
      offset,
      stage: 'fetch_closed_tasks',
    });

    const response = await coworkRequest('GET', path);
    const pageTasks = normalizeTaskListPayload(response);
    tasks.push(...pageTasks);

    const hasMore = Boolean(response?.meta?.hasMore || response?.hasMore);
    if (!hasMore || pageTasks.length === 0) break;
  }

  return tasks.filter(isTaskClosed);
}

function buildTaskLink(task) {
  const taskId = String(task?.id || task?.ID);
  const groupId = getGroupIdFromTask(task);
  return `${BITRIX_PORTAL_URL}/workgroups/group/${groupId}/tasks/task/view/${taskId}/`;
}

async function getTaskTimeState(taskId) {
  const rows = await queryDb('SELECT * FROM task_time_state WHERE task_id = ?', [String(taskId)]);
  return rows[0] || null;
}

async function saveTaskTimeState(task) {
  const now = new Date().toISOString();
  const taskId = String(task?.id || task?.ID);
  const groupId = getGroupIdFromTask(task);
  const taskLink = buildTaskLink(task);

  await runDb(`
    INSERT INTO task_time_state (
      task_id,
      time_spent_in_logs,
      duration_fact,
      status,
      closed_date,
      group_id,
      title,
      task_link,
      last_checked_at,
      updated_at
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ON CONFLICT(task_id) DO UPDATE SET
      time_spent_in_logs = excluded.time_spent_in_logs,
      duration_fact = excluded.duration_fact,
      status = excluded.status,
      closed_date = excluded.closed_date,
      group_id = excluded.group_id,
      title = excluded.title,
      task_link = excluded.task_link,
      last_checked_at = excluded.last_checked_at,
      updated_at = excluded.updated_at
  `, [
    taskId,
    getTaskTimeSpent(task),
    getTaskDurationFact(task),
    getStatusFromTask(task),
    getTaskClosedDate(task),
    groupId,
    getTaskTitle(task),
    taskLink,
    now,
    now,
  ]);
}

async function rememberClosedTaskTime(taskId) {
  const taskResponse = await coworkRequest('GET', `/tasks/${taskId}`);
  const task = normalizeTaskPayload(taskResponse);

  if (!isTaskClosed(task)) {
    return { ok: true, skipped: true, reason: 'task_not_closed', task_id: taskId };
  }

  await saveTaskTimeState(task);
  return {
    ok: true,
    task_id: String(taskId),
    time_spent_in_logs: getTaskTimeSpent(task),
    duration_fact: getTaskDurationFact(task),
  };
}

async function saveTimeAlert(change) {
  await runDb(`
    INSERT INTO task_time_alerts (
      task_id,
      old_time_spent_in_logs,
      new_time_spent_in_logs,
      old_duration_fact,
      new_duration_fact,
      task_link,
      sent_at
    ) VALUES (?, ?, ?, ?, ?, ?, ?)
  `, [
    change.taskId,
    change.oldTimeSpent,
    change.newTimeSpent,
    change.oldDurationFact,
    change.newDurationFact,
    change.taskLink,
    new Date().toISOString(),
  ]);
}

function formatHours(minutes) {
  return (Math.abs(minutes) / 60).toFixed(2).replace('.', ',');
}

function getChangeIcon(minutes) {
  return minutes >= 0 ? '➕' : '➖';
}

function getChangeVerb(minutes) {
  return minutes >= 0 ? 'увеличена' : 'уменьшена';
}

function getHistoryUserName(historyItem) {
  const name = historyItem?.user?.name || '';
  const lastName = historyItem?.user?.lastName || '';
  const fullName = `${name} ${lastName}`.trim();
  return fullName || 'Неизвестный пользователь';
}

function getHistoryUserId(historyItem) {
  return historyItem?.user?.id || historyItem?.userId || historyItem?.USER_ID || null;
}

function getHistoryTimeDiffMinutes(historyItem) {
  const from = Number.parseInt(historyItem?.value?.from ?? 0, 10);
  const to = Number.parseInt(historyItem?.value?.to ?? 0, 10);
  const safeFrom = Number.isFinite(from) ? from : 0;
  const safeTo = Number.isFinite(to) ? to : 0;
  return Math.round((safeTo - safeFrom) / 60);
}

function isTimeSpentHistoryItem(historyItem) {
  return (
    normalizeHistoryField(historyItem?.field) === normalizeHistoryField('TIME_SPENT_IN_LOGS') &&
    String(historyItem?.value?.from) !== String(historyItem?.value?.to)
  );
}

async function getTimeHistoryChangesForTask(task, previousState, fallbackChange) {
  const taskId = String(task?.id || task?.ID);
  const lastCheckedAtMs = Date.parse(previousState?.last_checked_at);
  const history = await getTaskHistory(taskId);
  const taskLink = buildTaskLink(task);

  const historyChanges = history
    .filter(isTimeSpentHistoryItem)
    .filter(item => !Number.isFinite(lastCheckedAtMs) || item.createdAtMs > lastCheckedAtMs)
    .sort((a, b) => a.createdAtMs - b.createdAtMs)
    .map(item => ({
      task,
      taskId,
      taskLink,
      title: getTaskTitle(task),
      groupId: getGroupIdFromTask(task),
      userId: getHistoryUserId(item),
      userName: getHistoryUserName(item),
      oldTimeSpent: Number.parseInt(item.value?.from ?? 0, 10) || 0,
      newTimeSpent: Number.parseInt(item.value?.to ?? 0, 10) || 0,
      oldDurationFact: Math.round((Number.parseInt(item.value?.from ?? 0, 10) || 0) / 60),
      newDurationFact: Math.round((Number.parseInt(item.value?.to ?? 0, 10) || 0) / 60),
      diffMinutes: getHistoryTimeDiffMinutes(item),
      history_id: item.id || null,
      history_created_date: item.createdDate || null,
    }));

  if (historyChanges.length > 0) return historyChanges;

  return [{
    ...fallbackChange,
    userId: null,
    userName: 'Неизвестный пользователь',
    history_id: null,
    history_created_date: null,
  }];
}

function saveTimeChatDecision(changes, message) {
  const taskIds = [...new Set(changes.map(change => change.taskId))];
  const users = [...new Map(changes.map(change => [
    `${change.userId || ''}:${change.userName || 'Неизвестный пользователь'}`,
    {
      user_id: change.userId || null,
      user_name: change.userName || 'Неизвестный пользователь',
    },
  ])).values()];

  saveDebug('lastTaskTimeChatDecision', {
    sent: Boolean(message),
    reason: changes.length > 0 ? 'time_changes_found' : 'no_time_changes',
    chat_id: ELAPSED_NOTIFICATION_CHAT_ID,
    changed_tasks: taskIds.length,
    changed_time_events: changes.length,
    task_ids: taskIds,
    users,
  });
}

function buildTimeChangesMessage(changes) {
  const groups = new Map();

  for (const change of changes) {
    const userName = change.userName || 'Неизвестный пользователь';
    if (!groups.has(userName)) groups.set(userName, []);
    groups.get(userName).push(change);
  }

  const lines = ['🐀 ☣️ Время в закрытых задачах изменено:'];

  for (const [userName, userChanges] of groups) {
    lines.push('', `[b]${userName}[/b]`);

    for (const change of userChanges) {
      lines.push(`${getChangeIcon(change.diffMinutes)} Задача ${change.taskId} ${getChangeVerb(change.diffMinutes)} на [b]${formatHours(change.diffMinutes)} ч.[/b]`);
      lines.push(change.taskLink);
    }
  }

  return lines.join('\n').trim();
}

async function sendTimeChangesReport(changes) {
  if (changes.length === 0) return null;

  const message = buildTimeChangesMessage(changes);
  await coworkRequest('POST', `/chats/${ELAPSED_NOTIFICATION_CHAT_ID}/messages`, { message });

  for (const change of changes) {
    await saveTimeAlert(change);
  }

  return message;
}

async function cleanupOldTimeAlerts() {
  const cutoffDate = new Date();
  cutoffDate.setDate(cutoffDate.getDate() - TASK_TIME_ALERT_RETENTION_DAYS);

  const result = await runDb(
    'DELETE FROM task_time_alerts WHERE sent_at < ?',
    [cutoffDate.toISOString()]
  );

  return result.changes || 0;
}

async function runTaskTimeCheck() {
  if (taskTimeCheckInProgress) {
    const result = {
      ok: true,
      skipped: true,
      reason: 'task_time_check_already_running',
      started_at: taskTimeCheckStartedAt,
      stage: taskTimeCheckStage,
    };
    saveDebug('lastTaskTimeCheck', result);
    return result;
  }

  taskTimeCheckInProgress = true;
  taskTimeCheckStartedAt = new Date().toISOString();
  taskTimeCheckStage = 'fetch_closed_tasks';
  saveDebug('lastTaskTimeCheckRequest', {
    method: 'START',
    path: null,
    stage: taskTimeCheckStage,
    scheduled_hour_msk: TASK_TIME_CHECK_HOUR_MSK,
    scheduled_minute_msk: TASK_TIME_CHECK_MINUTE_MSK,
  });

  try {
    const tasks = await fetchClosedTasksForTimeCheck();
    const changes = [];
    let initialized = 0;
    let unchanged = 0;
    let historyChecked = 0;

    taskTimeCheckStage = 'compare_with_sqlite';

    for (const task of tasks) {
      const taskId = String(task?.id || task?.ID);
      if (!taskId) continue;

      const previousState = await getTaskTimeState(taskId);
      const newTimeSpent = getTaskTimeSpent(task);
      const newDurationFact = getTaskDurationFact(task);

      if (!previousState) {
        initialized += 1;
        await saveTaskTimeState(task);
        continue;
      }

      const oldTimeSpent = Number(previousState.time_spent_in_logs || 0);
      const oldDurationFact = Number(previousState.duration_fact || 0);

      if (oldTimeSpent === newTimeSpent) {
        unchanged += 1;
        await saveTaskTimeState(task);
        continue;
      }

      const fallbackChange = {
        task,
        taskId,
        taskLink: buildTaskLink(task),
        title: getTaskTitle(task),
        groupId: getGroupIdFromTask(task),
        oldTimeSpent,
        newTimeSpent,
        oldDurationFact,
        newDurationFact,
        diffMinutes: Math.round((newTimeSpent - oldTimeSpent) / 60),
      };

      historyChecked += 1;
      const historyChanges = await getTimeHistoryChangesForTask(task, previousState, fallbackChange);
      changes.push(...historyChanges);
    }

    taskTimeCheckStage = 'send_chat_report';
    const message = await sendTimeChangesReport(changes);
    saveTimeChatDecision(changes, message);

    taskTimeCheckStage = 'save_changed_tasks';
    for (const change of changes) {
      await saveTaskTimeState(change.task);
    }

    taskTimeCheckStage = 'cleanup_old_alerts';
    const deletedOldAlerts = await cleanupOldTimeAlerts();

    const visibleChanges = changes.map(({ task, ...change }) => change);
    const uniqueChangedTasks = new Set(changes.map(change => change.taskId)).size;
    const result = {
      ok: true,
      lookback_days: TASK_TIME_LOOKBACK_DAYS,
      alert_retention_days: TASK_TIME_ALERT_RETENTION_DAYS,
      checked_tasks: tasks.length,
      initialized_tasks: initialized,
      unchanged_tasks: unchanged,
      changed_tasks: uniqueChangedTasks,
      changed_time_events: changes.length,
      history_checked_tasks: historyChecked,
      deleted_old_alerts: deletedOldAlerts,
      chat_id: ELAPSED_NOTIFICATION_CHAT_ID,
      message_sent: Boolean(message),
      changes: visibleChanges,
    };

    saveDebug('lastTaskTimeCheck', result);
    log('Task time check completed', result);
    return result;
  } finally {
    taskTimeCheckInProgress = false;
    taskTimeCheckStartedAt = null;
    taskTimeCheckStage = null;
  }
}

async function addAccomplice(taskId, userId) {
  const taskResponse = await coworkRequest('GET', `/tasks/${taskId}`);
  const task = normalizeTaskPayload(taskResponse);
  const accomplices = normalizeIds(task?.accomplices || task?.accompliceIds || task?.ACCOMPLICES);
  const normalizedUserId = normalizeId(userId);

  if (!normalizedUserId) throw new Error(`Invalid accomplice user ID: ${userId}`);
  if (accomplices.includes(normalizedUserId)) {
    return { added: false, reason: 'already_accomplice', taskId, userId: normalizedUserId };
  }

  await coworkRequest('PATCH', `/tasks/${taskId}`, { accomplices: [...accomplices, normalizedUserId] });
  return { added: true, taskId, userId: normalizedUserId };
}

function normalizeTaskHistoryPayload(response) {
  const data = unwrapData(response);
  const history = data?.history || data?.items || data?.list || data;
  return Array.isArray(history) ? history : [];
}

async function getTaskHistory(taskId) {
  const response = await coworkRequest('GET', `/tasks/${encodeURIComponent(taskId)}/history?order=desc`);

  return normalizeTaskHistoryPayload(response)
    .map(item => ({ ...item, createdAtMs: Date.parse(item.createdDate) }))
    .filter(item => Number.isFinite(item.createdAtMs))
    .sort((a, b) => b.createdAtMs - a.createdAtMs);
}

function getWebhookTimestampMs(data) {
  const timestamp = Number.parseInt(data?.ts, 10);
  return Number.isFinite(timestamp) && timestamp > 0 ? timestamp * 1000 : null;
}

function getHistoryBatchNearWebhook(history, webhookTimestampMs) {
  if (!Number.isFinite(webhookTimestampMs)) return [];

  const nearbyHistory = history.filter(item => Math.abs(item.createdAtMs - webhookTimestampMs) <= 10 * 1000);
  const latestNearbyChange = nearbyHistory[0];
  if (!latestNearbyChange) return [];

  return nearbyHistory.filter(item => Math.abs(latestNearbyChange.createdAtMs - item.createdAtMs) <= 500);
}

async function getLatestUpdateBatch(taskId, webhookTimestampMs = null) {
  const sortedHistory = await getTaskHistory(taskId);
  const webhookBatch = getHistoryBatchNearWebhook(sortedHistory, webhookTimestampMs);
  if (webhookBatch.length > 0) return webhookBatch;

  if (Number.isFinite(webhookTimestampMs)) return [];

  const latestChange = sortedHistory[0];
  if (!latestChange) return [];
  return sortedHistory.filter(item => latestChange.createdAtMs - item.createdAtMs <= 2000);
}

function isInsufficientInfoComment(comment) {
  return String(comment || '').includes('Недостаточно информации');
}

function extractTitleFromAiComment(comment) {
  const text = String(comment || '');
  const titleMatch = text.match(/\[b\]📝 TITLE:\[\/b\]\s*([\s\S]*)$/i);
  if (!titleMatch) return null;

  return titleMatch[1]
    .split('\n')
    .map(line => line.trim())
    .filter(Boolean)
    .map(line => line.replace(/^\d+\.\s*/, '').trim())
    .join('\n')
    .trim() || null;
}

async function updateTaskSummaryField(taskId, value) {
  try {
    await coworkRequest('PATCH', `/tasks/${taskId}`, {
      [TASK_SUMMARY_FIELD_CODE]: value,
    });
    return { updated: true, field: TASK_SUMMARY_FIELD_CODE, error: null };
  } catch (error) {
    return {
      updated: false,
      field: TASK_SUMMARY_FIELD_CODE,
      error: error.message,
    };
  }
}

function getImageMetadata(images) {
  return images.map(image => ({
    source: image.source,
    name: image.name || null,
    mimeType: image.mimeType || null,
    fileId: image.fileId || null,
    bytes: image.bytes || null,
  }));
}

function buildPrompt({ taskId, groupId, responsibleId, creatorId, mainTask, mainComments, mainTimeLogs, mainTimeSpentInLogs, mainImages, mainImageFacts, mainAudioTranscripts, contextparentID }) {
  const context = {
    currentTaskId: taskId,
    groupId,
    responsibleId,
    creatorId,
    currentTask: mainTask,
    currentTaskComments: mainComments,
    currentTaskTime: mainTimeLogs,
    currentTaskTimeSpentInLogs: mainTimeSpentInLogs,
    currentTaskImages: getImageMetadata(mainImages),
    currentTaskImageFacts: mainImageFacts,
    currentTaskAudioTranscripts: mainAudioTranscripts,
  };

  return `Ты - профессиональный консультант 1С. Твоя задача - проанализировать данные задачи и написать краткий итог. Ты анализируешь задачи компании-франчайзи 1С. Используй профессиональную терминологию, принятую в сфере внедрения и сопровождения продуктов 1С.

НИЖЕ ПРИВЕДЕН КОНТЕКСТ ЗАДАЧИ (JSON):
${JSON.stringify(context, null, 2)}

НИЖЕ ПРИВЕДЕН КОНТЕКСТ РОДИТЕЛЬСКОЙ ЗАДАЧИ (JSON):
${JSON.stringify(contextparentID, null, 2)}

Правила анализа:
- Основной объект анализа - currentTask, currentTaskComments, currentTaskTimeSpentInLogs, currentTaskId, currentTaskImageFacts, currentTaskAudioTranscripts.
- Если контекст родительской задачи не равен null, используй parentTask, parentTaskComments, parentTaskTimeSpentInLogs, parentTaskImageFacts, parentTaskAudioTranscripts только как дополнительный контекст.
- Если есть несоответствие данных задачи и комментариев, ориентируйся на комментарии.
- Используй только информацию, содержащуюся в JSON. Ничего не выдумывай и не добавляй от себя.
- Не добавляй технические детали, если они отсутствуют в исходных данных.
- Не называй конкретные механизмы 1С (планы обмена, регистры, EnterpriseData, СКД и т.п.), если они прямо не указаны или очевидно не следуют из контекста.

ЖЕСТКИЕ ПРАВИЛА нормализации отраслевой терминологии 1С <Таблица трансформации>:
Категорически запрещено переносить в результат разговорную речь, жалобы клиентов или бытовой сленг сотрудников, слова "срочно", "важно", "помощь". Обязательно переводи текстовые маркеры из левой части в строго профессиональные термины 1С из правой части:
1. Инфраструктура, базы данных и доработки:
- "программа", "1ска", "конфа" -> база / информационная база / типовая конфигурация / отраслевое решение.
- "накатить обнову", "обновить 1С", "поставить релиз" -> обновление конфигурации (типовой/доработанной) / обновление платформы 1С:Предприятие.
- "сделать доработку", "дописать код", "добавить кнопку", "запилить функцию" -> доработка конфигурации / разработка расширения конфигурации / добавление реквизита.
- "печатная форма не печатает", "сделать макет", "поправить ТОРГ-12/счет" -> модификация макета печатной формы / разработка внешней печатной формы.
- "написать отчет", "сделать выгрузку в Excel" -> разработка отчета / создание внешнего отчета.
- "обработка", "скрипт", "сделайте штуку чтобы заполнялось" -> разработка/применение внешней обработки.
- "доработка" - изменение уже имеющегося функционала, "разработка" - добавление нового функционала, "доработка" и "разработка" это не одно и то же, не искажай смысл этих слов из полученных сведений по задаче.
2. Обмены, интеграции и регламентные процедуры
- "глюк/косяк/проблема с обменом", "не идет обмен", "двоятся данные" -> ошибка в механизме синхронизации данных / сбой плана обмена / рассинхронизация объектов метаданных.
- "обмен через ED", "новый обмен" -> обмен данными по стандарту EnterpriseData.
- "робот не отработал", "запустить регламентное", "автоматом не считается" -> сбой выполнения регламентного задания / настройка расписания регламентного задания.
- "загрузить выписку", "проблема с банком/клиент-банком" -> обмен с банком / загрузка выписок из банковских приложений.
- "настроить ЭДО", "не уходит подпись", "проблема с криптой/калугой/такскомом" -> настройка контура электронного документооборота / актуализация сертификата.
3. Учетные механизмы, документы и отчетность
- "посмотреть почему не проводится", "проверить документы" -> анализ причин невозможности проведения документа / проверка корректности заполнения аналитики.
- "перепровести за период", "полетела последовательность", "слетела последовательность" -> групповое перепроведение документов / восстановление последовательности проведения документов.
- "не закрывается месяц", "ошибка при закрытии", "не считается себестоимость/20 счет" -> регламентная операция закрытия периода / анализ распределения затрат / расчет себестоимости.
- "не идет отчет", "неправильно считает налог/НДФЛ/НДС", "неверное сальдо" -> некорректный расчет налога/НДФЛ/НДС / некорректное формирование движений по регистрам накопления (или регистрам сведений).
- "завести сотрудника", "уволить", "посчитать зп" -> кадровый учет / расчет и начисление заработной платы / формирование документов кадрового учета / расчет сотрудника.
4. Права доступа и администрирование
- "пользователь не может зайти", "права слетели", "дать доступ к складу" -> настройка прав доступа / назначение профилей групп доступа / ограничение прав/ролей пользователя.
- "программа зависла", "всех выбило", "ошибка СУБД" -> аварийное завершение сеанса / оптимизация работы сервера 1С:Предприятие.

ИЗВЛЕЧЕНИЕ ФАЙЛОВ (внутренний этап, не выводить пользователю):
Перед формированием результата последовательно определи:
- какая подтвержденная проблема была выявлена;
- какой подтвержденный запрос был поставлен;
- какие действия действительно выполнены;
- какой подтвержденный результат получен.
Используй только факты, прямо подтвержденные JSON.
Если какого-либо пункта нет — считай его отсутствующим, а не предполагаемым.

Правила технической точности:
- Не называй объекты 1С, механизмы, регистры, документы, обработки, отчеты, обмены, расширения, релизы или настройки, если они прямо не указаны в JSON.
- Если указан конкретный объект 1С, сохрани его название максимально близко к исходному тексту.

ЗАДАНИЕ:
Сформируй результат строго в следующем формате.

[b]✅ SUMMARY:[/b]
В 2-4 предложениях максимально полно, но кратко опиши суть задачи. Обязательно укажи:
- Сначала укажи суть проблемы или запроса клиента.
- Затем укажи выполненные действия, предложенное решение или результат проверки.
- Если есть подтвержденный предметный результат, последнее предложение должно отражать, что именно стало работать, формироваться, проводиться, загружаться, обновляться или отображаться.
- Если подтвержден только факт выполнения работ без предметного результата, не добавляй отдельное итоговое предложение.
- Отдавай предпочтение точности, затем краткости.
- Не добавляй оценочные формулировки и не усиливай результат словами "полностью", "окончательно", "успешно"

[b]📝 TITLE:[/b]
- Выведи только один вариант TITLE.
- TITLE должен быть максимально точным и полным, но без лишних деталей.
- TITLE должен содержать от 3 до 25 слов.
- Если задача является консультацией, начни TITLE со слов "Консультация по...".
- Если в задаче выполнены работы, настройка, исправление, разработка, подключение, обновление или проверка, начни TITLE со слов "Проведение работ по...".
- Если задача содержит одновременно:одтвержденную проблему и подтвержденное решение,то TITLE формируется по схеме: <Проблема>. Решение: <способ устранения>.
- Не используй общие или расплывчатые формулировки.
- Не добавляй никаких пояснений, вступлений, комментариев или рассуждений.

Проверка релиза (только для задач, наименование которых начинается со слов "Обновление базы"):
- Если наименование задачи начинается со слов "Обновление базы", сравни номер релиза, указанный в названии задачи, с фактическим номером релиза, указанным в комментариях.
- Если определить фактический релиз по комментариям невозможно, проверку не выполняй.
- Если номера релизов совпадают:
  - Выведи только раздел ✅ SUMMARY.
  - Раздел 📝 TITLE не выводи.
- Если номера релизов отличаются:
  - После заголовка ✅ SUMMARY выведи отдельной строкой:
    ❗️[USER=<creatorId>]Постановщик[/USER], релиз, указанный в задаче, не совпадает с фактическим.
  - В разделе 📝 TITLE выведи только одно наименование задачи с фактическим номером релиза, указанным в комментариях.
  - Не предлагай второй вариант названия и не выводи никаких других вариантов.
- Если номер релиза отсутствует в названии задачи или комментариях либо определить его невозможно, предупреждение не выводи и обработай задачу по общим правилам.

Проверка достаточности информации:
- Перед формированием результата оцени, достаточно ли информации для понимания проблемы и выполненных работ.
- Если из JSON задачи и базовой задачи невозможно определить, в чем заключалась проблема или какие действия были выполнены, не пытайся делать предположения и не придумывай содержание.
- В этом случае не выводи разделы ✅ SUMMARY и 📝 TITLE.
- Вместо них выведи только следующую строку:
⚠️ [b]Недостаточно информации.[/b] [USER=<responsibleId>]Исполнитель[/USER], пожалуйста, напиши пояснения.
- Не добавляй никаких других комментариев или пояснений.

Проверка времени в задаче:
1. Используй значение currentTaskTimeSpentInLogs как фактическое время по текущей задаче.
2. Не используй currentTaskTimeSpentInLogs для оценки достаточности информации. Достаточность информации определяй только по JSON задачи, базовой задачи, комментариям, изображениям и расшифровкам аудио.
3. Если информации достаточно, обработай задачу по применимым правилам этого промпта независимо от того, равно currentTaskTimeSpentInLogs 0 или больше 0.
4. Если currentTaskTimeSpentInLogs равно 0 и из JSON задачи и базовой задачи невозможно определить, в чем заключалась проблема или какие действия были выполнены, не выводи разделы ✅ SUMMARY и 📝 TITLE. В этом случае ничего не выводи, сообщение не пиши.
5. Если currentTaskTimeSpentInLogs больше 0 и из JSON задачи и базовой задачи невозможно определить, в чем заключалась проблема или какие действия были выполнены, обработай задачу по правилу "Проверка достаточности информации".
- Не используй currentTaskTimeSpentInLogs как источник описания проблемы или выполненных работ.

Упоминание исполнителя:
- Используй значение переменной responsibleId.
- Выведи его в формате: [USER=<responsibleId>]Исполнитель[/USER]
- Не пытайся определять исполнителя по данным JSON.

Упоминание постановщика:
- Используй значение переменной creatorId.
- Выведи его в формате: [USER=<creatorId>]Постановщик[/USER]
- Не пытайся определять постановщика по данным JSON.

Выведи только результат в следующем формате.

Для обычных задач:

[b]✅ SUMMARY:[/b]
<2-4 предложения:проблема/запрос, выполненные или предложенные действия, подтвержденный результат при наличии>

[b]📝 TITLE:[/b]
<Одно наиболее точное название>

Для задач из группы с id = 276:
- Если groupId текущей задачи равен 276, выведи только раздел ✅ SUMMARY:

[b]✅ SUMMARY:[/b]
<2-4 предложения:проблема/запрос, выполненные или предложенные действия, подтвержденный результат при наличии>

- Раздел 📝 TITLE не выводи.

Для задач "Обновление базы", если обновление выполнено на тот же релиз:

[b]✅ SUMMARY:[/b]
<2-4 предложения:проблема/запрос, выполненные или предложенные действия, подтвержденный результат при наличии>

Для задач "Обновление базы", если обновление выполнено на другой релиз:

[b]✅ SUMMARY:[/b]
<2-4 предложения:проблема/запрос, выполненные или предложенные действия, подтвержденный результат при наличии>

❗️[USER=<creatorId>]Постановщик[/USER], релиз, указанный в задаче, не совпадает с фактическим.

[b]📝 TITLE:[/b]
Обновление базы ... на релиз <фактический релиз>

Если информации недостаточно, вместо SUMMARY и TITLE выведи только:
⚠️ [b]Недостаточно информации.[/b] [USER=<responsibleId>]Исполнитель[/USER], пожалуйста, напиши пояснения.`;
}

async function processClosedTask(taskId) {
  const { task: mainTask, comments: mainComments, commentsSource } = await fetchTaskWithComments(taskId);
  const filteredMainComments = filterGemmaComments(mainComments);
  const parentId = getParentIdFromTask(mainTask);
  const responsibleId = getResponsibleIdFromTask(mainTask);
  const creatorId = getCreatorIdFromTask(mainTask);
  const groupId = getGroupIdFromTask(mainTask);
  const groupName = getGroupNameFromTask(mainTask);
  const timeLogs = await fetchTaskTimeLogs(taskId);
  const timeSpentInLogs = getTaskTimeSpent(mainTask);
  let contextparentID = null;

  if (isCollabGroupName(groupName)) {
    return {
      ok: true,
      skipped: true,
      reason: 'collab_group_name',
      task_id: taskId,
      group_id: groupId,
      group_name: groupName,
    };
  }

  if (isGemmaExcludedGroupId(groupId)) {
    return {
      ok: true,
      skipped: true,
      reason: 'excluded_group_id',
      task_id: taskId,
      group_id: groupId,
      group_name: groupName,
    };
  }

  const mainImageResult = await prepareTaskImages(mainTask, filteredMainComments, 'currentTask');
  const mainChatImageResult = await prepareTaskChatImages(mainTask, 'currentTask');
  const mainImages = [...mainImageResult.images, ...mainChatImageResult.images].slice(0, AI_MAX_IMAGES);
  const mainImageFacts = await extractImageFacts(mainImages, 'текущей задачи');
  const mainAudioResult = await prepareTaskChatAudioTranscripts(mainTask, 'currentTask');
  let parentImages = [];
  let parentImageCandidatesCount = 0;
  let parentImageCandidates = [];
  let parentImageFacts = null;
  let parentAudioResult = { candidatesCount: 0, candidates: [], transcripts: [] };

  if (parentId && parentId !== '0') {
    const { task: parentTask, comments: parentComments, commentsSource: parentCommentsSource } = await fetchTaskWithComments(parentId);
    const filteredParentComments = filterGemmaComments(parentComments);
    const parentTimeLogs = await fetchTaskTimeLogs(parentId);
    const parentImageResult = await prepareTaskImages(parentTask, filteredParentComments, 'parentTask');
    const parentChatImageResult = await prepareTaskChatImages(parentTask, 'parentTask');
    parentImages = [...parentImageResult.images, ...parentChatImageResult.images].slice(0, AI_MAX_IMAGES);
    parentImageCandidatesCount = parentImageResult.candidatesCount + parentChatImageResult.candidatesCount;
    parentImageCandidates = [...parentImageResult.candidates, ...parentChatImageResult.candidates];
    parentImageFacts = await extractImageFacts(parentImages, 'родительской задачи');
    parentAudioResult = await prepareTaskChatAudioTranscripts(parentTask, 'parentTask');
    contextparentID = {
      parentId,
      parentTask,
      parentTaskComments: filteredParentComments,
      parentTaskTime: parentTimeLogs,
      parentTaskTimeSpentInLogs: getTaskTimeSpent(parentTask),
      parentTaskImages: getImageMetadata(parentImages),
      parentTaskImageFacts: parentImageFacts,
      parentTaskAudioTranscripts: parentAudioResult.transcripts,
      parentTaskCommentsSource: parentCommentsSource,
    };
  }

  const aiImages = [...mainImages, ...parentImages].slice(0, AI_MAX_IMAGES);
  saveDebug('lastTaskImages', {
    task_id: taskId,
    comments_source: commentsSource,
    ai_image_candidates_count: mainImageResult.candidatesCount + mainChatImageResult.candidatesCount + parentImageCandidatesCount,
    current_image_candidates: [...mainImageResult.candidates, ...mainChatImageResult.candidates],
    parent_image_candidates: parentImageCandidates,
    ai_images_count: aiImages.length,
    ai_images: getImageMetadata(aiImages),
    current_image_facts_found: Boolean(mainImageFacts),
    parent_image_facts_found: Boolean(parentImageFacts),
    current_image_facts: mainImageFacts || '',
    parent_image_facts: parentImageFacts || '',
    current_image_facts_preview: truncateDebugText(mainImageFacts),
    parent_image_facts_preview: truncateDebugText(parentImageFacts),
    current_audio_candidates_count: mainAudioResult.candidatesCount,
    parent_audio_candidates_count: parentAudioResult.candidatesCount,
    current_audio_transcripts_count: mainAudioResult.transcripts.length,
    parent_audio_transcripts_count: parentAudioResult.transcripts.length,
    current_audio_transcripts_preview: truncateDebugText(mainAudioResult.transcripts.map(item => item.text).join('\n\n')),
    parent_audio_transcripts_preview: truncateDebugText(parentAudioResult.transcripts.map(item => item.text).join('\n\n')),
  });
  const prompt = buildPrompt({
    taskId,
    groupId,
    responsibleId,
    creatorId,
    mainTask,
    mainComments: filteredMainComments,
    mainTimeLogs: timeLogs,
    mainTimeSpentInLogs: timeSpentInLogs,
    mainImages,
    mainImageFacts,
    mainAudioTranscripts: mainAudioResult.transcripts,
    contextparentID,
  });

  const aiResponse = await coworkRequest('POST', '/chat/completions', {
    model: MODEL_NAME,
    messages: [{
      role: 'user',
      content: prompt,
    }],
  });

  const aiComment = aiResponse?.choices?.[0]?.message?.content?.trim();
  if (!aiComment && timeSpentInLogs === 0) {
    return {
      ok: true,
      skipped: true,
      reason: 'zero_time_and_empty_ai_comment',
      task_id: taskId,
      parent_id: parentId || null,
      responsible_id: responsibleId,
      creator_id: creatorId,
      group_id: groupId || null,
      group_name: groupName || null,
      time_spent_in_logs: timeSpentInLogs,
      time_logs_count: timeLogs.length,
      ai_images_count: aiImages.length,
      current_image_facts_found: Boolean(mainImageFacts),
      parent_image_facts_found: Boolean(parentImageFacts),
      comment_posted: false,
    };
  }
  if (!aiComment) throw new Error('AI model returned empty comment');

  if (timeSpentInLogs === 0 && isInsufficientInfoComment(aiComment)) {
    return {
      ok: true,
      skipped: true,
      reason: 'zero_time_and_insufficient_info',
      task_id: taskId,
      parent_id: parentId || null,
      responsible_id: responsibleId,
      creator_id: creatorId,
      group_id: groupId || null,
      group_name: groupName || null,
      time_spent_in_logs: timeSpentInLogs,
      time_logs_count: timeLogs.length,
      ai_images_count: aiImages.length,
      current_image_facts_found: Boolean(mainImageFacts),
      parent_image_facts_found: Boolean(parentImageFacts),
      comment_posted: false,
    };
  }

  await coworkRequest('POST', `/tasks/${taskId}/comments`, {
    message: aiComment,
  });
  const generatedTitle = extractTitleFromAiComment(aiComment);
  let summaryFieldUpdated = false;
  let summaryFieldCode = TASK_SUMMARY_FIELD_CODE;
  let summaryFieldError = null;

  if (generatedTitle) {
    const summaryFieldResult = await updateTaskSummaryField(taskId, generatedTitle);
    summaryFieldCode = summaryFieldResult.field;
    summaryFieldUpdated = summaryFieldResult.updated;
    summaryFieldError = summaryFieldResult.error;
  }

  return {
    ok: true,
    task_id: taskId,
    parent_id: parentId || null,
    responsible_id: responsibleId,
    creator_id: creatorId,
    group_id: groupId || null,
    group_name: groupName || null,
    time_spent_in_logs: timeSpentInLogs,
    time_logs_count: timeLogs.length,
    ai_images_count: aiImages.length,
    current_image_facts_found: Boolean(mainImageFacts),
    parent_image_facts_found: Boolean(parentImageFacts),
    comment_posted: true,
    summary_field: summaryFieldCode,
    summary_field_updated: summaryFieldUpdated,
    summary_field_error: summaryFieldError,
    generated_title: generatedTitle,
    ai_comment: aiComment,
  };
}

async function handleWebhook(body) {
  const data = parseFormUrlEncoded(body);

  const taskId = getTaskIdFromOutgoingWebhook(data);
  if (!taskId) throw new Error('No task ID found');

  if (data.auth?.application_token !== WEBHOOK_TOKEN) {
    return { statusCode: 403, data: { ok: false, error: 'Invalid webhook token' } };
  }

  if (data.event !== 'ONTASKUPDATE') {
    return { statusCode: 200, data: { ok: true, ignored: true, reason: 'unsupported_event' } };
  }

  const webhookTimestampMs = getWebhookTimestampMs(data);
  const updateBatch = await getLatestUpdateBatch(taskId, webhookTimestampMs);
  const primaryChange = updateBatch[0] || null;
  const primaryField = normalizeHistoryField(primaryChange?.field);
  const responsibleChange = primaryField === normalizeHistoryField('RESPONSIBLE_ID') ? primaryChange : null;
  const statusChange = primaryField === normalizeHistoryField('STATUS') ? primaryChange : null;
  const actions = [];

  // Ветка 1: изменение ответственного, добавление прошлого исполнителя в соисполнители.
  const previousResponsibleId = normalizeId(responsibleChange?.value?.from);
  if (previousResponsibleId) {
    const result = await addAccomplice(taskId, previousResponsibleId);
    actions.push({ branch: 'responsible_changed', ...result });
  }

  // Ветка 2: закрытие задачи, сбор контекста и вызов Геммы.
  if (String(statusChange?.value?.to) === '5') {
    await rememberClosedTaskTime(taskId);
    const result = await processClosedTask(taskId);
    actions.push({ branch: 'task_closed', ...result });
  }

  if (actions.length === 0) {
    return {
      statusCode: 200,
      data: {
        ok: true,
        ignored: true,
        reason: 'latest_update_has_no_tracked_fields',
        task_id: taskId,
        webhook_ts: data.ts || null,
        primary_field: primaryChange?.field || null,
        latest_fields: updateBatch.map(item => item.field),
      },
    };
  }

  return { statusCode: 200, data: { ok: true, task_id: taskId, actions } };
}

function sendJson(res, statusCode, data) {
  res.writeHead(statusCode, { 'Content-Type': 'application/json; charset=utf-8' });
  res.end(JSON.stringify(data));
}

function getNextTaskTimeCheckDate(now = new Date()) {
  const scheduledUtcHour = TASK_TIME_CHECK_HOUR_MSK - MSK_UTC_OFFSET_HOURS;
  const nextRun = new Date(Date.UTC(
    now.getUTCFullYear(),
    now.getUTCMonth(),
    now.getUTCDate(),
    scheduledUtcHour,
    TASK_TIME_CHECK_MINUTE_MSK,
    0,
    0
  ));

  if (nextRun <= now) {
    nextRun.setUTCDate(nextRun.getUTCDate() + 1);
  }

  return nextRun;
}

function scheduleNextTaskTimeCheck() {
  const nextRun = getNextTaskTimeCheckDate();
  const delayMs = Math.max(0, nextRun.getTime() - Date.now());

  saveDebug('nextTaskTimeCheck', {
    scheduled_at_utc: nextRun.toISOString(),
    scheduled_at_msk: `${nextRun.toISOString().slice(0, 10)}T${String(TASK_TIME_CHECK_HOUR_MSK).padStart(2, '0')}:${String(TASK_TIME_CHECK_MINUTE_MSK).padStart(2, '0')}:00+03:00`,
    delay_ms: delayMs,
  });

  setTimeout(() => {
    runTaskTimeCheck()
      .catch(error => {
        saveDebug('lastError', { error: error.message });
        log('Task time check failed', { error: error.message });
      })
      .finally(scheduleNextTaskTimeCheck);
  }, delayMs);
}

const server = http.createServer(async (req, res) => {
  try {
    const pathname = req.url.split('?')[0];

    saveDebug('lastRequest', {
      method: req.method,
      path: pathname,
      url: req.url,
      headers: getSafeHeaders(req.headers),
    });

    if ((req.method === 'GET' || req.method === 'HEAD') && pathname === '/debug') {
      sendJson(res, 200, {
        ok: true,
        taskTimeCheckRunning: taskTimeCheckInProgress,
        taskTimeCheckStartedAt,
        taskTimeCheckStage,
        ...debugState,
      });
      return;
    }

    if ((req.method === 'GET' || req.method === 'POST') && pathname === '/time-check') {
      if (req.method === 'POST') await readBody(req);
      const result = await runTaskTimeCheck();
      sendJson(res, 200, result);
      return;
    }

    if (req.method === 'GET' || req.method === 'HEAD') {
      res.writeHead(200, { 'Content-Type': 'text/plain; charset=utf-8' });
      res.end(req.method === 'HEAD' ? undefined : 'OK');
      return;
    }

    if (req.method !== 'POST') {
      sendJson(res, 404, { ok: false, error: 'Not found' });
      return;
    }

    const body = await readBody(req);

    if (pathname === '/webhook') {
      const result = await handleWebhook(body);
      sendJson(res, result.statusCode, result.data);
      return;
    }

    sendJson(res, 404, { ok: false, error: 'Not found' });
  } catch (error) {
    saveDebug('lastError', { error: error.message });
    log('Request failed', { error: error.message });
    sendJson(res, 500, { ok: false, error: error.message });
  }
});

server.listen(PORT, () => {
  log(`Server running on port ${PORT}`);

  if (!TASK_TIME_CHECK_ENABLED) return;

  if (TASK_TIME_CHECK_RUN_ON_START) {
    setTimeout(() => {
      runTaskTimeCheck().catch(error => {
        saveDebug('lastError', { error: error.message });
        log('Task time check failed', { error: error.message });
      });
    }, 5000);
  }

  scheduleNextTaskTimeCheck();
});
