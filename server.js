const http = require('http');
const https = require('https');
const fs = require('fs');
const path = require('path');
const sqlite3 = require('sqlite3').verbose();

function requireEnv(name) {
  const value = process.env[name];
  if (!value) throw new Error(`Missing required env ${name}`);
  return value;
}

const PORT = process.env.PORT || 3000;
const APP_VERSION = 'time-check-search-real-status-plain-pagination-2026-08-13-01';
const BASE_URL = requireEnv('BASE_URL');
const API_KEY = requireEnv('API_KEY');
const SUMMARY_MODEL_NAME = process.env.SUMMARY_MODEL_NAME || process.env.MODEL_NAME || 'bitrix/google/gemma-4-26B-A4B-it';
const IMAGE_MODEL_NAME = process.env.IMAGE_MODEL_NAME || 'bitrix/bitrixgpt-5.5';
const WEBHOOK_TOKEN = requireEnv('WEBHOOK_TOKEN');
const ELAPSED_NOTIFICATION_CHAT_ID = process.env.ELAPSED_NOTIFICATION_CHAT_ID || 'chat42358';
const BITRIX_PORTAL_URL = process.env.BITRIX_PORTAL_URL || 'https://elros.bitrix24.ru';
const DATA_DIR = process.env.DATA_DIR || (process.env.NODE_ENV === 'production' ? '/data' : __dirname);
const DB_FILENAME = process.env.DB_FILENAME || 'task_time_logs_v2.db';
const DB_PATH = process.env.DB_PATH || path.join(DATA_DIR, DB_FILENAME);
const OPEN_TASK_DB_FILENAME = process.env.OPEN_TASK_DB_FILENAME || 'open_task_watch.db';
const OPEN_TASK_DB_PATH = process.env.OPEN_TASK_DB_PATH || path.join(DATA_DIR, OPEN_TASK_DB_FILENAME);
const TASK_TIME_LOOKBACK_DAYS = Number(process.env.TASK_TIME_LOOKBACK_DAYS || 180);
const TASK_TIME_CHECK_HOUR_MSK = Number(process.env.TASK_TIME_CHECK_HOUR_MSK || 20);
const TASK_TIME_CHECK_MINUTE_MSK = Number(process.env.TASK_TIME_CHECK_MINUTE_MSK || 0);
const TASK_TIME_CHECK_ENABLED = process.env.TASK_TIME_CHECK_ENABLED !== 'false';
const TASK_TIME_CHECK_RUN_ON_START = process.env.TASK_TIME_CHECK_RUN_ON_START === 'true';
const TASK_TIME_ALERT_RETENTION_DAYS = Number(process.env.TASK_TIME_ALERT_RETENTION_DAYS || 180);
const TASK_TIME_SEARCH_PAGE_SIZE = Number(process.env.TASK_TIME_SEARCH_PAGE_SIZE || 5000);
const API_REQUEST_TIMEOUT_MS = Number(process.env.API_REQUEST_TIMEOUT_MS || 60 * 1000);
const AI_OCR_REQUEST_TIMEOUT_MS = Number(process.env.AI_OCR_REQUEST_TIMEOUT_MS || 30 * 1000);
const OPEN_TASK_AI_REQUEST_TIMEOUT_MS = Number(process.env.OPEN_TASK_AI_REQUEST_TIMEOUT_MS || 120 * 1000);
const OPEN_TASK_ANALYSIS_TIMEOUT_MS = Number(process.env.OPEN_TASK_ANALYSIS_TIMEOUT_MS || 180 * 1000);
const MSK_UTC_OFFSET_HOURS = 3;
const GEMMA_EXCLUDED_GROUP_IDS = new Set(['12', '58', '92', '140', '376', '490']);
const GEMMA_COMMENT_AUTHOR_ID = String(process.env.GEMMA_COMMENT_AUTHOR_ID || 204);
const AI_IMAGE_PROCESSING_ENABLED = true;
const AI_MAX_IMAGES = 15;
const AI_MAX_IMAGE_BYTES = 20 * 1024 * 1024;
const AI_MAX_IMAGE_BATCH_BYTES = 18 * 1024 * 1024;
const AI_MAX_IMAGES_PER_BATCH = 1;
const AI_SUPPORTED_IMAGE_MIME_TYPES = new Set(['image/png', 'image/jpeg', 'image/gif', 'image/webp']);
const AUDIO_TRANSCRIPTION_MODEL = 'bitrix/deepdml/faster-whisper-large-v3-turbo-ct2';
const AI_MAX_AUDIO_FILES = 10;
const AI_MAX_AUDIO_BYTES = 25 * 1024 * 1024;
const TASK_SUMMARY_FIELD_CODE = 'UF_TASK_TITLE';
const UNKNOWN_USER_NAME = 'Неизвестный пользователь';
const OPEN_TASK_MIN_AGE_DAYS = Number(process.env.OPEN_TASK_MIN_AGE_DAYS || 7);
const OPEN_TASK_REMIND_AFTER_DAYS = Number(process.env.OPEN_TASK_REMIND_AFTER_DAYS || 7);
const OPEN_TASK_STALE_AFTER_DAYS = Number(process.env.OPEN_TASK_STALE_AFTER_DAYS || 14);
const OPEN_TASK_DEFAULT_RECHECK_DAYS = Number(process.env.OPEN_TASK_DEFAULT_RECHECK_DAYS || 3);
const OPEN_TASK_DEFAULT_LIMIT = Number(process.env.OPEN_TASK_DEFAULT_LIMIT || 10);
const OPEN_TASK_REPORT_MAX_MESSAGE_CHARS = Number(process.env.OPEN_TASK_REPORT_MAX_MESSAGE_CHARS || 7000);
const OPEN_TASK_EXCLUDED_GROUP_IDS = new Set(['0', '12', '58', '92', '140', '276', '376', '490']);

let taskTimeCheckInProgress = false;
let taskTimeCheckStartedAt = null;
let taskTimeCheckStage = null;
let openTaskCheckInProgress = false;
let openTaskCheckStartedAt = null;
let openTaskCheckStage = null;
const userNameCache = new Map();
const workgroupCache = new Map();
const debugState = {
  lastRequest: null,
  lastTaskTimeCheckRequest: null,
  nextTaskTimeCheck: null,
  lastTaskTimeChatDecision: null,
  lastTaskTimeCheck: null,
  lastTaskCloseDecision: null,
  lastClosedTaskProcessing: null,
  lastOpenTaskNextStep: null,
  lastOpenTaskWatchCheck: null,
  lastOpenTaskWatchSearch: null,
  lastOpenTaskWatchChatDecision: null,
  lastTaskImages: null,
  lastImageOcr: null,
  lastError: null,
};
const closedTaskProcessingTaskIds = new Set();

fs.mkdirSync(path.dirname(DB_PATH), { recursive: true });
fs.mkdirSync(path.dirname(OPEN_TASK_DB_PATH), { recursive: true });
const db = new sqlite3.Database(DB_PATH);
const openTaskDb = new sqlite3.Database(OPEN_TASK_DB_PATH);
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

openTaskDb.serialize(() => {
  openTaskDb.run(`CREATE TABLE IF NOT EXISTS open_task_watch_state (
    task_id TEXT PRIMARY KEY,
    created_at TEXT,
    group_id TEXT,
    group_name TEXT,
    responsible_id TEXT,
    responsible_name TEXT,
    task_link TEXT,
    first_seen_at TEXT NOT NULL,
    last_checked_at TEXT,
    last_ai_result_json TEXT,
    last_reason TEXT,
    last_action TEXT,
    last_summary TEXT,
    last_recheck_at TEXT,
    last_alert_sent_at TEXT,
    last_alert_status TEXT,
    resolved_at TEXT,
    resolved_reason TEXT,
    updated_at TEXT NOT NULL
  )`);

  openTaskDb.run(`CREATE TABLE IF NOT EXISTS open_task_watch_alerts (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    task_id TEXT NOT NULL,
    alert_status TEXT NOT NULL,
    reason TEXT,
    action TEXT,
    summary TEXT,
    responsible_id TEXT,
    responsible_name TEXT,
    group_id TEXT,
    group_name TEXT,
    task_link TEXT,
    sent_at TEXT NOT NULL,
    ai_result_json TEXT
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

function checkPathWritable(targetPath) {
  try {
    fs.accessSync(targetPath, fs.constants.W_OK);
    return true;
  } catch {
    return false;
  }
}

function getStorageDebugInfo() {
  const dbDir = path.dirname(DB_PATH);
  const openTaskDbDir = path.dirname(OPEN_TASK_DB_PATH);
  return {
    data_dir: DATA_DIR,
    db_path: DB_PATH,
    db_dir: dbDir,
    open_task_db_path: OPEN_TASK_DB_PATH,
    open_task_db_dir: openTaskDbDir,
    data_dir_exists: fs.existsSync(DATA_DIR),
    db_dir_exists: fs.existsSync(dbDir),
    open_task_db_dir_exists: fs.existsSync(openTaskDbDir),
    data_dir_writable: fs.existsSync(DATA_DIR) ? checkPathWritable(DATA_DIR) : false,
    db_dir_writable: fs.existsSync(dbDir) ? checkPathWritable(dbDir) : false,
    open_task_db_dir_writable: fs.existsSync(openTaskDbDir) ? checkPathWritable(openTaskDbDir) : false,
    db_file_exists: fs.existsSync(DB_PATH),
    db_file_writable: fs.existsSync(DB_PATH) ? checkPathWritable(DB_PATH) : null,
    open_task_db_file_exists: fs.existsSync(OPEN_TASK_DB_PATH),
    open_task_db_file_writable: fs.existsSync(OPEN_TASK_DB_PATH) ? checkPathWritable(OPEN_TASK_DB_PATH) : null,
  };
}

function stringifyForLog(value) {
  if (typeof value === 'string') return value;
  try {
    return JSON.stringify(value);
  } catch {
    return String(value);
  }
}

function getErrorMessage(value, fallback = 'Unknown error') {
  if (!value) return fallback;
  if (typeof value === 'string') return value;
  if (value instanceof Error) return value.message;
  if (typeof value.message === 'string') return value.message;
  if (typeof value.error_description === 'string') return value.error_description;
  if (typeof value.error === 'string') return value.error;
  if (typeof value.code === 'string') return value.code;
  return stringifyForLog(value);
}

function withTimeout(promise, timeoutMs, label) {
  if (!Number.isFinite(timeoutMs) || timeoutMs <= 0) return promise;

  return new Promise((resolve, reject) => {
    const timer = setTimeout(() => {
      reject(new Error(`${label} timeout after ${timeoutMs} ms`));
    }, timeoutMs);

    promise.then(
      value => {
        clearTimeout(timer);
        resolve(value);
      },
      error => {
        clearTimeout(timer);
        reject(error);
      }
    );
  });
}

function safePromise(promise) {
  promise.catch(() => {});
  return promise;
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
  const timeoutMs = Number(options.timeoutMs || API_REQUEST_TIMEOUT_MS);
  const requestOptions = { ...options };
  delete requestOptions.timeoutMs;

  return new Promise((resolve, reject) => {
    let settled = false;
    const req = client.request(endpoint, requestOptions, res => {
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
          reject(new Error(getErrorMessage(json.error_description || json.error)));
          return;
        }

        if (json?.success === false) {
          reject(new Error(getErrorMessage(json.error, 'API returned success=false')));
          return;
        }

        resolve(json);
      });
    });

    req.setTimeout(timeoutMs, () => {
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

function coworkRequest(method, path, body, options = {}) {
  return requestJson(new URL(`/v1${path}`, BASE_URL), {
    method,
    headers: {
      'Content-Type': 'application/json',
      'X-Api-Key': API_KEY,
    },
    body: body ? JSON.stringify(body) : undefined,
    timeoutMs: options.timeoutMs,
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

function queryOpenTaskDb(sql, params = []) {
  return new Promise((resolve, reject) => {
    openTaskDb.all(sql, params, (err, rows) => {
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

function runOpenTaskDb(sql, params = []) {
  return new Promise((resolve, reject) => {
    openTaskDb.run(sql, params, function onRun(err) {
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
  const groupId = (
    task?.groupId ??
    task?.groupID ??
    task?.GroupID ??
    task?.group_id ??
    task?.GROUP_ID ??
    task?.group?.id ??
    task?.group?.ID ??
    task?.GROUP?.id ??
    task?.GROUP?.ID
  );
  return groupId == null || groupId === '' ? null : String(groupId);
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

function getUserIdFromObject(user) {
  const userId = user?.id ?? user?.ID ?? user?.userId ?? user?.USER_ID;
  return userId == null ? null : String(userId).replace(/^user_/i, '');
}

function getUserDisplayName(user) {
  if (!user || typeof user !== 'object') return null;

  const firstName = user.name || user.firstName || user.NAME || user.FIRST_NAME || '';
  const lastName = user.lastName || user.LAST_NAME || '';
  const nameParts = [firstName, lastName]
    .map(part => String(part || '').trim())
    .filter(Boolean);

  const firstAndLastName = nameParts.join(' ').trim();
  if (firstAndLastName) return firstAndLastName;

  const fullName = user.fullName || user.FULL_NAME;
  return typeof fullName === 'string' && fullName.trim() ? fullName.trim() : null;
}

function getTaskPersonName(task, prefix) {
  const firstName = task?.[`${prefix}_NAME`];
  const lastName = task?.[`${prefix}_LAST_NAME`];
  const nameParts = [firstName, lastName]
    .map(part => String(part || '').trim())
    .filter(Boolean);

  return nameParts.length ? nameParts.join(' ') : null;
}

function getResponsibleNameFromTask(task) {
  return (
    getUserDisplayName(task?.responsible) ||
    getTaskPersonName(task, 'RESPONSIBLE') ||
    null
  );
}

function normalizeUserPayload(response) {
  const data = unwrapData(response);
  return data?.user || data?.User || data;
}

function rememberUserName(user) {
  const userId = getUserIdFromObject(user);
  const userName = getUserDisplayName(user);
  if (userId && userName) userNameCache.set(userId, userName);
  return userName;
}

function rememberTaskUserNames(task) {
  [
    task?.creator,
    task?.responsible,
    ...Object.values(task?.accomplicesData || {}),
    ...Object.values(task?.auditorsData || {}),
  ].forEach(rememberUserName);
}

async function getUserNameById(userId) {
  const normalizedUserId = normalizeId(userId);
  if (!normalizedUserId) return null;

  const cacheKey = String(normalizedUserId);
  if (userNameCache.has(cacheKey)) return userNameCache.get(cacheKey);

  try {
    const response = await coworkRequest('GET', `/users/${encodeURIComponent(cacheKey)}`);
    const userName = rememberUserName(normalizeUserPayload(response));
    userNameCache.set(cacheKey, userName || null);
    return userName || null;
  } catch (error) {
    log('User name fetch failed', { user_id: cacheKey, error: error.message });
    userNameCache.set(cacheKey, null);
    return null;
  }
}

function getGroupNameFromTask(task) {
  return (
    task?.groupName ||
    task?.GROUP_NAME ||
    task?.group?.name ||
    task?.group?.NAME ||
    task?.group?.title ||
    task?.group?.TITLE ||
    task?.GROUP?.name ||
    task?.GROUP?.NAME ||
    task?.GROUP?.title ||
    task?.GROUP?.TITLE ||
    null
  );
}

function getTaskCreatedDate(task) {
  return task?.createdDate || task?.CREATED_DATE || null;
}

function getTaskActivityDate(task) {
  return task?.activityDate || task?.ACTIVITY_DATE || null;
}

function getTaskLastMovementDate(task) {
  return (
    getTaskActivityDate(task) ||
    task?.changedDate ||
    task?.CHANGED_DATE ||
    getTaskCreatedDate(task) ||
    null
  );
}

function getGroupArchivedFromTask(task) {
  const archived = task?.group?.archived ?? task?.group?.ARCHIVED ?? task?.groupArchived ?? task?.GROUP_ARCHIVED;
  if (typeof archived === 'boolean') return archived;
  if (archived == null) return null;
  return ['Y', 'YES', 'TRUE', '1'].includes(String(archived).toUpperCase());
}

function getTaskChatId(task) {
  const chatId = task?.chatId || task?.CHAT_ID || task?.chat?.id;
  return chatId ? String(chatId).replace(/^chat/i, '') : null;
}

function isCollabGroupName(groupName) {
  const normalized = String(groupName || '')
    .toLowerCase()
    .replace(/ё/g, 'е')
    .replace(/\s+/g, ' ')
    .trim();

  return normalized.includes('коллаб');
}

function isGemmaExcludedGroupId(groupId) {
  return GEMMA_EXCLUDED_GROUP_IDS.has(String(groupId || ''));
}

function isOpenTaskExcludedGroupId(groupId) {
  return OPEN_TASK_EXCLUDED_GROUP_IDS.has(String(groupId ?? ''));
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

  return [
    { type: 'text', text: prompt },
    ...images.map(image => ({
      type: 'image_url',
      image_url: { url: image.dataUrl },
    })),
  ];
}

function getImageBytes(image) {
  if (Number.isFinite(image?.bytes) && image.bytes > 0) return image.bytes;

  const base64 = String(image?.dataUrl || '').split(',')[1] || '';
  return Math.ceil(base64.length * 3 / 4);
}

function buildImageBatches(images) {
  const batches = [];
  let batch = [];
  let batchBytes = 0;

  for (const image of images) {
    const imageBytes = getImageBytes(image);
    const batchWouldBeTooLarge = batchBytes + imageBytes > AI_MAX_IMAGE_BATCH_BYTES;
    const batchWouldHaveTooManyImages = batch.length >= AI_MAX_IMAGES_PER_BATCH;

    if (batch.length && (batchWouldBeTooLarge || batchWouldHaveTooManyImages)) {
      batches.push(batch);
      batch = [];
      batchBytes = 0;
    }

    batch.push(image);
    batchBytes += imageBytes;
  }

  if (batch.length) batches.push(batch);
  return batches;
}

function getImageBatchMetadata(batch) {
  return {
    images_count: batch.length,
    bytes: batch.reduce((sum, image) => sum + getImageBytes(image), 0),
    images: getImageMetadata(batch),
  };
}

function truncateDebugText(value, maxLength = 1500) {
  const text = String(value || '');
  return text.length > maxLength ? `${text.slice(0, maxLength)}...` : text;
}

async function extractImageFacts(images, scope, taskId = null) {
  if (!images.length) return null;

  const prompt = `Ты анализируешь изображения, приложенные к задачам технической поддержки 1С.
Твоя задача — извлечь из изображения максимум достоверной информации для дальнейшего анализа задачи.

Сначала выполни полный OCR:
- перепиши весь читаемый текст максимально дословно;
- сохрани числа, проценты, даты, номера документов, названия систем, документов, форм, ошибок, пунктов и подпунктов;
- сохрани названия объектов 1С, методов, областей макета, реквизитов, отчетов, обработок, регистров и настроек;
- не исправляй и не дополняй текст.

Затем перечисли все подтвержденные факты, которые видны на изображении:
- тексты ошибок и предупреждений;
- названия объектов, документов, форм, отчетов, обработок и настроек;
- даты, номера, суммы, статусы и пользователей;
- видимые результаты проверок или выполнения действий.

Используй только сведения, которые действительно присутствуют на изображении.
Не объясняй причины.
Не предлагай решение.
Не делай выводов, которых нет на изображении.
Если читаемый текст и факты невозможно определить, напиши: Не удалось определить.`;
  const batches = buildImageBatches(images);
  const facts = [];
  const debugBatches = batches.map((batch, index) => ({
    batch: index + 1,
    status: 'pending',
    ...getImageBatchMetadata(batch),
  }));

  saveDebug('lastImageOcr', {
    task_id: taskId,
    scope,
    status: 'started',
    image_model: IMAGE_MODEL_NAME,
    images_count: images.length,
    batches_count: batches.length,
    max_batch_bytes: AI_MAX_IMAGE_BATCH_BYTES,
    max_images_per_batch: AI_MAX_IMAGES_PER_BATCH,
    batches: debugBatches,
  });

  for (const [index, batch] of batches.entries()) {
    try {
      debugBatches[index].status = 'running';
      saveDebug('lastImageOcr', {
        task_id: taskId,
        scope,
        status: 'running',
        image_model: IMAGE_MODEL_NAME,
        images_count: images.length,
        batches_count: batches.length,
        current_batch: index + 1,
        batches: debugBatches,
      });

      const response = await withTimeout(
        safePromise(coworkRequest('POST', '/chat/completions', {
          model: IMAGE_MODEL_NAME,
          messages: [{
            role: 'user',
            content: buildAiMessageContent(prompt, batch),
          }],
        }, {
          timeoutMs: AI_OCR_REQUEST_TIMEOUT_MS,
        })),
        AI_OCR_REQUEST_TIMEOUT_MS,
        `Image OCR batch ${index + 1}`
      );

      const text = normalizeAiContent(response?.choices?.[0]?.message?.content) || null;
      debugBatches[index].status = text ? 'ok' : 'empty';
      debugBatches[index].facts_preview = truncateDebugText(text, 500);
      if (text) facts.push(`Партия изображений ${index + 1}:\n${text}`);
    } catch (error) {
      debugBatches[index].status = 'failed';
      debugBatches[index].error = error.message;
      log('Image OCR batch failed', {
        scope,
        task_id: taskId,
        batch: index + 1,
        images: batch.length,
        bytes: debugBatches[index].bytes,
        error: error.message,
      });
    }

    saveDebug('lastImageOcr', {
      task_id: taskId,
      scope,
      status: index === batches.length - 1 ? 'finalizing' : 'running',
      image_model: IMAGE_MODEL_NAME,
      images_count: images.length,
      batches_count: batches.length,
      current_batch: index + 1,
      facts_found: facts.length,
      batches: debugBatches,
    });
  }

  saveDebug('lastImageOcr', {
    task_id: taskId,
    scope,
    status: facts.length === batches.length ? 'ok' : facts.length ? 'partial' : 'empty',
    image_model: IMAGE_MODEL_NAME,
    images_count: images.length,
    batches_count: batches.length,
    facts_found: facts.length,
    batches: debugBatches,
  });

  return facts.length ? facts.join('\n\n') : null;
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

function buildTaskTimeSearchBody(offset) {
  return {
    autoWindow: false,
    sort: 'id',
    order: {
      ID: 'ASC',
    },
    filter: {
      REAL_STATUS: 5,
      '>=CLOSED_DATE': getLookbackDate(TASK_TIME_LOOKBACK_DAYS),
    },
    limit: TASK_TIME_SEARCH_PAGE_SIZE,
    offset,
    select: [
      'ID',
      'TITLE',
      'STATUS',
      'REAL_STATUS',
      'GROUP_ID',
      'CLOSED_DATE',
      'TIME_SPENT_IN_LOGS',
      'DURATION_FACT',
    ],
  };
}

async function fetchClosedTasksForTimeCheck() {
  const tasks = [];
  const requests = [];

  for (let offset = 0, page = 1; page <= 200; offset += TASK_TIME_SEARCH_PAGE_SIZE, page += 1) {
    const body = buildTaskTimeSearchBody(offset);
    saveDebug('lastTaskTimeCheckRequest', {
      method: 'POST',
      path: '/tasks/search',
      offset,
      page,
      body,
      requests,
      stage: 'fetch_closed_tasks',
    });

    const response = await coworkRequest('POST', '/tasks/search', body);
    const pageTasks = normalizeTaskListPayload(response);
    requests.push({
      offset,
      page,
      page_count: pageTasks.length,
      response_meta: response?.meta || null,
      total_loaded: tasks.length + pageTasks.length,
    });
    tasks.push(...pageTasks);

    const hasMore = Boolean(response?.meta?.hasMore || response?.hasMore);
    const pageIsFull = pageTasks.length === TASK_TIME_SEARCH_PAGE_SIZE;
    if (!hasMore && !pageIsFull) break;
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
  return getUserDisplayName(historyItem?.user) || null;
}

function getHistoryUserId(historyItem) {
  return getUserIdFromObject(historyItem?.user) || historyItem?.userId || historyItem?.USER_ID || null;
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
  rememberTaskUserNames(task);

  const historyChanges = await Promise.all(history
    .filter(isTimeSpentHistoryItem)
    .filter(item => !Number.isFinite(lastCheckedAtMs) || item.createdAtMs > lastCheckedAtMs)
    .sort((a, b) => a.createdAtMs - b.createdAtMs)
    .map(async item => {
      const userId = getHistoryUserId(item);
      const userName = getHistoryUserName(item) || await getUserNameById(userId);

      return {
        task,
        taskId,
        taskLink,
        title: getTaskTitle(task),
        groupId: getGroupIdFromTask(task),
        userId,
        userName: userName || (userId ? `Пользователь ${userId}` : UNKNOWN_USER_NAME),
        oldTimeSpent: Number.parseInt(item.value?.from ?? 0, 10) || 0,
        newTimeSpent: Number.parseInt(item.value?.to ?? 0, 10) || 0,
        oldDurationFact: Math.round((Number.parseInt(item.value?.from ?? 0, 10) || 0) / 60),
        newDurationFact: Math.round((Number.parseInt(item.value?.to ?? 0, 10) || 0) / 60),
        diffMinutes: getHistoryTimeDiffMinutes(item),
        history_id: item.id || null,
        history_created_date: item.createdDate || null,
      };
    }));

  if (historyChanges.length > 0) return historyChanges;

  return [{
    ...fallbackChange,
    userId: null,
    userName: UNKNOWN_USER_NAME,
    history_id: null,
    history_created_date: null,
  }];
}

function saveTimeChatDecision(changes, message) {
  const taskIds = [...new Set(changes.map(change => change.taskId))];
  const users = [...new Map(changes.map(change => [
    `${change.userId || ''}:${change.userName || UNKNOWN_USER_NAME}`,
    {
      user_id: change.userId || null,
      user_name: change.userName || UNKNOWN_USER_NAME,
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
    const userName = change.userName || UNKNOWN_USER_NAME;
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

async function cleanupOldTaskTimeState() {
  const cutoffDate = getLookbackDate(TASK_TIME_LOOKBACK_DAYS);

  const result = await runDb(
    'DELETE FROM task_time_state WHERE closed_date IS NOT NULL AND closed_date < ?',
    [cutoffDate]
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

    taskTimeCheckStage = 'cleanup_old_task_time_state';
    const deletedOldState = await cleanupOldTaskTimeState();

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
      deleted_old_task_time_state: deletedOldState,
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

async function getLatestUpdateContext(taskId, webhookTimestampMs = null) {
  const sortedHistory = await getTaskHistory(taskId);
  const webhookBatch = getHistoryBatchNearWebhook(sortedHistory, webhookTimestampMs);
  if (webhookBatch.length > 0) return { history: sortedHistory, batch: webhookBatch };

  if (Number.isFinite(webhookTimestampMs)) return { history: sortedHistory, batch: [] };

  const latestChange = sortedHistory[0];
  if (!latestChange) return { history: sortedHistory, batch: [] };
  return {
    history: sortedHistory,
    batch: sortedHistory.filter(item => latestChange.createdAtMs - item.createdAtMs <= 2000),
  };
}

async function getLatestUpdateBatch(taskId, webhookTimestampMs = null) {
  const context = await getLatestUpdateContext(taskId, webhookTimestampMs);
  return context.batch;
}

function isInsufficientInfoComment(comment) {
  const text = String(comment || '').trim();
  return text === 'INSUFFICIENT_INFORMATION' || text.includes('Недостаточно информации');
}

function buildInsufficientInfoComment(responsibleId) {
  return `⚠️ [b]Недостаточно информации.[/b] [USER=${responsibleId}]Исполнитель[/USER], пожалуйста, напиши пояснения.`;
}

function extractSummaryFromAiComment(comment) {
  const text = String(comment || '').trim();
  const summaryMatch = text.match(/\[b\]✅ SUMMARY:\[\/b\]\s*([\s\S]*?)(?=\n\s*\[b\]📝 TITLE:\[\/b\]|$)/i);
  if (!summaryMatch) return text;

  const summaryText = summaryMatch[1].trim();
  return `[b]✅ SUMMARY:[/b]\n${summaryText}`.trim();
}

function isSummaryOnlyGroup(groupId) {
  return String(groupId || '') === '276';
}

function isDoneStageChange(change) {
  return normalizeHistoryField(change?.field) === 'STAGE' && String(change?.value?.to || '').trim() === 'Сделаны';
}

function isStatusClosedChange(change) {
  return normalizeHistoryField(change?.field) === 'STATUS' && String(change?.value?.to) === '5';
}

function findClosedStatusChangeNearDoneStage(history, doneStageChange) {
  if (!doneStageChange || !Number.isFinite(doneStageChange.createdAtMs)) return null;

  const stageTimeMs = doneStageChange.createdAtMs;
  return history.find(change =>
    isStatusClosedChange(change) &&
    Number.isFinite(change.createdAtMs) &&
    change.createdAtMs >= stageTimeMs - 1000 &&
    change.createdAtMs <= stageTimeMs
  ) || null;
}

function normalizeAiContent(content) {
  if (content == null) return '';
  if (typeof content === 'string') return content.trim();
  if (Array.isArray(content)) {
    return content
      .map(item => {
        if (typeof item === 'string') return item;
        if (typeof item?.text === 'string') return item.text;
        if (typeof item?.content === 'string') return item.content;
        return stringifyForLog(item);
      })
      .join('\n')
      .trim();
  }
  if (typeof content.text === 'string') return content.text.trim();
  if (typeof content.content === 'string') return content.content.trim();
  return stringifyForLog(content).trim();
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

  return `Ты - профессиональный консультант 1С. Твоя задача — проанализировать сведения задачи компании-франчайзи 1С и подготовить итог выполненных работ. Используй профессиональную терминологию, принятую в сфере внедрения и сопровождения продуктов 1С.

НИЖЕ ПРИВЕДЕН КОНТЕКСТ ЗАДАЧИ (JSON):
${JSON.stringify(context, null, 2)}

НИЖЕ ПРИВЕДЕН КОНТЕКСТ РОДИТЕЛЬСКОЙ ЗАДАЧИ (JSON):
${JSON.stringify(contextparentID, null, 2)}

ПРАВИЛА АНАЛИЗА:
Основной источник:
- currentTask
- currentTaskComments
- currentTaskTimeSpentInLogs
- currentTaskImageFacts
- currentTaskAudioTranscripts
Дополнительный источник (только если существует):
- parentTask
- parentTaskComments
- parentTaskTimeSpentInLogs
- parentTaskImageFacts
- parentTaskAudioTranscripts
Если сведения противоречат друг другу: комментарии имеют приоритет над описанием задачи.

ПРИНЦИП ДОКАЗАТЕЛЬНОСТИ:
Любое утверждение в результате должно быть прямо подтверждено JSON.
Если факт нельзя подтвердить исходными данными, он считается отсутствующим.
Если технический факт подтвержден исходными данными и существенен для понимания причины или выполненных работ, не удаляй его ради краткости и не заменяй общими словами.
Запрещено:
- делать предположения;
- использовать типичные знания о 1С вместо фактов;
- достраивать цепочку действий;
- объединять связанные технические объекты;
- повышать техническую детализацию;
- заменять конкретный объект более общим;
- заменять один объект несколькими;
- использовать множественное число, если подтвержден один объект.
Если существует сомнение — не упоминай этот факт.

ПРАВИЛА ТЕХНИЧЕСКОЙ ТОЧНОСТИ:
1. Категорически запрещено переносить в результат разговорную речь, жалобы клиентов или бытовой сленг сотрудников, слова "глюк", "косяк", "слетели", "срочно", "важно", "помощь".
2. Не называй объекты 1С, если они прямо не указаны в JSON.
3. Если в исходных данных указаны названия объектов, методов, областей макета, реквизитов, обработок, отчетов, документов, регистров, ошибок или исключений, обязательно сохрани их в SUMMARY.
4. Не заменяй точные технические наименования обобщениями
5. Точность важнее краткости. Точное техническое наименование важнее красивой формулировки.

ЭЛЕМЕНТЫ КОНФИГУРАЦИИ, КОТОРЫЕ ДОЛЖНЫ БЫТЬ УКАЗАНЫ В SUMMARY ПРИ ИХ НАЛИЧИИ В JSON:
1C, Доработка, Разработка, Обмен данными, Обновление конфигурации, Обработка, Объекты конфигурации, Отладчик, Отображение ошибок, Отчет, Внешние обработки, Внешние отчеты, Документы, Динамический список, Журнал документов, Журнал регистрации, Интеграция, Консоль запросов, Константа, Конфигуратор, Макеты, Механизм запросов, Нумератор, Панель инструментов, Панель разделов, Параметр сеанса, Параметры информационной базы, Перечисления, План обмена, План счетов, Подписка на событие, Подсистемы, Полнотекстовый поиск, Профили групп доступа, Расширения, Регистр бухгалтерии, Регистр накопления, Регистр расчета, Регистр сведений, Регламентное задание, Режим технического специалиста, Роль, Список пользователей, Справочники, Табличный документ, Технологический журнал, Толстый клиент, Тонкий клиент, Универсальный механизм обмена данными, Фоновое задание, Функциональная опция, Характеристики.
Различай: "Доработка" = изменение имеющегося функционала; "Разработка" = создание нового.

К элементам конфигурации относятся названия:
Методов, функций;
- названия областей макетов;
- названия реквизитов;
- названия модулей;
- названия печатных форм;
- названия внешних обработок;
- названия внешних отчетов;
- названия кон;
- текст ошибки;
- названия объектов метаданных.

Технические идентификаторы имеют более высокий приоритет,чем краткость текста.

ПЕРЕД ФОРМИРОВАНИЕМ SUMMARY ВНУТРЕННЕ ИЗВЛЕКИ:

1. Точный объект, с которым возникла проблема или по которому выполнялись работы.
2. Точный текст или тип ошибки, если указан.
3. Точный элемент конфигурации, связанный с причиной проблемы, если он подтвержден исходными данными.
4. Условия возникновения проблемы, если указаны.
5. Точные выполненные действия: что именно проверено, изменено, добавлено, удалено, настроено или разработано и в каком объекте.
6. Подтвержденный результат выполненных работ.
При формировании SUMMARY сохрани все извлеченные технические наименования, которые необходимы для понимания причины и выполненных работ.

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
- Если в JSON указано конкретное название объекта, организации, обособленного подразделения, сотрудника, пользователя, базы, отчета, документа, обработки, настройки или другого предмета работ, используй это конкретное название в TITLE вместо родового обозначения.
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
- Если из JSON видно, что клиент задал запрос, а сотрудники пытались связаться или уточнить детали, но клиент не ответил, это не считается недостатком информации. В таком случае запрещено выводить INSUFFICIENT_INFORMATION: в SUMMARY укажи исходный запрос клиента и факт попыток связи; результат: задача закрыта из-за отсутствия обратной связи.
- Только если информации действительно недостаточно, не выводи разделы ✅ SUMMARY и 📝 TITLE.
- При действительной недостаточности информации вместо них выведи только следующую строку: INSUFFICIENT_INFORMATION
- Не добавляй никаких других комментариев или пояснений.

Упоминание постановщика:
- Используй значение переменной creatorId.
- Выведи его в формате: [USER=<creatorId>]Постановщик[/USER]
- Не пытайся определять постановщика по данным JSON.

ВЫВЕДИ РЕЗУЛЬТАТ В СЛЕДУЮЩЕМ ФОРМАТЕ

Для обычных задач:

[b]✅ SUMMARY:[/b]
<2-4 предложения:проблема/запрос, выполненные или предложенные действия, подтвержденный результат при наличии>

[b]📝 TITLE:[/b]
<Одно наиболее точное название>


Для задач "Обновление базы", если обновление выполнено на тот же релиз:

[b]✅ SUMMARY:[/b]
<2-4 предложения:проблема/запрос, выполненные или предложенные действия, подтвержденный результат при наличии>

Для задач "Обновление базы", если обновление выполнено на другой релиз:

[b]✅ SUMMARY:[/b]
<2-4 предложения:проблема/запрос, выполненные или предложенные действия, подтвержденный результат при наличии>

❗️[USER=<creatorId>]Постановщик[/USER], релиз, указанный в задаче, не совпадает с фактическим.

[b]📝 TITLE:[/b]
Обновление базы ... на релиз <фактический релиз>

Если информации недостаточно, вместо SUMMARY и TITLE выведи только: INSUFFICIENT_INFORMATION`;
}

function buildNextStepPrompt({ taskId, groupId, responsibleId, creatorId, task, comments, timeLogs, history, images, imageFacts, audioTranscripts }) {
  const context = {
    currentDate: new Date().toISOString(),
    currentTaskId: taskId,
    groupId,
    responsibleId,
    creatorId,
    task,
    comments,
    timeLogs,
    timeSpentInLogs: getTaskTimeSpent(task),
    durationFact: getTaskDurationFact(task),
    history,
    images: getImageMetadata(images),
    imageFacts,
    audioTranscripts,
  };

  return `Ты - профессиональный консультант 1С и координатор поддержки. Твоя задача — проанализировать незакрытую задачу и предложить следующий шаг, а не итог выполненных работ.

НИЖЕ ПРИВЕДЕН КОНТЕКСТ ЗАДАЧИ (JSON):
${JSON.stringify(context, null, 2)}

ПРАВИЛА АНАЛИЗА:
- Используй только факты из JSON.
- Не придумывай выполненные работы, договоренности, причины и сроки, если они прямо не подтверждены.
- Учитывай описание задачи, комментарии, историю изменений, трудозатраты, OCR изображений и расшифровки аудио.
- Комментарии и история последних действий важнее исходного описания задачи.
- Если последняя активность по задаче была давно или в JSON явно написано, что сотрудники дважды пытались связаться с заказчиком, выбери статус "🔴 Нет активности".
- Если из JSON видно, что ЭЛРОС написал клиенту, передал результат, задал уточняющий вопрос или ожидает подтверждения от заказчика, выбери статус "🟡 Ждет ответа заказчика". В следующем шаге предложи напомнить заказчику о задаче.
- Если из JSON видно, что задача находится на стороне ЭЛРОС, превышен срок, указанный разработчиком в комментарии, или срок отсутствует и нужно уточнить статус и срок, выбери статус "🟠 Ждет ответа от ЭЛРОС". В следующем шаге предложи актуализировать статус и срок выполнения.
- Если невозможно уверенно определить сторону ожидания, выбери наиболее осторожный статус и объясни, какого факта не хватает.

ФОРМАТ ОТВЕТА:
[b]СТАТУС:[/b] <ровно один статус из списка: 🟡 Ждет ответа заказчика / 🟠 Ждет ответа от ЭЛРОС / 🔴 Нет активности>

[b]ОБОСНОВАНИЕ:[/b]
<1-3 предложения: какие факты из JSON подтверждают выбранный статус>

[b]СЛЕДУЮЩИЙ ШАГ:[/b]
<1-2 предложения: что нужно сделать дальше>`;
}

function buildOpenTaskWatchPrompt({ taskId, groupId, responsibleId, creatorId, task, comments, timeLogs, history, images, imageFacts, audioTranscripts, parentContext }) {
  const context = {
    currentDate: new Date().toISOString(),
    currentTaskId: taskId,
    groupId,
    responsibleId,
    creatorId,
    minAgeDays: OPEN_TASK_MIN_AGE_DAYS,
    remindAfterDaysWithoutMovement: OPEN_TASK_REMIND_AFTER_DAYS,
    defaultRecheckDays: OPEN_TASK_DEFAULT_RECHECK_DAYS,
    staleAfterDaysWithoutUsefulUpdates: OPEN_TASK_STALE_AFTER_DAYS,
    task,
    comments,
    timeLogs,
    timeSpentInLogs: getTaskTimeSpent(task),
    durationFact: getTaskDurationFact(task),
    history,
    images: getImageMetadata(images),
    imageFacts,
    audioTranscripts,
    parentContext,
  };

  return `Ты - координатор поддержки 1С. Твоя задача — проанализировать открытую задачу в статусе "Ждет выполнения" и решить, нужно ли сейчас привлекать внимание.

НИЖЕ ПРИВЕДЕН КОНТЕКСТ ЗАДАЧИ И БАЗОВОЙ ЗАДАЧИ ПРИ НАЛИЧИИ (JSON):
${JSON.stringify(context, null, 2)}

ВЕРНИ ТОЛЬКО ВАЛИДНЫЙ JSON БЕЗ MARKDOWN:
{
  "reason": "WAIT_CLIENT",
  "summary": "Клиент должен предоставить резервную копию базы.",
  "action": "WAIT",
  "recheck_days": 7,
  "needs_attention": false
}

ДОПУСТИМЫЕ ЗНАЧЕНИЯ:
- reason: WAIT_CLIENT или WAIT_ELROS.
- action: WAIT, REMIND или STALE.
- recheck_days: целое число дней до следующей проверки.
- needs_attention: boolean.

ПРАВИЛА:
- Используй только факты из JSON, не придумывай договоренности, сроки и выполненные работы.
- Учитывай описание задачи, комментарии, историю, трудозатраты, OCR изображений, расшифровки аудио и базовую задачу.
- Комментарии и история последних действий важнее исходного описания.
- Если сотрудник описал решение проблемы, результат проверки, настройку или инструкцию для клиента, считай, что это отправлено заказчику и теперь ожидается ответ клиента: reason WAIT_CLIENT.
- WAIT_CLIENT означает, что следующий шаг или ответ находится на стороне клиента.
- WAIT_ELROS означает, что следующий шаг находится на стороне ЭЛРОС.
- action WAIT ставь только если была недавняя полезная активность или явно указан будущий срок/звонок/ожидание, поэтому сейчас не нужно никого дергать.
- action REMIND ставь, если ${OPEN_TASK_REMIND_AFTER_DAYS}+ дней нет полезной активности и понятно, чью сторону нужно напомнить.
- action STALE ставь, если ${OPEN_TASK_STALE_AFTER_DAYS}+ дней нет полезных апдейтов, непонятно что происходит, клиент не отвечает после попыток связи или участники не добавили новых условий/сроков.
- Старые задачи без движения за месяцы или с прошлого года не могут иметь action WAIT.
- action WAIT будет показан в отчете как статус "⚪️ Идет работа", чтобы задача не пропадала из ручной проверки.
- Для action STALE всегда ставь needs_attention true.
- Для WAIT_CLIENT + WAIT и WAIT_ELROS + WAIT ставь needs_attention false.
- Для WAIT_CLIENT + REMIND и WAIT_ELROS + REMIND ставь needs_attention true.
- Если в задаче прямо указан будущий срок, звонок или отпуск, recheck_days рассчитай от currentDate до первого рабочего дня, когда задачу нужно проверить после этого события. Например, звонок 12.08 — проверить 13.08; отпуск до сентября — проверить 02.09.
- Если явного срока нет, recheck_days = ${OPEN_TASK_DEFAULT_RECHECK_DAYS}.
- summary должен быть одним коротким предложением: что ожидается или почему задача требует внимания.`;
}

function parseAiJsonObject(text) {
  const raw = String(text || '').trim();
  const fenced = raw.match(/```(?:json)?\s*([\s\S]*?)```/i);
  const candidate = fenced ? fenced[1].trim() : raw;
  const objectMatch = candidate.match(/\{[\s\S]*\}/);
  if (!objectMatch) throw new Error(`AI response does not contain JSON object: ${truncateDebugText(raw, 500)}`);
  return JSON.parse(objectMatch[0]);
}

function normalizeOpenTaskAiResult(value) {
  const reason = ['WAIT_CLIENT', 'WAIT_ELROS'].includes(value?.reason) ? value.reason : 'WAIT_ELROS';
  const action = ['WAIT', 'REMIND', 'STALE'].includes(value?.action) ? value.action : 'STALE';
  const recheckDays = Number.parseInt(value?.recheck_days, 10);
  const normalizedAction = action === 'STALE' ? 'STALE' : action;
  const needsAttention = normalizedAction === 'STALE' || normalizedAction === 'REMIND';

  return {
    reason,
    summary: String(value?.summary || '').trim() || 'Нужно уточнить актуальное состояние задачи.',
    action: normalizedAction,
    recheck_days: Number.isInteger(recheckDays) && recheckDays > 0 ? recheckDays : OPEN_TASK_DEFAULT_RECHECK_DAYS,
    needs_attention: needsAttention,
  };
}

function getDaysSince(dateValue, now = new Date()) {
  const timestamp = Date.parse(dateValue);
  if (!Number.isFinite(timestamp)) return null;
  return Math.floor((now.getTime() - timestamp) / (24 * 60 * 60 * 1000));
}

function applyOpenTaskMovementPolicy(aiResult, task, now = new Date()) {
  const lastMovementDate = getTaskLastMovementDate(task);
  const daysWithoutMovement = getDaysSince(lastMovementDate, now);
  const adjusted = { ...aiResult };
  let policyApplied = null;

  if (daysWithoutMovement == null || daysWithoutMovement < OPEN_TASK_REMIND_AFTER_DAYS) {
    return { aiResult: adjusted, policyApplied, lastMovementDate, daysWithoutMovement };
  }

  if (daysWithoutMovement >= OPEN_TASK_STALE_AFTER_DAYS && adjusted.action !== 'STALE') {
    adjusted.action = 'STALE';
    adjusted.needs_attention = true;
    policyApplied = 'stale_after_days_without_movement';
  } else if (adjusted.action === 'WAIT') {
    adjusted.action = 'REMIND';
    adjusted.needs_attention = true;
    policyApplied = 'remind_after_days_without_movement';
  }

  return { aiResult: adjusted, policyApplied, lastMovementDate, daysWithoutMovement };
}

function getOpenTaskAlertStatus(aiResult) {
  if (aiResult.action === 'STALE') return '🔴 Требует решения по актуальности';
  if (aiResult.reason === 'WAIT_CLIENT' && aiResult.action === 'REMIND') return '🟡 Ждет ответа заказчика';
  if (aiResult.reason === 'WAIT_ELROS' && aiResult.action === 'REMIND') return '🟠 Ждет ответа от ЭЛРОС';
  if (aiResult.action === 'WAIT') return '⚪️ Идет работа';
  return null;
}

function getIsoDaysAgo(days) {
  const date = new Date();
  date.setDate(date.getDate() - days);
  return date.toISOString();
}

function getNextRecheckAt(recheckDays) {
  const date = new Date();
  date.setDate(date.getDate() + recheckDays);
  return date.toISOString();
}

function formatDateForMessage(isoDate) {
  if (!isoDate) return 'не указана';
  const date = new Date(isoDate);
  if (!Number.isFinite(date.getTime())) return isoDate;
  return date.toLocaleDateString('ru-RU', { timeZone: 'Europe/Moscow' });
}

async function processClosedTask(taskId, options = {}) {
  const dryRun = Boolean(options.dryRun);
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
  saveDebug('lastTaskImages', {
    task_id: taskId,
    stage: 'current_images_downloaded_before_ocr',
    comments_source: commentsSource,
    image_model: IMAGE_MODEL_NAME,
    summary_model: SUMMARY_MODEL_NAME,
    ai_image_candidates_count: mainImageResult.candidatesCount + mainChatImageResult.candidatesCount,
    current_image_candidates: [...mainImageResult.candidates, ...mainChatImageResult.candidates],
    parent_image_candidates: [],
    ai_images_count: mainImages.length,
    ai_images: getImageMetadata(mainImages),
    current_image_facts_found: false,
    parent_image_facts_found: false,
  });
  const mainImageFacts = await extractImageFacts(mainImages, 'текущей задачи', taskId);
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
    parentImageFacts = await extractImageFacts(parentImages, 'родительской задачи', taskId);
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
    image_model: IMAGE_MODEL_NAME,
    summary_model: SUMMARY_MODEL_NAME,
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

  const aiResponse = await withTimeout(
    safePromise(coworkRequest('POST', '/chat/completions', {
      model: SUMMARY_MODEL_NAME,
      messages: [{
        role: 'user',
        content: prompt,
      }],
    }, {
      timeoutMs: OPEN_TASK_AI_REQUEST_TIMEOUT_MS,
    })),
    OPEN_TASK_AI_REQUEST_TIMEOUT_MS,
    `Open task AI ${taskId}`
  );

  const aiComment = normalizeAiContent(aiResponse?.choices?.[0]?.message?.content);
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
      image_model: IMAGE_MODEL_NAME,
      summary_model: SUMMARY_MODEL_NAME,
      time_spent_in_logs: timeSpentInLogs,
      time_logs_count: timeLogs.length,
      ai_images_count: aiImages.length,
      current_image_facts_found: Boolean(mainImageFacts),
      parent_image_facts_found: Boolean(parentImageFacts),
      comment_posted: false,
    };
  }
  if (!aiComment) throw new Error('AI model returned empty comment');

  if (isInsufficientInfoComment(aiComment)) {
    if (timeSpentInLogs > 0) {
      const insufficientInfoComment = buildInsufficientInfoComment(responsibleId);
      if (!dryRun) {
        await coworkRequest('POST', `/tasks/${taskId}/comments`, {
          message: insufficientInfoComment,
        });
      }

      return {
        ok: true,
        dry_run: dryRun,
        task_id: taskId,
        parent_id: parentId || null,
        responsible_id: responsibleId,
        creator_id: creatorId,
        group_id: groupId || null,
        group_name: groupName || null,
        image_model: IMAGE_MODEL_NAME,
        summary_model: SUMMARY_MODEL_NAME,
        time_spent_in_logs: timeSpentInLogs,
        time_logs_count: timeLogs.length,
        ai_images_count: aiImages.length,
        current_image_facts_found: Boolean(mainImageFacts),
        parent_image_facts_found: Boolean(parentImageFacts),
        comment_posted: !dryRun,
        comment_would_be_posted: dryRun,
        summary_field: TASK_SUMMARY_FIELD_CODE,
        summary_field_updated: false,
        summary_field_error: null,
        generated_title: null,
        raw_ai_comment: aiComment,
        ai_comment: insufficientInfoComment,
      };
    }

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
      image_model: IMAGE_MODEL_NAME,
      summary_model: SUMMARY_MODEL_NAME,
      time_spent_in_logs: timeSpentInLogs,
      time_logs_count: timeLogs.length,
      ai_images_count: aiImages.length,
      current_image_facts_found: Boolean(mainImageFacts),
      parent_image_facts_found: Boolean(parentImageFacts),
      comment_posted: false,
    };
  }

  const commentToPost = isSummaryOnlyGroup(groupId) ? extractSummaryFromAiComment(aiComment) : aiComment;

  if (!dryRun) {
    await coworkRequest('POST', `/tasks/${taskId}/comments`, {
      message: commentToPost,
    });
  }
  const generatedTitle = extractTitleFromAiComment(aiComment);
  let summaryFieldUpdated = false;
  let summaryFieldCode = TASK_SUMMARY_FIELD_CODE;
  let summaryFieldError = null;

  if (generatedTitle && !isSummaryOnlyGroup(groupId) && !dryRun) {
    const summaryFieldResult = await updateTaskSummaryField(taskId, generatedTitle);
    summaryFieldCode = summaryFieldResult.field;
    summaryFieldUpdated = summaryFieldResult.updated;
    summaryFieldError = summaryFieldResult.error;
  }

  return {
    ok: true,
    dry_run: dryRun,
    task_id: taskId,
    parent_id: parentId || null,
    responsible_id: responsibleId,
    creator_id: creatorId,
    group_id: groupId || null,
    group_name: groupName || null,
    image_model: IMAGE_MODEL_NAME,
    summary_model: SUMMARY_MODEL_NAME,
    time_spent_in_logs: timeSpentInLogs,
    time_logs_count: timeLogs.length,
    ai_images_count: aiImages.length,
    current_image_facts_found: Boolean(mainImageFacts),
    parent_image_facts_found: Boolean(parentImageFacts),
    comment_posted: !dryRun,
    comment_would_be_posted: dryRun,
    summary_field: summaryFieldCode,
    summary_field_updated: summaryFieldUpdated,
    summary_field_would_be_updated: Boolean(generatedTitle && !isSummaryOnlyGroup(groupId) && dryRun),
    summary_field_error: summaryFieldError,
    generated_title: generatedTitle,
    raw_ai_comment: aiComment,
    ai_comment: commentToPost,
  };
}

async function processOpenTaskNextStep(taskId) {
  saveDebug('lastOpenTaskNextStep', {
    task_id: taskId,
    status: 'started',
  });

  const { task, comments, commentsSource } = await fetchTaskWithComments(taskId);
  const filteredComments = filterGemmaComments(comments);
  const groupId = getGroupIdFromTask(task);
  const groupName = getGroupNameFromTask(task);
  const responsibleId = getResponsibleIdFromTask(task);
  const creatorId = getCreatorIdFromTask(task);
  const timeLogs = await fetchTaskTimeLogs(taskId);
  const history = await getTaskHistory(taskId);

  if (isTaskClosed(task)) {
    return {
      ok: false,
      skipped: true,
      reason: 'task_is_closed',
      task_id: taskId,
      status: getStatusFromTask(task),
    };
  }

  const imageResult = await prepareTaskImages(task, filteredComments, 'openTask');
  const chatImageResult = await prepareTaskChatImages(task, 'openTask');
  const images = [...imageResult.images, ...chatImageResult.images].slice(0, AI_MAX_IMAGES);

  saveDebug('lastOpenTaskNextStep', {
    task_id: taskId,
    status: 'images_downloaded_before_ocr',
    comments_source: commentsSource,
    group_id: groupId || null,
    group_name: groupName || null,
    task_status: getStatusFromTask(task),
    image_candidates_count: imageResult.candidatesCount + chatImageResult.candidatesCount,
    images_count: images.length,
    images: getImageMetadata(images),
  });

  const imageFacts = await extractImageFacts(images, 'незакрытой задачи', taskId);
  const audioResult = await prepareTaskChatAudioTranscripts(task, 'openTask');

  saveDebug('lastOpenTaskNextStep', {
    task_id: taskId,
    status: 'summary_started',
    comments_source: commentsSource,
    group_id: groupId || null,
    group_name: groupName || null,
    task_status: getStatusFromTask(task),
    comments_count: filteredComments.length,
    time_logs_count: timeLogs.length,
    history_count: history.length,
    image_candidates_count: imageResult.candidatesCount + chatImageResult.candidatesCount,
    images_count: images.length,
    image_facts_found: Boolean(imageFacts),
    audio_candidates_count: audioResult.candidatesCount,
    audio_transcripts_count: audioResult.transcripts.length,
  });

  const prompt = buildNextStepPrompt({
    taskId,
    groupId,
    responsibleId,
    creatorId,
    task,
    comments: filteredComments,
    timeLogs,
    history,
    images,
    imageFacts,
    audioTranscripts: audioResult.transcripts,
  });

  const aiResponse = await coworkRequest('POST', '/chat/completions', {
    model: SUMMARY_MODEL_NAME,
    messages: [{
      role: 'user',
      content: prompt,
    }],
  });
  const aiComment = normalizeAiContent(aiResponse?.choices?.[0]?.message?.content);
  if (!aiComment) throw new Error('AI model returned empty next step');

  const result = {
    ok: true,
    task_id: taskId,
    group_id: groupId || null,
    group_name: groupName || null,
    task_status: getStatusFromTask(task),
    responsible_id: responsibleId,
    creator_id: creatorId,
    comments_source: commentsSource,
    summary_model: SUMMARY_MODEL_NAME,
    image_model: IMAGE_MODEL_NAME,
    time_spent_in_logs: getTaskTimeSpent(task),
    time_logs_count: timeLogs.length,
    history_count: history.length,
    ai_images_count: images.length,
    image_facts_found: Boolean(imageFacts),
    audio_candidates_count: audioResult.candidatesCount,
    audio_transcripts_count: audioResult.transcripts.length,
    raw_ai_comment: aiComment,
    ai_comment: aiComment,
  };

  saveDebug('lastOpenTaskNextStep', {
    task_id: taskId,
    status: 'completed',
    ...result,
  });

  return result;
}

function normalizeWorkgroupPayload(response) {
  const data = unwrapData(response);
  return data?.workgroup || data?.group || data?.Workgroup || data;
}

async function fetchWorkgroup(groupId) {
  const normalizedGroupId = String(groupId || '');
  if (!normalizedGroupId) return null;
  if (workgroupCache.has(normalizedGroupId)) return workgroupCache.get(normalizedGroupId);

  try {
    const response = await coworkRequest('GET', `/workgroups/${encodeURIComponent(normalizedGroupId)}`);
    const workgroup = normalizeWorkgroupPayload(response);
    workgroupCache.set(normalizedGroupId, workgroup || null);
    return workgroup || null;
  } catch (error) {
    log('Workgroup fetch failed', { group_id: normalizedGroupId, error: error.message });
    workgroupCache.set(normalizedGroupId, null);
    return null;
  }
}

function getWorkgroupName(workgroup) {
  return workgroup?.name || workgroup?.NAME || workgroup?.title || workgroup?.TITLE || null;
}

function isWorkgroupArchived(workgroup) {
  const archived = workgroup?.archived ?? workgroup?.ARCHIVED;
  if (typeof archived === 'boolean') return archived;
  if (archived == null) return false;
  return ['Y', 'YES', 'TRUE', '1'].includes(String(archived).toUpperCase());
}

function buildOpenTaskSearchBody(offset) {
  return {
    order: {
      ID: 'ASC',
    },
    filter: {
      REAL_STATUS: 2,
      '<=CREATED_DATE': getIsoDaysAgo(OPEN_TASK_MIN_AGE_DAYS),
      '!GROUP_ID': Array.from(OPEN_TASK_EXCLUDED_GROUP_IDS).map(Number),
    },
    limit: 5000,
    offset,
    select: [
      'ID',
      'TITLE',
      'STATUS',
      'REAL_STATUS',
      'GROUP_ID',
      'CREATED_DATE',
      'ACTIVITY_DATE',
      'RESPONSIBLE_ID',
      'RESPONSIBLE_NAME',
      'RESPONSIBLE_LAST_NAME',
      'RESPONSIBLE_SECOND_NAME',
      'CREATED_BY',
      'CREATED_BY_NAME',
      'CREATED_BY_LAST_NAME',
      'CREATED_BY_SECOND_NAME',
      'PARENT_ID',
      'TIME_SPENT_IN_LOGS',
      'DURATION_FACT',
      'GROUP',
    ],
  };
}

function isOpenTaskOldEnough(task) {
  const createdAtMs = Date.parse(getTaskCreatedDate(task));
  if (!Number.isFinite(createdAtMs)) return false;
  return createdAtMs <= Date.parse(getIsoDaysAgo(OPEN_TASK_MIN_AGE_DAYS));
}

async function fetchOpenTaskWatchCandidates() {
  const tasks = [];
  const searchDebug = {
    status: 'fetch_open_tasks',
    method: 'POST',
    path: '/tasks/search',
    requests: [],
    raw_tasks_from_api: 0,
    status_2_tasks: 0,
    old_enough_tasks: 0,
    min_age_days: OPEN_TASK_MIN_AGE_DAYS,
  };

  for (let offset = 0, page = 1; page <= 200; page += 1) {
    const body = buildOpenTaskSearchBody(offset);
    saveDebug('lastOpenTaskWatchSearch', {
      ...searchDebug,
      current_offset: offset,
      current_page: page,
      current_body: body,
    });

    const response = await coworkRequest('POST', '/tasks/search', body);
    const pageTasks = normalizeTaskListPayload(response);
    searchDebug.requests.push({
      offset,
      page,
      body,
      response_meta: {
        total: response?.total ?? response?.meta?.total ?? null,
        next: response?.next ?? null,
        hasMore: response?.meta?.hasMore ?? null,
      },
      page_count: pageTasks.length,
      sample_task_ids: pageTasks.slice(0, 20).map(task => String(task?.id || task?.ID || '')),
      sample_group_ids: pageTasks.slice(0, 20).map(task => getGroupIdFromTask(task)),
      sample_group_names: pageTasks.slice(0, 20).map(task => getGroupNameFromTask(task)),
    });
    tasks.push(...pageTasks);
    searchDebug.raw_tasks_from_api = tasks.length;

    const hasMore = Boolean(response?.meta?.hasMore || response?.hasMore);
    if (!hasMore || pageTasks.length === 0) break;
    offset += 5000;
  }

  const openTasks = tasks.filter(task => getStatusFromTask(task) === '2');
  const oldEnoughTasks = openTasks.filter(isOpenTaskOldEnough);
  searchDebug.status_2_tasks = openTasks.length;
  searchDebug.old_enough_tasks = oldEnoughTasks.length;

  saveDebug('lastOpenTaskWatchSearch', searchDebug);

  return oldEnoughTasks;
}

async function getOpenTaskCandidateInfo(task) {
  const taskId = String(task?.id || task?.ID || '');
  const groupId = getGroupIdFromTask(task);
  if (!groupId) return { task, taskId, included: false, reason: 'no_group' };
  if (isOpenTaskExcludedGroupId(groupId)) return { task, taskId, included: false, reason: 'excluded_group_id', groupId };

  let groupName = getGroupNameFromTask(task);
  if (!groupName) {
    const workgroup = await fetchWorkgroup(groupId);
    groupName = getWorkgroupName(workgroup);
  }

  return {
    task,
    taskId,
    included: true,
    groupId,
    groupName,
    responsibleId: getResponsibleIdFromTask(task),
    responsibleName: getResponsibleNameFromTask(task),
  };
}

async function checkOpenTaskWatchEligibility(taskId) {
  const response = await coworkRequest('GET', `/tasks/${encodeURIComponent(taskId)}`);
  const task = normalizeTaskPayload(response);
  const info = await getOpenTaskCandidateInfo(task);

  return {
    ok: true,
    task_id: String(taskId),
    status: getStatusFromTask(task),
    created_date: getTaskCreatedDate(task),
    min_age_days: OPEN_TASK_MIN_AGE_DAYS,
    old_enough: isOpenTaskOldEnough(task),
    group_id: getGroupIdFromTask(task),
    group_name: getGroupNameFromTask(task) || info.groupName || null,
    included_by_group_filters: info.included,
    group_filter_skip_reason: info.included ? null : info.reason,
    would_be_candidate: getStatusFromTask(task) === '2' && isOpenTaskOldEnough(task) && info.included,
  };
}

async function getOpenTaskWatchState(taskId) {
  const rows = await queryOpenTaskDb(
    'SELECT * FROM open_task_watch_state WHERE task_id = ?',
    [String(taskId)]
  );
  return rows[0] || null;
}

async function getUnresolvedOpenTaskWatchStates() {
  return queryOpenTaskDb(
    'SELECT * FROM open_task_watch_state WHERE resolved_at IS NULL'
  );
}

async function saveOpenTaskWatchState({ task, groupId, groupName, aiResult, nextRecheckAt }) {
  const taskId = String(task?.id || task?.ID);
  const now = new Date().toISOString();
  const responsibleId = getResponsibleIdFromTask(task);
  const responsibleName = await getUserNameById(responsibleId);
  const existing = await getOpenTaskWatchState(taskId);

  await runOpenTaskDb(`
    INSERT INTO open_task_watch_state (
      task_id,
      created_at,
      group_id,
      group_name,
      responsible_id,
      responsible_name,
      task_link,
      first_seen_at,
      last_checked_at,
      last_ai_result_json,
      last_reason,
      last_action,
      last_summary,
      last_recheck_at,
      last_alert_sent_at,
      last_alert_status,
      resolved_at,
      resolved_reason,
      updated_at
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, NULL, ?)
    ON CONFLICT(task_id) DO UPDATE SET
      created_at = excluded.created_at,
      group_id = excluded.group_id,
      group_name = excluded.group_name,
      responsible_id = excluded.responsible_id,
      responsible_name = excluded.responsible_name,
      task_link = excluded.task_link,
      last_checked_at = excluded.last_checked_at,
      last_ai_result_json = excluded.last_ai_result_json,
      last_reason = excluded.last_reason,
      last_action = excluded.last_action,
      last_summary = excluded.last_summary,
      last_recheck_at = excluded.last_recheck_at,
      last_alert_sent_at = COALESCE(excluded.last_alert_sent_at, open_task_watch_state.last_alert_sent_at),
      last_alert_status = COALESCE(excluded.last_alert_status, open_task_watch_state.last_alert_status),
      resolved_at = NULL,
      resolved_reason = NULL,
      updated_at = excluded.updated_at
  `, [
    taskId,
    getTaskCreatedDate(task),
    groupId,
    groupName,
    responsibleId,
    responsibleName || null,
    buildTaskLink(task),
    existing?.first_seen_at || now,
    now,
    JSON.stringify(aiResult),
    aiResult.reason,
    aiResult.action,
    aiResult.summary,
    nextRecheckAt,
    null,
    null,
    now,
  ]);

  return {
    task_id: taskId,
    responsible_id: responsibleId,
    responsible_name: responsibleName || (responsibleId ? `Пользователь ${responsibleId}` : UNKNOWN_USER_NAME),
    group_id: groupId,
    group_name: groupName,
    task_link: buildTaskLink(task),
  };
}

async function saveOpenTaskWatchAlert(alert) {
  await runOpenTaskDb(`
    INSERT INTO open_task_watch_alerts (
      task_id,
      alert_status,
      reason,
      action,
      summary,
      responsible_id,
      responsible_name,
      group_id,
      group_name,
      task_link,
      sent_at,
      ai_result_json
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
  `, [
    alert.task_id,
    alert.alert_status,
    alert.reason,
    alert.action,
    alert.summary,
    alert.responsible_id,
    alert.responsible_name,
    alert.group_id,
    alert.group_name,
    alert.task_link,
    new Date().toISOString(),
    JSON.stringify(alert.ai_result),
  ]);
}

async function markOpenTaskWatchAlertSent(taskId, alertStatus) {
  await runOpenTaskDb(`
    UPDATE open_task_watch_state
    SET last_alert_sent_at = ?, last_alert_status = ?, updated_at = ?
    WHERE task_id = ?
  `, [
    new Date().toISOString(),
    alertStatus,
    new Date().toISOString(),
    String(taskId),
  ]);
}

async function markOpenTaskWatchResolved(taskId, reason) {
  await runOpenTaskDb(`
    UPDATE open_task_watch_state
    SET resolved_at = ?, resolved_reason = ?, updated_at = ?
    WHERE task_id = ? AND resolved_at IS NULL
  `, [
    new Date().toISOString(),
    reason,
    new Date().toISOString(),
    String(taskId),
  ]);
}

function isOpenTaskWatchDue(state, now = new Date()) {
  if (!state) return true;
  if (state.resolved_at && state.resolved_reason === 'collab_group_name') return true;
  if (state.resolved_at) return false;
  // Ручная проверка открытых задач должна повторно анализировать уже найденные задачи.
  // Иначе сохраненный last_recheck_at скрывает их из ai-test/debug до следующей даты.
  return true;
  // if (!state.last_recheck_at) return true;
  // return Date.parse(state.last_recheck_at) <= now.getTime();
}

function incrementOpenTaskGroupCount(counts, taskOrInfo) {
  const groupId = taskOrInfo.groupId || getGroupIdFromTask(taskOrInfo.task || taskOrInfo) || 'NO_GROUP';
  const groupName = taskOrInfo.groupName || getGroupNameFromTask(taskOrInfo.task || taskOrInfo) || null;
  const key = String(groupId);

  counts[key] ||= {
    group_id: key === 'NO_GROUP' ? null : key,
    group_name: groupName,
    count: 0,
    excluded: isOpenTaskExcludedGroupId(key),
  };

  counts[key].count += 1;
  if (!counts[key].group_name && groupName) counts[key].group_name = groupName;
}

function sortOpenTaskGroupCounts(counts) {
  return Object.values(counts).sort((a, b) => b.count - a.count);
}

async function collectOpenTaskWatchContext(taskId) {
  const { task, comments, commentsSource } = await fetchTaskWithComments(taskId);
  const filteredComments = filterGemmaComments(comments);
  const groupId = getGroupIdFromTask(task);
  let groupName = getGroupNameFromTask(task);
  if (!groupName && groupId) {
    const workgroup = await fetchWorkgroup(groupId);
    groupName = getWorkgroupName(workgroup);
  }
  const responsibleId = getResponsibleIdFromTask(task);
  const creatorId = getCreatorIdFromTask(task);
  const parentId = getParentIdFromTask(task);
  const timeLogs = await fetchTaskTimeLogs(taskId);
  const history = await getTaskHistory(taskId);

  if (isTaskClosed(task)) {
    return { task, skipped: true, reason: 'task_is_closed' };
  }

  const imageResult = await prepareTaskImages(task, filteredComments, 'openTaskWatch.currentTask');
  const chatImageResult = await prepareTaskChatImages(task, 'openTaskWatch.currentTask');
  const images = [...imageResult.images, ...chatImageResult.images].slice(0, AI_MAX_IMAGES);
  const imageFacts = await extractImageFacts(images, 'открытой задачи', taskId);
  const audioResult = await prepareTaskChatAudioTranscripts(task, 'openTaskWatch.currentTask');
  let parentContext = null;

  if (parentId && parentId !== '0') {
    const { task: parentTask, comments: parentComments, commentsSource: parentCommentsSource } = await fetchTaskWithComments(parentId);
    const filteredParentComments = filterGemmaComments(parentComments);
    const parentTimeLogs = await fetchTaskTimeLogs(parentId);
    const parentHistory = await getTaskHistory(parentId);
    const parentImageResult = await prepareTaskImages(parentTask, filteredParentComments, 'openTaskWatch.parentTask');
    const parentChatImageResult = await prepareTaskChatImages(parentTask, 'openTaskWatch.parentTask');
    const parentImages = [...parentImageResult.images, ...parentChatImageResult.images].slice(0, AI_MAX_IMAGES);
    const parentImageFacts = await extractImageFacts(parentImages, 'базовой задачи открытой задачи', taskId);
    const parentAudioResult = await prepareTaskChatAudioTranscripts(parentTask, 'openTaskWatch.parentTask');

    parentContext = {
      parentId,
      parentTask,
      parentComments: filteredParentComments,
      parentCommentsSource,
      parentTimeLogs,
      parentTimeSpentInLogs: getTaskTimeSpent(parentTask),
      parentDurationFact: getTaskDurationFact(parentTask),
      parentHistory,
      parentImages: getImageMetadata(parentImages),
      parentImageFacts,
      parentAudioTranscripts: parentAudioResult.transcripts,
    };
  }

  return {
    task,
    comments: filteredComments,
    commentsSource,
    groupId,
    groupName,
    responsibleId,
    creatorId,
    timeLogs,
    history,
    images,
    imageFacts,
    audioResult,
    parentContext,
  };
}

async function analyzeOpenTaskWatchTask(taskId, candidateInfo) {
  saveDebug('lastOpenTaskWatchCheck', {
    status: 'task_analysis_started',
    task_id: taskId,
    group_id: candidateInfo.groupId || null,
    group_name: candidateInfo.groupName || null,
  });

  const context = await collectOpenTaskWatchContext(taskId);
  if (context.skipped) {
    // Тестово отключено: открытая ветка больше не ведет состояние проверки в базе.
    // await markOpenTaskWatchResolved(taskId, context.reason);
    return { task_id: taskId, skipped: true, reason: context.reason };
  }

  if (isCollabGroupName(context.groupName)) {
    return {
      task_id: taskId,
      skipped: true,
      reason: 'collab_group_name',
      group_id: context.groupId || candidateInfo.groupId || null,
      group_name: context.groupName || candidateInfo.groupName || null,
    };
  }

  rememberTaskUserNames(context.task);
  const prompt = buildOpenTaskWatchPrompt({
    taskId,
    groupId: context.groupId,
    responsibleId: context.responsibleId,
    creatorId: context.creatorId,
    task: context.task,
    comments: context.comments,
    timeLogs: context.timeLogs,
    history: context.history,
    images: context.images,
    imageFacts: context.imageFacts,
    audioTranscripts: context.audioResult.transcripts,
    parentContext: context.parentContext,
  });

  saveDebug('lastOpenTaskWatchCheck', {
    status: 'task_ai_started',
    task_id: taskId,
    group_id: context.groupId || null,
    group_name: context.groupName || candidateInfo.groupName || null,
    comments_count: context.comments.length,
    history_count: context.history.length,
    images_count: context.images.length,
    image_facts_found: Boolean(context.imageFacts),
    audio_transcripts_count: context.audioResult.transcripts.length,
    parent_found: Boolean(context.parentContext),
  });

  const aiResponse = await coworkRequest('POST', '/chat/completions', {
    model: SUMMARY_MODEL_NAME,
    messages: [{
      role: 'user',
      content: prompt,
    }],
  });
  const rawAiResult = normalizeAiContent(aiResponse?.choices?.[0]?.message?.content);
  if (!rawAiResult) throw new Error('AI model returned empty open task watch result');

  const rawParsedAiResult = normalizeOpenTaskAiResult(parseAiJsonObject(rawAiResult));
  const movementPolicy = applyOpenTaskMovementPolicy(rawParsedAiResult, context.task);
  const aiResult = movementPolicy.aiResult;
  const alertStatus = getOpenTaskAlertStatus(aiResult);
  const nextRecheckAt = getNextRecheckAt(aiResult.recheck_days);
  // Тестово отключено: каждый ручной запуск собирает свежие сведения и не опирается на сохраненную историю проверок.
  // const saved = await saveOpenTaskWatchState({
  //   task: context.task,
  //   groupId: context.groupId || candidateInfo.groupId,
  //   groupName: context.groupName || candidateInfo.groupName,
  //   aiResult,
  //   nextRecheckAt,
  // });

  const responsibleId = getResponsibleIdFromTask(context.task);
  const responsibleName = getResponsibleNameFromTask(context.task)
    || candidateInfo.responsibleName
    || await getUserNameById(responsibleId);
  const saved = {
    task_id: taskId,
    responsible_id: responsibleId,
    responsible_name: responsibleName || (responsibleId ? `Пользователь ${responsibleId}` : UNKNOWN_USER_NAME),
    group_id: context.groupId || candidateInfo.groupId,
    group_name: context.groupName || candidateInfo.groupName,
    task_link: buildTaskLink(context.task),
  };

  const result = {
    ok: true,
    task_id: taskId,
    alert_status: alertStatus,
    needs_attention: aiResult.needs_attention,
    reason: aiResult.reason,
    action: aiResult.action,
    summary: aiResult.summary,
    recheck_days: aiResult.recheck_days,
    next_recheck_at: nextRecheckAt,
    raw_ai_result: rawAiResult,
    original_ai_result: rawParsedAiResult,
    ai_result: aiResult,
    movement_policy: {
      applied: movementPolicy.policyApplied,
      last_movement_date: movementPolicy.lastMovementDate,
      days_without_movement: movementPolicy.daysWithoutMovement,
      remind_after_days: OPEN_TASK_REMIND_AFTER_DAYS,
      stale_after_days: OPEN_TASK_STALE_AFTER_DAYS,
    },
    ...saved,
  };

  saveDebug('lastOpenTaskWatchCheck', {
    status: 'task_analysis_completed',
    ...result,
  });

  return result;
}

function buildOpenTaskWatchMessage(alerts) {
  const statusOrder = [
    '⚪️ Идет работа',
    '🟡 Ждет ответа заказчика',
    '🟠 Ждет ответа от ЭЛРОС',
    '🔴 Требует решения по актуальности',
  ];
  const byStatus = new Map();

  for (const alert of alerts) {
    if (!byStatus.has(alert.alert_status)) byStatus.set(alert.alert_status, new Map());
    const byResponsible = byStatus.get(alert.alert_status);
    const responsibleName = alert.responsible_name || UNKNOWN_USER_NAME;
    if (!byResponsible.has(responsibleName)) byResponsible.set(responsibleName, new Map());
    const byGroup = byResponsible.get(responsibleName);
    const groupName = alert.group_name || `Группа ${alert.group_id || 'без названия'}`;
    if (!byGroup.has(groupName)) byGroup.set(groupName, []);
    byGroup.get(groupName).push(alert);
  }

  const lines = ['📈 Контроль открытых задач:'];

  for (const status of statusOrder) {
    const byResponsible = byStatus.get(status);
    if (!byResponsible) continue;

    const statusMatch = status.match(/^(\S+)\s+(.+)$/);
    const statusIcon = statusMatch ? statusMatch[1] : '';
    const statusText = statusMatch ? statusMatch[2] : status;
    lines.push('', `${statusIcon} [b]${statusText}[/b]`);

    for (const [responsibleName, byGroup] of [...byResponsible.entries()].sort(([a], [b]) => a.localeCompare(b, 'ru'))) {
      lines.push('', `[b]${responsibleName}[/b]`);

      for (const [groupName, groupAlerts] of [...byGroup.entries()].sort(([a], [b]) => a.localeCompare(b, 'ru'))) {
        lines.push('', `[b]${groupName}[/b]`);

        for (const [index, alert] of groupAlerts.entries()) {
          lines.push(`${index + 1}. ${alert.summary}`);
          lines.push(`[i]Следующая проверка:[/i] ${formatDateForMessage(alert.next_recheck_at)}`);
          lines.push(alert.task_link);
          if (index < groupAlerts.length - 1) lines.push('');
        }
      }
    }
  }

  return lines.join('\n').trim();
}

function splitMessageByLines(message, maxChars) {
  const normalizedMaxChars = Number.isFinite(maxChars) && maxChars > 1000 ? maxChars : 7000;
  if (message.length <= normalizedMaxChars) return [message];

  const chunks = [];
  let current = '';

  for (const line of message.split('\n')) {
    const next = current ? `${current}\n${line}` : line;
    if (next.length > normalizedMaxChars && current) {
      chunks.push(current.trim());
      current = line;
    } else {
      current = next;
    }
  }

  if (current.trim()) chunks.push(current.trim());
  if (chunks.length <= 1) return chunks;

  return chunks.map((chunk, index) => {
    const title = `📈 Контроль открытых задач, часть ${index + 1}/${chunks.length}:`;
    return chunk.replace(/^📈 Контроль открытых задач:/, title);
  });
}

async function sendOpenTaskWatchReport(alerts) {
  if (alerts.length === 0) return null;

  const message = buildOpenTaskWatchMessage(alerts);
  const messages = splitMessageByLines(message, OPEN_TASK_REPORT_MAX_MESSAGE_CHARS);

  for (const chunk of messages) {
    await coworkRequest('POST', `/chats/${ELAPSED_NOTIFICATION_CHAT_ID}/messages`, { message: chunk });
  }

  for (const alert of alerts) {
    // Тестово отключено: открытая ветка не пишет историю ручных проверок в базу.
    // await saveOpenTaskWatchAlert(alert);
    // await markOpenTaskWatchAlertSent(alert.task_id, alert.alert_status);
  }

  saveDebug('lastOpenTaskWatchChatDecision', {
    sent: true,
    chat_id: ELAPSED_NOTIFICATION_CHAT_ID,
    alerts_count: alerts.length,
    message_parts: messages.length,
    message_lengths: messages.map(chunk => chunk.length),
    task_ids: alerts.map(alert => alert.task_id),
  });

  return message;
}

async function resolveOpenTaskWatchStates(candidateIds) {
  const candidateSet = new Set(candidateIds.map(String));
  const states = await getUnresolvedOpenTaskWatchStates();
  let resolved = 0;

  for (const state of states) {
    if (candidateSet.has(String(state.task_id))) continue;

    try {
      const response = await coworkRequest('GET', `/tasks/${encodeURIComponent(state.task_id)}`);
      const task = normalizeTaskPayload(response);
      const status = getStatusFromTask(task);
      const groupId = getGroupIdFromTask(task);
      let groupName = getGroupNameFromTask(task);

      if (groupId && !groupName) {
        const workgroup = await fetchWorkgroup(groupId);
        groupName ||= getWorkgroupName(workgroup);
      }

      if (isTaskClosed(task)) {
        await markOpenTaskWatchResolved(state.task_id, 'task_closed');
        resolved += 1;
      } else if (status !== '2') {
        await markOpenTaskWatchResolved(state.task_id, `status_changed_to_${status || 'unknown'}`);
        resolved += 1;
      } else if (!groupId) {
        await markOpenTaskWatchResolved(state.task_id, 'no_group');
        resolved += 1;
      } else if (isOpenTaskExcludedGroupId(groupId)) {
        await markOpenTaskWatchResolved(state.task_id, 'excluded_group_id');
        resolved += 1;
      // Тестово отключено: проверяем открытую ветку без отсева групп по названию.
      // } else if (groupName && isCollabGroupName(groupName)) {
      //   await markOpenTaskWatchResolved(state.task_id, 'collab_group_name');
      //   resolved += 1;
      }
    } catch (error) {
      log('Open task watch resolve check failed', { task_id: state.task_id, error: error.message });
    }
  }

  return resolved;
}

async function runOpenTaskWatchCheck(options = {}) {
  if (openTaskCheckInProgress) {
    const result = {
      ok: true,
      skipped: true,
      reason: 'open_task_check_already_running',
      started_at: openTaskCheckStartedAt,
      stage: openTaskCheckStage,
    };
    saveDebug('lastOpenTaskWatchCheck', result);
    return result;
  }

  const limit = Number.parseInt(options.limit, 10);
  const maxToAnalyze = Number.isInteger(limit) && limit > 0 ? limit : null;
  openTaskCheckInProgress = true;
  openTaskCheckStartedAt = new Date().toISOString();
  openTaskCheckStage = 'fetch_open_tasks';

  try {
    const rawTasks = await fetchOpenTaskWatchCandidates();
    openTaskCheckStage = 'filter_candidates';
    const candidateInfos = [];
    const skippedByReason = {};
    const skippedExamplesByReason = {};
    const rawGroupCounts = {};
    const candidateGroupCounts = {};

    for (const task of rawTasks) {
      incrementOpenTaskGroupCount(rawGroupCounts, task);
      const info = await getOpenTaskCandidateInfo(task);
      if (info.included) {
        candidateInfos.push(info);
        incrementOpenTaskGroupCount(candidateGroupCounts, info);
      }
      else {
        skippedByReason[info.reason] = (skippedByReason[info.reason] || 0) + 1;
        skippedExamplesByReason[info.reason] ||= [];
        if (skippedExamplesByReason[info.reason].length < 10) {
          skippedExamplesByReason[info.reason].push({
            task_id: info.taskId || String(task?.id || task?.ID || ''),
            group_id: info.groupId || getGroupIdFromTask(task) || null,
            group_name: info.groupName || getGroupNameFromTask(task) || null,
          });
        }
      }
    }

    openTaskCheckStage = 'resolve_existing_states';
    // Тестово отключено: не сверяемся с сохраненным состоянием, каждый запрос работает по актуальному списку из API.
    // const resolvedStates = await resolveOpenTaskWatchStates(candidateInfos.map(info => info.taskId));
    const resolvedStates = 0;

    openTaskCheckStage = 'select_due_tasks';
    const dueInfos = [];
    for (const info of candidateInfos) {
      if (isCollabGroupName(info.groupName)) {
        skippedByReason.collab_group_name = (skippedByReason.collab_group_name || 0) + 1;
        skippedExamplesByReason.collab_group_name ||= [];
        if (skippedExamplesByReason.collab_group_name.length < 10) {
          skippedExamplesByReason.collab_group_name.push({
            task_id: info.taskId,
            group_id: info.groupId || null,
            group_name: info.groupName || null,
          });
        }
        continue;
      }

      // Тестово отключено: last_recheck_at/resolved_at больше не ограничивают ручную проверку.
      // const state = await getOpenTaskWatchState(info.taskId);
      // if (isOpenTaskWatchDue(state)) dueInfos.push(info);
      dueInfos.push(info);
    }

    const selectedInfos = maxToAnalyze ? dueInfos.slice(0, maxToAnalyze) : dueInfos;
    const analyzed = [];
    const alerts = [];
    const failed = [];

    for (const [index, info] of selectedInfos.entries()) {
      openTaskCheckStage = `analyze_task_${index + 1}_of_${selectedInfos.length}`;
      try {
        const result = await withTimeout(
          safePromise(analyzeOpenTaskWatchTask(info.taskId, info)),
          OPEN_TASK_ANALYSIS_TIMEOUT_MS,
          `Open task analysis ${info.taskId}`
        );
        analyzed.push(result);

        if (result.alert_status) {
          alerts.push(result);
        }
      } catch (error) {
        failed.push({ task_id: info.taskId, error: error.message });
        saveDebug('lastOpenTaskWatchCheck', {
          status: 'task_analysis_failed',
          task_id: info.taskId,
          error: error.message,
        });
      }
    }

    openTaskCheckStage = 'send_chat_report';
    const message = await sendOpenTaskWatchReport(alerts);
    if (!message) {
      saveDebug('lastOpenTaskWatchChatDecision', {
        sent: false,
        reason: 'no_attention_tasks',
        chat_id: ELAPSED_NOTIFICATION_CHAT_ID,
        alerts_count: 0,
      });
    }

    const result = {
      ok: true,
      limit: maxToAnalyze,
      limit_applied: Boolean(maxToAnalyze),
      task_analysis_timeout_ms: OPEN_TASK_ANALYSIS_TIMEOUT_MS,
      ai_request_timeout_ms: OPEN_TASK_AI_REQUEST_TIMEOUT_MS,
      old_enough_open_tasks: rawTasks.length,
      candidates: candidateInfos.length,
      due_tasks: dueInfos.length,
      analyzed_tasks: analyzed.length,
      attention_tasks: alerts.length,
      failed_tasks: failed.length,
      resolved_states: resolvedStates,
      skipped_by_reason: skippedByReason,
      skipped_examples_by_reason: skippedExamplesByReason,
      raw_group_counts: sortOpenTaskGroupCounts(rawGroupCounts),
      candidate_group_counts: sortOpenTaskGroupCounts(candidateGroupCounts),
      message_sent: Boolean(message),
      chat_id: ELAPSED_NOTIFICATION_CHAT_ID,
      analyzed,
      failed,
    };

    saveDebug('lastOpenTaskWatchCheck', result);
    log('Open task watch check completed', result);
    return result;
  } finally {
    openTaskCheckInProgress = false;
    openTaskCheckStartedAt = null;
    openTaskCheckStage = null;
  }
}

function queueOpenTaskWatchCheck(options = {}) {
  if (openTaskCheckInProgress) {
    return {
      queued: false,
      reason: 'open_task_check_already_running',
      started_at: openTaskCheckStartedAt,
      stage: openTaskCheckStage,
    };
  }

  const queuedAt = new Date().toISOString();
  saveDebug('lastOpenTaskWatchCheck', {
    status: 'queued',
    queued_at: queuedAt,
    options,
  });

  setTimeout(() => {
    runOpenTaskWatchCheck(options)
      .catch(error => {
        saveDebug('lastOpenTaskWatchCheck', {
          status: 'failed',
          error: error.message,
        });
        saveDebug('lastError', { error: error.message });
        log('Open task watch check failed', { error: error.message });
      });
  }, 0);

  return {
    queued: true,
    queued_at: queuedAt,
    limit: options.limit || null,
    limit_applied: Boolean(options.limit),
  };
}

function queueClosedTaskProcessing(taskId, trigger) {
  const normalizedTaskId = String(taskId);

  if (closedTaskProcessingTaskIds.has(normalizedTaskId)) {
    saveDebug('lastClosedTaskProcessing', {
      task_id: normalizedTaskId,
      trigger,
      status: 'already_running',
    });
    return { queued: false, reason: 'closed_task_processing_already_running', task_id: normalizedTaskId };
  }

  closedTaskProcessingTaskIds.add(normalizedTaskId);
  saveDebug('lastClosedTaskProcessing', {
    task_id: normalizedTaskId,
    trigger,
    status: 'queued',
  });

  setTimeout(async () => {
    saveDebug('lastClosedTaskProcessing', {
      task_id: normalizedTaskId,
      trigger,
      status: 'started',
    });

    try {
      await rememberClosedTaskTime(normalizedTaskId);
      const result = await processClosedTask(normalizedTaskId);
      saveDebug('lastClosedTaskProcessing', {
        task_id: normalizedTaskId,
        trigger,
        status: 'completed',
        result,
      });
    } catch (error) {
      saveDebug('lastClosedTaskProcessing', {
        task_id: normalizedTaskId,
        trigger,
        status: 'failed',
        error: error.message,
      });
      saveDebug('lastError', {
        task_id: normalizedTaskId,
        stage: 'closed_task_background_processing',
        error: error.message,
      });
      log('Closed task background processing failed', {
        task_id: normalizedTaskId,
        trigger,
        error: error.message,
      });
    } finally {
      closedTaskProcessingTaskIds.delete(normalizedTaskId);
    }
  }, 0);

  return { queued: true, task_id: normalizedTaskId };
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
  const updateContext = await getLatestUpdateContext(taskId, webhookTimestampMs);
  const updateBatch = updateContext.batch;
  const primaryChange = updateBatch[0] || null;
  const primaryField = normalizeHistoryField(primaryChange?.field);
  const responsibleChange = primaryField === normalizeHistoryField('RESPONSIBLE_ID') ? primaryChange : null;
  const statusChange = primaryField === normalizeHistoryField('STATUS') ? primaryChange : null;
  const doneStageChange = isDoneStageChange(primaryChange) ? primaryChange : null;
  const actions = [];

  // Ветка 1: изменение ответственного, добавление прошлого исполнителя в соисполнители.
  const previousResponsibleId = normalizeId(responsibleChange?.value?.from);
  if (previousResponsibleId) {
    const result = await addAccomplice(taskId, previousResponsibleId);
    actions.push({ branch: 'responsible_changed', ...result });
  }

  let shouldProcessClosedTask = String(statusChange?.value?.to) === '5';
  let closeTrigger = shouldProcessClosedTask ? 'status_to_5' : null;
  let statusChangeNearDoneStage = null;

  if (!shouldProcessClosedTask && doneStageChange) {
    statusChangeNearDoneStage = findClosedStatusChangeNearDoneStage(updateContext.history, doneStageChange);
    shouldProcessClosedTask = Boolean(statusChangeNearDoneStage);
    closeTrigger = shouldProcessClosedTask ? 'done_stage_with_recent_status_to_5' : null;
  }

  saveDebug('lastTaskCloseDecision', {
    task_id: taskId,
    webhook_ts: data.ts || null,
    primary_field: primaryChange?.field || null,
    primary_value: primaryChange?.value || null,
    latest_fields: updateBatch.map(item => item.field),
    status_change_to: statusChange?.value?.to || null,
    done_stage_change: Boolean(doneStageChange),
    status_change_near_done_stage_id: statusChangeNearDoneStage?.id || null,
    status_change_near_done_stage_at: statusChangeNearDoneStage?.createdDate || null,
    should_process_closed_task: shouldProcessClosedTask,
    close_trigger: closeTrigger,
  });

  // Ветка 2: закрытие задачи, сбор контекста и вызов Геммы.
  if (shouldProcessClosedTask) {
    const result = queueClosedTaskProcessing(taskId, closeTrigger);
    actions.push({ branch: 'task_closed_queued', trigger: closeTrigger, ...result });
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

function sendAiTestPage(res) {
  res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8' });
  res.end(`<!doctype html>
<html lang="ru">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>AI preview</title>
  <style>
    body { font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif; margin: 24px; color: #1f2937; background: #f6f7f9; }
    main { max-width: 1100px; margin: 0 auto; }
    form { display: flex; gap: 8px; align-items: center; margin-bottom: 16px; flex-wrap: wrap; }
    input { font-size: 16px; padding: 8px 10px; border: 1px solid #cbd5e1; border-radius: 6px; width: 160px; }
    button { font-size: 16px; padding: 9px 14px; border: 0; border-radius: 6px; background: #2563eb; color: white; cursor: pointer; }
    button:disabled { opacity: .65; cursor: default; }
    section { background: white; border: 1px solid #e5e7eb; border-radius: 8px; padding: 14px; margin-top: 12px; }
    h1 { font-size: 24px; margin: 0 0 16px; }
    h2 { font-size: 16px; margin: 0 0 10px; }
    pre { white-space: pre-wrap; word-break: break-word; margin: 0; font-size: 14px; line-height: 1.45; }
    .grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(260px, 1fr)); gap: 12px; }
    .muted { color: #64748b; }
    .error { color: #b91c1c; }
  </style>
</head>
<body>
  <main>
    <h1>AI preview</h1>
	    <form id="form">
	      <input id="taskId" name="taskId" value="184538" inputmode="numeric">
	      <button id="run" type="submit">Итог закрытой задачи</button>
	      <span id="status" class="muted"></span>
	    </form>
	    <form id="openTaskForm">
	      <input id="openTaskId" name="openTaskId" value="184538" inputmode="numeric">
	      <button id="runOpenTask" type="submit">Следующий шаг</button>
	      <span id="openTaskStatus" class="muted"></span>
	    </form>
    <div class="grid">
      <section>
        <h2>Что было бы опубликовано</h2>
        <pre id="final"></pre>
      </section>
      <section>
        <h2>Сырой ответ AI</h2>
        <pre id="raw"></pre>
      </section>
    </div>
    <section>
      <h2>Детали</h2>
      <pre id="details"></pre>
    </section>
  </main>
  <script>
    function renderValue(value) {
      if (value === null || value === undefined) return '';
      if (typeof value === 'string') return value;
      try {
        return JSON.stringify(value, null, 2);
      } catch {
        return String(value);
      }
    }

	    const form = document.getElementById('form');
	    const openTaskForm = document.getElementById('openTaskForm');
	    const run = document.getElementById('run');
	    const runOpenTask = document.getElementById('runOpenTask');
	    const status = document.getElementById('status');
	    const openTaskStatus = document.getElementById('openTaskStatus');
	    const final = document.getElementById('final');
    const raw = document.getElementById('raw');
    const details = document.getElementById('details');

	    form.addEventListener('submit', async (event) => {
      event.preventDefault();
      run.disabled = true;
      status.textContent = 'Анализирую...';
      final.textContent = '';
      raw.textContent = '';
      details.textContent = '';

      try {
        const taskId = document.getElementById('taskId').value.trim();
        const response = await fetch('/ai-preview?taskId=' + encodeURIComponent(taskId));
        const data = await response.json();
        if (!response.ok || !data.ok) throw new Error(renderValue(data.error || data));

        final.textContent = renderValue(data.ai_comment);
        raw.textContent = renderValue(data.raw_ai_comment);
        details.textContent = JSON.stringify({
          task_id: data.task_id,
          group_id: data.group_id,
          image_model: data.image_model,
          summary_model: data.summary_model,
          time_spent_in_logs: data.time_spent_in_logs,
          comment_would_be_posted: data.comment_would_be_posted,
          summary_field: data.summary_field,
          summary_field_would_be_updated: data.summary_field_would_be_updated,
          generated_title: data.generated_title,
          current_image_facts_found: data.current_image_facts_found,
          parent_image_facts_found: data.parent_image_facts_found,
        }, null, 2);
        status.textContent = 'Готово';
      } catch (error) {
        status.textContent = 'Ошибка';
        final.textContent = renderValue(error.message || error);
        final.className = 'error';
      } finally {
        run.disabled = false;
      }
	    });

	    openTaskForm.addEventListener('submit', async (event) => {
	      event.preventDefault();
	      runOpenTask.disabled = true;
	      openTaskStatus.textContent = 'Анализирую...';
	      final.textContent = '';
	      raw.textContent = '';
	      details.textContent = '';
	      final.className = '';

	      try {
	        const taskId = document.getElementById('openTaskId').value.trim();
	        const response = await fetch('/ai-next-step-preview?taskId=' + encodeURIComponent(taskId));
	        const data = await response.json();
	        if (!response.ok || !data.ok) throw new Error(renderValue(data.error || data));

	        final.textContent = renderValue(data.ai_comment);
	        raw.textContent = renderValue(data.raw_ai_comment);
	        details.textContent = JSON.stringify({
	          task_id: data.task_id,
	          group_id: data.group_id,
	          task_status: data.task_status,
	          summary_model: data.summary_model,
	          time_spent_in_logs: data.time_spent_in_logs,
	          time_logs_count: data.time_logs_count,
	          history_count: data.history_count,
	          ai_images_count: data.ai_images_count,
	          image_facts_found: data.image_facts_found,
	          audio_candidates_count: data.audio_candidates_count,
	          audio_transcripts_count: data.audio_transcripts_count,
	        }, null, 2);
	        openTaskStatus.textContent = 'Готово';
	      } catch (error) {
	        openTaskStatus.textContent = 'Ошибка';
	        final.textContent = renderValue(error.message || error);
	        final.className = 'error';
	      } finally {
	        runOpenTask.disabled = false;
	      }
	    });
	  </script>
</body>
</html>`);
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
        app_version: APP_VERSION,
        storage: getStorageDebugInfo(),
        taskTimeCheckRunning: taskTimeCheckInProgress,
        taskTimeCheckStartedAt,
        taskTimeCheckStage,
        openTaskCheckRunning: openTaskCheckInProgress,
        openTaskCheckStartedAt,
        openTaskCheckStage,
        ...debugState,
      });
      return;
    }

    if ((req.method === 'GET' || req.method === 'HEAD') && pathname === '/ai-test') {
      if (req.method === 'HEAD') {
        res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8' });
        res.end();
      } else {
        sendAiTestPage(res);
      }
      return;
    }

    if ((req.method === 'GET' || req.method === 'HEAD') && pathname === '/ai-preview') {
      const taskId = new URL(req.url, `http://${req.headers.host || 'localhost'}`).searchParams.get('taskId') || '184538';
      if (!normalizeId(taskId)) {
        sendJson(res, 400, { ok: false, error: 'Invalid taskId' });
        return;
      }

      if (req.method === 'HEAD') {
        sendJson(res, 200, { ok: true });
        return;
      }

      const result = await processClosedTask(taskId, { dryRun: true });
      sendJson(res, 200, result);
      return;
    }

    if ((req.method === 'GET' || req.method === 'HEAD') && pathname === '/ai-next-step-preview') {
      const taskId = new URL(req.url, `http://${req.headers.host || 'localhost'}`).searchParams.get('taskId') || '184538';
      if (!normalizeId(taskId)) {
        sendJson(res, 400, { ok: false, error: 'Invalid taskId' });
        return;
      }

      if (req.method === 'HEAD') {
        sendJson(res, 200, { ok: true });
        return;
      }

      const result = await processOpenTaskNextStep(taskId);
      sendJson(res, result.ok ? 200 : 400, result);
      return;
    }

    if ((req.method === 'GET' || req.method === 'POST') && pathname === '/open-tasks-check') {
      if (req.method === 'POST') await readBody(req);
      const searchParams = new URL(req.url, `http://${req.headers.host || 'localhost'}`).searchParams;
      const limit = Number.parseInt(searchParams.get('limit'), 10);
      const wait = searchParams.get('wait') === 'true';
      const options = {};
      if (Number.isInteger(limit) && limit > 0) options.limit = limit;

      if (wait) {
        const result = await runOpenTaskWatchCheck(options);
        sendJson(res, 200, result);
        return;
      }

      const result = queueOpenTaskWatchCheck(options);
      sendJson(res, result.queued ? 202 : 200, {
        ok: true,
        ...result,
      });
      return;
    }

    if ((req.method === 'GET' || req.method === 'HEAD') && pathname === '/open-task-eligibility') {
      const taskId = new URL(req.url, `http://${req.headers.host || 'localhost'}`).searchParams.get('taskId');
      if (!normalizeId(taskId)) {
        sendJson(res, 400, { ok: false, error: 'Invalid taskId' });
        return;
      }

      if (req.method === 'HEAD') {
        sendJson(res, 200, { ok: true });
        return;
      }

      const result = await checkOpenTaskWatchEligibility(taskId);
      sendJson(res, 200, result);
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
