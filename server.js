const http = require('http');
const https = require('https');
const sqlite3 = require('sqlite3').verbose();

function requireEnv(name) {
  const value = process.env[name];
  if (!value) throw new Error(`Missing required env ${name}`);
  return value;
}

function getBitrixRestWebhookUrl() {
  const webhookUrl = (
    process.env.BITRIX_REST_WEBHOOK_URL ||
    process.env.WEBHOOK_COMMENT_URL
  );
  if (!webhookUrl) throw new Error('Missing required env BITRIX_REST_WEBHOOK_URL');

  return webhookUrl
    .replace(/task\.commentitem\.add(?:\.json)?$/i, '')
    .replace(/\/?$/, '/');
}

const PORT = process.env.PORT || 3000;
const BASE_URL = requireEnv('BASE_URL');
const API_KEY = requireEnv('API_KEY');
const MODEL_NAME = process.env.MODEL_NAME || 'bitrix/google/gemma-4-26B-A4B-it';
const BITRIX_REST_WEBHOOK_URL = getBitrixRestWebhookUrl();
const WEBHOOK_TOKEN = requireEnv('WEBHOOK_TOKEN');
const ELAPSED_NOTIFICATION_CHAT_ID = process.env.ELAPSED_NOTIFICATION_CHAT_ID || 'chat42358';
const BITRIX_PORTAL_URL = process.env.BITRIX_PORTAL_URL || 'https://elros.bitrix24.ru';
const DB_PATH = process.env.DB_PATH || './task_time_logs.db';
const TASK_TIME_LOOKBACK_DAYS = Number(process.env.TASK_TIME_LOOKBACK_DAYS || 10);
const TASK_TIME_CHECK_INTERVAL_MS = Number(process.env.TASK_TIME_CHECK_INTERVAL_MS || 4 * 60 * 60 * 1000);
const TASK_TIME_CHECK_ENABLED = process.env.TASK_TIME_CHECK_ENABLED !== 'false';
const TASK_TIME_CHECK_RUN_ON_START = process.env.TASK_TIME_CHECK_RUN_ON_START !== 'false';
const TASK_TIME_ALERT_RETENTION_DAYS = Number(process.env.TASK_TIME_ALERT_RETENTION_DAYS || 180);
const API_REQUEST_TIMEOUT_MS = Number(process.env.API_REQUEST_TIMEOUT_MS || 60 * 1000);
const SERVER_SLEEP_MODE = process.env.SERVER_SLEEP_MODE === 'true';

let taskTimeCheckInProgress = false;
let taskTimeCheckStartedAt = null;
let taskTimeCheckStage = null;
const debugState = {
  lastRequest: null,
  lastTaskTimeCheck: null,
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

function bitrixCall(method, params = {}) {
  const endpoint = new URL(`${method}.json`, BITRIX_REST_WEBHOOK_URL);
  const body = new URLSearchParams();
  appendParams(body, params);

  return requestJson(endpoint, {
    method: 'POST',
    headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
    body: body.toString(),
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

function getGroupNameFromTask(task) {
  return task?.groupName || task?.group?.name || task?.group?.title || null;
}

function isCollabGroupName(groupName) {
  return String(groupName || '').toLowerCase().includes('коллаба');
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

function normalizeTimePayload(response) {
  const data = unwrapData(response);
  return Array.isArray(data) ? data : [];
}

async function fetchTaskWithComments(taskId) {
  const [taskResult, commentsResult] = await Promise.allSettled([
    coworkRequest('GET', `/tasks/${taskId}`),
    coworkRequest('GET', `/tasks/${taskId}/comments`),
  ]);

  if (taskResult.status === 'rejected') {
    throw new Error(`Failed to fetch task ${taskId}: ${taskResult.reason.message}`);
  }

  return {
    task: normalizeTaskPayload(taskResult.value),
    comments: commentsResult.status === 'fulfilled'
      ? normalizeCommentsPayload(commentsResult.value)
      : [],
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
    const response = await coworkRequest('GET', `/tasks?${buildTaskListQuery(offset)}`);
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
    userName: 'Неизвестный пользователь',
    history_id: null,
    history_created_date: null,
  }];
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

async function getTaskHistory(taskId) {
  const response = await bitrixCall('tasks.task.history.list', {
    taskId,
    order: { createdDate: 'DESC' },
  });

  const history = Array.isArray(response?.result?.list)
    ? response.result.list
    : Array.isArray(response?.result)
      ? response.result
      : [];

  return history
    .map(item => ({ ...item, createdAtMs: Date.parse(item.createdDate) }))
    .filter(item => Number.isFinite(item.createdAtMs))
    .sort((a, b) => b.createdAtMs - a.createdAtMs);
}

async function getLatestUpdateBatch(taskId) {
  const sortedHistory = await getTaskHistory(taskId);
  const latestChange = sortedHistory[0];
  if (!latestChange) return [];
  return sortedHistory.filter(item => latestChange.createdAtMs - item.createdAtMs <= 2000);
}

function findLatestFieldChange(updateBatch, fieldName) {
  const normalizedFieldName = normalizeHistoryField(fieldName);
  return updateBatch.find(item => normalizeHistoryField(item.field) === normalizedFieldName && item.value);
}

function buildPrompt({ taskId, responsibleId, mainTask, mainComments, contextparentID }) {
  const context = {
    currentTaskId: taskId,
    responsibleId,
    currentTask: mainTask,
    currentTaskComments: mainComments,
  };

  return `Ты - профессиональный консультант 1С. Твоя задача - проанализировать данные задачи и написать краткий итог. Ты анализируешь задачи компании-франчайзи 1С. Используй профессиональную терминологию, принятую в сфере внедрения и сопровождения продуктов 1С.

НИЖЕ ПРИВЕДЕН КОНТЕКСТ ЗАДАЧИ (JSON):
${JSON.stringify(context, null, 2)}

НИЖЕ ПРИВЕДЕН КОНТЕКСТ РОДИТЕЛЬСКОЙ ЗАДАЧИ (JSON):
${JSON.stringify(contextparentID, null, 2)}

Правила анализа:
- Основной объект анализа - currentTask и currentTaskComments.
- Если контекст родительской задачи не равен null, используй parentTask и parentTaskComments только как дополнительный контекст.
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

ЗАДАНИЕ:
Сформируй результат строго в следующем формате.

[b]✅ SUMMARY:[/b]
В 1-2 предложениях кратко, но максимально полно опиши суть задачи. Обязательно укажи:
- какая проблема или запрос были у клиента;
- каким способом проблема была решена или какие работы были выполнены.
- Не используй оценочные или шаблонные формулировки, такие как: "задача успешно решена", "работы успешно выполнены", "проблема полностью устранена", "вопрос закрыт" и аналогичные.
- Не используй слова "успешно", "полностью", "окончательно", "окончательно решена", если это прямо не указано в предоставленных данных.

[b]📝 TITLE:[/b]
- Предложи 2 наиболее подходящих варианта наименования задачи.
- Каждый вариант должен содержать от 3 до 20 слов.
- Название должно быть кратким, но максимально точно отражать выполненную работу.
- Если основная суть задачи - консультация или предоставление информации, начинай название со слов "Консультация по...".
- Если основная суть задачи - выполнение работ, настройка, исправление, разработка, подключение, обновление или иные действия, начинай название со слов "Проведение работ по...".
- Не используй общие или расплывчатые формулировки.
- Не добавляй никаких пояснений, вступлений, комментариев или рассуждений.

Проверка релиза (только для задач с наименованием, начинающимся на "Обновление баз..."):
- Если в исходном названии задачи и в комментариях упоминаются номера релизов (версий), сравни их.
- Если номера релизов отличаются, сразу после заголовка 📝 TITLE: выведи отдельной строкой:
❗️Релиз, указанный в задаче, не совпадает с фактическим.
- Если в названии задачи или комментариях номер релиза отсутствует либо определить его невозможно, предупреждение не выводи.

Проверка достаточности информации:
- Перед формированием результата оцени, достаточно ли информации для понимания проблемы и выполненных работ.
- Если из JSON задачи и базовой задачи невозможно определить, в чем заключалась проблема или какие действия были выполнены, не пытайся делать предположения и не придумывай содержание.
- В этом случае не выводи разделы ✅ SUMMARY и 📝 TITLE.
- Вместо них выведи только следующую строку:
⚠️ [b]Недостаточно информации.[/b] [USER=<responsibleId>]Исполнитель[/USER], пожалуйста, напиши пояснения.
- Не добавляй никаких других комментариев или пояснений.

Упоминание исполнителя:
- Возьми значение поля "assignee" из объекта currentTask.
- Возьми только значение ID поля "assignee".
- Выведи значение ID поля "assignee" в формате: [USER=<responsibleId>]Исполнитель[/USER]
- Не изменяй значение "assignee" и не используй другие поля JSON.

Выведи только результат в следующем формате:
[b]✅ SUMMARY:[/b]
<1-2 предложения>

[b]📝 TITLE:[/b]
❗️Релиз, указанный в задаче, не совпадает с фактическим.
1. ...
2. ...

Если предупреждение не требуется, строку с ❗️ не выводи.

Если информации недостаточно, вместо SUMMARY и TITLE выведи только:

⚠️ [b]Недостаточно информации.[/b] [USER=<responsibleId>]Исполнитель[/USER], пожалуйста, напиши пояснения.`;
}

async function processClosedTask(taskId) {
  const { task: mainTask, comments: mainComments } = await fetchTaskWithComments(taskId);
  const parentId = getParentIdFromTask(mainTask);
  const responsibleId = getResponsibleIdFromTask(mainTask);
  const groupId = getGroupIdFromTask(mainTask);
  const groupName = getGroupNameFromTask(mainTask);
  const timeLogs = await fetchTaskTimeLogs(taskId);
  let contextparentID = null;

  if (timeLogs.length === 0) {
    return {
      ok: true,
      skipped: true,
      reason: 'empty_time_logs',
      task_id: taskId,
      time_logs_count: timeLogs.length,
    };
  }

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

  if (parentId && parentId !== '0') {
    const { task: parentTask, comments: parentComments } = await fetchTaskWithComments(parentId);
    contextparentID = { parentId, parentTask, parentTaskComments: parentComments };
  }

  const aiResponse = await coworkRequest('POST', '/chat/completions', {
    model: MODEL_NAME,
    messages: [{
      role: 'user',
      content: buildPrompt({ taskId, responsibleId, mainTask, mainComments, contextparentID }),
    }],
  });

  const aiComment = aiResponse?.choices?.[0]?.message?.content?.trim();
  if (!aiComment) throw new Error('Gemma returned empty comment');

  await coworkRequest('POST', `/tasks/${taskId}/comments`, {
    message: aiComment,
  });

  return {
    ok: true,
    task_id: taskId,
    parent_id: parentId || null,
    responsible_id: responsibleId,
    group_id: groupId || null,
    group_name: groupName || null,
    time_logs_count: timeLogs.length,
    comment_posted: true,
    ai_comment: aiComment,
  };
}

async function handleWebhook(body) {
  const data = parseFormUrlEncoded(body);

  if (SERVER_SLEEP_MODE) {
    return {
      statusCode: 200,
      data: {
        ok: true,
        sleeping: true,
        reason: 'server_sleep_mode_enabled',
        event: data.event || null,
      },
    };
  }

  const taskId = getTaskIdFromOutgoingWebhook(data);
  if (!taskId) throw new Error('No task ID found');

  if (data.auth?.application_token !== WEBHOOK_TOKEN) {
    return { statusCode: 403, data: { ok: false, error: 'Invalid webhook token' } };
  }

  if (data.event !== 'ONTASKUPDATE') {
    return { statusCode: 200, data: { ok: true, ignored: true, reason: 'unsupported_event' } };
  }

  const updateBatch = await getLatestUpdateBatch(taskId);
  const responsibleChange = findLatestFieldChange(updateBatch, 'RESPONSIBLE_ID');
  const statusChange = findLatestFieldChange(updateBatch, 'STATUS');
  const actions = [];

  // Ветка 1: изменение ответственного, добавление прошлого исполнителя в соисполнители.
  // Сейчас отключено.
  // const previousResponsibleId = normalizeId(responsibleChange?.value?.from);
  // if (previousResponsibleId) {
  //   const result = await addAccomplice(taskId, previousResponsibleId);
  //   actions.push({ branch: 'responsible_changed', ...result });
  // }

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
        sleeping: SERVER_SLEEP_MODE,
        taskTimeCheckRunning: taskTimeCheckInProgress,
        taskTimeCheckStartedAt,
        taskTimeCheckStage,
        ...debugState,
      });
      return;
    }

    if ((req.method === 'GET' || req.method === 'POST') && pathname === '/time-check') {
      if (req.method === 'POST') await readBody(req);
      if (SERVER_SLEEP_MODE) {
        sendJson(res, 200, {
          ok: true,
          sleeping: true,
          reason: 'server_sleep_mode_enabled',
        });
        return;
      }

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

  if (SERVER_SLEEP_MODE) {
    log('Server sleep mode enabled');
    return;
  }

  if (!TASK_TIME_CHECK_ENABLED) return;

  if (TASK_TIME_CHECK_RUN_ON_START) {
    setTimeout(() => {
      runTaskTimeCheck().catch(error => {
        saveDebug('lastError', { error: error.message });
        log('Task time check failed', { error: error.message });
      });
    }, 5000);
  }

  setInterval(() => {
    runTaskTimeCheck().catch(error => {
      saveDebug('lastError', { error: error.message });
      log('Task time check failed', { error: error.message });
    });
  }, TASK_TIME_CHECK_INTERVAL_MS);
});
