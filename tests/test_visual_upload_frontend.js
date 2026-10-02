const fs = require('fs');
const path = require('path');
const vm = require('vm');

const html = fs.readFileSync(path.join(__dirname, '..', 'index.html'), 'utf8');

function extractFunction(name) {
  let start = html.indexOf(`async function ${name}(`);
  if (start < 0) start = html.indexOf(`function ${name}(`);
  if (start < 0) throw new Error(`Function not found: ${name}`);
  const brace = html.indexOf('{', start);
  let depth = 0;
  let quote = null;
  let escaped = false;
  for (let i = brace; i < html.length; i++) {
    const ch = html[i];
    if (quote) {
      if (escaped) { escaped = false; continue; }
      if (ch === '\\') { escaped = true; continue; }
      if (ch === quote) quote = null;
      continue;
    }
    if (ch === '"' || ch === "'" || ch === '`') { quote = ch; continue; }
    if (ch === '{') depth++;
    if (ch === '}' && --depth === 0) return html.slice(start, i + 1);
  }
  throw new Error(`Unclosed function: ${name}`);
}

const progress = [];
class FakeFormData {
  append() {}
}

class FakeAbortController {
  constructor() {
    this.signal = {
      aborted: false,
      listeners: [],
      addEventListener: (_event, callback) => this.signal.listeners.push(callback),
    };
  }
  abort() {
    this.signal.aborted = true;
    this.signal.listeners.forEach(callback => callback());
  }
}

const context = {
  console,
  aiAddMsg: () => 'progress-id',
  aiUpdateMsg: (_id, message) => progress.push(message),
  aiCurrentData: {},
  window: {crypto: {randomUUID: () => 'upload-test-id'}},
  FormData: FakeFormData,
  AbortController: FakeAbortController,
  setTimeout: (callback, ms) => {
    if (ms >= 120000) {
      context.uploadTimeout = callback;
      return 'upload-timeout';
    }
    callback();
    return 'poll-timeout';
  },
  clearTimeout: () => {},
};
vm.createContext(context);
vm.runInContext([
  extractFunction('aiReadFile'),
  extractFunction('aiReadJsonResponse'),
  extractFunction('aiIsTransientChatError'),
  extractFunction('aiPostChatWithRetry'),
  extractFunction('aiStartArchiveUpload'),
  extractFunction('aiFileReadError'),
  extractFunction('aiArchiveIsBusyError'),
  extractFunction('aiArchiveUploadId'),
  extractFunction('aiArchiveUploadIsRetryable'),
  extractFunction('aiReadArchiveAsync'),
].join('\n\n'), context);

(async () => {
  let visualStarts = 0;
  context.fetch = async (url) => {
    if (url === '/api/extract-archive-async') {
      visualStarts += 1;
      return {text: async () => JSON.stringify({success: true, task_id: 'visual-task'}), ok: true};
    }
    if (url === '/api/task/visual-task') {
      return {text: async () => JSON.stringify({status: 'done', text: 'прочитано', warnings: []})};
    }
    throw new Error(`unexpected URL ${url}`);
  };
  const result = await context.aiReadFile({name: 'паспорт.jpg', size: 512});
  if (visualStarts !== 1) {
    throw new Error('visual file did not enter the background archive pipeline');
  }
  if (result.name !== 'паспорт.jpg' || result.content !== 'прочитано') {
    throw new Error('background result was not returned under the original filename');
  }

  let directWordReads = 0;
  visualStarts = 0;
  context.fetch = async (url) => {
    if (url === '/api/extract-text') {
      directWordReads += 1;
      return {text: async () => JSON.stringify({success: true, text: 'ООО «Тест»: реквизиты'})};
    }
    if (url === '/api/extract-archive-async') {
      visualStarts += 1;
      return {text: async () => JSON.stringify({success: true, task_id: 'word-ocr-task'}), ok: true};
    }
    if (url === '/api/task/word-ocr-task') {
      return {text: async () => JSON.stringify({status: 'done', text: 'скан прочитан', warnings: []})};
    }
    throw new Error(`unexpected URL ${url}`);
  };
  const ordinaryWord = await context.aiReadFile({name: 'реквизиты.docx', size: 512});
  if (directWordReads !== 1 || visualStarts !== 0 || !ordinaryWord.content.includes('реквизиты')) {
    throw new Error('ordinary Word document was incorrectly sent to the OCR queue');
  }

  context.fetch = async (url) => {
    if (url === '/api/extract-text') {
      return {text: async () => JSON.stringify({success: true, text: '', needs_visual_ocr: true})};
    }
    if (url === '/api/extract-archive-async') {
      visualStarts += 1;
      return {text: async () => JSON.stringify({success: true, task_id: 'word-ocr-task'}), ok: true};
    }
    if (url === '/api/task/word-ocr-task') {
      return {text: async () => JSON.stringify({status: 'done', text: 'скан прочитан', warnings: []})};
    }
    throw new Error(`unexpected URL ${url}`);
  };
  const scannedWord = await context.aiReadFile({name: 'трудовая.docx', size: 512});
  if (visualStarts !== 1 || scannedWord.content !== 'скан прочитан') {
    throw new Error('Word document with embedded scans did not enter the OCR queue');
  }
  if (context.aiFileReadError('Файл слишком большой для обработки').includes('различить текст')) {
    throw new Error('size limit was misclassified as an unreadable scan');
  }
  if (!context.aiArchiveIsBusyError('Сейчас уже разбирается другой архив')) {
    throw new Error('busy archive processing was not identified');
  }

  let starts = 0;
  let queuePolls = 0;
  context.fetch = async (url) => {
    if (url === '/api/extract-archive-async') {
      starts += 1;
      return {text: async () => JSON.stringify({success: true, task_id: 'task-1'}), ok: true};
    }
    if (url === '/api/task/task-1') {
      queuePolls += 1;
      return {text: async () => JSON.stringify(queuePolls === 1
        ? {status: 'queued', queuePosition: 2}
        : {status: 'done', text: 'данные', warnings: []})};
    }
    throw new Error(`unexpected URL ${url}`);
  };
  const queued = await context.aiReadArchiveAsync({name: 'паспорта.zip', size: 512});
  if (starts !== 1 || queued.content !== 'данные') {
    throw new Error('queued archive was uploaded more than once or did not finish');
  }
  if (!progress.some(message => message.includes('в очереди на обработку') && message.includes('загрузка сохранена'))) {
    throw new Error('saved queue status was not shown truthfully');
  }

  context.fetch = async () => ({
    status: 502,
    ok: false,
    text: async () => '<html>temporary proxy page</html>',
  });
  const failed = await context.aiReadArchiveAsync({name: 'Иванов трудовая.pdf', size: 512});
  if (!failed.content.includes('файл не был передан') || failed.content.includes('Unexpected token')) {
    throw new Error('an HTML proxy response was exposed as a technical JSON error');
  }
  try {
    await context.aiReadJsonResponse({
      status: 502,
      ok: false,
      text: async () => '<html>temporary proxy page</html>',
    }, 'chat');
    throw new Error('the chat response should have failed');
  } catch (error) {
    if (error.message.includes('Unexpected token') || !error.message.includes('временно не ответил')) {
      throw new Error('an HTML chat response was exposed as a technical JSON error');
    }
  }

  let chatAttempts = 0;
  context.fetch = async () => {
    chatAttempts += 1;
    if (chatAttempts === 1) throw new Error('Failed to fetch');
    return {status: 200, ok: true, text: async () => JSON.stringify({success: true, text: 'готово'})};
  };
  const retriedChat = await context.aiPostChatWithRetry({messages: []});
  if (chatAttempts !== 2 || retriedChat.text !== 'готово') {
    throw new Error('a short chat connection drop was not retried safely');
  }

  context.fetch = async (_url, options) => new Promise((_resolve, reject) => {
    options.signal.addEventListener('abort', () => reject(Object.assign(new Error('aborted'), {name: 'AbortError'})));
  });
  const stalled = context.aiStartArchiveUpload(new FakeFormData());
  context.uploadTimeout();
  try {
    await stalled;
    throw new Error('stalled upload should have failed');
  } catch (error) {
    if (!error.message.includes('не был передан') || error.message.includes('AbortError')) {
      throw new Error('a stalled PDF upload did not receive a user-facing timeout message');
    }
  }
  console.log('visual upload frontend: PASS');
})().catch(error => {
  console.error(error);
  process.exitCode = 1;
});
