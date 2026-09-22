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

const context = {
  console,
  aiAddMsg: () => 'progress-id',
  aiUpdateMsg: (_id, message) => progress.push(message),
  aiCurrentData: {},
  FormData: FakeFormData,
  setTimeout: callback => callback(),
};
vm.createContext(context);
vm.runInContext([
  extractFunction('aiReadFile'),
  extractFunction('aiReadJsonResponse'),
  extractFunction('aiFileReadError'),
  extractFunction('aiArchiveIsBusyError'),
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
  if (context.aiFileReadError('Файл слишком большой для обработки').includes('различить текст')) {
    throw new Error('size limit was misclassified as an unreadable scan');
  }
  if (!context.aiArchiveIsBusyError('Сейчас уже разбирается другой архив')) {
    throw new Error('busy archive processing was not identified');
  }

  let starts = 0;
  context.fetch = async (url) => {
    if (url === '/api/extract-archive-async') {
      starts += 1;
      return {text: async () => JSON.stringify(starts === 1
        ? {success: false, error: 'Сейчас уже разбирается другой архив'}
        : {success: true, task_id: 'task-1'}), ok: true};
    }
    if (url === '/api/task/task-1') {
      return {text: async () => JSON.stringify({status: 'done', text: 'данные', warnings: []})};
    }
    throw new Error(`unexpected URL ${url}`);
  };
  const queued = await context.aiReadArchiveAsync({name: 'паспорта.zip', size: 512});
  if (starts !== 2 || queued.content !== 'данные') {
    throw new Error('busy archive was not retried after the previous file completed');
  }
  if (!progress.some(message => message.includes('ожидаю завершения обработки предыдущего файла'))) {
    throw new Error('queue status was not shown truthfully');
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
  console.log('visual upload frontend: PASS');
})().catch(error => {
  console.error(error);
  process.exitCode = 1;
});
