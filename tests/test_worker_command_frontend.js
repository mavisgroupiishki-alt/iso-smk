const fs = require('fs');
const path = require('path');
const vm = require('vm');

const html = fs.readFileSync(path.join(__dirname, '..', 'index.html'), 'utf8');

function extractFunction(name) {
  const start = html.indexOf(`function ${name}(`);
  if (start < 0) throw new Error(`Function not found: ${name}`);
  let depth = 0, started = false, quote = '', escaped = false;
  for (let index = start; index < html.length; index++) {
    const char = html[index];
    if (quote) {
      if (escaped) escaped = false;
      else if (char === '\\') escaped = true;
      else if (char === quote) quote = '';
      continue;
    }
    if (char === '"' || char === "'" || char === '`') { quote = char; continue; }
    if (char === '{') { depth++; started = true; }
    else if (char === '}' && started && --depth === 0) return html.slice(start, index + 1);
  }
  throw new Error(`Function ${name} is not complete`);
}

const context = {
  aiCurrentData: {workers: ['Штукатур']},
  aiRenderCardFromCurrent: () => {},
  aiSaveCurrentCompany: () => ({catch: () => {}}),
  console,
};
vm.createContext(context);
vm.runInContext([
  extractFunction('aiNormIdentity'),
  extractFunction('aiWorkerCommand'),
  extractFunction('aiApplyWorkerCommand'),
].join('\n\n'), context);

const incomplete = context.aiWorkerCommand('внеси трех рабочих');
if (!incomplete || incomplete.kind !== 'needs_professions') {
  throw new Error('a worker command without professions must be answered locally');
}

const explicit = context.aiWorkerCommand(
  'добавь рабочих: Электромонтажник, Монтажник связи; Подсобный рабочий'
);
if (!explicit || explicit.kind !== 'add' || explicit.professions.join('|') !==
    'Электромонтажник|Монтажник связи|Подсобный рабочий') {
  throw new Error(`worker professions were not parsed: ${JSON.stringify(explicit)}`);
}

if (context.aiWorkerCommand('добавь объект') !== null) {
  throw new Error('unrelated command was treated as a worker command');
}

const message = context.aiApplyWorkerCommand(explicit);
if (context.aiCurrentData.workers.join('|') !==
    'Штукатур|Электромонтажник|Монтажник связи|Подсобный рабочий') {
  throw new Error('explicit worker professions were not put into the card');
}
if (!/добавлены/i.test(message)) throw new Error('card update was not confirmed');

console.log('worker command fast path: PASS');
