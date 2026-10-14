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
  AI_WORKER_PROFESSION_OPTIONS: ['Монтажник стальных конструкций', 'Электромонтажник'],
  aiRenderCardFromCurrent: () => {},
  aiSaveCurrentCompany: () => ({catch: () => {}}),
  aiAddMsg: () => {},
  igorEscape: value => String(value == null ? '' : value),
  console,
};
vm.createContext(context);
vm.runInContext([
  extractFunction('aiNormIdentity'),
  extractFunction('aiWorkerCommand'),
  extractFunction('aiWorkerPickerHtml'),
  extractFunction('aiAddWorkerProfessions'),
  extractFunction('aiApplyWorkerPicker'),
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

const pickerAdded = context.aiAddWorkerProfessions([
  'Монтажник стальных конструкций', 'Электромонтажник', 'Монтажник стальных конструкций', 'Разнорабочий'
]);
if (pickerAdded.join('|') !== 'Монтажник стальных конструкций|Разнорабочий') {
  throw new Error(`manual picker did not preserve unique selected professions: ${JSON.stringify(pickerAdded)}`);
}
if (!html.includes('Добавить профессии вручную') || !html.includes('Добавить выбранные')) {
  throw new Error('manual worker picker controls are absent from the card');
}
if (!context.aiWorkerPickerHtml().includes('Монтажник стальных конструкций')) {
  throw new Error('manual picker does not render professions from the catalog');
}

const hint = {textContent: ''};
const picker = {
  querySelectorAll: () => [{value: 'Бетонщик'}, {value: 'Штукатур'}],
  querySelector: selector => selector.includes('custom') ? {value: 'Слесарь-сантехник'} : hint,
};
context.aiApplyWorkerPicker({closest: () => picker});
if (!context.aiCurrentData.workers.includes('Бетонщик') || !context.aiCurrentData.workers.includes('Слесарь-сантехник')) {
  throw new Error('selected or custom manual profession was not added to the card');
}

console.log('worker command fast path: PASS');
