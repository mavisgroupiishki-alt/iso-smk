const fs = require('fs');
const path = require('path');
const vm = require('vm');

const html = fs.readFileSync(path.join(__dirname, '..', 'index.html'), 'utf8');

function extractFunction(name) {
  const start = html.indexOf(`function ${name}(`);
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

const names = [
  'aiHasMeaningfulValue',
  'aiArchiveCardTransferDescription',
  'aiMakeArchiveCardTransferMessage',
  'aiCanFinishArchiveWithoutChat',
];
const context = {};
vm.createContext(context);
vm.runInContext(names.map(extractFunction).join('\n\n'), context);

const complete = context.aiMakeArchiveCardTransferMessage({
  company: {
    name: 'ОмиТрейд', form: 'ООО', unp: '790798634',
    director_fio: 'Глушинский Олег Иванович',
  },
  staff: [{}, {}, {}],
  objects: [{}],
  dates: {audit_date: '27.03.2026'},
});
if (!complete.includes('название компании') || !complete.includes('штат (3)')) {
  throw new Error('completed archive fields are not reported to the user');
}

const datesOnly = context.aiMakeArchiveCardTransferMessage({
  dates: {audit_date: '27.03.2026'},
});
if (datesOnly !== 'В карточку автоматически перенесено из архива: дата оформления.') {
  throw new Error('dates-only archive must not be described as a fully updated card');
}

if (!context.aiCanFinishArchiveWithoutChat('', { staff: [{}] })) {
  throw new Error('an upload with deterministic source data must not wait for a chat reply');
}
if (context.aiCanFinishArchiveWithoutChat('добавь объект', { staff: [{}] })) {
  throw new Error('a user correction still needs the chat');
}

console.log('archive card-transfer status regression: PASS');
