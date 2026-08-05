const fs = require('fs');
const vm = require('vm');
const path = require('path');
const html = fs.readFileSync(path.join(__dirname, '..', 'index.html'), 'utf8');

function extractFunction(name) {
  const needle = `function ${name}(`;
  const start = html.indexOf(needle);
  if (start < 0) throw new Error(`Function not found: ${name}`);
  const brace = html.indexOf('{', start);
  let depth = 0;
  let quote = null;
  let escape = false;
  for (let i = brace; i < html.length; i++) {
    const ch = html[i];
    if (quote) {
      if (escape) { escape = false; continue; }
      if (ch === '\\') { escape = true; continue; }
      if (ch === quote) quote = null;
      continue;
    }
    if (ch === '"' || ch === "'" || ch === '`') { quote = ch; continue; }
    if (ch === '{') depth++;
    else if (ch === '}') {
      depth--;
      if (depth === 0) return html.slice(start, i + 1);
    }
  }
  throw new Error(`Unclosed function: ${name}`);
}

const names = [
  'aiHasMeaningfulValue',
  'aiNormalizeFioKey',
  'aiMergeItrRecords',
  'aiIsRedundantWorkerQuestion',
  'aiRemoveRedundantWorkerRequest',
  'aiFinalizeCompanyAttestationReply',
];
const context = {
  console,
  aiCurrentData: {
    certification: {standard: 'company_att'},
    company_attestation: {
      work_items: ['7.2', '7.3', '7.4', '7.5', '7.6'],
      workers: [{profession: 'Каменщик', razryad: 'II', count: 1, source: 'auto'}],
    },
  },
};
vm.createContext(context);
vm.runInContext(names.map(extractFunction).join('\n\n'), context);

const base = [{
  fio: 'Горбунов Олег Васильевич',
  source: 'document',
  stage_years: '49 лет',
  stage_years_here: 'Менее года',
  trudovye_numbers: ['Трудовая книжка б/н', 'Вкладыш ПК № 00154290'],
}];
const patch = [{
  fio: 'Горбунов Олег Васильевич',
  position: 'Заместитель директора-главный инженер',
  stage_years: '',
  stage_years_here: '',
  trudovye_numbers: [],
}];
const merged = context.aiMergeItrRecords(base, patch, false)[0];
if (merged.stage_years !== '49 лет') throw new Error('stage_years was overwritten');
if (merged.stage_years_here !== 'Менее года') throw new Error('stage_years_here was overwritten');
if (merged.trudovye_numbers.length !== 2) throw new Error('labour-book inserts were overwritten');

const cleaned = context.aiFinalizeCompanyAttestationReply({
  message: "1. Уточните, пожалуйста, количество и разряды рабочих для 'Общестроительных работ', чтобы я заполнил Форму №2.",
  questions: ["Уточните количество и разряды рабочих"],
});
if (/уточните/i.test(cleaned.message)) throw new Error('redundant worker question remains in message');
if (cleaned.questions.length) throw new Error('redundant worker question remains in questions');
if (!/подставлены автоматически/i.test(cleaned.message)) throw new Error('automatic-worker confirmation missing');

console.log('V6 FRONTEND REGRESSION TEST PASSED');
