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

const context = {};
vm.createContext(context);
vm.runInContext([
  extractFunction('aiIsPeriodikaRequest'),
  extractFunction('aiRequestedIsoSuotProduct'),
  extractFunction('aiRequestedPackageMode'),
  extractFunction('aiApplyRequestedPackageMode'),
  extractFunction('aiBuildArchivePeriodikaContext'),
  extractFunction('aiArchiveProductForRequest'),
].join('\n\n'), context);

if (!context.aiIsPeriodikaRequest('сделай периодику')) {
  throw new Error('periodika command was not detected');
}
if (context.aiRequestedPackageMode('сделай периодику ИСО') !== 'periodika') {
  throw new Error('periodika command did not select the annual-update mode');
}
if (context.aiRequestedPackageMode('сформируй новый пакет ISO') !== 'initial') {
  throw new Error('new package command did not reset the annual-update mode');
}
const card = {certification: {standard: 'iso'}};
context.aiApplyRequestedPackageMode(card, 'сделай периодику');
if (card.certification.package_mode !== 'periodika' || card.certification.standard !== 'iso') {
  throw new Error('periodika mode was not stored without changing ISO selection');
}
const suotCard = {certification: {}};
context.aiApplyRequestedPackageMode(suotCard, 'сделай периодику СУОТ');
if (suotCard.certification.standard !== 'suot') {
  throw new Error('SUOT periodika was changed into a combined ISO/SUOT package');
}
const unknownCard = {certification: {}};
context.aiApplyRequestedPackageMode(unknownCard, 'сделай периодику');
if (unknownCard.certification.standard) {
  throw new Error('an unspecified periodika was incorrectly changed into a combined package');
}
if (context.aiIsPeriodikaRequest('сформируй новый пакет ISO')) {
  throw new Error('ordinary package was mistaken for periodika');
}

const base = context.aiBuildArchivePeriodikaContext(
  'сделай периодику',
  '===== ФАЙЛ: прошлый ИСО+СУОТ.zip =====\nООО «Тест», штат и объекты'
);
if (!base.includes('НАЙДЕНЫ РАНЕЕ СОХРАНЁННЫЕ ДАННЫЕ КОМПАНИИ') ||
    !base.includes('Не создавай пустую новую компанию')) {
  throw new Error('previous package is not passed to periodika as the base');
}
if (context.aiBuildArchivePeriodikaContext('сделай новый пакет', 'архив')) {
  throw new Error('ordinary archive was mistakenly marked as periodika');
}
if (context.aiArchiveProductForRequest('сделай периодику', 'all') !== 'iso_suot') {
  throw new Error('periodika archive did not use ISO/SUOT recognition');
}
if (context.aiArchiveProductForRequest('сделай периодику', 'iso') !== 'iso') {
  throw new Error('selected ISO product was overwritten');
}
if (context.aiArchiveProductForRequest('сделай периодику СУОТ', 'all') !== 'suot') {
  throw new Error('SUOT periodika archive was routed through the combined product');
}

console.log('periodika archive frontend: PASS');
