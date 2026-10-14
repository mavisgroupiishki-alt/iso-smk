const fs = require('fs');
const path = require('path');
const vm = require('vm');

const html = fs.readFileSync(path.join(__dirname, '..', 'index.html'), 'utf8');
if (!html.includes('id="ai-document-date"') || !html.includes('aiSetDocumentDateToday()')) {
  throw new Error('blocked generation does not offer a direct document-date control');
}
function extractFunction(name) {
  const start = html.indexOf(`function ${name}(`);
  if (start < 0) throw new Error(`Function not found: ${name}`);
  let depth = 0, started = false, quoted = '', escaped = false;
  for (let i = start; i < html.length; i++) {
    const char = html[i];
    if (quoted) {
      if (escaped) escaped = false;
      else if (char === '\\') escaped = true;
      else if (char === quoted) quoted = '';
      continue;
    }
    if (char === '"' || char === "'" || char === '`') { quoted = char; continue; }
    if (char === '{') { depth++; started = true; }
    else if (char === '}') { depth--; if (started && depth === 0) return html.slice(start, i + 1); }
  }
  throw new Error(`Function ${name} is not complete`);
}

let rendered = 0;
const context = {
  aiCurrentData: {
    company: {name: 'ОмиТрейд', form: 'ООО', director_fio: 'Глушинский Олег Иванович'},
    certification: {standard: 'iso_suot', scope: 'СМР'},
    dates: {},
    staff: [{fio: 'Глушинский Олег Иванович', position: 'Директор'}],
  },
  aiRenderCardFromCurrent: () => { rendered += 1; },
  aiSaveCurrentCompany: () => Promise.resolve(),
};
vm.createContext(context);
vm.runInContext([
  extractFunction('aiIsOfficialStaffRow'),
  extractFunction('aiOfficialStaffRows'),
  extractFunction('aiSourceStaffRows'),
  extractFunction('aiCardStaffRows'),
  extractFunction('aiValidateReadiness'),
  extractFunction('aiNormalizeDocumentDate'),
  extractFunction('aiSetDocumentDate'),
].join('\n\n'), context);

if (context.aiValidateReadiness(context.aiCurrentData).ready) {
  throw new Error('fixture must be blocked before an explicit document date is set');
}
context.aiSetDocumentDate('2026-10-13');
if (context.aiCurrentData.certification.audit_date ||
    context.aiCurrentData.dates.audit_date ||
    context.aiCurrentData.dates.document_date !== '13.10.2026' || rendered !== 1) {
  throw new Error('date picker does not make an ISO/SUOT card ready for generation');
}
if (!context.aiValidateReadiness(context.aiCurrentData).ready) {
  throw new Error('generation stays disabled after the only missing required date is selected');
}
if (context.aiNormalizeDocumentDate('13.10.2026') !== '13.10.2026' ||
    context.aiNormalizeDocumentDate('bad') !== '') {
  throw new Error('document date validation accepts an invalid value');
}
console.log('document date frontend test passed');
