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
  'aiCloneForStorage', 'aiHasMeaningfulValue', 'aiDeepMergeDefined',
  'aiNormIdentity', 'aiMergeEntityArray', 'aiMergeStructuredSource',
];
const context = {};
vm.createContext(context);
vm.runInContext(names.map(extractFunction).join('\n\n'), context);

const result = context.aiMergeStructuredSource(
  {staff: [{fio: 'Рощин Александр Викторович', position: 'Главный инженер'}]},
  {staff: [{
    fio: 'Рощин Александр Викторович',
    diplomas: [{full_text: 'Диплом АБ № 12345'}],
    trudovye_numbers: ['ТК № 7654321'],
    source: 'archive_person_summary',
  }]},
);

if (result.staff.length !== 1) throw new Error('source staff was duplicated');
if (result.staff[0].position !== 'Главный инженер') throw new Error('existing role was lost');
if (result.staff[0].diplomas[0].full_text !== 'Диплом АБ № 12345') throw new Error('diploma was lost');
if (result.staff[0].trudovye_numbers[0] !== 'ТК № 7654321') throw new Error('workbook number was lost');

// The card is rendered immediately after every chat response. A profile from
// SPK must therefore not crash the entire chat with a missing local variable.
const card = { innerHTML: '', insertAdjacentHTML() {} };
const status = {};
const generateButton = { style: {} };
context.document = {
  getElementById(id) {
    return id === 'ai-card-body' ? card : (id === 'ai-gen-btn' ? generateButton : status);
  },
};
context.aiCurrentData = {};
context.aiPhotoThumbnails = {};
context.aiNormalizeCompanyAttestation = () => {};
context.aiSaveCurrentCompany = () => ({ catch() {} });
context.aiValidateReadiness = () => ({ ready: false, warnings: [], missing: [] });
context.igorEscape = value => String(value);
vm.runInContext(extractFunction('aiRenderCard'), context);
context.aiRenderCard({ spk: { activity_profile: 'metal_only' } });
if (!card.innerHTML.includes('Производство металлоконструкций')) {
  throw new Error('SPK activity profile was not rendered in the card');
}

console.log('SPK frontend structured-staff regression: PASS');
