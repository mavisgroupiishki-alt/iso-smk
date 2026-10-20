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
  'aiNormIdentity', 'aiIsOfficialStaffRow', 'aiOfficialStaffRows',
  'aiSourceStaffRows',
  'aiMergeEntityArray', 'aiNormCompanyIdentity', 'aiStructuredCompanyConflict', 'aiMergeStructuredSource', 'aiCompanyForSourceConfirmation', 'aiSourceBatchCompany',
];
const context = {};
vm.createContext(context);
vm.runInContext(names.map(extractFunction).join('\n\n'), context);

if (context.aiStructuredCompanyConflict({company: {name: 'АГС-монтаж'}}, {company: {name: 'АГC-монтаж'}})) throw new Error('Latin C should not split a Cyrillic company name');

if (context.aiCompanyForSourceConfirmation({}, {}) !== '') throw new Error('unknown company must not be confirmed');
if (context.aiCompanyForSourceConfirmation({company: {name: 'Альфа'}}, {}) !== 'Альфа') throw new Error('selected company missing');
if (context.aiCompanyForSourceConfirmation({}, {company: {name: 'Бета'}}) !== 'Бета') throw new Error('source company missing');

const identity = context.aiSourceBatchCompany({}, [
  {structured_data: {_source_company_unverified: true, staff: [{fio: 'Иванов Иван Иванович'}]}},
  {structured_data: {company: {name: 'Альфа', form: 'ООО', unp: '123456789'}}},
]);
if (identity.conflict || identity.name !== 'Альфа') throw new Error('later requisites must identify the batch');
if (!context.aiSourceBatchCompany({}, [
  {structured_data: {company: {name: 'Альфа', unp: '123456789'}}},
  {structured_data: {company: {name: 'Бета', unp: '987654321'}}},
]).conflict) throw new Error('different companies must not be confirmed as one batch');
if (context.aiSourceBatchCompany({}, [{structured_data: {staff: [{fio: 'Иванов Иван Иванович'}]}}]).name) throw new Error('anonymous batch must not invent a company');

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

// ISO archive data must survive both combining file fragments and merging
// into a card that already contains earlier projects and suppliers.
const oldCard = {objects: [{name: 'Объект 2025', customer: 'Заказчик'}], suppliers: [{name: 'Краска', type: 'краска'}]};
const fragment = {objects: [{name: 'Объект 2026'}, {name: 'Объект 2025', customer: ''}],
                  suppliers: [{name: 'Кабель', type: 'кабель'}],
                  iso_suot: {satisfaction_customers: [{name: 'Старый заказчик'}],
                             policy_evidence: {approval_date: '05.02.2025', familiarizations: [{fio: 'Иванов Иван Иванович', date: '06.02.2025'}]}}};
const combined = context.aiMergeStructuredSource({}, fragment);
const merged = context.aiMergeStructuredSource(oldCard, combined);
if (merged.objects.length !== 2 || merged.objects[0].customer !== 'Заказчик' || merged.objects[1].name !== 'Объект 2026') {
  throw new Error('archive objects were lost or replaced existing data');
}
if (merged.suppliers.length !== 2 || merged.suppliers[1].name !== 'Кабель') {
  throw new Error('archive suppliers were lost or replaced existing data');
}
if (merged.iso_suot.satisfaction_customers[0].name !== 'Старый заказчик') {
  throw new Error('historical customer from Word report was lost');
}
if (merged.iso_suot.policy_evidence.approval_date !== '05.02.2025') throw new Error('policy approval date was lost');
const conflictingPolicy = context.aiMergeStructuredSource(merged, {iso_suot: {policy_evidence: {
  approval_date: '06.02.2025', familiarizations: [{fio: 'Иванов Иван Иванович', date: '07.02.2025'}],
}}});
if (conflictingPolicy.iso_suot.policy_evidence.approval_date || conflictingPolicy.iso_suot.policy_evidence.familiarizations[0].date) {
  throw new Error('conflicting policy dates must require review');
}
const rejected = context.aiMergeStructuredSource(
  {company: {name: 'Компания А', form: 'ООО'}, objects: [{name: 'А'}]},
  {company: {name: 'Компания Б', form: 'ООО'}, objects: [{name: 'Б'}]});
if (!rejected._source_company_conflict || rejected.company.name !== 'Компания А' || rejected.objects.length !== 1) {
  throw new Error('company data from another archive must not be merged');
}
const unverified = context.aiMergeStructuredSource(
  {company: {name: 'Компания А'}, staff: []},
  {_source_company_unverified: true, staff: [{fio: 'Иванов Иван Иванович'}]});
if (!unverified._source_company_unverified || unverified.staff.length) {
  throw new Error('anonymous staff must not be merged before confirmation');
}

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
context.aiCanonicalizeStaffWorkerFlags = value => value;
context.aiNormalizeCompanyAttestation = () => {};
context.aiApplySelectedRequestProfile = () => {};
context.aiSyncRequestProfileControls = () => {};
context.aiWorkerPickerHtml = () => '';
context.aiSaveCurrentCompany = () => ({ catch() {} });
context.aiValidateReadiness = () => ({ ready: false, warnings: [], missing: [] });
context.igorEscape = value => String(value).replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;').replace(/"/g, '&quot;').replace(/'/g, '&#39;');
vm.runInContext(html.slice(html.indexOf('function aiEscapeHtml('), html.indexOf('function aiWorkspaceLocalPrefix(')), context);
vm.runInContext(extractFunction('aiRenderCard'), context);
context.aiRenderCard({ spk: { activity_profile: 'metal_only' } });
if (!card.innerHTML.includes('Производство металлоконструкций')) {
  throw new Error('SPK activity profile was not rendered in the card');
}
context.aiCurrentData = {company: {name: '<script>alert(1)</script>'}, certification: {scope: '<img src=x onerror=alert(1)>'}, staff: [{fio: '<img src=x>', position: '<script>x</script>'}], objects: [{name: '<img src=x onerror=alert(1)>'}], suppliers: [{name: '<script>alert(1)</script>'}]};
context.aiRenderCard({});
if (card.innerHTML.includes('<img src=x onerror=alert(1)>') || card.innerHTML.includes('<script>alert(1)</script>')) {
  throw new Error('untrusted object and supplier names must be escaped');
}

console.log('SPK frontend structured-staff regression: PASS');
