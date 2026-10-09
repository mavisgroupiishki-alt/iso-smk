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
    else if (ch === '}' && --depth === 0) return html.slice(start, i + 1);
  }
  throw new Error(`Unclosed function: ${name}`);
}

const context = {
  console,
  aiCurrentData: {
    staff: [{fio: 'Шлягевич Александр Апанасьевич', position: 'Инженер-строитель', role: 'itr'}],
    company_attestation: {work_items: [], workers: []},
  },
  aiNormalizeItrStage: value => value,
  aiFindCompanyAttPreset: () => null,
  aiBuildStandardWorkers: () => [],
  aiWorkerKey: worker => `${worker.profession}|${worker.razryad}`,
  COMPANY_ATT_SCOPE_PRESETS: {},
  COMPANY_ATT_WORKER_PRESETS: {},
  STANDARD_WORKER_RAZRYAD: 'III',
  STANDARD_WORKER_COUNT: 1,
};
vm.createContext(context);
vm.runInContext([
  extractFunction('aiIsOfficialStaffRow'),
  extractFunction('aiOfficialStaffRows'),
  extractFunction('aiNormalizeCompanyAttestation'),
  extractFunction('aiValidateReadiness'),
].join('\n\n'), context);

context.aiNormalizeCompanyAttestation();
if (context.aiCurrentData.company_attestation.itr.length !== 1 ||
    context.aiCurrentData.company_attestation.itr[0].fio !== 'Шлягевич Александр Апанасьевич') {
  throw new Error('confirmed staff was not projected into Form №2 ITR');
}

const draft = context.aiValidateReadiness({
  company: {name: 'ЛидингСтрой', form: 'ООО', unp: '692109706', director_fio: 'Казяковский Максим Анатольевич'},
  certification: {standard: 'company_att'},
  company_attestation: context.aiCurrentData.company_attestation,
});
if (!draft.ready || !draft.warnings.some(warning => /виды работ/.test(warning)) ||
    !draft.warnings.some(warning => /рабочие/.test(warning))) {
  throw new Error('incomplete Form №2 should be a generatable draft with plain warnings');
}

const missingDirector = context.aiValidateReadiness({
  company: {name: 'ЛидингСтрой', form: 'ООО'},
  certification: {standard: 'company_att'},
  company_attestation: {},
});
if (missingDirector.ready || !missingDirector.missing.includes('ФИО директора')) {
  throw new Error('company basics must still block generation');
}

console.log('COMPANY ATTESTATION READINESS FRONTEND TEST PASSED');
