const fs = require('fs');
const vm = require('vm');
const path = require('path');
const html = fs.readFileSync(path.join(__dirname, '..', 'index.html'), 'utf8');

function extractFunction(name) {
  const needle = `function ${name}(`;
  const start = html.indexOf(needle);
  if (start < 0) throw new Error(`Function not found: ${name}`);
  const tail = html.slice(start + needle.length);
  const match = tail.match(/\n(?:async\s+)?function\s+[A-Za-z0-9_]+\s*\(/);
  const end = match ? start + needle.length + match.index : html.length;
  return html.slice(start, end).trim();
}

const names = [
  'aiHasMeaningfulValue',
  'aiStageTextLines',
  'aiNormalizeItrStage',
  'aiPersonHasFinalStage',
  'aiIsRedundantWorkerQuestion',
  'aiRemoveRedundantWorkerRequest',
  'aiIsRedundantStageRequest',
  'aiRemoveRedundantStageRequest',
  'aiFinalizeCompanyAttestationReply',
];
const context = {
  console,
  aiCurrentData: {
    certification: {standard: 'company_att'},
    company_attestation: {
      work_items: ['7.2','7.3','7.4','7.5','7.6'],
      workers: [{profession:'Каменщик', razryad:'II', count:1, source:'auto'}],
      itr: [{
        fio:'Горбунов Олег Васильевич',
        stage_reference_total:'49 лет', stage_reference_here:'Менее года',
        stage_years:'', stage_years_here:'', stage_source:'missing', stage_is_final:false,
        stage_needs_review:true,
        trudovaya_number:'Трудовая книжка б/н',
        employment_periods:[{start:'', start_text:'1972 год', position:'Главный инженер'}],
      }],
    },
  },
};
vm.createContext(context);
vm.runInContext(names.map(extractFunction).join('\n\n'), context);

const person = context.aiCurrentData.company_attestation.itr[0];
context.aiNormalizeItrStage(person);
if (person.stage_years || person.stage_years_here) {
  throw new Error('Form 2 reference was copied into live stage fields');
}
if (context.aiPersonHasFinalStage(person)) {
  throw new Error('uncertain labour chronology incorrectly marked final');
}

const reply = context.aiFinalizeCompanyAttestationReply({
  message: 'Стаж в поле employment_periods неполный. Пришлите точные даты и фото трудовых книжек.',
  questions: ['Пришлите фото трудовых книжек для точного расчета стажа'],
});
if (/employment_periods|пришлите.+фото трудов|точн.+дат/i.test(reply.message)) {
  throw new Error(`blocking technical stage request remains: ${reply.message}`);
}
if (reply.questions.length) throw new Error('blocking stage question remains');
if (!/рассчитан автоматически по подтверждённым периодам трудовой/i.test(reply.message)) {
  throw new Error(`automatic calculation/yellow explanation missing: ${reply.message}`);
}
console.log('V8 FRONTEND TEST PASSED');
