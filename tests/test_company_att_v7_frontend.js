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
      itr: [
        {fio:'Горбунов Олег Васильевич', stage_form2_text:'49 лет\nМенее года', source:'document'},
        {fio:'Крот Евгений Васильевич', stage_years:'28 лет', stage_years_here:'1 год', source:'document'},
      ],
    },
  },
};
vm.createContext(context);
vm.runInContext(names.map(extractFunction).join('\n\n'), context);

const first = context.aiCurrentData.company_attestation.itr[0];
if (!context.aiPersonHasFinalStage(first)) throw new Error('final Form 2 stage not restored');
if (first.stage_years !== '49 лет' || first.stage_years_here !== 'Менее года') {
  throw new Error(`wrong restored stage: ${first.stage_years} / ${first.stage_years_here}`);
}

const reply = context.aiFinalizeCompanyAttestationReply({
  message: 'Стаж в поле `employment_periods` заполнен. Почему он кажется неполным: отсутствуют точные даты. Я ставлю 01.01. Пришлите фото трудовых книжек, и я пересчитаю всё точно.',
  questions: ['Пришлите фото трудовых книжек для точного расчета стажа'],
});
if (/employment_periods|точн.+дат|01\.01|фото трудов/i.test(reply.message)) {
  throw new Error(`redundant stage request remains: ${reply.message}`);
}
if (reply.questions.length) throw new Error('redundant stage question remains');
if (!/стаж уже перенесён из заполненной Формы №2/i.test(reply.message)) {
  throw new Error(`final-stage confirmation missing: ${reply.message}`);
}
console.log('V7 FRONTEND REGRESSION TEST PASSED');
