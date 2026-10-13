const fs = require('fs');
const path = require('path');
const vm = require('vm');

const html = fs.readFileSync(path.join(__dirname, '..', 'index.html'), 'utf8');

function extractFunction(name) {
  const start = html.indexOf(`function ${name}(`);
  if (start < 0) throw new Error(`Function not found: ${name}`);
  let depth = 0;
  let started = false;
  let quoted = '';
  let escaped = false;
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

const context = {
  AI_WORKER_POSITION_STEMS: [
    'штукатур', 'маляр', 'сварщик', 'электрогаз', 'облицовщик', 'плиточник',
    'кровельщик', 'монтажник', 'электромонтажник', 'плотник', 'бетонщик',
    'каменщик', 'арматурщик', 'разнорабочий', 'подсобный', 'стропальщик',
    'водитель', 'машинист', 'оператор', 'слесарь', 'токарь', 'фрезеровщик',
    'сантехник', 'электрик', 'тракторист', 'экскаваторщик', 'крановщик',
  ],
  AI_ITR_POSITION_MARKERS: [
    'производитель работ', 'директор', 'инженер', 'прораб', 'мастер',
    'бухгалтер', 'техник-программист', 'заведующий', 'юрист', 'кадров',
    'экономист', 'менеджер', 'начальник',
  ],
};
vm.createContext(context);
vm.runInContext([
  extractFunction('aiIsOfficialStaffRow'),
  extractFunction('aiOfficialStaffRows'),
  extractFunction('aiIsWorkerPosition'),
  extractFunction('aiCanonicalizeStaffWorkerFlags'),
].join('\n\n'), context);

const staff = context.aiCanonicalizeStaffWorkerFlags([
  {fio: 'Брикин Денис Александрович', position: 'Подсобный рабочий', role: 'itr', is_worker: false},
  {fio: 'Грук Сергей Борисович', position: 'Штукатур', role: 'itr', is_worker: false},
  {fio: 'Глушинский Олег Иванович', position: 'Директор', role: 'itr', is_worker: true},
  {fio: 'Лукашик Сергей Васильевич', position: 'Заместитель директора-главный инженер', role: 'itr', is_worker: true},
]);

if (staff.filter(person => person.is_worker).length !== 2) {
  throw new Error('workers were not separated from ITR by their positions');
}
if (staff.some(person => person.is_worker && person.role === 'itr')) {
  throw new Error('worker still displays an ITR role');
}
if (staff.filter(person => !person.is_worker).length !== 2) {
  throw new Error('director and chief engineer must remain ITR despite a bad model flag');
}
console.log('staff classification frontend test passed');
