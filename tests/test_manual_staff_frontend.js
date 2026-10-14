const fs = require('fs');
const path = require('path');
const vm = require('vm');

const html = fs.readFileSync(path.join(__dirname, '..', 'index.html'), 'utf8');
function extractFunction(name) {
  const start = html.indexOf(`function ${name}(`);
  if (start < 0) throw new Error(`Function not found: ${name}`);
  let depth = 0, started = false, quote = '', escaped = false;
  for (let index = start; index < html.length; index++) {
    const char = html[index];
    if (quote) {
      if (escaped) escaped = false;
      else if (char === '\\') escaped = true;
      else if (char === quote) quote = '';
      continue;
    }
    if (char === '"' || char === "'" || char === '`') { quote = char; continue; }
    if (char === '{') { depth++; started = true; }
    else if (char === '}' && started && --depth === 0) return html.slice(start, index + 1);
  }
  throw new Error(`Function ${name} is not complete`);
}

const context = {};
vm.createContext(context);
vm.runInContext([
  extractFunction('aiIsOfficialStaffRow'),
  extractFunction('aiOfficialStaffRows'),
  extractFunction('aiSourceStaffRows'),
  extractFunction('aiCardStaffRows'),
  extractFunction('aiParseRussianHireDate'),
  extractFunction('aiExtractExplicitStaffFromText'),
  extractFunction('aiValidateReadiness'),
].join('\n\n'), context);

const rows = context.aiExtractExplicitStaffFromText(`Список сотрудников — Гаранин Александр Владимирович, производитель работ (с аттестатом, официально не устроен) — Масюк Александр Михайлович (с удостоверением по ТБ), принят на работу 7 июля 2025 г. — Масюк Татьяна Николаевна (с удостоверением по ТБ), делопроизводитель (официально не устроена) — Масюк Дмитрий Александрович (с удостоверением по ТБ), директор и главный бухгалтер по совместительству, принят на работу 4 марта 2022 г. — Кирейчик Александр Александрович, монтажник связи, принят на работу 12 января 2026 г.`);
if (rows.length !== 5) throw new Error(`expected 5 employees, got ${rows.length}`);
if (rows[0].position !== 'производитель работ' || rows[0].manual_position_needs_review) {
  throw new Error('known position was not retained');
}
if (rows[1].position !== 'Требует уточнения' || !rows[1].manual_position_needs_review || rows[1].hire_date !== '07.07.2025') {
  throw new Error('employee without a stated position was not kept as a visible review row');
}
if (!rows[2].ot_certificate || rows[2].position !== 'делопроизводитель') {
  throw new Error('certificate or position was not extracted');
}
if (!rows[3].part_time || rows[3].hire_date !== '04.03.2022') {
  throw new Error('part-time status or hire date was not extracted');
}
if (rows[4].position !== 'монтажник связи' || rows[4].hire_date !== '12.01.2026') {
  throw new Error('worker row was not extracted');
}
if (context.aiCardStaffRows(rows).length !== 5) {
  throw new Error('explicit manual employees disappear before the card is rendered');
}
const readiness = context.aiValidateReadiness({
  company: {name: 'МасКомпани', form: 'ООО', director_fio: 'Масюк Дмитрий Александрович'},
  certification: {standard: 'iso_suot', scope: 'СМР'},
  dates: {document_date: '13.10.2026'},
  staff: rows,
});
if (!readiness.missing.includes('должность сотрудника: Масюк Александр Михайлович')) {
  throw new Error('a visible employee without a stated position does not block generation');
}
console.log('manual staff transfer regression: PASS');
