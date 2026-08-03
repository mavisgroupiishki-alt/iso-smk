import fs from 'node:fs';
import { createClient } from '@supabase/supabase-js';

const [, , csvPath] = process.argv;
const url = process.env.SUPABASE_URL;
const serviceKey = process.env.SUPABASE_SERVICE_ROLE_KEY;

if (!csvPath || !url || !serviceKey) {
  console.log('Использование: SUPABASE_URL=... SUPABASE_SERVICE_ROLE_KEY=... node scripts/import-tasks-from-csv.mjs ./tasks.csv');
  process.exit(1);
}

const supabase = createClient(url, serviceKey);

function parseCsvLine(line) {
  const result = [];
  let current = '';
  let inQuotes = false;
  for (let i = 0; i < line.length; i += 1) {
    const char = line[i];
    const next = line[i + 1];
    if (char === '"' && inQuotes && next === '"') {
      current += '"';
      i += 1;
    } else if (char === '"') {
      inQuotes = !inQuotes;
    } else if ((char === ',' || char === ';') && !inQuotes) {
      result.push(current.trim());
      current = '';
    } else {
      current += char;
    }
  }
  result.push(current.trim());
  return result;
}

function get(row, names, fallback = '') {
  for (const name of names) {
    const key = Object.keys(row).find((item) => item.toLowerCase().trim() === name.toLowerCase().trim());
    if (key && row[key]) return row[key];
  }
  return fallback;
}

function normalizePeriod(value) {
  const v = String(value || '').toLowerCase();
  if (v.includes('нед') || v === 'week') return 'week';
  if (v.includes('мес') || v === 'month') return 'month';
  return 'day';
}

const text = fs.readFileSync(csvPath, 'utf8');
const lines = text.split(/\r?\n/).filter((line) => line.trim());
const headers = parseCsvLine(lines[0]);
const payload = lines.slice(1).map((line) => {
  const values = parseCsvLine(line);
  const row = {};
  headers.forEach((header, index) => {
    row[header] = values[index] || '';
  });
  return {
    title: get(row, ['title', 'задача', 'название', 'название задачи'], 'Без названия'),
    owner: get(row, ['owner', 'ответственный', 'сотрудник'], 'Виктория'),
    deadline: get(row, ['deadline', 'срок', 'дата'], new Date().toISOString().slice(0, 10)),
    period: normalizePeriod(get(row, ['period', 'период'], 'day')),
    status: get(row, ['status', 'статус'], 'Новая'),
    priority: get(row, ['priority', 'приоритет'], 'Средний'),
    hours: Number(String(get(row, ['hours', 'часы', 'загрузка'], '1')).replace(',', '.')),
    comment: get(row, ['comment', 'комментарий'], ''),
    resource_url: get(row, ['resource_url', 'ссылка', 'ссылка на материалы', 'материалы', 'url'], ''),
    result: get(row, ['result', 'результат', 'измеримый результат'], ''),
  };
});

const { error } = await supabase.from('tasks').insert(payload);
if (error) throw error;
console.log(`Импортировано задач: ${payload.length}`);
