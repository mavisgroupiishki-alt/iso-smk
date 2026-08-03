const fs = require('fs');
const assert = require('assert');
const html = fs.readFileSync(require('path').join(__dirname, '..', 'index.html'), 'utf8');

function oneLineFunction(name) {
  const re = new RegExp(`function ${name}\\([^\\n]*\\}\\n?`);
  const match = html.match(re);
  if (!match) throw new Error(`Function ${name} not found`);
  return match[0];
}

for (const name of ['stageParseDate','stageDaysToYmd','stageMergeIntervals','stageIntervalDays']) {
  eval(oneLineFunction(name));
}
// dependency used by stageDaysToYmd
const pluralMatch = html.match(/function stagePlural\([^\n]*\}\n?/);
if (!pluralMatch) throw new Error('stagePlural not found');
eval(pluralMatch[0]);

let result = stageDaysToYmd(395);
assert.deepStrictEqual({years:result.years, months:result.months, days:result.days}, {years:1, months:1, days:5});

const a = {start:new Date(Date.UTC(2020,0,1)), end:new Date(Date.UTC(2020,0,30))};
const b = {start:new Date(Date.UTC(2020,0,15)), end:new Date(Date.UTC(2020,1,13))};
assert.strictEqual(stageIntervalDays([a,b], true, true), 44);
assert.strictEqual(stageIntervalDays([a,b], true, false), 60);
assert(stageParseDate('2026-08-03') instanceof Date);
assert.strictEqual(stageParseDate('03.08.2026'), null);
console.log('experience calculator v14: OK');
