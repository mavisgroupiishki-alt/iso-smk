const fs = require('fs');
const vm = require('vm');
const html = fs.readFileSync(require('path').join(__dirname, '..', 'index.html'), 'utf8');
const names = ['aiIsCorrectionIntent','aiStableSemanticValue','aiCardFingerprint','aiSuccessClaimWithoutDataChange'];
let code = '';
for (const name of names) {
  const start = html.indexOf('function ' + name + '(');
  if (start < 0) throw new Error('missing ' + name);
  let depth = 0, inBody = false, end = -1;
  for (let i = start; i < html.length; i++) {
    if (html[i] === '{') { depth++; inBody = true; }
    else if (html[i] === '}') { depth--; if (inBody && depth === 0) { end = i + 1; break; } }
  }
  code += html.slice(start, end) + '\n';
}
const ctx = {};
vm.createContext(ctx);
vm.runInContext(code, ctx);
if (!ctx.aiIsCorrectionIntent('исправь директора на Иванова')) throw new Error('correction not detected');
if (ctx.aiIsCorrectionIntent('проанализируй архив')) throw new Error('false correction');
const a = {company:{name:'A'}, readiness:'partial', flags:[1]};
const b = {company:{name:'A'}, readiness:'ready', flags:[2]};
if (ctx.aiCardFingerprint(a) !== ctx.aiCardFingerprint(b)) throw new Error('status-only change counted as correction');
const c = {company:{name:'B'}, readiness:'ready'};
if (ctx.aiCardFingerprint(a) === ctx.aiCardFingerprint(c)) throw new Error('real data change missed');
if (!ctx.aiSuccessClaimWithoutDataChange('Да, всё исправил и готово')) throw new Error('success claim not detected');
console.log('frontend v15 helpers OK');
