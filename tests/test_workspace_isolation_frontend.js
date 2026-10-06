const fs = require('fs');
const path = require('path');
const vm = require('vm');

const html = fs.readFileSync(path.join(__dirname, '..', 'index.html'), 'utf8');

function extractFunction(name) {
  const start = html.indexOf(`function ${name}(`);
  if (start < 0) throw new Error(`Function not found: ${name}`);
  const brace = html.indexOf('{', start);
  let depth = 0;
  for (let index = brace; index < html.length; index++) {
    if (html[index] === '{') depth++;
    if (html[index] === '}' && --depth === 0) return html.slice(start, index + 1);
  }
  throw new Error(`Unclosed function: ${name}`);
}

const values = new Map();
const localStorage = {
  get length() { return values.size; },
  key(index) { return [...values.keys()][index] || null; },
  getItem(key) { return values.has(key) ? values.get(key) : null; },
  setItem(key, value) { values.set(key, String(value)); },
  removeItem(key) { values.delete(key); },
};
const context = { console, localStorage, igorAuthUser: {username: 'Настя'} };
vm.createContext(context);
vm.runInContext([
  "const AI_COMPANY_KEY_PREFIX = 'igor:company:';",
  extractFunction('aiWorkspaceLocalPrefix'),
  extractFunction('aiWorkspaceLastCompanyKey'),
  extractFunction('aiLocalCompanyStorageKey'),
  extractFunction('aiLocalSetCompany'),
  extractFunction('aiLocalGetCompany'),
  extractFunction('aiLocalListCompanies'),
].join('\n\n'), context);

const key = 'igor:company:one';
context.aiLocalSetCompany(key, {label: 'Карточка Насти'});
context.igorAuthUser = {username: 'Кристина'};
if (context.aiLocalGetCompany(key) !== null || context.aiLocalListCompanies().length !== 0) {
  throw new Error('browser storage leaked one account workspace into another');
}
context.aiLocalSetCompany(key, {label: 'Карточка Кристины'});
context.igorAuthUser = {username: 'Настя'};
if (context.aiLocalGetCompany(key).label !== 'Карточка Насти') {
  throw new Error('browser workspace did not restore the original account data');
}
if (html.includes('const AI_LOCAL_PREFIX') || html.includes('const AI_LAST_COMPANY_KEY')) {
  throw new Error('legacy shared browser storage keys remain');
}

console.log('workspace isolation frontend: PASS');
