const fs = require('fs');
const path = require('path');
const vm = require('vm');

const html = fs.readFileSync(path.join(__dirname, '..', 'index.html'), 'utf8');

if (!html.includes('>ИСО 9001</button>') ||
    !html.includes('>СУОТ</button>') ||
    !html.includes('>ИСО 9001 + СУОТ</button>')) {
  throw new Error('primary ISO/SUOT standards are not shown as separate choices');
}
if (!html.includes('>Новый пакет</button>') ||
    !html.includes('>Периодика / ИК</button>') ||
    !html.includes('Новый пакет — для первичного оформления')) {
  throw new Error('initial package and periodika choices are not explained separately');
}

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

const context = {
  AI_PRODUCT_LABELS: {
    iso: 'ИСО 9001', suot: 'СУОТ', iso_suot: 'ИСО 9001 + СУОТ',
    spk_bisp: 'СПК БИСП', spk_stroy: 'СПК Стройкомплекс',
    company_att: 'Аттестация компании', att: 'Аттестация специалиста',
  },
  aiCurrentData: {},
  aiSelectedProduct: 'iso',
  aiSelectedPackageMode: 'periodika',
  document: {
    getElementById: () => null,
    querySelectorAll: () => [],
    activeElement: null,
  },
};
vm.createContext(context);
vm.runInContext([
  extractFunction('aiIsPeriodikaRequest'),
  extractFunction('aiRequestedIsoSuotProduct'),
  extractFunction('aiRequestedPackageMode'),
  extractFunction('aiGetSelectedProduct'),
  extractFunction('aiApplySelectedRequestProfile'),
  extractFunction('aiApplyRequestedPackageMode'),
  extractFunction('aiBuildArchivePeriodikaContext'),
  extractFunction('aiArchiveProductForRequest'),
].join('\n\n'), context);

if (!context.aiIsPeriodikaRequest('сделай периодику')) {
  throw new Error('periodika command was not detected');
}
if (context.aiRequestedPackageMode('сделай периодику ИСО') !== 'periodika') {
  throw new Error('periodika command did not select the annual-update mode');
}
if (context.aiRequestedPackageMode('сформируй новый пакет ISO') !== 'initial') {
  throw new Error('new package command did not reset the annual-update mode');
}
const card = {certification: {standard: 'spk_bisp'}};
context.aiApplyRequestedPackageMode(card, 'сделай периодику ИСО');
if (card.certification.package_mode !== 'periodika' || card.certification.standard !== 'iso') {
  throw new Error('explicitly selected ISO product was not locked into the card');
}
context.aiSelectedProduct = 'spk_bisp';
const spkCard = {certification: {standard: 'iso'}};
context.aiApplyRequestedPackageMode(spkCard, 'сделай периодику ИСО');
if (spkCard.certification.standard !== 'spk_bisp') {
  throw new Error('chat text overrode an explicitly selected SPK product');
}
context.aiSelectedProduct = 'suot';
if (context.aiArchiveProductForRequest('сделай периодику ИСО', 'spk_bisp') !== 'suot') {
  throw new Error('archive recognition was not locked to the selected product');
}
if (context.aiIsPeriodikaRequest('сформируй новый пакет ISO')) {
  throw new Error('ordinary package was mistaken for periodika');
}

context.aiSelectedProduct = 'iso';
const base = context.aiBuildArchivePeriodikaContext(
  'любой текст',
  '===== ФАЙЛ: прошлый ИСО+СУОТ.zip =====\nООО «Тест», штат и объекты'
);
if (!base.includes('ЭТО ПЕРИОДИКА: ИСО 9001') ||
    !base.includes('НАЙДЕНЫ РАНЕЕ СОХРАНЁННЫЕ ДАННЫЕ КОМПАНИИ') ||
    !base.includes('Не создавай пустую новую компанию')) {
  throw new Error('previous package is not passed to periodika as the base');
}
context.aiSelectedPackageMode = 'initial';
if (context.aiBuildArchivePeriodikaContext('сделай периодику', 'архив')) {
  throw new Error('initial mode was mistakenly marked as periodika');
}
context.aiSelectedProduct = 'spk_bisp';
context.aiSelectedPackageMode = 'periodika';
const spkBase = context.aiBuildArchivePeriodikaContext('текст не должен менять продукт', 'прошлый пакет');
if (!spkBase.includes('ЭТО ПЕРИОДИКА: СПК БИСП') || spkBase.includes('ИСО/СУОТ')) {
  throw new Error('SPK periodika was incorrectly described as an ISO/SUOT package');
}

console.log('periodika archive frontend: PASS');
