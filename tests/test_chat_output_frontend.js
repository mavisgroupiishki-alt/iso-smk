const fs = require('fs');
const path = require('path');
const vm = require('vm');

const html = fs.readFileSync(path.join(__dirname, '..', 'index.html'), 'utf8');
function extractFunction(name) {
  const start = html.indexOf(`function ${name}(`);
  if (start < 0) throw new Error(`Function not found: ${name}`);
  let depth = 0, started = false, quoted = '', escaped = false;
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

const context = {};
vm.createContext(context);
vm.runInContext(['aiExtractJsonObject', 'aiParseResponse', 'aiPreventFalseGenerationClaim', 'aiIsGenerateCommand'].map(extractFunction).join('\n\n'), context);

const parsed = context.aiParseResponse('{"message":"Пакет готов","questions":[]}\nГотово.');
if (parsed.message !== 'Пакет готов' || parsed.questions.length !== 0) {
  throw new Error('trailing prose leaked into chat response');
}
const wrapped = context.aiParseResponse(JSON.stringify(
  JSON.stringify({message: 'Данные перенесены в карточку.', questions: [], data: {company: {name: 'ОмиТрейд'}}})
));
if (wrapped.message !== 'Данные перенесены в карточку.' || wrapped.data?.company?.name !== 'ОмиТрейд') {
  throw new Error('a JSON response wrapped as a string leaked into chat instead of updating the card');
}
const correctedClaim = context.aiPreventFalseGenerationClaim({
  message: 'Пакет документов организации ООО «ОмиТрейд» полностью сформирован и готов к выдаче.',
});
if (!correctedClaim.message.includes('ещё не запускался')) {
  throw new Error('ordinary chat output can falsely claim that package generation completed');
}
if (!context.aiIsGenerateCommand('формируй') || !context.aiIsGenerateCommand('Сформируй пакет документов')) {
  throw new Error('generation command was not recognized');
}
if (context.aiIsGenerateCommand('подскажи, как сформировать пакет')) {
  throw new Error('ordinary question was mistaken for a generation command');
}
console.log('chat output frontend test passed');
