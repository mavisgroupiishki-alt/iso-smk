const {chromium} = require('playwright');
const fs = require('fs');
const path = require('path');

(async () => {
  const browser = await chromium.launch({headless: true});
  const page = await browser.newPage();
  try {
    const html = fs.readFileSync(path.join(__dirname, '..', 'index.html'), 'utf8');
    await page.route('**/*', route => route.request().url() === 'https://test.local/'
      ? route.fulfill({status: 200, contentType: 'text/html', body: html})
      : route.fulfill({status: 200, contentType: 'application/json', body: '{}'}));
    await page.goto('https://test.local/');
    await page.evaluate(() => {
      aiCurrentData = {company: {name: 'Альфа', unp: '123456789'}, certification: {standard: 'iso'}};
      aiGetSelectedProduct = () => 'iso';
      aiRenderCardFromCurrent = () => {};
      aiDetectCompanyLookupQuery = async () => null;
      aiReadFile = async file => ({name: file.name, content: file.name === 'old.zip' ? 'СТАРЫЕ СВЕДЕНИЯ' : 'НОВЫЕ СВЕДЕНИЯ', structured_data: file.name === 'old.zip' ? {company: {name: 'Альфа', unp: '123456789', director_fio: 'Прежний Директор'}, staff: [{fio: 'Прежний Сотрудник', position: 'Директор'}], workers: ['Старая профессия'], dates: {audit_date: '01.01.2025'}} : {company: {name: 'Альфа', unp: '123456789'}}});
      aiPostChatWithRetry = async payload => { window.sent = payload.messages.at(-1).content; return {success: true, text: JSON.stringify({message: 'Файлы прочитаны', data: {workers: ['Старая профессия']}})}; };
    });
    await page.evaluate(() => aiSelectPackageMode('periodika'));
    const input = page.locator('#ai-previous-package-input');
    if (!(await input.count())) throw new Error('previous package input is missing');
    await input.setInputFiles({name: 'old.zip', mimeType: 'application/zip', buffer: Buffer.from('archive')});
    await page.locator('#ai-file-input').setInputFiles({name: 'current.docx', mimeType: 'application/vnd.openxmlformats-officedocument.wordprocessingml.document', buffer: Buffer.from('doc')});
    await page.evaluate(() => aiHandleFiles(document.getElementById('ai-file-input').files));
    if (!(await page.locator('#ai-file-list').innerText()).includes('Предыдущий пакет')) throw new Error('historical file is not labelled');
    page.on('dialog', dialog => dialog.accept('Альфа'));
    await page.evaluate(() => aiSend());
    const content = await page.evaluate(() => window.sent);
    if (!content) throw new Error('model not called: ' + await page.locator('#ai-messages').innerText());
    if (!content || !/ПРЕДЫДУЩИЙ ПАКЕТ[^\n]*old.zip/.test(content) || !/ТЕКУЩИЙ ИСХОДНЫЙ ФАЙЛ[^\n]*current.docx/.test(content)) throw new Error('history/current file roles were not passed to the model: ' + String(content).slice(0, 1200));
    if (!content.includes('СТАРЫЕ СВЕДЕНИЯ') || !content.includes('НОВЫЕ СВЕДЕНИЯ')) throw new Error('file contents were not passed');
    const card = await page.evaluate(() => aiCurrentData);
    if (card.staff?.length || card.company?.director_fio || card.dates?.audit_date) throw new Error('historical staff or dates treated as current');
    if (card.workers?.includes('Старая профессия')) throw new Error('historical occupation treated as current');
    console.log('Previous package browser scenario: PASS');
  } finally { await browser.close(); }
})().catch(error => { console.error(error.message); process.exitCode = 1; });
