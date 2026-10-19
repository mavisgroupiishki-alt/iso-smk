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
      aiCurrentData = {certification: {standard: 'iso'}};
      aiGetSelectedProduct = () => 'iso';
      aiApplySelectedRequestProfile = (data = aiCurrentData) => data;
      aiRenderCardFromCurrent = () => {};
      aiDetectCompanyLookupQuery = async () => null;
      aiReadFile = async file => file.name === 'сотрудники.xlsx'
        ? {name: file.name, content: 'штат', structured_data: {_source_company_unverified: true, staff: [{fio: 'Иванов Иван Иванович', position: 'Директор'}]}}
        : {name: file.name, content: 'реквизиты', structured_data: {company: {name: 'Альфа', unp: '123456789'}}};
      aiPostChatWithRetry = async () => ({success: true, text: '{"message":"Готово","data":{}}'});
      aiAttachedFiles = [{name: 'сотрудники.xlsx'}, {name: 'счет.docx'}];
      document.getElementById('ai-input').value = '';
    });
    page.on('dialog', dialog => dialog.type() === 'prompt' ? dialog.accept('Бета') : dialog.accept());
    await page.evaluate(() => aiSend());
    const card = await page.evaluate(() => aiCurrentData);
    if (card.company?.name !== 'Альфа' || card.staff?.[0]?.fio !== 'Иванов Иван Иванович') throw new Error('batch not transferred');
    await page.evaluate(() => {
      aiCurrentData = {certification: {standard: 'iso'}};
      aiAttachedFiles = [{name: 'сотрудники.xlsx'}];
    });
    await page.evaluate(() => aiSend());
    const named = await page.evaluate(() => aiCurrentData);
    if (named.company?.name !== 'Бета' || named.staff?.[0]?.fio !== 'Иванов Иван Иванович') throw new Error('manual company prompt failed');
    await page.evaluate(() => {
      aiCurrentData = {certification: {standard: 'iso'}};
      aiAttachedFiles = [{name: 'документ.docx'}];
      aiReadFile = async file => ({name: file.name, content: 'данные документа', structured_data: {_source_company_unverified: true}});
      window.modelCalledForDocument = false;
      aiPostChatWithRetry = async () => { window.modelCalledForDocument = true; return {success: true, text: JSON.stringify({message: 'Готово', data: {}})}; };
      document.getElementById('ai-input').value = '';
    });
    await page.evaluate(() => aiSend());
    if (!(await page.evaluate(() => window.modelCalledForDocument))) throw new Error('document with only company name skipped AI analysis');
    await page.evaluate(() => {
      aiReadFile = async file => file.name === 'сотрудники.xlsx'
        ? {name: file.name, content: 'штат', structured_data: {_source_company_unverified: true, staff: [{fio: 'Иванов Иван Иванович', position: 'Директор'}]}}
        : {name: file.name, content: 'реквизиты', structured_data: {company: {name: 'Альфа', unp: '123456789'}}};
    });
    await page.evaluate(() => {
      aiCurrentData = {company: {name: 'Чужая', unp: '987654321'}, certification: {standard: 'iso'}};
      aiAttachedFiles = [{name: 'счет.docx'}];
    });
    await page.evaluate(() => aiSend());
    const conflict = await page.evaluate(() => ({card: aiCurrentData, files: aiAttachedFiles.length}));
    if (conflict.card.company?.name !== 'Чужая' || conflict.files !== 1) throw new Error('conflicting company must be refused and file retained');
    await page.evaluate(() => {
      aiCurrentData = {company: {name: 'Альфа', unp: '123456789'}, certification: {standard: 'iso'}};
      aiAttachedFiles = [];
      document.getElementById('ai-input').value = 'Проверь карточку';
      window.modelCalled = false;
      aiPostChatWithRetry = async () => { window.modelCalled = true; return {success: true, text: JSON.stringify({message: 'Данные перенесены.', data: {company: {name: 'Бета', unp: '987654321'}, staff: [{fio: 'Петров Петр Петрович', position: 'Директор'}]}})}; };
    });
    await page.evaluate(() => aiSend());
    const modelConflict = await page.evaluate(() => ({card: aiCurrentData, called: window.modelCalled}));
    if (!modelConflict.called) throw new Error('mock model was not reached');
    if (modelConflict.card.company?.name !== 'Альфа' || modelConflict.card.staff?.length) throw new Error('conflicting AI data entered the card');
    await page.evaluate(() => {
      aiCurrentData = {company: {name: 'Альфа', unp: '123456789'}, certification: {standard: 'iso'}};
      aiAttachedFiles = [{name: 'счет.docx'}];
      document.getElementById('ai-input').value = 'Проверь документы';
      aiPostChatWithRetry = async () => ({success: true, text: JSON.stringify({message: 'Все сотрудники перенесены.', data: {staff: [{fio: 'Петров Петр Петрович', position: 'Директор'}]}})});
    });
    await page.evaluate(() => aiSend());
    if ((await page.evaluate(() => aiCurrentData.staff || [])).length) throw new Error('unverified AI-only staff entered the card');
    await page.evaluate(() => {
      aiCurrentData = {company: {name: 'Альфа', unp: '123456789'}, certification: {standard: 'iso'}, staff: [{fio: 'Иванов Иван Иванович', position: 'Бухгалтер'}]};
      aiAttachedFiles = [{name: 'счет.docx'}];
      document.getElementById('ai-input').value = 'Проверь документы';
      aiPostChatWithRetry = async () => ({success: true, text: JSON.stringify({message: 'Персонал обновлён.', data: {staff: [{fio: 'Иванов Иван Иванович', position: 'Директор'}]}})});
    });
    await page.evaluate(() => aiSend());
    if ((await page.evaluate(() => aiCurrentData.staff[0].position)) !== 'Бухгалтер') throw new Error('AI changed an existing staff position without evidence');
    await page.evaluate(() => {
      aiAttachedFiles = [{name: 'ошибка.docx'}];
      aiReadFile = async file => ({name: file.name, read_failed: true});
      document.getElementById('ai-input').value = 'Дополнение к файлу';
    });
    await page.evaluate(() => aiSend());
    const failedUpload = await page.evaluate(() => ({text: document.getElementById('ai-input').value, files: aiAttachedFiles.length}));
    if (failedUpload.text !== 'Дополнение к файлу' || failedUpload.files !== 1) throw new Error('failed file must preserve the draft and attachment');
    await page.evaluate(() => {
      aiCurrentData = {company: {name: 'Альфа', unp: '123456789'}, certification: {standard: 'iso'}};
      aiAttachedFiles = [{name: 'реквизиты.docx'}];
      aiReadFile = async file => ({name: file.name, content: 'реквизиты', structured_data: {company: {name: 'Альфа', unp: '123456789'}}});
      aiPostChatWithRetry = async () => { throw new Error('Провайдер недоступен'); };
      document.getElementById('ai-input').value = 'Обработай файл';
    });
    await page.evaluate(() => aiSend());
    const failedModel = await page.evaluate(() => ({text: document.getElementById('ai-input').value, files: aiAttachedFiles.map(file => file.name)}));
    if (failedModel.text !== 'Обработай файл' || failedModel.files[0] !== 'реквизиты.docx') throw new Error('failed AI request must preserve the draft and attachment');
    await page.evaluate(() => {
      aiCurrentData = {company: {name: 'Альфа', unp: '123456789'}, certification: {standard: 'iso'}};
      aiAttachedFiles = [{name: 'реквизиты.docx'}];
      aiPostChatWithRetry = async () => {
        aiAttachedFiles.push({name: 'добавлен_позже.docx'});
        return {success: true, text: JSON.stringify({message: 'Готово', data: {}})};
      };
      document.getElementById('ai-input').value = 'Проверь документы';
    });
    await page.evaluate(() => aiSend());
    const remaining = await page.evaluate(() => aiAttachedFiles.map(file => file.name));
    if (remaining.length !== 1 || remaining[0] !== 'добавлен_позже.docx') throw new Error('new attachment lost during processing');
    console.log('Batch upload browser scenario: PASS');
  } finally {
    await browser.close();
  }
})().catch(error => { console.error(error.message); process.exitCode = 1; });
