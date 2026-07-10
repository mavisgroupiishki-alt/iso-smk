#!/usr/bin/env python3
"""ИСО/СМК Генератор с ИИ-оформителем. Запуск: python server.py → http://localhost:8766"""
import sys,json,os,shutil,tempfile,base64,zipfile,re,requests as req_lib
import http.server,socketserver
from pathlib import Path
from datetime import datetime,timedelta

BASE_DIR = Path(__file__).parent.resolve()

# Импортируем умный генератор
try:
    from generator import generate_package, calculate_dates, LIBS
    SMART_GENERATOR = True
    print("✅ Умный генератор загружен")
except Exception as e:
    SMART_GENERATOR = False
    print(f"⚠️  Генератор не загружен: {e}")

TPL_DIR  = BASE_DIR/'templates'/'ISO_shablon'/'ИСО ЭнергоМагистраль'

# Render: без persistent disk — храним в /tmp или рядом с приложением
_DATA = Path('/data') if Path('/data').exists() else BASE_DIR/'_data'
JOURNAL_DIR = _DATA/'journal'
CO_DIR      = _DATA/'companies'
OUT_DIR     = _DATA/'output'
PORT = int(os.environ.get("PORT", 8766))

for d in [JOURNAL_DIR, CO_DIR, OUT_DIR]: d.mkdir(parents=True, exist_ok=True)

# Хранилище фоновых задач генерации (на диске - переживает перезапуск)
import threading
# Render Free = 512 МБ RAM. Генерация пакета (параллельные запросы к BitrixGPT + сборка ZIP)
# сама по себе близка к лимиту. Если запустить вторую генерацию одновременно с первой —
# гарантированный OOM. Не даём двум генерациям идти параллельно.
GENERATION_LOCK = threading.Lock()
GENERATION_IN_PROGRESS = {'active': False}
# Не более 2 одновременных vision-запросов — на Render Free (512 МБ) 4+ параллельных
# тяжёлых запроса к медленной модели гарантированно роняют инстанс.
VISION_SEMAPHORE = threading.Semaphore(2)
TASKS = {}  # task_id -> {status, progress, result, error}

def _prune_tasks(keep=2):
    """
    Каждая завершённая задача несёт готовый ZIP в base64 (десятки МБ).
    Без очистки TASKS растёт неограниченно за сессию сервера и рано или поздно
    выедает всю память Render (512 МБ на Free) — сервер падает в OOM и Render
    его перезапускает без traceback (просто 'Instance restarted').
    Держим в памяти только последние `keep` ЗАВЕРШЁННЫХ задач — активные ('running') не трогаем.
    """
    try:
        finished_ids = [tid for tid, t in TASKS.items() if t.get('status') in ('done', 'error')]
        if len(finished_ids) > keep:
            for old_id in finished_ids[:-keep]:
                TASKS.pop(old_id, None)
    except Exception:
        pass
TASKS_DIR = BASE_DIR / 'tasks'
TASKS_DIR.mkdir(exist_ok=True)

def save_task(task_id, data):
    try:
        # zipB64 может быть очень большим (десятки МБ) — не пишем его в файл задачи на диск,
        # он нужен только в памяти TASKS для одноразовой отдачи фронту
        to_save = {k: v for k, v in data.items() if k != 'zipB64'}
        (TASKS_DIR / f'{task_id}.json').write_text(
            json.dumps(to_save, ensure_ascii=False), 'utf-8')
    except: pass

def load_task(task_id):
    try:
        f = TASKS_DIR / f'{task_id}.json'
        if f.exists():
            return json.loads(f.read_text('utf-8'))
    except: pass
    return None

# ── Vibe Code AI ─────────────────────────────────────────────
VIBE_URL   = "https://vibecode.bitrix24.tech/v1/ai/chat/completions"
VIBE_MODEL = "bitrix/bitrixgpt-5.5"
VIBE_MODEL_VISION = "bitrix/bitrixgpt-5.5-thinking"  # vision + reasoning для анализа документов

AI_SYSTEM = """Ты — ИИгорь, оформитель документов ИСО/СУОТ/СПК (Mavis Group, Беларусь).

ПРОДУКТЫ: ISO 9001, ISO 45001/СУОТ, ISO 9001+45001, СПК Строй Комплекс, СПК БИСП, Периодика.

ДАТЫ от даты выезда эксперта:
- Политика = выезд минус 34 дня
- Цели/Приказы = политика + 5 дней
- Реестр рисков = дата политики
- Отчёты = выезд минус 7 дней

РЕКВИЗИТЫ: обязательно заполняй company.city (г. Могилев, г. Минск, г. Брест и т.д.) — используется в шапках приказов.
Название компании пиши БЕЗ формы: name="ОмиТрейд" (не "ООО ОмиТрейд"), form="ООО" отдельно.
В документах используй «ёлочки»: ООО «ОмиТрейд», не ООО "ОмиТрейд".

ПЕРИОДИКА (если в сообщении есть блок "НАЙДЕНЫ РАНЕЕ СОХРАНЁННЫЕ ДАННЫЕ КОМПАНИИ"):
Это означает что у компании уже была сделана генерация (ИСО/СУОТ/СПК), и теперь нужно её актуализировать.
1. Используй найденные данные как стартовую базу — не переспрашивай реквизиты компании (название, УНП, адрес, директор), если они уже есть.
2. ОБЯЗАТЕЛЬНО уточни у пользователя свежие данные за прошедший период (это всегда меняется при периодике):
   - Новую дату выезда эксперта (от неё пересчитываются все остальные даты)
   - Актуальное штатное расписание — кто уволился, кто принят новый (старый штат мог устареть)
   - Список 2-3 объектов за текущий/прошлый год (старые объекты могли завершиться)
   - Актуальность поставщиков
3. Если пользователь прислал только корректировки (например "Иванов уволился, теперь работает Петров") —
   примени их поверх старых данных, не теряя остальной штат.
4. Не меняй то что не менялось: реквизиты компании, форму, область деятельности — если пользователь явно не сказал об изменении.
5. Предупреди пользователя если видишь риск "мёртвых душ" — сотрудник в старых данных мог уже не работать, уточни.

ШТАТ: ИТР идут в ИСО, рабочие идут в СУОТ (инструкции ОТ + карты рисков под каждую профессию).
ВАЖНО: если в штате есть электрогазосварщик или сварщик — ставь company.has_welding:true (нужен график валидации).

РАСПОЗНАВАНИЕ РАБОЧИХ (is_worker:true):
Рабочие — это НЕ ИТР. Признаки: профессия (не должность), работают руками на объекте.
ВСЕГДА is_worker:true для: Штукатур, Маляр, Сварщик, Электрогазосварщик, Облицовщик, Плиточник,
Кровельщик, Монтажник, Электромонтажник, Слесарь, Плотник, Каменщик, Бетонщик, Арматурщик,
Подсобный рабочий, Разнорабочий, Водитель, Машинист, Крановщик, Стропальщик, Токарь, Сантехник.
ВСЕГДА is_worker:false для: Директор, Зам.директора, Главный инженер, Прораб, Мастер, Бухгалтер,
Инженер, Техник, Экономист, Кадровик, Юрист, Заведующий, Производитель работ.

Из ZIP/DOCX файлов: ищи разделы «Рабочие», «Рабочий состав», инструкции ОТ по профессиям,
перечни персонала, штатное расписание — там всегда указаны профессии рабочих.

После распознавания ОБЯЗАТЕЛЬНО заполни поле workers списком профессий:
"workers": ["Штукатур","Маляр","Электрогазосварщик"]
Для ИСО: аудиторы = 3 чел. с удостоверением ОТ из ИТР.
Для СУОТ: минимум 3 чел. с удостоверением ОТ.

СПК (Свидетельство о технической компетентности):
- Виды работ: берём из КП если прикреплено, иначе спрашиваем у клиента
- Клиент называет своими словами ("штукатурка, плитка") — ты переводишь в официальные формулировки
- Орган: spk_stroy = Стройкомплекс (12 докум.), spk_bisp = БИСП (+8 докум.)
- Минимум 2 ИТР-строителя по основному месту (директор, прораб, ГИ — не бухгалтер)
- Гарантийное письмо 9.3 (лаборатория): общая формулировка без реквизитов лаборатории
- Тех.требования: отдельный файл на каждый вид работ
- has_welding: если есть сварщик — true
- При генерации СПК обязательно спроси: виды работ (если нет КП), орган (Стройкомплекс или БИСП)

ТИПОВЫЕ ВИДЫ РАБОТ СПК (переводи слова клиента в официальные формулировки):
земляные/котлован/траншея → "Земляные работы"
фундамент/основание → "Устройство оснований и фундаментов зданий и сооружений"
кирпич/кладка/каменные → "Возведение каменных и армокаменных конструкций"
бетон/монолит/заливка → "Возведение монолитных бетонных и железобетонных конструкций"
сборный железобетон/плиты → "Монтаж сборных бетонных и железобетонных конструкций"
металлоконструкции/стальные → "Монтаж стальных конструкций"
деревянные конструкции/дерево → "Монтаж деревянных конструкций"
лёгкие конструкции/сэндвич/профнастил → "Монтаж лёгких ограждающих конструкций"
антикоррозия/окраска металла → "Устройство антикоррозионных покрытий"
изоляция/гидроизоляция → "Устройство изоляционных покрытий"
утепление/теплоизоляция/фасад → "Устройство тепловой изоляции наружных ограждающих конструкций"
кровля/крыша/кровельные → "Устройство кровель"
штукатурка/шпатлёвка/малярка/покраска/обои/стекло → "Штукатурные, малярные, обойные и стекольные работы"
окна/двери/проёмы → "Заполнение оконных и дверных проёмов"
полы/стяжка/напольное покрытие → "Устройство полов"
благоустройство/озеленение/асфальт → "Благоустройство территорий"
сантехника/водопровод/канализация/трубы → "Монтаж систем внутреннего и наружного водоснабжения и канализации"
отопление/котёл/тепловой пункт → "Монтаж систем отопления, вентиляции и кондиционирования"
электрика/электромонтаж/кабель/подстанция → "Электромонтажные работы"
автоматика/КИПиА → "Монтаж систем автоматизации"
слаботочные/связь/видеонаблюдение → "Монтаж систем связи и диспетчеризации"
дороги/дорожные работы → "Устройство дорог и улиц"
геодезия → "Геодезические работы"
сварка → "Сварочные работы"

СТРАТЕГИЯ: рекомендуй клиенту брать максимум видов работ сразу — расширение после выдачи СПК стоит дороже (базовая пошлина + 50%)"

ВАЖНОЕ ПРАВИЛО — ОШИБКИ ЧТЕНИЯ ФАЙЛОВ:
- Если в содержимом файла видишь "[Ошибка...]", "[не удалось начать обработку]" или файл пришёл пустым — НЕ ПРИДУМЫВАЙ данные и не переключайся на другой продукт (например СПК) молча. Прямо скажи: "Файл не прочитался, попробуйте загрузить ещё раз" — и жди повторной попытки.
- Никогда не игнорируй явные подсказки в САМОМ ИМЕНИ файла — если имя содержит "аттестац" (без указания органа СПК) → это АТТ спеца (продукт "att"), не СПК. Если имя содержит "спк" → это СПК. Если "исо"/"суот" → соответствующий продукт. Название файла — сильный сигнал какой продукт нужен, используй его даже если содержимое ещё не прочиталось.
- Не продолжай молча логику из более ранних сообщений в этом же чате (например "делаем СПК по общестрою"), если новый файл явно про другой продукт — переспроси, если есть противоречие, а не выбирай сам.

ВАЖНОЕ ПРАВИЛО — ТЕКСТ И ДАННЫЕ ДОЛЖНЫ СОВПАДАТЬ:
- Кнопка "Сформировать пакет" у оформителя берёт продукт СТРОГО из поля certification.standard в твоём JSON — не из того что ты написал текстом в чате. Если ты говоришь "делаю аттестацию" или "делаю СПК" — это ОБЯЗАТЕЛЬНО должно сопровождаться обновлением certification.standard в JSON в этом же ответе (att / company_att / iso / suot / iso_suot / spk_stroy / spk_bisp). Никогда не меняй словами то, что не поменял в JSON — иначе кнопка сгенерирует не то, что ты только что пообещал.
- Если клиент явно поправляет тебя ("не СПК, а аттестация" / "не генподряд, а просто атт") — ПЕРВЫМ ДЕЛОМ обнови certification.standard на правильное значение в этом же ответе, и только потом пиши подтверждающий текст.
- Слово "атт"/"АТТ" без уточнения = продукт "att" (аттестация ОДНОГО специалиста). Если явно сказано "компания"/"юрлицо"/"генподряд"/"на организацию" = продукт "company_att" (аттестация компании). "СПК"/"стройкомплекс"/"БИСП" = spk_stroy/spk_bisp. Никогда не путай эти слова даже если они встречаются в одном сообщении.

ВАЖНОЕ ПРАВИЛО — АТТЕСТАЦИЯ СПЕЦИАЛИСТА vs АТТЕСТАЦИЯ КОМПАНИИ (ЭТО ДВЕ РАЗНЫЕ УСЛУГИ, НЕ ПУТАТЬ):
- "Аттестация специалиста" (продукт "att") — документ на ОДНОГО человека (заявление в Белстройцентр).
- "Аттестация компании" (продукт "company_att") — документ на ЮРЛИЦО целиком (аттестат соответствия — статья 35 Кодекса, НЕ статья 40). Бывает без генподряда (только пункт 7 — виды СМР, суффикс "СТ", любые классы сложности 1-4, категорий нет) и с генподрядом (пункт 6 + пункт 7, суффикс "ГС", категории 1-4 с порогами по штату).
- Оба продукта технически реализованы. НЕ путай их и НЕ подменяй один другим молча.
- Если клиент прислал архив с несколькими людьми (диплом+трудовая) и файлом "Перечень копий" — почти всегда это данные для company_att (Форма №2/3/4/5 берут именно такие данные по каждому ИТР), но перед этим убедись что у людей уже есть или готовятся индивидуальные аттестаты (продукт "att") — Форма №5 ссылается на них.
- ВСЕГДА используй данные о стаже/образовании, если они уже написаны в присланном файле — не спрашивай то, что уже есть.

АТТЕСТАЦИЯ КОМПАНИИ (продукт "company_att"):
- Документы: 1.Заявление (обязательно с ФИО директора И главного бухгалтера — заполни поле glavbukh_fio), 2.Опись, 3.ИТР (Форма №2, самая подробная таблица), 4.Сведения о рабочих (профессии рабочих под виды работ, отдельно от ИТР!), 5.Трудовые (Форма №3), 6.Дипломы (Форма №4), 7.Аттестаты (Форма №5, сводка уже полученных индивидуальных аттестатов), 8.Опыт (только для генподряда категорий 1-3, для категории 4 — не нужна и не генерируется).
- В отличие от аттестации специалиста (там опись не наша зона) — для КОМПАНИИ опись и сведения о рабочих делаем МЫ.
- Виды работ — пункт 7 классификатора (Постановление №26 от 15.04.2024), коды вида "7.1", "7.4.2", "7.6" и т.д. — подставляй в work_items массив кодов максимально точно по официальным формулировкам, не своими словами.
- Генподряд (пункт 6) НИКОГДА не бывает без пункта 7 — если клиент просит генподряд, обязательно спроси и заяви виды работ тоже.
- Простой подряд (только пункт 7, без генподряда) — вполне самостоятельный вариант, category остаётся null/не указан.
- Категорию (1-4) называет клиент, но ты ОБЯЗАН проверить реалистичность по штату: 4я=от 10 чел, 3я=от 50, 2я=от 150, 1я=от 600 (по основному месту работы), плюс обязательный аттестованный сметчик для любой категории генподряда, плюс для категорий 1-3 — 2 подтверждённых объекта (введены в эксплуатацию не позднее 5 лет назад, генподрядчик привлекал субподряд, текущий ремонт не считается) и стаж владения предыдущей категорией (3я нужен 1 год на 4й, 2я — 2 года на 3й, 1я — 5 лет на 2й). Если что-то не сходится — прямо предупреди клиента и предложи реалистичный вариант (например начать с 4й категории), не формируй пакет с заведомо провальной категорией молча.
- РАСЧЁТ СТАЖА ПО РОЛЯМ (для Формы ИТР) — стаж считается не общий, а по совокупности конкретных должностей:
  прораб + мастер + начальник участка + зам.директора по строительству + зам.директора-главный инженер + главный инженер — сумма стажа по этим должностям.
  Директор: если он НЕ закрывает своим аттестатом ни одно направление работ — стаж не важен, ставь любой. Если закрывает — считай стаж как директор в строительной фирме (можно проверить по факту что фирма была строительной, если есть сомнения — предложи справку-расшифровку).
  Руководитель в области строительства (генподряд): минимум 5 лет на руководящих должностях — тот же список должностей что выше.
  Зам.директора-ГИ: тот же список должностей, аналогично директору по логике "закрывает направление или нет".
  Инженер-проектировщик: считай стаж отдельно, только в проектировании (не в СМР).
  Инженер-сметчик: минимум 2 года в сметном деле, ИЛИ если у человека уже есть аттестат — этого достаточно, дополнительно стаж можно не проверять строго.
  Если стаж на грани/непонятен по документам — предложи подготовить справку-расшифровку (как и для аттестации специалиста).
- "Заявление на отмену" — нужно в 3 случаях: (1) переоформление при расширении видов работ на единый новый аттестат, (2) исключение части видов работ (например ушёл прораб по направлению) вместо полного аннулирования, (3) полное прекращение по инициативе владельца. Спроси причину если неясно, номер старого аттестата (с суффиксом СТ/ГС) обязателен. Также напомни: если у компании уже был аттестат — сначала стоит проверить реестр (https://att.bsc.by/reestr) по УНП, нужно ли заявление на отмену перед новой подачей.
- "Оказание инженерных услуг" (пункт 4 — функции заказчика/технадзор) — отдельный, тоже актуальный запрос, но пока НЕ реализован технически как company_att (нет готового генератора под него) — если просят именно это, скажи что пока не умеешь, не подменяй пунктом 6/7.

АТТЕСТАЦИЯ СПЕЦИАЛИСТА (продукт "att", кнопка «АТТ спеца»):
- Готовим ТОЛЬКО заявление (+ справку-расшифровку если нужна). Доверенность, опись, сопроводительное письмо — не наша зона, их делает клиент/эксперт вручную.
- На КАЖДУЮ специализацию — отдельное заявление. Если человек подаётся на 3 направления — 3 отдельных заявления с разным полем "специализация аттестации".
- Данные специалиста (паспорт, ИН, адрес прописки, диплом) читай из фото/скана который прислал клиент — через vision, как с удостоверениями ОТ. Не выдумывай цифры.
- Специализацию бери из официального классификатора (Постановление №70 от 14.06.2024), а не своими словами. Если клиент говорит "делаем на сантехника" — сопоставь с ближайшей формулировкой из классификатора (это сделает бэкенд автоматически по ключевым словам, но ты должен передать текст специализации максимально близко к официальной формулировке).
- Грейд (Мастер / Производитель работ (прораб) / Главный инженер) определяется по стажу ИМЕННО в этой специализации (не общему стажу) и уровню образования:
  Мастер: высшее — от 3 мес., ссуз — от 6 мес.
  Прораб: высшее — от 1 года, ссуз — от 3 лет.
  Гл.инженер: высшее — от 5 лет, ссуз — от 8 лет.
  Если грейд явно не назван клиентом — передай stage_months и education_level, бэкенд посчитает грейд сам.
- Если пользователь просит несколько специализаций на одного человека — уточни стаж по каждой отдельно, стаж по одному направлению не действует на другое.
- Если должность в трудовой не совпадает явно со специализацией (например "директор" аттестуется как "прораб") — нужна справка-расшифровка (need_spravka:true) с перечнем объектов/видов работ, подтверждающих опыт именно по этой специализации — спроси у клиента какие объекты и виды работ он выполнял.
- Обязательно уточни: если это данные о должности "производитель работ" — это ИТР, НЕ путай с "прораб" как грейд аттестации (разные вещи).

НАЗНАЧЕНИЕ РОЛЕЙ:
- За СМК = директор
- За процесс = главный инженер / прораб / директор
- Аудиторы (3 чел.) = директор + ИТР с ОТ
- За ДИ = директор или кадровик или бухгалтер
- За ФНПА = главный инженер или зам директора
- 2 удостоверения ОТ у одного = берём более свежее

ФЛАГИ:
- Строительство в области + нет аттестата = критическая ошибка
- Генподряд = нужен аттестат в течение месяца
- Меньше 3 чел. с ОТ для СУОТ = предупреждение

Принимай правки: "исправь директора", "добавь объект", "поменяй дату" — обновляй данные.

ОТВЕЧАЙ СТРОГО JSON без оберток json:
{
  "message": "ответ оформителю (по-русски)",
  "questions": ["вопрос если не хватает данных"],
  "data": {
    "company": {"name":"","form":"","unp":"","address":"","city":"","director_fio":"","director_position":"","glavbukh_fio":"","scope":"","has_welding": false, "machinery": ["Автомобиль"], "bisp_org": "РУП «СтройМедиаПроект»", "phone":"", "email":"", "bank_details":""},
    "certification": {"standard":"iso|suot|iso_suot|spk_stroy|spk_bisp|att|company_att","scope":"","body":"","audit_date":""},
    "dates": {"audit_date":"","development_date":"","implementation_date":""},
    "staff": [{"fio":"","position":"","role":"director|auditor|responsible|itr","is_worker":false,"ot_certificate":false,"ot_certificate_date":"","hire_date":""}],
    "workers": ["Штукатур","Маляр","Электрогазосварщик"],
    "objects": [{"name":"","year":"","customer":""}],
    "suppliers": [{"name":"","type":""}],
    "flags": [{"type":"error|warning|ok","text":""}],
    "readiness": "waiting|partial|review|ready",
    "work_types": ["Производство штукатурных работ","Производство малярных работ"],
    "attestation": {"persons": [{
        "fio":"Иванов Иван Иванович","fio_dative":"Иванову Ивану Ивановичу",
        "education_level":"высшее|среднее специальное",
        "diploma_speciality":"","diploma_qualification":"","diploma_number":"",
        "passport_series":"","passport_number":"","id_number":"","passport_issuer":"",
        "address":"","trudovaya_number":"",
        "requests": [{"specialization":"монтаж наружных сетей водоснабжения и канализации...","grade":"","stage_months":0,"need_spravka":false,"work_experience_text":""}]
    }]},
    "company_attestation": {
        "category": "1|2|3|4|null (null = только подряд, без генподряда)",
        "work_items": ["7.4.1","7.6"],
        "work_scope_text": "свободное описание видов работ если коды ещё не подобраны",
        "staff_total": 0,
        "has_smetchik": false,
        "prior_category_years": 0,
        "experience_objects": [{"name":"","complexity_class":"К-4"}],
        "workers": [{"profession":"","count":0,"razryad":""}],
        "is_cancellation": false,
        "old_attestat_number": "",
        "cancellation_reason": "",
        "itr": [{
            "fio":"","position":"","education_level":"высшее|среднее специальное",
            "diploma_number":"","diploma_date":"","diploma_institution":"","diploma_speciality":"","diploma_qualification":"",
            "stage_years":0,"stage_years_here":0,
            "trudovaya_number":"","order_number":"","hire_date":"",
            "attestat_number":"","attestat_date_from":"","attestat_date_to":"","attestat_specialization":""
        }]
    }
  }
}
Включай только заполненные поля."""




def call_ai(messages, api_key):
    """Вызов BitrixGPT через Vibe Code API — с повторными попытками при таймауте.
    Чат с Игорем может обрабатывать сразу несколько файлов/специалистов за раз (например,
    аттестация 3 человек одним сообщением) — 60 сек не всегда хватает, особенно если
    системный промпт большой (ИСО+СУОТ+СПК+АТТ правила сразу)."""
    import time
    last_err = None
    for attempt in range(3):
        try:
            resp = req_lib.post(
                VIBE_URL,
                headers={"Content-Type":"application/json","X-Api-Key":api_key},
                json={"model":VIBE_MODEL,"max_tokens":3000,"messages":[
                    {"role":"system","content":AI_SYSTEM},
                    *messages[-10:]
                ]},
                timeout=150
            )
            resp.raise_for_status()
            data = resp.json()
            if "error" in data:
                raise RuntimeError(data["error"])
            text = "".join(c.get("message",{}).get("content","") for c in data.get("choices",[]))
            if text:
                return text
            last_err = "Пустой ответ от модели"
            time.sleep(2)
        except req_lib.exceptions.Timeout:
            last_err = "Timeout"
            print(f"  ⚠️  Таймаут чата (попытка {attempt+1}/3), повтор...")
            time.sleep(3 * (attempt + 1))
        except req_lib.exceptions.RequestException as e:
            last_err = str(e)
            print(f"  ⚠️  Ошибка запроса: {e}, повтор...")
            time.sleep(3 * (attempt + 1))
    raise RuntimeError(f"BitrixGPT не ответил после 3 попыток: {last_err}. "
                        f"Попробуйте прислать меньше файлов за раз (например по одному специалисту).")


# ── Дата-утилиты (без изменений) ─────────────────────────────
def date_dot(s):
    s=str(s).strip()
    if '-' in s:
        p=s.split('-'); return f"{int(p[2]):02d}.{int(p[1]):02d}.{p[0]}"
    return s

def parse_date(s):
    s=date_dot(s); p=s.split('.'); return int(p[0]),int(p[1]),int(p[2])

def date_minus(s,days):
    d,m,y=parse_date(s)
    dt=datetime(y,m,d)-timedelta(days=days)
    return dt.strftime('%d.%m.%Y')

def year_of(s):
    _,_,y=parse_date(s); return str(y)


# ── XML-патч для разорванных дат/имён (без изменений) ────────
def merge_date_runs(xml):
    import re as _re
    pattern = (
        r'(<w:t[^>]*>)(\d{2})(</w:t></w:r>)'
        r'(?:<w:bookmarkStart[^/]*/>\s*<w:bookmarkEnd[^/]*/>\s*)?'
        r'(?:<w:proofErr[^/]*/>\s*)*'
        r'(<w:r[^>]*>(?:<w:rPr>.*?</w:rPr>)?<w:t[^>]*>)'
        r'(\.\d{2}\.\d{4})'
    )
    return _re.sub(pattern,
        lambda m: m.group(1)+m.group(2)+m.group(5)+m.group(3)+m.group(4),
        xml, flags=_re.DOTALL)

def merge_name_runs(xml):
    import re as _re
    p1 = (
        r'(<w:t[^>]*>)([А-ЯA-Z])(</w:t></w:r>)'
        r'(?:<w:bookmarkStart[^/]*/>\s*<w:bookmarkEnd[^/]*/>\s*)?'
        r'(<w:r[^>]*>(?:<w:rPr>.*?</w:rPr>)?<w:t[^>]*>)'
        r'([а-яё]{3,})'
    )
    xml = _re.sub(p1, lambda m: m.group(1)+m.group(2)+m.group(5)+m.group(3)+m.group(4),
        xml, flags=_re.DOTALL)
    p2 = (
        r'(>[А-Я][а-яё\-]{2,})(</w:t></w:r>)'
        r'(?:<w:proofErr[^/]*/>\s*)+'
        r'(<w:r[^>]*>(?:<w:rPr>.*?</w:rPr>)?<w:t[^>]*>)'
        r'( [А-ЯA-Z]\.[А-ЯA-Z]\.)'
    )
    xml = _re.sub(p2, lambda m: m.group(1)+m.group(4)+m.group(2)+m.group(3),
        xml, flags=_re.DOTALL)
    p3 = (
        r'(>[А-Яа-яё\-]{4,})(</w:t></w:r>)'
        r'(?:<w:bookmarkStart[^/]*/>\s*<w:bookmarkEnd[^/]*/>\s*)?'
        r'(<w:r[^>]*>(?:<w:rPr>.*?</w:rPr>)?<w:t[^>]*>)'
        r'([а-яё]{1,3}</w:t>)'
    )
    def _join_p3(m):
        suffix = m.group(4).replace('</w:t>','')
        return m.group(1)+suffix+m.group(2)+m.group(3)+'</w:t>'
    xml = _re.sub(p3, _join_p3, xml, flags=_re.DOTALL)
    return xml


# ── Генерация документов (без изменений) ─────────────────────
def clean_after_replace(xml, org_name):
    """
    После замены убираем буквы которые 'приклеились' из соседних runs.
    Primer: OmiTreidr gde r -- nachalo sleduyushego slova.
    Работает для: названия компании, инициалов директора.
    """
    if not org_name:
        return xml
    import re as _re

    # 1. Чистим название компании: убираем строчные буквы которые приклеились
    escaped = _re.escape(org_name)
    # Название компании не может заканчиваться строчными русскими/латинскими буквами
    # если оно стоит перед » или пробелом
    pattern = r'(<w:t[^>]*>)(' + escaped + r')([а-яёa-z]{1,15})(</w:t>)'
    def fix_org(m):
        suffix = m.group(3)
        # Оставляем суффикс только если это падежное окончание самого названия
        # (т.е. название используется без кавычек в падеже)
        # Признак проблемы: предыдущий run заканчивается на «
        return m.group(1) + m.group(2) + m.group(4)
    xml = _re.sub(pattern, fix_org, xml)

    # 2. Фикс «ООО «НАЗВАНИЕ»» — между кавычками должно быть только название
    # Паттерн: после « идёт <w:t>НАЗВАНИЕ+мусор</w:t> перед »
    xml = _re.sub(
        r'(«</w:t></w:r>[^»]{0,500}?<w:t[^>]*>)(' + escaped + r')([а-яёa-z]{1,15})(</w:t>)',
        lambda m: m.group(1) + m.group(2) + m.group(4),
        xml, flags=_re.DOTALL
    )

    return xml


def replace_in_docx(src, dst, reps):
    # Извлекаем название компании из замен для пост-обработки
    # Шаблонные компании-образцы — всегда ищем их для замены
    TEMPLATE_ORGS = ['ЭнергоМагистраль', 'Варта', 'Сфера Секьюрити', 'Кастом-Инвест']
    org_name = ''
    for o, n in reps:
        if o in TEMPLATE_ORGS and n:
            org_name = n
            break

    with tempfile.TemporaryDirectory() as td:
        tmp=os.path.join(td,'s.docx'); shutil.copy2(str(src),tmp)
        up=os.path.join(td,'up'); os.makedirs(up)
        with zipfile.ZipFile(tmp,'r') as z: z.extractall(up)
        for root,dirs,files in os.walk(up):
            for fn in files:
                if not fn.endswith('.xml'): continue
                fp=os.path.join(root,fn)
                try:
                    with open(fp,'r',encoding='utf-8') as f: c=f.read()
                    c=merge_date_runs(c); c=merge_name_runs(c)
                    ch=False
                    for o,n in reps:
                        if o and n is not None and o in c: c=c.replace(o,n); ch=True
                    if ch:
                        # Пост-обработка: убираем приклеившиеся буквы
                        if org_name:
                            c = clean_after_replace(c, org_name)
                        with open(fp,'w',encoding='utf-8') as f: f.write(c)
                except: pass
        Path(dst).parent.mkdir(parents=True,exist_ok=True)
        with zipfile.ZipFile(str(dst),'w',zipfile.ZIP_DEFLATED) as zo:
            for root,dirs,files in os.walk(up):
                for fn in files:
                    fp=os.path.join(root,fn)
                    zo.write(fp,os.path.relpath(fp,up))


def build_reps(data):
    """Строит список замен из данных карточки (старая логика + поддержка ИИ-данных)"""
    org  = data.get('orgName','')
    form = data.get('orgForm','ООО')
    city = data.get('city','Минск')
    scope= data.get('scope','производства строительно-монтажных работ')

    # Поддержка данных от ИИ (вложенная структура)
    ai = data.get('ai_data', {})
    if ai.get('company',{}).get('name'): org  = ai['company']['name']
    if ai.get('company',{}).get('form'): form = ai['company']['form']
    if ai.get('company',{}).get('city'): city = ai['company']['city']
    if ai.get('certification',{}).get('scope'): scope = ai['certification']['scope']

    ds=data.get('dirSurname',''); di=data.get('dirInitials','')
    # ИИ может дать полное ФИО директора
    if ai.get('company',{}).get('director_fio'):
        parts = ai['company']['director_fio'].split()
        if len(parts)>=2:
            ds = parts[0]
            di = '.'.join(p[0] for p in parts[1:] if p) + '.' if len(parts)>1 else di

    a1p=data.get('aud1Post','директор'); a1s=data.get('aud1Surname',ds); a1i=data.get('aud1Initials',di)
    a2p=data.get('aud2Post',''); a2s=data.get('aud2Surname',''); a2i=data.get('aud2Initials','')
    a3p=data.get('aud3Post',''); a3s=data.get('aud3Surname',''); a3i=data.get('aud3Initials','')

    # Если ИИ нашёл сотрудников — берём аудиторов оттуда
    staff = ai.get('staff',[])
    auditors = [s for s in staff if s.get('role')=='auditor']
    if len(auditors)>=1:
        p=auditors[0].get('fio','').split()
        a2p=auditors[0].get('position',''); a2s=p[0] if p else ''; a2i='.'.join(x[0] for x in p[1:] if x)+'.' if len(p)>1 else ''
    if len(auditors)>=2:
        p=auditors[1].get('fio','').split()
        a3p=auditors[1].get('position',''); a3s=p[0] if p else ''; a3i='.'.join(x[0] for x in p[1:] if x)+'.' if len(p)>1 else ''

    sp=data.get('secPost',a2p); ss=data.get('secSurname',a2s); si=data.get('secInitials',a2i)
    impl=date_dot(data.get('implDate',''))
    if ai.get('dates',{}).get('implementation_date'): impl=ai['dates']['implementation_date']
    start=date_dot(data.get('startDate',impl)); end=date_dot(data.get('endDate',impl))
    ord1=date_minus(impl,4) if impl else ''; yr=year_of(impl) if impl else '2026'

    def cap(s): return s[0].upper()+s[1:] if s else s
    def gen(s):
        if s.endswith(('ов','ев','ин','ын')): return s+'а'
        return s+'а'

    r=[
        ('ЭнергоМагистраль',org), ('«ЭнергоМагистраль»',f'«{org}»'),
        (f'ООО «ЭнергоМагистраль»',f'{form} «{org}»'),
        ('А.А. Шакуро',f'{di} {ds}'), ('Шакуро А.А.',f'{ds} {di}'),
        ('директора А.А. Шакуро',f'директора {di} {ds}'),
        ('директора Шакуро А.А.',f'директора {ds} {di}'),
        ('Директора А.А. Шакуро',f'Директора {di} {ds}'),
        ('Директора Шакуро А.А.',f'Директора {ds} {di}'),
        ('Директору А.А. Шакуро',f'Директору {di} {ds}'),
        ('В.В. Семенчуков',f'{a2i} {a2s}'),
        ('Семенчуков В.В.',f'{a2s} {a2i}'),
        ('Семенчукова В.В.',f'{gen(a2s)} {a2i}'),
        ('главного инженера Семенчукова В.В.',f'{a2p} {gen(a2s)} {a2i}'),
        ('Главного инженера Семенчукова В.В.',f'{cap(a2p)} {gen(a2s)} {a2i}'),
        ('Главного инженера В.В. Семенчукова',f'{cap(a2p)} {a2i} {gen(a2s)}'),
        ('Главный инженер В.В. Семенчуков',f'{cap(a2p)} {a2i} {a2s}'),
        ('С.Д. Нестерёнок',f'{a3i} {a3s}' if a3s else None),
        ('Нестерёнок С.Д.',f'{a3s} {a3i}' if a3s else None),
        ('производителя работ Нестерёнок С.Д.',f'{a3p} {a3s} {a3i}' if a3s else None),
        ('Производителю работ С.Д. Нестерёнку', f'{cap(a3p)}у {a3i} {a3s}' if a3s else None),
        ('производителю работ С.Д. Нестерёнку', f'{a3p}у {a3i} {a3s}' if a3s else None),
        ('производителя работ С.Д. Нестерёнок',f'{a3p} {a3i} {a3s}' if a3s else None),
        ('Производитель работ С.Д. Нестерёнок',f'{cap(a3p)} {a3i} {a3s}' if a3s else None),
        ('Председатель КС: Директор А.А. Шакуро',f'Председатель КС: {cap(a1p)} {a1i} {a1s}'),
        ('Члены КС: Главный инженер В.В. Семенчуков',f'Члены КС: {cap(a2p)} {a2i} {a2s}'),
        ('        Производитель работ С.Д. Нестерёнок',
            f'        {cap(a3p)} {a3i} {a3s}' if a3s else f'        {cap(a3p)}'),
        ('директора Шакуро А.А., главного инженера Семенчукова В.В., производителя работ С.Д. Нестерёнок',
            f'{a1p} {a1s} {a1i}, {a2p} {a2s} {a2i}'+(f', {a3p} {a3s} {a3i}' if a3s else '')),
        ('Назначить Главного инженера Семенчукова В.В.  ответственным за управление фондом ТТК, ТНПА, НПА.',
            f'Назначить {cap(sp)} {ss} {si} ответственным за управление фондом ТТК, ТНПА, НПА.'),
        ('выполнения функций генерального подрядчика, производства строительно-монтажных работ',scope),
        ('выполнение функций генерального подрядчика, производство строительно-монтажных работ',scope),
        ('16.02.2026 г. № 1',f'{ord1} г. № 1'),('09.02.2026 г. № 1',f'{ord1} г. № 1'),
        ('04.02.2026 г. № 1',f'{ord1} г. № 1'),
        ('16.02.2026',impl),('09.02.2026',ord1),('04.02.2026',ord1),
        ('09.03.2026',end),('02.03.2026',end),('12.01.2026',start),
        ('2026г.',f'{yr}г.'),('2026 г.',f'{yr} г.'),
        ('на 2026 год',f'на {yr} год'),('на 2026 г',f'на {yr} г'),
        (' 2026год',f' {yr}год'),('2026год',f'{yr}год'),
        ('г. Минск',f'г. {city}'),
        ('Шакуро',ds),
        ('Нестерёнок',a3s if a3s else None),
        ('Семенчуков',a2s if a2s else None),
    ]
    return [(o,n) for o,n in r if o and n is not None]


def replace_itr_table(src, dst, itr_list, impl_date):
    import shutil as _sh, tempfile as _tmp, os as _os
    with _tmp.TemporaryDirectory() as td:
        tmp=_os.path.join(td,'s.docx'); _sh.copy2(str(src),tmp)
        up=_os.path.join(td,'up'); _os.makedirs(up)
        with zipfile.ZipFile(tmp,'r') as z: z.extractall(up)
        fp=_os.path.join(up,'word','document.xml')
        with open(fp,'r',encoding='utf-8') as f: xml=f.read()
        row_matches=list(re.finditer(r'<w:tr[ >].*?</w:tr>',xml,re.DOTALL))
        if len(row_matches)>1:
            tmpl=row_matches[1].group(0); new_rows=''
            for person in itr_list:
                fio=person.get('fio','').strip()
                if not fio: continue
                row=tmpl
                row=re.sub(r'(<w:t[^>]*>)[^<]*(</w:t>)',
                    lambda m,v=fio:m.group(1)+v+m.group(2),row,count=1)
                row=re.sub(r'(<w:t[^>]*>)\d{2}\.\d{2}\.\d{4}(</w:t>)',
                    lambda m,d=impl_date:m.group(1)+d+m.group(2),row)
                new_rows+=row
            s=row_matches[1].start(); e=row_matches[-1].end()
            xml=xml[:s]+new_rows+xml[e:]
        with open(fp,'w',encoding='utf-8') as f: f.write(xml)
        Path(dst).parent.mkdir(parents=True,exist_ok=True)
        with zipfile.ZipFile(str(dst),'w',zipfile.ZIP_DEFLATED) as zo:
            for root,_,files in _os.walk(up):
                for fn in files:
                    fpath=_os.path.join(root,fn)
                    zo.write(fpath,_os.path.relpath(fpath,up))




def build_reps_spk(data, variant='stroy'):
    """Замены для СПК (Строй Комплекс или БИСП)"""
    ai = data.get('ai_data', {})
    c = ai.get('company', {}) or {}
    org = c.get('name') or data.get('orgName', '')
    form = c.get('form') or data.get('orgForm', 'ООО')
    
    # Директор
    dir_fio = c.get('director_fio', '')
    parts = dir_fio.split() if dir_fio else []
    dir_s = parts[0] if parts else data.get('dirSurname', '')
    dir_i = '.'.join(p[0] for p in parts[1:] if p) + '.' if len(parts) > 1 else data.get('dirInitials', '')
    
    # Даты
    impl = ai.get('dates', {}).get('implementation_date', '') or date_dot(data.get('implDate', ''))
    
    if variant == 'stroy':
        # Сфера Секьюрити / Пеганов В.Н. / Артюх А.В.
        staff = ai.get('staff', [])
        gi_fio = next((s['fio'] for s in staff if s.get('role') in ('responsible','auditor')), '')
        gi_parts = gi_fio.split() if gi_fio else []
        gi_s = gi_parts[0] if gi_parts else 'Артюх'
        gi_i = '.'.join(p[0] for p in gi_parts[1:] if p)+'.' if len(gi_parts)>1 else 'А.В.'
        
        return [
            ('Сфера Секьюрити', org), ('«Сфера Секьюрити»', f'«{org}»'),
            (f'ООО «Сфера Секьюрити»', f'{form} «{org}»'),
            ('Пеганов Владимир Николаевич', f'{dir_fio}' if dir_fio else 'Пеганов Владимир Николаевич'),
            ('Пеганов В.Н.', f'{dir_s} {dir_i}' if dir_s else 'Пеганов В.Н.'),
            ('В.Н. Пеганов', f'{dir_i} {dir_s}' if dir_s else 'В.Н. Пеганов'),
            ('Артюх Андрей Владимирович', gi_fio or 'Артюх Андрей Владимирович'),
            ('Артюх А.В.', f'{gi_s} {gi_i}' if gi_s else 'Артюх А.В.'),
            ('А.В. Артюх', f'{gi_i} {gi_s}' if gi_s else 'А.В. Артюх'),
        ] + ([('27.05.2026', impl), ('27.05.2025', impl)] if impl else [])
    else:
        # Кастом-Инвест / Юковец А.К.
        return [
            ('Кастом-Инвест', org), ('«Кастом-Инвест»', f'«{org}»'),
            (f'ООО «Кастом-Инвест»', f'{form} «{org}»'),
            ('Юковец А.К.', f'{dir_s} {dir_i}' if dir_s else 'Юковец А.К.'),
            ('А.К. Юковец', f'{dir_i} {dir_s}' if dir_s else 'А.К. Юковец'),
        ] + ([('27.05.2026', impl), ('27.05.2025', impl)] if impl else [])


def build_reps_suot(data):
    """Замены для СУОТ (Варта / Василенко)"""
    ai = data.get('ai_data', {})
    c = ai.get('company', {}) or {}
    org = c.get('name') or data.get('orgName', '')
    form = c.get('form') or data.get('orgForm', 'ООО')
    scope = ai.get('certification', {}).get('scope', '') or data.get('scope', '')
    
    dir_fio = c.get('director_fio', '')
    parts = dir_fio.split() if dir_fio else []
    dir_s = parts[0] if parts else ''
    dir_i = '.'.join(p[0] for p in parts[1:] if p)+'.' if len(parts)>1 else ''
    
    impl = ai.get('dates', {}).get('implementation_date', '') or date_dot(data.get('implDate', ''))
    yr = year_of(impl) if impl else '2026'
    
    reps = [
        ('Варта', org), ('«Варта»', f'«{org}»'),
        ('ООО «Варта»', f'{form} «{org}»'),
        ('Василенко С.Ф.', f'{dir_s} {dir_i}' if dir_s else 'Василенко С.Ф.'),
        ('С.Ф. Василенко', f'{dir_i} {dir_s}' if dir_s else 'С.Ф. Василенко'),
        ('Василенко', dir_s if dir_s else 'Василенко'),
        ('монтаж внутренних систем электроснабжения; монтаж наружных сетей электроснабжения, трансформаторных подстанций и распределительных устройств; устройство систем связи и сигнализации, видеонаблюдения', scope or 'монтаж внутренних систем электроснабжения; монтаж наружных сетей электроснабжения, трансформаторных подстанций и распределительных устройств; устройство систем связи и сигнализации, видеонаблюдения'),
        ('13.04.2026', impl or '13.04.2026'),
        ('2026 года', f'{yr} года'), ('2026г.', f'{yr}г.'), ('2026 г.', f'{yr} г.'),
        ('на 2026', f'на {yr}'),
    ]
    return [(o,n) for o,n in reps if o and n]


def generate_all(data, out_dir):
    product = data.get('product','iso') or 'iso'
    ai_name = data.get('ai_data',{}).get('company',{}).get('name','')
    org = ai_name or data.get('orgName','')

    # Выбираем шаблоны и замены по продукту
    if product == 'spk_stroy':
        tpl_dir = BASE_DIR/'templates'/'СПК_Строй'
        reps = build_reps_spk(data, 'stroy')
    elif product == 'spk_bisp':
        tpl_dir = BASE_DIR/'templates'/'СПК_БИСП'
        reps = build_reps_spk(data, 'bisp')
    elif product in ('suot','iso_suot'):
        tpl_dir = BASE_DIR/'templates'/'ИСО_СУОТ'
        reps = build_reps_suot(data)
        if product == 'iso_suot':
            reps += build_reps(data)[:]  # + ИСО замены
    else:
        tpl_dir = TPL_DIR  # ИСО ЭнергоМагистраль
        reps = build_reps(data)
    impl=date_dot(data.get('implDate',''))
    ai_impl=data.get('ai_data',{}).get('dates',{}).get('implementation_date','')
    if ai_impl: impl=ai_impl

    itr_raw=data.get('itrList','')
    itr_list=[]
    if isinstance(itr_raw,list): itr_list=itr_raw
    elif itr_raw:
        for line in itr_raw.strip().split('\n'):
            line=line.strip()
            if line: itr_list.append({'fio':line})
    # Добавляем ИТР из ИИ-данных
    ai_staff=data.get('ai_data',{}).get('staff',[])
    if ai_staff and not itr_list:
        itr_list=[{'fio':s['fio']} for s in ai_staff if s.get('is_itr') or s.get('role')=='itr']

    Path(out_dir).mkdir(parents=True,exist_ok=True); done=[]
    for src in Path(tpl_dir).rglob('*'):
        if src.is_dir(): continue
        if not src.name.endswith(('.docx','.doc')): continue
        parts=list(src.relative_to(TPL_DIR).parts)
        parts[-1]=parts[-1].replace('ЭнергоМагистраль', org)
        rel=os.path.join(*parts); dst=Path(out_dir)/rel
        try:
            is_22='2.2' in src.name and 'ознакомл' in src.name.lower()
            if src.name.endswith('.docx') and is_22 and itr_list:
                replace_itr_table(src,dst,itr_list,impl)
                replace_in_docx(dst,dst,reps)
            elif src.name.endswith('.docx'): replace_in_docx(src,dst,reps)
            else: dst.parent.mkdir(parents=True,exist_ok=True); shutil.copy2(src,dst)
            done.append({'name':parts[-1],'path':str(dst),'rel':rel})
        except Exception as e: print(f'  ERR {src.name}: {e}')
    return done


# ── Хранилище ─────────────────────────────────────────────────
def get_companies():
    return [json.loads(f.read_text('utf-8')) for f in sorted(CO_DIR.glob('*.json'))
            if not f.name.startswith('.')]

def save_company(data):
    cid=data.get('id') or f"c{int(datetime.now().timestamp()*1000)}"
    data['id']=cid
    (CO_DIR/f'{cid}.json').write_text(json.dumps(data,ensure_ascii=False,indent=2),'utf-8')
    return cid

def get_journal():
    return [json.loads(f.read_text('utf-8')) for f in sorted(JOURNAL_DIR.glob('*.json'),reverse=True)]

def save_journal(entry):
    eid=f"j{int(datetime.now().timestamp()*1000)}"
    entry.update({'id':eid,'created':datetime.now().strftime('%d.%m.%Y %H:%M')})
    (JOURNAL_DIR/f'{eid}.json').write_text(json.dumps(entry,ensure_ascii=False,indent=2),'utf-8')
    return eid

def get_zip(eid):
    for f in JOURNAL_DIR.glob('*.json'):
        try:
            e=json.loads(f.read_text('utf-8'))
            if e.get('id')==eid:
                zp=e.get('zipPath'); return zp if zp and os.path.exists(zp) else None
        except: pass



# ── Vision: распознавание фото/скана через BitrixGPT (переиспользуется и для одиночной
#    загрузки, и для фото внутри архивов) ─────────────────────────────────────────────
VISION_PROMPT = ("Извлеки весь текст с этого документа/изображения. Укажи все ФИО, должности, даты, "
                  "названия организаций, номера документов. Если это удостоверение — укажи кому выдано, "
                  "должность, организация, дата выдачи, основание (протокол №, дата). "
                  "Отвечай только извлечёнными данными, без лишних слов.")

def _downscale_image(file_bytes, max_dim=1600, quality=72):
    """Уменьшает фото перед отправкой в vision — камера даёт 3-6 МБ на файл,
    а для распознавания текста хватает гораздо меньшего разрешения.
    Ускоряет запрос и снижает риск OOM при пакетной обработке."""
    try:
        from PIL import Image
        import io as _io2
        img = Image.open(_io2.BytesIO(file_bytes))
        if img.mode in ('RGBA', 'P'):
            img = img.convert('RGB')
        if max(img.size) > max_dim:
            ratio = max_dim / max(img.size)
            img = img.resize((int(img.size[0]*ratio), int(img.size[1]*ratio)), Image.LANCZOS)
        buf = _io2.BytesIO()
        img.save(buf, 'JPEG', quality=quality, optimize=True)
        return buf.getvalue(), 'image/jpeg'
    except Exception:
        return file_bytes, None


def vision_extract(file_bytes, filename, api_key, media_type=None):
    """Синхронный вызов vision для одного файла (фото/скан). Уменьшает изображение
    перед отправкой. Ограничивает параллелизм через VISION_SEMAPHORE (не более 2 разом
    на весь сервер — иначе Render Free падает по памяти)."""
    import base64 as _b64
    ext = filename.rsplit('.', 1)[-1].lower() if '.' in filename else ''
    if ext in ('jpg', 'jpeg', 'png', 'webp'):
        small_bytes, mt = _downscale_image(file_bytes)
        file_bytes = small_bytes
        media_type = mt or (media_type or 'image/jpeg')
    elif not media_type:
        media_type = 'application/pdf' if ext == 'pdf' else 'image/jpeg'

    b64_data = _b64.b64encode(file_bytes).decode('utf-8')
    vibe_payload = {
        "model": VIBE_MODEL_VISION,
        "max_tokens": 1000,
        "messages": [{
            "role": "user",
            "content": [
                {"type": "image_url", "image_url": {"url": f"data:{media_type};base64,{b64_data}"}},
                {"type": "text", "text": VISION_PROMPT}
            ]
        }]
    }
    VISION_SEMAPHORE.acquire()
    try:
        resp = req_lib.post(
            VIBE_URL, headers={"Content-Type": "application/json", "X-Api-Key": api_key},
            json=vibe_payload, timeout=90
        )
        resp.raise_for_status()
        data = resp.json()
        text = "".join(c.get("message", {}).get("content", "") for c in data.get("choices", []))
        return text or '[vision: пустой ответ]'
    except req_lib.exceptions.Timeout:
        # Не повторяем тем же медленным способом — сразу быстрый текстовый фоллбэк
        return extract_text_from_file(file_bytes, filename)
    except Exception as e:
        try:
            return extract_text_from_file(file_bytes, filename)
        except Exception:
            return f'[vision ошибка: {e}]'
    finally:
        VISION_SEMAPHORE.release()


# ── Извлечение текста из файлов (рекурсивно) ────────────────
def extract_text_from_file(file_bytes, filename, _depth=0):
    """Рекурсивно читает файлы и архивы внутри архивов (до 3 уровней)"""
    if _depth > 3:
        return '[слишком глубокая вложенность архивов]'

    ext = filename.rsplit('.', 1)[-1].lower() if '.' in filename else ''
    try:
        if ext in ('txt', 'csv'):
            return file_bytes.decode('utf-8', errors='replace')[:8000]

        elif ext == 'docx' or ext == 'doc':
            import io
            try:
                with zipfile.ZipFile(io.BytesIO(file_bytes)) as z:
                    if 'word/document.xml' in z.namelist():
                        xml = z.read('word/document.xml').decode('utf-8', errors='replace')
                        text = re.sub(r'<[^>]+>', ' ', xml)
                        text = re.sub(r'\s+', ' ', text).strip()
                        return text[:8000]
            except: pass
            return '[docx: не удалось прочитать]'

        elif ext == 'pdf':
            # BT/ET поток парсер
            # ВАЖНО: сканируем только начало файла, а не весь файл целиком.
            # Большие PDF (десятки МБ) почти всегда — сканы без текстового слоя (BT/ET там нет вообще),
            # а decode()+regex по всем байтам такого файла — верный способ упасть в OOM на Render Free.
            PDF_SCAN_LIMIT = 800 * 1024  # 800 КБ хватает с запасом для обычных текстовых PDF
            try:
                raw = file_bytes[:PDF_SCAN_LIMIT].decode('latin-1', errors='replace')
                blocks = re.findall(r'BT(.*?)ET', raw, re.DOTALL)
                result = []
                for b in blocks:
                    strings = re.findall(r'\(([^)]{2,})\)', b)
                    result.extend(strings)
                if result:
                    return ' '.join(result)[:8000]
            except Exception:
                pass
            return '[PDF_SCAN: файл является сканом]'

        elif ext in ('xlsx', 'xls'):
            import io, re as _re2
            # Сначала пробуем как XLSX (ZIP-формат)
            try:
                with zipfile.ZipFile(io.BytesIO(file_bytes)) as z:
                    shared = []
                    if 'xl/sharedStrings.xml' in z.namelist():
                        xml = z.read('xl/sharedStrings.xml').decode('utf-8', errors='replace')
                        shared = re.findall(r'<t[^>]*>([^<]+)</t>', xml)
                    sheets = [n for n in z.namelist() if n.startswith('xl/worksheets/sheet')]
                    if sheets:
                        xml = z.read(sheets[0]).decode('utf-8', errors='replace')
                        refs = re.findall(r'<v>(\d+)</v>', xml)
                        values = [shared[int(r)] for r in refs if int(r) < len(shared)]
                        if values:
                            return ' | '.join(values[:300])
                return '[xlsx: данные не найдены]'
            except zipfile.BadZipFile:
                pass
            except Exception:
                pass
            # Старый XLS формат — эвристика
            try:
                text_utf16 = file_bytes.decode('utf-16-le', errors='ignore')
                chunks = _re2.findall(r'[\u0400-\u04ff\w][\u0400-\u04ff\w\s\.\,\-]{3,}', text_utf16)
                if len(chunks) > 3:
                    return ' | '.join(c.strip() for c in chunks if len(c.strip()) > 3)[:6000]
            except Exception:
                pass
            try:
                raw = file_bytes.decode('cp1251', errors='ignore')
                lines = [l.strip() for l in raw.split('\n') if len(l.strip()) > 5]
                readable = [l for l in lines if any('а' <= c <= 'я' or 'А' <= c <= 'Я' for c in l)]
                if readable:
                    return '\n'.join(readable[:200])
            except Exception:
                pass
            return '[XLS: не удалось прочитать. Пересохраните файл как .xlsx]'

        elif ext in ('zip',):
            return _extract_archive_zip(file_bytes, filename, _depth)

        elif ext == 'rar':
            return _extract_archive_rar(file_bytes, filename, _depth)

    except Exception as e:
        return f'[Ошибка чтения {filename}: {e}]'

    return '[Неизвестный формат файла]'


def extract_archive_with_vision(file_bytes, filename, api_key, progress_cb=None):
    """
    Полный разбор архива для фонового режима (не ограничен HTTP-таймаутом):
    - текстовые файлы (docx/pdf/txt/csv/xlsx) читаются как раньше, быстро
    - ФОТО (jpg/jpeg/png/webp) теперь тоже читаются — через vision, по одному,
      с уменьшением размера перед отправкой
    Раньше картинки внутри zip вообще пропускались (не было привязки к vision) —
    это и была причина, почему данные людей с одними фото (не PDF) не попадали в карточку.
    """
    import io
    TEXT_EXTS = ('docx', 'doc', 'txt', 'csv', 'xlsx')  # без pdf — у него своя ветка ниже
    IMAGE_EXTS = ('jpg', 'jpeg', 'png', 'webp')
    TEXT_INNER_LIMIT = 4 * 1024 * 1024     # текстовые файлы — как раньше, 4 МБ
    IMAGE_INNER_LIMIT = 15 * 1024 * 1024   # фото крупнее (сами уменьшаются перед отправкой)
    PDF_INNER_LIMIT = 20 * 1024 * 1024     # PDF-сканы (паспорта/трудовые) часто крупнее — до 20 МБ
    MAX_ITEMS = 60  # защита от архивов с сотнями фото — вышло бы на часы обработки

    def p(msg):
        if progress_cb: progress_cb(msg)

    # Собираем список читаемых записей заранее, чтобы знать общее количество для прогресса
    entries = []  # (name, size, kind)
    try:
        with zipfile.ZipFile(io.BytesIO(file_bytes)) as z:
            for info in z.infolist():
                name = info.filename
                if name.endswith('/'): continue
                try:
                    fixed = name.encode('cp437').decode('utf-8')
                except Exception:
                    fixed = name
                if '__MACOSX' in fixed or fixed.endswith('.DS_Store'): continue
                ext = fixed.rsplit('.', 1)[-1].lower() if '.' in fixed else ''
                if ext == 'pdf' and info.file_size <= PDF_INNER_LIMIT:
                    entries.append((name, fixed, info.file_size, 'pdf'))
                elif ext in TEXT_EXTS and info.file_size <= TEXT_INNER_LIMIT:
                    entries.append((name, fixed, info.file_size, 'text'))
                elif ext in IMAGE_EXTS and info.file_size <= IMAGE_INNER_LIMIT:
                    entries.append((name, fixed, info.file_size, 'image'))
                elif ext == 'pdf' or ext in TEXT_EXTS or ext in IMAGE_EXTS:
                    entries.append((name, fixed, info.file_size, 'skip_size'))
    except Exception as e:
        return f'[Ошибка открытия архива: {e}]'

    # Сначала текстовые (быстро), потом PDF и фото (медленно, vision) — чтобы данные из
    # docx были в карточке даже если распознавание сканов ещё не закончилось
    entries.sort(key=lambda e: 0 if e[3] == 'text' else (1 if e[3] in ('image', 'pdf') else 2))

    to_process = [e for e in entries if e[3] in ('text', 'image', 'pdf')][:MAX_ITEMS]
    skipped = [e for e in entries if e[3] == 'skip_size']
    total = len(to_process)

    texts = []
    skipped_notes = [f"{fn.split('/')[-1]} ({sz//1024//1024} МБ)" for _, fn, sz, _ in skipped]

    text_entries = [e for e in to_process if e[3] == 'text']
    image_entries = [e for e in to_process if e[3] in ('image', 'pdf')]  # оба идут через vision-путь ниже
    done_count = [0]

    with zipfile.ZipFile(io.BytesIO(file_bytes)) as z:
        # Текстовые файлы — быстро, по очереди
        for raw_name, fixed_name, size, kind in text_entries:
            short = fixed_name.split('/')[-1]
            folder = '/'.join(fixed_name.split('/')[:-1])
            done_count[0] += 1
            p(f"Читаю {done_count[0]}/{total}: {short}")
            try:
                data = z.read(raw_name)
                txt = extract_text_from_file(data, short)
                if txt and len(txt) > 10 and not txt.startswith('['):
                    texts.append(f"--- {folder + '/' if folder else ''}{short} ---\n" + txt)
            except Exception as e:
                texts.append(f"--- {short} --- [ошибка: {e}]")

        # PDF и фото — медленно (vision), поэтому по 2 одновременно вместо строго по одному.
        # VISION_SEMAPHORE всё равно не даст больше 2 разом на весь сервер.
        # Каждый файл читаем из архива только в момент обработки (не грузим все фото
        # в память разом — на 121-мегабайтном архиве это была бы лишняя сотня МБ).
        from concurrent.futures import ThreadPoolExecutor, as_completed

        def process_image(entry):
            raw_name, fixed_name, size, kind = entry
            short = fixed_name.split('/')[-1]
            folder = '/'.join(fixed_name.split('/')[:-1])
            try:
                with zipfile.ZipFile(io.BytesIO(file_bytes)) as z2:
                    data = z2.read(raw_name)
            except Exception:
                return None

            if kind == 'pdf':
                # Сначала быстрая попытка вытащить текстовый слой (мгновенно, бесплатно).
                # Если это скан (нет текстового слоя) — падаем в vision, как с фото.
                fast_txt = extract_text_from_file(data, short)
                if fast_txt and len(fast_txt) > 10 and not fast_txt.startswith('['):
                    return f"--- {folder + '/' if folder else ''}{short} ---\n" + fast_txt
                # PDF отправляется в vision целиком (страница не рендерится отдельно — такой
                # возможности сейчас нет), поэтому очень большие сканы намеренно не шлём —
                # рискованно по времени/памяти. Лучше явно попросить переснять по страницам.
                PDF_VISION_LIMIT = 8 * 1024 * 1024
                if len(data) > PDF_VISION_LIMIT:
                    return (f"--- {folder + '/' if folder else ''}{short} ---\n"
                            f"[Скан слишком большой ({len(data)//1024//1024} МБ) для распознавания целиком — "
                            f"пришлите этот документ отдельными фото по 1-2 страницы вместо одного большого PDF]")
                txt = vision_extract(data, short, api_key)
            else:
                txt = vision_extract(data, short, api_key)

            if txt and len(txt) > 10 and not txt.startswith('['):
                return f"--- {folder + '/' if folder else ''}{short} ---\n" + txt
            return None

        if image_entries:
            with ThreadPoolExecutor(max_workers=2) as ex:
                futures = {ex.submit(process_image, e): e for e in image_entries}
                for fut in as_completed(futures):
                    e = futures[fut]
                    done_count[0] += 1
                    short = e[1].split('/')[-1]
                    p(f"Распознано {done_count[0]}/{total}: {short}")
                    try:
                        r = fut.result()
                        if r: texts.append(r)
                    except Exception as ex2:
                        texts.append(f"--- {short} --- [ошибка: {ex2}]")

    PRIORITY_KEYWORDS = ['поставщик', 'объект', 'сотрудник', 'штатн', 'персонал',
                          'список', 'перечень', 'работник', 'паспорт', 'диплом', 'трудов']
    priority = [t for t in texts if any(k in t.lower() for k in PRIORITY_KEYWORDS)]
    other = [t for t in texts if t not in priority]
    result = '\n\n'.join(priority + other)
    if len(result) > 60000:
        result = result[:60000] + '\n...[обрезано, слишком много данных]'
    if skipped_notes:
        result += ('\n\n[Пропущены файлы крупнее лимита: ' + ', '.join(skipped_notes[:20]) + ']')
    if len(entries) > MAX_ITEMS:
        result += f'\n\n[В архиве {len(entries)} файлов, обработаны первые {MAX_ITEMS} — пришлите остальных отдельным заходом]'
    return result or '[Архив: читаемых данных не найдено]'


def _extract_archive_zip(file_bytes, filename, _depth=0):
    """Рекурсивно распаковывает ZIP включая вложенные архивы"""
    import io
    texts = []
    skipped = []
    READABLE = ('docx','doc','txt','csv','xlsx','pdf','zip','rar')
    # Файлы крупнее этого — почти всегда сканы актов/договоров/ОПЗ без полезного текста.
    # Читать их целиком (decode+regex) на Render Free (512 МБ) — верный способ упасть в OOM.
    # Пропускаем такие файлы, а не весь архив — так реальные объёмные папки клиента (сотни МБ
    # со сканами объектов) можно грузить одним архивом, не разбирая вручную.
    INNER_FILE_LIMIT = 4 * 1024 * 1024  # 4 МБ на один файл внутри архива
    MAX_FILES = 400  # защита от zip-бомб (архив с миллионом крошечных файлов)
    try:
        with zipfile.ZipFile(io.BytesIO(file_bytes)) as z:
            processed = 0
            for info in z.infolist():
                name = info.filename
                if name.endswith('/'): continue
                inner_ext = name.rsplit('.',1)[-1].lower() if '.' in name else ''
                if inner_ext not in READABLE: continue
                fn = name.split('/')[-1].split('\\')[-1]
                if info.file_size > INNER_FILE_LIMIT:
                    skipped.append(f"{fn} ({info.file_size//1024//1024} МБ)")
                    continue
                if processed >= MAX_FILES:
                    break
                try:
                    inner_bytes = z.read(name)
                    processed += 1
                    # Рекурсия для вложенных архивов
                    inner_text = extract_text_from_file(inner_bytes, fn, _depth+1)
                    if inner_text and len(inner_text) > 10 and not inner_text.startswith('['):
                        texts.append('--- ' + fn + ' ---\n' + inner_text)
                except: pass
        if texts or skipped:
            # Умная нарезка: сначала ищем приоритетные файлы
            PRIORITY_KEYWORDS = [
                'поставщик', 'supplier', 'объект', 'object', 'сотрудник',
                'штатн', 'персонал', 'список', 'перечень', 'работник'
            ]
            priority = []
            other = []
            for t in texts:
                tl = t.lower()
                if any(kw in tl for kw in PRIORITY_KEYWORDS):
                    priority.append(t)
                else:
                    other.append(t)
            # Приоритетные идут первыми, потом остальные
            ordered = priority + other
            result = '\n\n'.join(ordered)
            # Лимит 40000 символов
            if len(result) > 40000:
                result = result[:40000] + '\n...[обрезано, файл большой]'
            if skipped:
                result += ('\n\n[Пропущены крупные файлы без анализа (не читаются для извлечения данных, '
                           'но остаются в исходном архиве для подачи в орган): ' + ', '.join(skipped[:20]) + ']')
            return result
        return '[zip: читаемых файлов не найдено]'
    except Exception as e:
        return f'[zip ошибка: {e}]'


def _extract_archive_rar(file_bytes, filename, _depth=0):
    """Распаковывает RAR через rarfile или эвристику"""
    import io
    texts = []
    READABLE = ('docx','doc','txt','csv','xlsx','pdf','zip','rar')
    INNER_FILE_LIMIT = 4 * 1024 * 1024
    try:
        import rarfile as _rar
        rf = _rar.RarFile(io.BytesIO(file_bytes))
        for info in rf.infolist():
            name = info.filename
            inner_ext = name.rsplit('.',1)[-1].lower() if '.' in name else ''
            if inner_ext not in READABLE: continue
            if getattr(info, 'file_size', 0) > INNER_FILE_LIMIT: continue
            try:
                inner_bytes = rf.read(name)
                fn = name.split('/')[-1].split('\\')[-1]
                inner_text = extract_text_from_file(inner_bytes, fn, _depth+1)
                if inner_text and len(inner_text) > 10 and not inner_text.startswith('['):
                    texts.append('--- ' + fn + ' ---\n' + inner_text)
            except: pass
        if texts:
            return '\n\n'.join(texts)[:12000]
        return '[rar: файлы не найдены]'
    except ImportError:
        pass
    except Exception:
        pass

    # Fallback — системный 7z/unrar
    import tempfile, subprocess, os as _os
    with tempfile.TemporaryDirectory() as td:
        rar_path = _os.path.join(td, 'arch.rar')
        with open(rar_path, 'wb') as f_out:
            f_out.write(file_bytes)
        for cmd in [
            ['7z', 'x', '-y', f'-o{td}', rar_path],
            ['unrar', 'x', '-y', rar_path, td],
            ['unrar-free', 'x', '-y', rar_path, td],
        ]:
            try:
                r = subprocess.run(cmd, capture_output=True, timeout=30)
                if r.returncode == 0:
                    for root, dirs, files_list in _os.walk(td):
                        for fn in files_list:
                            if fn == 'arch.rar': continue
                            inner_ext = fn.rsplit('.',1)[-1].lower() if '.' in fn else ''
                            if inner_ext not in READABLE: continue
                            try:
                                fpath = _os.path.join(root, fn)
                                if _os.path.getsize(fpath) > INNER_FILE_LIMIT: continue
                                with open(fpath, 'rb') as f_in:
                                    inner_bytes = f_in.read()
                                inner_text = extract_text_from_file(inner_bytes, fn, _depth+1)
                                if inner_text and len(inner_text) > 10 and not inner_text.startswith('['):
                                    texts.append('--- ' + fn + ' ---\n' + inner_text)
                            except: pass
                    if texts:
                        return '\n\n'.join(texts)[:12000]
                    break
            except (FileNotFoundError, subprocess.TimeoutExpired):
                continue

    if texts:
        return '\n\n'.join(texts)[:12000]
    return '[RAR: не удалось распаковать. Перепакуйте в ZIP — правая кнопка → 7-Zip → ZIP]'


INDEX = (BASE_DIR/'index.html').read_text('utf-8')

# ── HTTP-сервер ───────────────────────────────────────────────
class H(http.server.BaseHTTPRequestHandler):
    def log_message(self,*a): pass

    def do_HEAD(self):
        """UptimeRobot и браузеры шлют HEAD — отвечаем 200"""
        self.send_response(200)
        self.send_header('Content-Type', 'text/html')
        self.end_headers()

    def do_GET(self):
        p=self.path.split('?')[0]
        if p in('/','//index.html'):          self._html(INDEX)
        elif p=='/api/companies':             self._json(get_companies())
        elif p=='/api/journal':               self._json(get_journal())
        elif p.startswith('/api/task/'):
            task_id = p.split('/')[-1]
            task = TASKS.get(task_id) or load_task(task_id)
            if task:
                TASKS[task_id] = task
                _prune_tasks()
                self._json({
                    'status':    task.get('status','running'),
                    'kind':      task.get('kind','generation'),
                    'step':      task.get('step',0),
                    'total':     task.get('total',100),
                    'progress':  task.get('progress',[]),
                    'journalId': task.get('journalId'),
                    'fileCount': task.get('fileCount',0),
                    'dates':     task.get('dates',{}),
                    'error':     task.get('error',''),
                    'zipB64':    task.get('zipB64'),
                    'orgName':   task.get('orgName',''),
                    'text':      task.get('text'),
                    'filename':  task.get('filename','')
                })
            else:
                self._json({'status':'not_found'})
        elif p.startswith('/api/download/'):
            zp=get_zip(p.split('/')[-1])
            if zp:
                d=open(zp,'rb').read()
                self.send_response(200)
                self.send_header('Content-Type','application/zip')
                self.send_header('Content-Disposition','attachment; filename="ISO_docs.zip"')
                self.send_header('Content-Length',str(len(d)))
                self.end_headers(); self.wfile.write(d)
            else: self.send_response(404); self.end_headers()
        else: self.send_response(404); self.end_headers()

    def do_POST(self):
        body=self.rfile.read(int(self.headers.get('Content-Length',0)))
        p=self.path.split('?')[0]
        try:
            # ── ИИ-чат ──────────────────────────────────────
            if p=='/api/analyze-image':
                # Анализ изображения/PDF через BitrixGPT vision
                import io as _io, re as _re, base64 as _b64
                api_key = os.environ.get('VIBE_API_KEY','')
                if not api_key:
                    self._json({'success':False,'error':'VIBE_API_KEY не задан'},500); return

                content_type = self.headers.get('Content-Type','')
                boundary = None
                for part in content_type.split(';'):
                    part = part.strip()
                    if part.startswith('boundary='):
                        boundary = part[9:].strip().encode()
                        break
                if not boundary:
                    self._json({'success':False,'error':'Нет boundary'},400); return

                parts = body.split(b'--' + boundary)
                filename = None
                file_bytes = None
                for part in parts:
                    if b'Content-Disposition' not in part or b'filename=' not in part: continue
                    header_end = part.find(b'\r\n\r\n')
                    if header_end == -1: continue
                    header = part[:header_end].decode('utf-8','replace')
                    m = _re.search(r'filename="([^"]+)"', header)
                    if not m: continue
                    filename = m.group(1)
                    file_bytes = part[header_end+4:].rstrip(b'\r\n--')
                    break

                if not file_bytes:
                    self._json({'success':False,'error':'Файл не найден'},400); return

                MAX_FILE_MB = 6
                if len(file_bytes) > MAX_FILE_MB * 1024 * 1024:
                    self._json({'success': False,
                                 'error': f'Файл слишком большой ({len(file_bytes)//1024//1024} МБ), лимит {MAX_FILE_MB} МБ на Render Free.'},
                                413)
                    return

                ext = filename.rsplit('.',1)[-1].lower() if '.' in filename else ''

                # Определяем media type
                if ext == 'pdf':
                    # PDF: сначала пробуем текстовый парсер (быстро и точно)
                    text_from_pdf = extract_text_from_file(file_bytes, filename)
                    if text_from_pdf and len(text_from_pdf) > 50 and not text_from_pdf.startswith('['):
                        self._json({'success':True,'text':text_from_pdf,'method':'text'}); return
                    # PDF не читается как текст (скан-удостоверение) — vision
                    media_type = 'image/jpeg'  # BitrixGPT лучше принимает как image
                elif ext in ('jpg','jpeg'):
                    media_type = 'image/jpeg'
                elif ext == 'png':
                    media_type = 'image/png'
                elif ext == 'webp':
                    media_type = 'image/webp'
                else:
                    # Не изображение — читаем как текст
                    text = extract_text_from_file(file_bytes, filename)
                    self._json({'success':True,'text':text,'method':'text'}); return

                # Кодируем в base64
                actual_media_type = media_type
                actual_bytes = file_bytes
                b64_data = _b64.b64encode(actual_bytes).decode('utf-8')

                # Отправляем в BitrixGPT vision
                prompt = "Извлеки весь текст с этого документа/изображения. Укажи все ФИО, должности, даты, названия организаций, номера документов. Если это удостоверение — укажи кому выдано, должность, организация, дата выдачи, основание (протокол №, дата). Отвечай только извлечёнными данными, без лишних слов."

                vibe_payload = {
                    "model": VIBE_MODEL_VISION,
                    "max_tokens": 1000,
                    "messages": [{
                        "role": "user",
                        "content": [
                            {
                                "type": "image_url",
                                "image_url": {
                                    "url": f"data:{actual_media_type};base64,{b64_data}"
                                }
                            },
                            {"type": "text", "text": prompt}
                        ]
                    }]
                }

                # Не более 2 таких тяжёлых запросов одновременно — иначе Render Free падает
                VISION_SEMAPHORE.acquire()
                try:
                    resp = req_lib.post(
                        VIBE_URL,
                        headers={"Content-Type":"application/json","X-Api-Key":api_key},
                        json=vibe_payload,
                        timeout=90
                    )
                    resp.raise_for_status()
                    data = resp.json()
                    text = "".join(c.get("message",{}).get("content","") for c in data.get("choices",[]))
                    self._json({'success':True,'text':text,'method':'vision'})
                except Exception as e:
                    was_timeout = isinstance(e, req_lib.exceptions.Timeout)
                    if was_timeout:
                        # Модель сама медленная (thinking-режим) — повтор другим форматом JSON
                        # ничего не ускорит, только удвоит ожидание (было до ~3 мин на файл).
                        # Сразу идём в текстовый fallback.
                        text = extract_text_from_file(file_bytes, filename)
                        self._json({'success':True,'text':text,'method':'fallback_timeout',
                                     'error':'BitrixGPT vision не ответил за 90 сек'})
                        return
                    # Fallback 1 — другой формат vision запроса (только если проблема НЕ в скорости,
                    # например неверный формат запроса/4xx — тогда другая структура JSON может помочь)
                    try:
                        vibe_payload2 = {
                            "model": VIBE_MODEL_VISION,
                            "max_tokens": 1000,
                            "messages": [{
                                "role": "user",
                                "content": [
                                    {"type": "text", "text": prompt},
                                    {
                                        "type": "image_url",
                                        "image_url": {"url": f"data:{media_type};base64,{b64_data}"}
                                    }
                                ]
                            }]
                        }
                        resp2 = req_lib.post(
                            VIBE_URL,
                            headers={"Content-Type":"application/json","X-Api-Key":api_key},
                            json=vibe_payload2,
                            timeout=90
                        )
                        if resp2.status_code == 200:
                            d2 = resp2.json()
                            text2 = "".join(c.get("message",{}).get("content","") for c in d2.get("choices",[]))
                            if text2 and len(text2) > 10:
                                self._json({'success':True,'text':text2,'method':'vision2'}); return
                    except Exception:
                        pass
                    # Fallback 2 — читаем как текст
                    text = extract_text_from_file(file_bytes, filename)
                    self._json({'success':True,'text':text,'method':'fallback','error':str(e)})
                finally:
                    VISION_SEMAPHORE.release()

            elif p=='/api/extract-text':
                import io, re as _re
                content_type = self.headers.get('Content-Type','')
                # Парсим multipart/form-data вручную (надёжнее cgi)
                boundary = None
                for part in content_type.split(';'):
                    part = part.strip()
                    if part.startswith('boundary='):
                        boundary = part[9:].strip().encode()
                        break
                if not boundary:
                    self._json({'success':False,'error':'Нет boundary в запросе'},400); return
                # Разбиваем по boundary
                parts = body.split(b'--' + boundary)
                filename = None
                file_bytes = None
                for part in parts:
                    if b'Content-Disposition' not in part: continue
                    if b'filename=' not in part: continue
                    # Извлекаем имя файла
                    header_end = part.find(b'\r\n\r\n')
                    if header_end == -1: continue
                    header = part[:header_end].decode('utf-8','replace')
                    m = _re.search(r'filename="([^"]+)"', header)
                    if not m: continue
                    filename = m.group(1)
                    file_bytes = part[header_end+4:].rstrip(b'\r\n--')
                    break
                if not filename or file_bytes is None:
                    self._json({'success':False,'error':'Файл не найден в запросе'},400); return
                # Защита от падения сервера (OOM) на Render Free (512 МБ RAM).
                # Для zip/rar лимит выше, потому что теперь ОГРОМНЫЕ файлы ВНУТРИ архива
                # (сканы актов/ОПЗ по 50-80 МБ) пропускаются на этапе распаковки и не читаются
                # целиком — опасность там обезврежена, поэтому сам контейнер может быть крупнее.
                _ext_check = filename.rsplit('.',1)[-1].lower() if '.' in filename else ''
                MAX_FILE_MB = 60 if _ext_check in ('zip','rar') else 6
                if len(file_bytes) > MAX_FILE_MB * 1024 * 1024:
                    self._json({'success': False,
                                 'error': f'Файл слишком большой ({len(file_bytes)//1024//1024} МБ). '
                                          f'Лимит {MAX_FILE_MB} МБ на Render Free — сервер может упасть от нехватки памяти. '
                                          f'Разбейте архив на несколько частей поменьше или пришлите файлы по отдельности.'},
                                413)
                    return
                try:
                    text = extract_text_from_file(file_bytes, filename)
                except MemoryError:
                    self._json({'success': False,
                                 'error': 'Не хватило памяти на обработку файла. Разбейте архив на части поменьше.'}, 500)
                    return
                except Exception as e:
                    self._json({'success': False, 'error': f'Ошибка обработки файла: {e}'}, 500)
                    return
                self._json({'success':True,'text':text,'filename':filename})

            elif p=='/api/extract-archive-async':
                # Асинхронный разбор архива с поддержкой распознавания фото внутри (не только docx/pdf).
                # Не блокирует HTTP-запрос — работает фоном, фронт опрашивает /api/task/<id>.
                # Благодаря этому можно грузить архивы гораздо крупнее, чем раньше.
                import re as _re3
                api_key = os.environ.get('VIBE_API_KEY','')
                if not api_key:
                    self._json({'success':False,'error':'VIBE_API_KEY не задан'},500); return
                content_type = self.headers.get('Content-Type','')
                boundary = None
                for part in content_type.split(';'):
                    part = part.strip()
                    if part.startswith('boundary='):
                        boundary = part[9:].strip().encode(); break
                if not boundary:
                    self._json({'success':False,'error':'Нет boundary в запросе'},400); return
                parts = body.split(b'--' + boundary)
                filename = None
                file_bytes = None
                for part in parts:
                    if b'Content-Disposition' not in part: continue
                    if b'filename=' not in part: continue
                    header_end = part.find(b'\r\n\r\n')
                    if header_end == -1: continue
                    header = part[:header_end].decode('utf-8','replace')
                    m = _re3.search(r'filename="([^"]+)"', header)
                    if not m: continue
                    filename = m.group(1)
                    file_bytes = part[header_end+4:].rstrip(b'\r\n--')
                    break
                if not filename or file_bytes is None:
                    self._json({'success':False,'error':'Файл не найден в запросе'},400); return

                # Асинхронный режим не привязан к таймауту одного HTTP-запроса, поэтому лимит
                # щедрее — реальный потолок теперь скорее у самого Render на приём тела запроса.
                MAX_ARCHIVE_MB = 200
                if len(file_bytes) > MAX_ARCHIVE_MB * 1024 * 1024:
                    self._json({'success': False,
                                 'error': f'Файл слишком большой ({len(file_bytes)//1024//1024} МБ), лимит {MAX_ARCHIVE_MB} МБ.'},
                                413)
                    return

                import uuid as _uuid
                task_id = str(_uuid.uuid4())[:8]
                TASKS[task_id] = {'status':'running','kind':'archive','progress':[],'step':0,'total':100}
                _prune_tasks()

                def run_archive(_tid=task_id, _bytes=file_bytes, _fn=filename, _key=api_key):
                    try:
                        def on_prog(msg):
                            TASKS[_tid]['progress'] = (TASKS[_tid].get('progress') or [])[-30:] + [msg]
                            print(f"  [archive {_tid}] {msg}")
                        result_text = extract_archive_with_vision(_bytes, _fn, _key, progress_cb=on_prog)
                        TASKS[_tid].update({'status':'done','kind':'archive','text':result_text,'filename':_fn})
                        _prune_tasks()
                    except Exception as _ex:
                        import traceback; traceback.print_exc()
                        TASKS[_tid].update({'status':'error','kind':'archive','error':str(_ex)})

                threading.Thread(target=run_archive, daemon=True).start()
                self._json({'success': True, 'async': True, 'task_id': task_id})

            elif p=='/api/ai/chat':
                req=json.loads(body)
                api_key=os.environ.get('VIBE_API_KEY','')
                if not api_key:
                    self._json({'success':False,'error':'VIBE_API_KEY не задан на сервере. Добавьте в Environment на Render.'},500); return
                messages=req.get('messages',[])
                text=call_ai(messages, api_key)
                self._json({'success':True,'text':text})

            elif p=='/api/companies/save':
                d=json.loads(body); self._json({'success':True,'id':save_company(d)})
            elif p=='/api/companies/delete':
                cid=json.loads(body)['id']; f=CO_DIR/f'{cid}.json'
                if f.exists(): f.unlink()
                self._json({'success':True})
            elif p=='/api/generate':
                data=json.loads(body)
                import uuid as _uuid
                api_key = os.environ.get('VIBE_API_KEY', '')
                product = data.get('product', 'iso')
                ai_data = data.get('ai_data', {})

                # Умный генератор — запускаем в фоне
                if SMART_GENERATOR and ai_data.get('company', {}).get('name'):
                    if GENERATION_IN_PROGRESS['active']:
                        self._json({'success': False,
                                     'error': 'Уже идёт другая генерация. На Render Free одновременно можно '
                                               'выполнять только одну — дождитесь её завершения и попробуйте снова.'},
                                    429)
                        return

                    task_id = str(_uuid.uuid4())[:8]
                    TASKS[task_id] = {'status': 'running', 'progress': [], 'step': 0, 'total': 100}
                    _prune_tasks()
                    save_task(task_id, TASKS[task_id])

                    def run_gen(_tid=task_id, _data=ai_data, _key=api_key, _prod=product):
                        GENERATION_IN_PROGRESS['active'] = True
                        try:
                            def on_prog(step, total, msg):
                                TASKS[_tid]['progress'] = (TASKS[_tid].get('progress') or []) + [msg]
                                TASKS[_tid]['step'] = step
                                TASKS[_tid]['total'] = total
                                save_task(_tid, TASKS[_tid])
                                print(f"  [{step}/{total}] {msg}")

                            result = generate_package(_data, _key, _prod, on_prog)
                            docs = result['docs']
                            if result.get('error') or not docs:
                                err_msg = result.get('error') or 'Генератор не вернул ни одного документа (0 файлов) — данные не подошли под продукт или модуль не смог отработать.'
                                raise RuntimeError(err_msg)
                            _org = re.sub(r'[^\w\-]', '_', _data.get('company', {}).get('name', 'org'))
                            _ts = datetime.now().strftime('%Y%m%d_%H%M%S')
                            _zp = str(OUT_DIR / f'{_org}_{_ts}.zip')
                            with zipfile.ZipFile(_zp, 'w', zipfile.ZIP_DEFLATED) as _zf:
                                for doc in docs:
                                    _zf.writestr(doc['name'], doc['bytes'])
                            _eid = save_journal({
                                'orgName': _data.get('company', {}).get('name', ''),
                                'implDate': result['dates'].get('goals', ''),
                                'fileCount': len(docs),
                                'zipPath': _zp,
                                'product': _prod,
                                'generator': 'smart'
                            })
                            # Кодируем ZIP в base64 для передачи фронту — диск Render эфемерный,
                            # фронт сохранит архив в window.storage и журнал переживёт перезапуск сервера
                            _zip_b64 = None
                            try:
                                with open(_zp, 'rb') as _zf2:
                                    _zip_b64 = base64.b64encode(_zf2.read()).decode('ascii')
                            except Exception as _zerr:
                                print(f"  ⚠️ Не удалось закодировать zip в base64: {_zerr}")
                            TASKS[_tid].update({'status':'done','journalId':_eid,
                                               'fileCount':len(docs),'dates':result['dates'],
                                               'zipB64': _zip_b64, 'orgName': _data.get('company', {}).get('name', '')})
                            _prune_tasks()
                            save_task(_tid, TASKS[_tid])
                            print(f"  ✅ Задача {_tid} завершена: {len(docs)} документов")
                        except Exception as _ex:
                            import traceback; traceback.print_exc()
                            TASKS[_tid].update({'status':'error','error':str(_ex)})
                            save_task(_tid, TASKS[_tid])
                        finally:
                            GENERATION_IN_PROGRESS['active'] = False

                    threading.Thread(target=run_gen, daemon=True).start()
                    self._json({'success': True, 'async': True, 'task_id': task_id})
                else:
                # Старый генератор (замена в шаблонах) — если нет данных ИИ

                    org=re.sub(r'[^\w\-]','_',
                        data.get('ai_data',{}).get('company',{}).get('name','') or
                    data.get('orgName','org'))
                ts=datetime.now().strftime('%Y%m%d_%H%M%S')
                out=OUT_DIR/f'{org}_{ts}'
                done=generate_all(data,out)
                zp=str(out)+'.zip'
                with zipfile.ZipFile(zp,'w',zipfile.ZIP_DEFLATED) as zf:
                    for item in done:
                        if os.path.exists(item['path']): zf.write(item['path'],item['rel'])
                zb=base64.b64encode(open(zp,'rb').read()).decode()
                eid=save_journal({
                    'orgName': data.get('ai_data',{}).get('company',{}).get('name','') or data.get('orgName',''),
                    'implDate': data.get('ai_data',{}).get('dates',{}).get('implementation_date','') or data.get('implDate',''),
                    'fileCount':len(done),'zipPath':zp
                })
                self._json({'success':True,'fileCount':len(done),'journalId':eid,'zip':zb})
            elif p=='/api/journal/delete':
                eid=json.loads(body)['id']
                for f in JOURNAL_DIR.glob('*.json'):
                    try:
                        e=json.loads(f.read_text('utf-8'))
                        if e.get('id')==eid:
                            zp=e.get('zipPath')
                            if zp and os.path.exists(zp): os.remove(zp)
                            f.unlink(); break
                    except: pass
                self._json({'success':True})
            else: self.send_response(404); self.end_headers()
        except Exception as e:
            import traceback
            self._json({'success':False,'error':str(e),'trace':traceback.format_exc()},500)

    def _json(self,d,code=200):
        b=json.dumps(d,ensure_ascii=False).encode('utf-8')
        self.send_response(code)
        self.send_header('Content-Type','application/json; charset=utf-8')
        self.send_header('Content-Length',str(len(b)))
        self.end_headers(); self.wfile.write(b)

    def _html(self,h):
        b=h.encode('utf-8')
        self.send_response(200)
        self.send_header('Content-Type','text/html; charset=utf-8')
        self.send_header('Content-Length',str(len(b)))
        self.end_headers(); self.wfile.write(b)

    def do_OPTIONS(self): self.send_response(200); self.end_headers()


if __name__=='__main__':
    print(f'\n✅  ИСО/СМК Генератор: http://localhost:{PORT}')
    print(f'   Откройте в браузере | Ctrl+C для остановки\n')
    class ThreadedServer(socketserver.ThreadingMixIn, socketserver.TCPServer):
        allow_reuse_address=True
    with ThreadedServer(('',PORT),H) as s:
        try: s.serve_forever()
        except KeyboardInterrupt: print('\n⏹  Остановлен')
