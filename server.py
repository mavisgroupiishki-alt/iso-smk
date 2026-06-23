#!/usr/bin/env python3
"""ИСО/СМК Генератор с ИИ-оформителем. Запуск: python server.py → http://localhost:8766"""
import sys,json,os,shutil,tempfile,base64,zipfile,re,requests as req_lib
import http.server,socketserver
from pathlib import Path
from datetime import datetime,timedelta

BASE_DIR = Path(__file__).parent.resolve()
TPL_DIR  = BASE_DIR/'templates'/'ISO_shablon'/'ИСО ЭнергоМагистраль'

# Render: без persistent disk — храним в /tmp или рядом с приложением
_DATA = Path('/data') if Path('/data').exists() else BASE_DIR/'_data'
JOURNAL_DIR = _DATA/'journal'
CO_DIR      = _DATA/'companies'
OUT_DIR     = _DATA/'output'
PORT = int(os.environ.get("PORT", 8766))

for d in [JOURNAL_DIR, CO_DIR, OUT_DIR]: d.mkdir(parents=True, exist_ok=True)

# ── Vibe Code AI ─────────────────────────────────────────────
VIBE_URL   = "https://vibecode.bitrix24.tech/v1/ai/chat/completions"
VIBE_MODEL = "bitrix/bitrixgpt-5.5"

AI_SYSTEM = """Ты — ИИ-оформитель документов ИСО/СМК для компании Mavis Group (Беларусь).

ТВОЯ РОЛЬ: анализируешь входящие данные от эксперта, сам решаешь кто куда идёт в документах, проверяешь корректность и задаёшь вопросы если чего-то не хватает. Принимаешь правки на человеческом языке.

ПРОДУКТЫ которые ты умеешь оформлять:
- ISO (ИСО) — ISO 9001, ISO 45001 или оба вместе
- СПК — Свидетельство о технической компетентности (два варианта: Строй Комплекс и БИСП)
- СУОТ — Система управления охраной труда (ISO 45001)
- Периодика — обновление части документов для ISO/СПК/СУОТ

ИЗВЛЕКАЙ И СТРУКТУРИРУЙ из текста эксперта:
1. Реквизиты: название компании, форма (ООО/ЧУП/ОДО), УНП, юр.адрес, город, ФИО директора, должность директора, email
2. Сертификация: стандарт (ISO 9001 / ISO 45001 / оба), область (дословно как в заявке), орган сертификации
3. Даты: дата выезда эксперта органа → дата разработки = выезд минус 14 дней; дата внедрения = выезд минус 7 дней
4. Сотрудники: ФИО, должность, роль (director/auditor/responsible/itr), удостоверения ОТ с датами (для ISO 45001 нужно минимум 3)
5. Объекты (2-3): название, год, заказчик/контрагент
6. Поставщики (3-5): название, тип

ПРАВИЛА НАЗНАЧЕНИЯ РОЛЕЙ:
- Аудиторы = ИТР сотрудники (не директор)
- Ответственный за ФНПА = главный инженер или зам директора
- Если два удостоверения ОТ у одного — берём более свежее
- Область в документах должна совпадать с заявкой слово в слово
- Строительство в области без строительного аттестата = критический флаг

ПРИНИМАЙ ПРАВКИ: "исправь директора на Иванов И.И.", "добавь объект", "поменяй дату" — обновляй данные и подтверждай изменение.

ОТВЕЧАЙ СТРОГО JSON без обёрток ```json:
{
  "message": "текст ответа оформителю (по-русски, коротко и профессионально)",
  "questions": ["вопрос если чего-то не хватает"],
  "data": {
    "company": {"name":"","form":"","unp":"","address":"","city":"","director_fio":"","director_position":"","email":""},
    "certification": {"standard":"","scope":"","body":"","audit_date":""},
    "dates": {"audit_date":"","development_date":"","implementation_date":""},
    "staff": [{"fio":"","position":"","role":"director|auditor|responsible|itr","ot_certificate":false,"ot_certificate_date":""}],
    "objects": [{"name":"","year":"","customer":""}],
    "suppliers": [{"name":"","type":""}],
    "flags": [{"type":"error|warning|ok","text":""}],
    "product": "iso|spk_stroy|spk_bisp|suot|iso_suot|periodika","readiness": "waiting|partial|review|ready"
  }
}
Включай только заполненные поля. Пустые не включай."""


def call_ai(messages, api_key):
    """Вызов BitrixGPT через Vibe Code API"""
    resp = req_lib.post(
        VIBE_URL,
        headers={"Content-Type":"application/json","X-Api-Key":api_key},
        json={"model":VIBE_MODEL,"max_tokens":3000,"messages":[
            {"role":"system","content":AI_SYSTEM},
            *messages[-10:]
        ]},
        timeout=60
    )
    resp.raise_for_status()
    data = resp.json()
    if "error" in data:
        raise RuntimeError(data["error"])
    return "".join(c.get("message",{}).get("content","") for c in data.get("choices",[]))


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
def replace_in_docx(src, dst, reps):
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
            text = file_bytes.decode('latin-1', errors='replace')
            import re as _re
            blocks = _re.findall(r'BT(.*?)ET', text, _re.DOTALL)
            result = []
            for b in blocks:
                strings = _re.findall(r'\(([^)]{2,})\)', b)
                result.extend(strings)
            if result:
                return ' '.join(result)[:8000]
            return '[PDF: не удалось извлечь текст]'

        elif ext in ('xlsx', 'xls'):
            import io
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
            except: pass
            return '[xlsx: не удалось прочитать]'

        elif ext in ('zip',):
            return _extract_archive_zip(file_bytes, filename, _depth)

        elif ext == 'rar':
            return _extract_archive_rar(file_bytes, filename, _depth)

    except Exception as e:
        return f'[Ошибка чтения {filename}: {e}]'

    return '[Неизвестный формат файла]'


def _extract_archive_zip(file_bytes, filename, _depth=0):
    """Рекурсивно распаковывает ZIP включая вложенные архивы"""
    import io
    texts = []
    READABLE = ('docx','doc','txt','csv','xlsx','pdf','zip','rar')
    try:
        with zipfile.ZipFile(io.BytesIO(file_bytes)) as z:
            for name in z.namelist():
                if name.endswith('/'): continue
                inner_ext = name.rsplit('.',1)[-1].lower() if '.' in name else ''
                if inner_ext not in READABLE: continue
                try:
                    inner_bytes = z.read(name)
                    fn = name.split('/')[-1].split('\\')[-1]
                    # Рекурсия для вложенных архивов
                    inner_text = extract_text_from_file(inner_bytes, fn, _depth+1)
                    if inner_text and len(inner_text) > 10 and not inner_text.startswith('['):
                        texts.append('--- ' + fn + ' ---\n' + inner_text)
                except: pass
        if texts:
            return '\n\n'.join(texts)[:12000]
        return '[zip: читаемых файлов не найдено]'
    except Exception as e:
        return f'[zip ошибка: {e}]'


def _extract_archive_rar(file_bytes, filename, _depth=0):
    """Распаковывает RAR через rarfile или эвристику"""
    import io
    texts = []
    READABLE = ('docx','doc','txt','csv','xlsx','pdf','zip','rar')
    try:
        import rarfile as _rar
        rf = _rar.RarFile(io.BytesIO(file_bytes))
        for name in rf.namelist():
            inner_ext = name.rsplit('.',1)[-1].lower() if '.' in name else ''
            if inner_ext not in READABLE: continue
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
                                with open(_os.path.join(root, fn), 'rb') as f_in:
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

    def do_GET(self):
        p=self.path.split('?')[0]
        if p in('/','//index.html'):          self._html(INDEX)
        elif p=='/api/companies':             self._json(get_companies())
        elif p=='/api/journal':               self._json(get_journal())
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
            if p=='/api/extract-text':
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
                text = extract_text_from_file(file_bytes, filename)
                self._json({'success':True,'text':text,'filename':filename})

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
