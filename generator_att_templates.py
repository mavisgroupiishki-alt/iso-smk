"""
Генератор заявления на аттестацию СПЕЦИАЛИСТА (продукт "att") — НАСТОЯЩИЙ шаблон.

Тот же принцип, что и для company_att: берём реальный поданный и принятый docx
(атт_Хмара_В.В.) буквально как есть, меняем только текст в конкретных абзацах.

Требует: att_spec_templates/1_zayavlenie.docx рядом с этим файлом.
"""
import re, io, zipfile
from pathlib import Path

BASE_DIR = Path(__file__).parent.resolve()
TPL_DIR = BASE_DIR / 'att_spec_templates'


def _load_parts(filename: str) -> dict:
    path = TPL_DIR / filename
    if not path.exists():
        raise FileNotFoundError(f"Не найден шаблон {path}.")
    parts = {}
    with zipfile.ZipFile(path) as z:
        for name in z.namelist():
            parts[name] = z.read(name)
    return parts


def _rebuild(parts: dict) -> bytes:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, 'w', zipfile.ZIP_DEFLATED) as zf:
        for name, data in parts.items():
            zf.writestr(name, data)
    return buf.getvalue()


def _paragraphs(xml: str) -> list:
    return re.findall(r'<w:p\b.*?</w:p>', xml, re.DOTALL)


def _esc(s) -> str:
    return (str(s) if s not in (None, '') else '').replace('&', '&amp;').replace('<', '&lt;').replace('>', '&gt;')


def _replace_para_text(para_xml: str, new_text: str) -> str:
    m = re.search(r'(<w:r\b[^P].*?)<w:t[^>]*>.*?</w:t>(.*?</w:r>)', para_xml, re.DOTALL)
    if not m:
        return para_xml
    run_prefix, run_suffix = m.group(1), m.group(2)
    new_run = f'{run_prefix}<w:t xml:space="preserve">{_esc(new_text)}</w:t>{run_suffix}'
    if '</w:pPr>' in para_xml:
        p_open_end = para_xml.find('</w:pPr>') + len('</w:pPr>')
    else:
        p_open_end = para_xml.find('>') + 1
    return para_xml[:p_open_end] + new_run + '</w:p>'


def _find_para_index(paras: list, predicate) -> int:
    for i, p in enumerate(paras):
        if predicate(re.sub(r'<[^>]+>', '', p).strip()):
            return i
    return -1


# ═══════════════════ Склонение русских ФИО (лучшее приближение) ═══════════════════
def _decline_word(word: str, case: str, is_first_name_or_patronymic: bool = False) -> str:
    """Best-effort склонение одного слова (фамилия/имя/отчество) для male ФИО.
    case: 'nom'(именительный, как есть) | 'acc'(винительный) | 'dat'(дательный).
    is_first_name_or_patronymic: важно для слов на -ий — ФАМИЛИЯ (Потоцкий) склоняется
    как прилагательное (Потоцкого), а ИМЯ (Юрий, Дмитрий, Василий) как обычное
    существительное (Юрия, Дмитрия, Василия) — то же окончание, разное правило."""
    if case == 'nom' or not word:
        return word
    w = word
    # Фамилии-прилагательные на -ий/-ый/-ой (Потоцкий, Достоевский, Толстой,
    # Чайковский) — склоняются как прилагательные: -ий/-ый -> -ого, -ому.
    # НЕ применяем это к именам/отчествам (Юрий, Дмитрий, Василий — другое правило).
    if not is_first_name_or_patronymic and w.endswith(('ий', 'ый', 'ой')):
        stem = w[:-2]
        return stem + 'ого' if case == 'acc' else stem + 'ому'
    # Отчества на -ович/-евич
    if w.endswith('ович') or w.endswith('евич'):
        return w + 'а' if case == 'acc' else w + 'у'
    # Имена на -ий/-ай/-ей (Юрий, Дмитрий, Николай, Андрей, Сергей) -> -ия/-ая/-ея + я/ю
    if w.endswith('й'):
        stem = w[:-1]
        return stem + 'я' if case == 'acc' else stem + 'ю'
    # Имена/отчества на согласную (Валентин, Александр, Владимир...)
    if w[-1] not in 'аяеёиоуыэюй' and not w.endswith('ь'):
        return w + 'а' if case == 'acc' else w + 'у'
    # Слова на -ь (мужские: Игорь -> Игоря/Игорю)
    if w.endswith('ь'):
        stem = w[:-1]
        return stem + 'я' if case == 'acc' else stem + 'ю'
    # Фамилии/имена на -а/-я (Кузьма, Никита, Хмара как фамилия) -> -у/-е
    if w.endswith('а'):
        return w[:-1] + 'у' if case == 'acc' else w[:-1] + 'е'
    if w.endswith('я'):
        return w[:-1] + 'ю' if case == 'acc' else w[:-1] + 'е'
    # Несклоняемые окончания (-их, -ых, -ко и т.п.) — оставляем как есть
    if w.endswith(('их', 'ых', 'ко', 'аго')):
        return w
    return w  # по умолчанию — не трогаем, лучше не склонять неправильно чем сломать


def decline_fio(fio: str, case: str) -> str:
    """Склоняет полное ФИО ('Фамилия Имя Отчество') в указанный падеж.
    Первое слово считается фамилией (может быть прилагательного типа), остальные —
    имя/отчество (другое правило для окончаний на -ий)."""
    parts = fio.strip().split()
    if len(parts) < 2 or case == 'nom':
        return fio
    declined = [_decline_word(p, case, is_first_name_or_patronymic=(i > 0))
                for i, p in enumerate(parts)]
    return ' '.join(declined)


def _dir_initials(fio: str) -> str:
    parts = (fio or '').strip().split()
    if len(parts) >= 3:
        return f"{parts[1][0]}.{parts[2][0]}. {parts[0]}"
    return fio or ''


LEGAL_FORMS_FULL = {
    'ООО': 'Общество с ограниченной ответственностью',
    'ОДО': 'Общество с дополнительной ответственностью',
    'ЧУП': 'Частное унитарное предприятие',
    'ЗАО': 'Закрытое акционерное общество',
    'ОАО': 'Открытое акционерное общество',
    'ИП': 'Индивидуальный предприниматель',
}


# ═══════════════════ Заявление на аттестацию специалиста ═══════════════════
def render_zayavlenie_spec(company: dict, person: dict, request: dict) -> bytes:
    """
    company: {form, name, reg_address, unp, bank_account, bank_name, bik, phone,
              email, post_address, director_position, director_fio}
    person: {fio, diploma_speciality, diploma_qualification, id_doc_type,
             passport_series, passport_number, id_number, issued_by,
             address, phone, email, diploma_number}
    request: {specialization, grade}  # grade не используется в тексте, только для файла
    """
    parts = _load_parts('1_zayavlenie.docx')
    xml = parts['word/document.xml'].decode('utf-8')
    paras = _paragraphs(xml)

    form = (company.get('form') or 'ООО').upper()
    legal_full = LEGAL_FORMS_FULL.get(form, form)
    full_name = f'{legal_full} «{company.get("name","")}»'
    short_name = f'{form} «{company.get("name","")}»'

    fio_acc = decline_fio(person.get('fio', ''), 'acc')  # "Хмару Валентина Валентиновича"
    fio_dat = decline_fio(person.get('fio', ''), 'dat')  # "Хмаре Валентину Валентиновичу"
    dir_init = _dir_initials(company.get('director_fio', ''))

    replacements = {
        6: full_name,
        10: short_name,
        13: company.get('reg_address', ''),
        17: company.get('unp', ''),
        20: f"р/с: {company.get('bank_account','')}",
        21: f"в {company.get('bank_name','')}, БИК {company.get('bik','')}",
        24: company.get('phone', ''),
        26: company.get('email', ''),
        28: company.get('post_address') or company.get('reg_address', ''),
        32: f"{company.get('director_position','Директора')} {company.get('director_fio','')}, на основании Устава",
        42: fio_acc,
        45: person.get('diploma_speciality', ''),
        47: person.get('diploma_qualification', ''),
        52: person.get('passport_series', ''),
        54: person.get('passport_number', ''),
        56: person.get('id_number', ''),
        58: person.get('issued_by', ''),
        61: person.get('address', ''),
        63: person.get('phone') or company.get('phone', ''),
        65: person.get('email') or company.get('email', ''),
        70: request.get('specialization', ''),
        74: fio_dat,
        82: f"1. Копия диплома о высшем образовании {person.get('diploma_number','')}",
        84: f"2. Копия трудовой книжки {person.get('trudovaya_number','б/н')}",
        94: dir_init,
    }

    for i, new_text in replacements.items():
        if i >= len(paras):
            continue
        old_para = paras[i]
        if old_para not in xml or not new_text:
            continue
        new_para = _replace_para_text(old_para, new_text)
        xml = xml.replace(old_para, new_para, 1)

    parts['word/document.xml'] = xml.encode('utf-8')
    return _rebuild(parts)
