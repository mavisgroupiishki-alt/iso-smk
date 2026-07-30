from __future__ import annotations

from copy import deepcopy
from io import BytesIO
from pathlib import Path
import re
import sys
import zipfile

BASE = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(BASE))

from company_attestation_source_parser import parse_company_attestation_docx  # noqa: E402
from generator_company_att import calculate_person_experience, calculate_stazh  # noqa: E402
from generator_company_att_templates import generate_company_attestation_package_v2  # noqa: E402


def doc_xml(blob: bytes) -> str:
    with zipfile.ZipFile(BytesIO(blob)) as archive:
        return archive.read('word/document.xml').decode('utf-8')


def complete_person() -> dict:
    return {
        'fio': 'Иванов Иван Иванович',
        'position': 'Производитель работ (прораб)',
        'education_full_text': [
            'Высшее', 'Диплом АА № 123456 выдан 15.06.2009 г.',
            'Белорусский национальный технический университет',
            'Промышленное и гражданское строительство', 'Инженер-строитель',
        ],
        'diploma_numbers': ['АА № 123456'],
        'trudovye_numbers': ['Трудовая книжка ПК № 1234567'],
        'trudovaya_form2_text': [
            'Трудовая книжка ПК № 1234567', 'Приказ № 5-к от 01.02.2024 г.',
        ],
        'attestat_number': 'СТ № 123456',
        'attestat_form2_text': [
            'СТ № 123456 от 01.06.2025 г.',
            'Производитель работ (прораб) (общестроительные работы)',
        ],
        'attestat_form5_text': [
            'СТ № 123456 с 01.06.2025 г. по 01.06.2030 г.',
            'Производитель работ (прораб) (общестроительные работы)',
        ],
        'attestat_specialization': 'Производитель работ (прораб) (общестроительные работы)',
        'employment_periods': [
            {
                'start': '01.01.2010', 'end': '31.12.2015',
                'position': 'Производитель работ', 'employer': 'Строймонтаж',
                'is_construction_employer': True, 'confidence': 1.0,
            },
            {
                # Deliberate overlap: 2015 must not be counted twice.
                'start': '01.01.2015', 'end': '31.12.2016',
                'position': 'Прораб', 'employer': 'Строймонтаж',
                'is_construction_employer': True, 'confidence': 1.0,
            },
            {
                'start': '01.02.2024', 'end': None,
                'position': 'Производитель работ (прораб)',
                'employer': 'ООО «Новая Строй»', 'employer_unp': '123456789',
                'is_construction_employer': True, 'is_current_employer': True,
                'confidence': 1.0,
            },
        ],
    }


def test_exact_calculation_and_overlap() -> None:
    person = complete_person()
    company = {'name': 'ООО «Новая Строй»', 'unp': '123456789'}
    calculate_person_experience(person, company, '30.07.2026')
    assert person['stage_source'] == 'calculated'
    assert person['stage_is_final'] is True, person['stage_review_reasons']
    assert person['stage_years'] == '9 лет 7 месяцев 18 дней'
    assert person['stage_years_here'] == '2 года 6 месяцев 11 дней'
    intervals = person['stage_calculation']['total']['intervals']
    assert intervals[0] == {'start': '01.01.2010', 'end': '31.12.2016'}


def test_form2_reference_is_never_used_as_result() -> None:
    person = {
        'fio': 'Горбунов Олег Васильевич',
        'position': 'Заместитель директора-главный инженер',
        'stage_reference_total': '49 лет',
        'stage_reference_here': 'Менее года',
        'employment_periods': [],
    }
    calculate_person_experience(person, {'name': 'ООО «Новая Строй»'}, '30.07.2026')
    assert person['stage_years'] == ''
    assert person['stage_years_here'] == ''
    assert person['stage_source'] == 'missing'
    assert person['stage_calculation']['reference_used_as_value'] is False
    assert person['stage_needs_review'] is True


def test_partial_dates_and_worker_roles_do_not_enter_stage() -> None:
    person = {
        'fio': 'Петров Пётр Петрович',
        'position': 'Главный инженер',
        'employment_periods': [
            {
                'start': '', 'start_text': '1986 год', 'end': '31.12.1990',
                'position': 'Главный инженер', 'employer': 'Стройтрест',
                'is_construction_employer': True, 'uncertain_fields': ['start'],
            },
            {
                'start': '01.01.2000', 'end': '31.12.2005',
                'position': 'Каменщик', 'employer': 'Стройтрест',
                'is_construction_employer': True, 'confidence': 1.0,
            },
        ],
    }
    calculate_person_experience(person, {'name': 'ООО «Новая Строй»'}, '30.07.2026')
    assert person['stage_years'] == ''
    assert person['stage_needs_review'] is True
    reasons = ' '.join(person['stage_review_reasons'])
    assert 'полной подтверждённой даты' in reasons


def test_plain_director_is_not_chief_engineer_stage() -> None:
    person = {
        'fio': 'Сидоров Сидор Сидорович',
        'position': 'Главный инженер',
        'attestat_specialization': 'Главный инженер (общестроительные работы)',
        'employment_periods': [
            {
                'start': '01.01.2010', 'end': '31.12.2020',
                'position': 'Директор', 'employer': 'ООО Строй',
                'is_construction_employer': True, 'confidence': 1.0,
            },
        ],
    }
    calculate_person_experience(person, {'name': 'ООО «Новая Строй»'}, '30.07.2026')
    assert person['stage_years'] == ''
    excluded = person['stage_calculation']['excluded_periods']
    assert any('без перевода/совмещения' in item['reason'] for item in excluded)


def test_form2_parser_keeps_stage_as_reference_only() -> None:
    path = BASE / 'att_templates' / '2__ИТР.docx'
    fragment = parse_company_attestation_docx(path.read_bytes(), '2. ИТР.docx')
    people = fragment['company_attestation']['itr']
    gorbunov = next(p for p in people if 'Горбунов' in p.get('fio', ''))
    assert gorbunov['stage_reference_total'] == '49 лет'
    assert gorbunov['stage_reference_here'] == 'Менее года'
    assert not gorbunov.get('stage_years')
    assert gorbunov['stage_is_final'] is False


def test_yellow_review_and_clean_calculated_stage() -> None:
    company = {
        'name': 'Новая Строй', 'form': 'ООО', 'unp': '123456789',
        'address': '220000, г. Минск, ул. Строителей, 1',
        'bank_account': 'BY00TEST00000000000000000000',
        'bank_name': 'ОАО «ТестБанк»', 'bik': 'TESTBY2X',
        'phone': '+375 29 000-00-00', 'email': 'office@example.by',
        'director_fio': 'Иванов Иван Иванович', 'director_position': 'Директор',
        'signature_date': '30.07.2026', 'outgoing_number': '15', 'outgoing_date': '30.07.2026',
    }
    good = complete_person()
    att = {
        'itr': [good],
        'work_items': ['7.4.1'],
        'workers': [{'profession': 'Каменщик', 'razryad': 'IV', 'count': 1, 'source': 'document'}],
        'as_of_date': '30.07.2026',
        'attachment_page_counts': {'form2': 2, 'form3': 1, 'form4': 1, 'form5': 1, 'total': 5},
    }
    result = generate_company_attestation_package_v2(deepcopy(company), deepcopy(att))
    form2 = next(d['bytes'] for d in result['docs'] if 'Форма №2' in d['name'])
    xml = doc_xml(form2)
    assert '9 лет 7 месяцев 18 дней' in xml
    # Complete calculated stage must not be yellow.
    stage_run = re.search(r'<w:r\b.*?9 лет 7 месяцев 18 дней.*?</w:r>', xml, re.DOTALL)
    assert stage_run and 'w:highlight w:val="yellow"' not in stage_run.group(0)

    bad = complete_person()
    bad['employment_periods'][0]['start'] = ''
    bad['employment_periods'][0]['start_text'] = '2010 год'
    bad['employment_periods'][0]['uncertain_fields'] = ['start']
    bad['diploma_numbers'] = []
    bad['education_full_text'] = []
    bad['uncertain_fields'] = ['education']
    bad_att = deepcopy(att)
    bad_att['itr'] = [bad]
    bad_company = deepcopy(company)
    bad_company['phone'] = ''
    bad_company.pop('outgoing_number')
    bad_company.pop('outgoing_date')
    bad_att.pop('attachment_page_counts')
    bad_result = generate_company_attestation_package_v2(bad_company, bad_att)
    all_xml = '\n'.join(doc_xml(d['bytes']) for d in bad_result['docs'])
    assert 'ТРЕБУЕТ УТОЧНЕНИЯ' in all_xml
    assert 'w:highlight w:val="yellow"' in all_xml
    assert 'w:fill="FFF2CC"' in all_xml


def main() -> None:
    test_exact_calculation_and_overlap()
    test_form2_reference_is_never_used_as_result()
    test_partial_dates_and_worker_roles_do_not_enter_stage()
    test_plain_director_is_not_chief_engineer_stage()
    test_form2_parser_keeps_stage_as_reference_only()
    test_yellow_review_and_clean_calculated_stage()
    print('V8 COMPANY ATTESTATION TESTS PASSED')


if __name__ == '__main__':
    main()
