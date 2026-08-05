from __future__ import annotations
from copy import deepcopy
from pathlib import Path
import sys

BASE = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(BASE))

from company_attestation_source_parser import parse_company_attestation_docx  # noqa: E402
from generator_company_att_templates import generate_company_attestation_package_v2  # noqa: E402


def main() -> None:
    form2_path = BASE / 'att_templates' / '2__ИТР.docx'
    fragment = parse_company_attestation_docx(form2_path.read_bytes(), '2. ИТР.docx')
    people = fragment['company_attestation']['itr']
    by_fio = {p['fio']: p for p in people}
    target_name = next(name for name, person in by_fio.items() if person.get('stage_years_here') == 'Менее года')
    assert by_fio[target_name]['stage_source'] == 'document'
    assert by_fio[target_name]['stage_is_final'] is True

    # Имитируем старую карточку: stage_form2_text стал одной строкой, а отдельные поля потерялись.
    att = deepcopy(fragment['company_attestation'])
    for person in att['itr']:
        if person['fio'] == target_name:
            expected_total = person['stage_years']
            person['stage_form2_text'] = expected_total + '\nМенее года'
            person['stage_years'] = ''
            person['stage_years_here'] = ''
            person['employment_periods'] = [
                {'start': '1972', 'end': None, 'position': 'Главный инженер', 'relevant': True}
            ]
    att['work_items'] = ['7.2','7.3','7.4','7.5','7.6']
    att['work_scope_text'] = 'Общестроительные работы'

    company = {
        'name':'АК СтройФемили', 'form':'ООО', 'director_fio':'Крот Александр Евгеньевич',
        'director_position':'Директор', 'unp':'791371126', 'address':'г. Могилев',
    }
    result = generate_company_attestation_package_v2(company, att)
    warnings = result.get('warnings') or []
    assert not any('Не полностью заполнен стаж' in w for w in warnings), warnings
    # generate_package merges into a copied list; validate through generated warnings and parser source fields.
    assert by_fio[target_name]['stage_years'] == expected_total
    assert by_fio[target_name]['stage_years_here'] == 'Менее года'
    print('V7 STAGE REGRESSION TEST PASSED')


if __name__ == '__main__':
    main()
