import io
import sys
import time
import zipfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import generator
import server
from generator_iso_suot_templates import generate_iso_suot_package_v2


def xml_text(doc_bytes):
    with zipfile.ZipFile(io.BytesIO(doc_bytes)) as z:
        return '\n'.join(
            z.read(n).decode('utf-8', 'ignore')
            for n in z.namelist()
            if n.startswith('word/') and n.endswith('.xml')
        )


def sample_data():
    return {
        'company': {
            'name': 'ОмиТрейд', 'form': 'ООО', 'unp': '790798634',
            'address': '212001, г. Могилев, ул. Кирова, 26Б/4', 'city': 'Могилев',
            'director_fio': 'Глушинский Олег Иванович', 'director_position': 'Директор',
            'scope': 'СТАРАЯ ОБЛАСТЬ КОМПАНИИ',
        },
        'certification': {
            'standard': 'iso_suot',
            'scope': 'Текущий ремонт и отделочные работы',
            'audit_date': '20.08.2026',
        },
        'staff': [
            {'fio':'Глушинский Олег Иванович','position':'Директор','is_worker':False,'ot_certificate':True},
            {'fio':'Лукашик Сергей Васильевич','position':'Заместитель директора-главный инженер','is_worker':False,'ot_certificate':True,'employment_type':'совместительство'},
            {'fio':'Волков Василий Анатольевич','position':'Производитель работ','is_worker':False,'ot_certificate':True},
            {'fio':'Грук Сергей Борисович','position':'Штукатур','is_worker':True},
        ],
        'workers': ['Штукатур'],
        'suppliers': [{'name':'РБК','type':'Плиты, профиль, смеси'}],
        'objects': [{'name':'Текущий ремонт поликлиники','year':'2026'}],
    }


def test_fast_deterministic_iso_suot_package():
    data = sample_data()
    company = dict(data['company'])
    company['scope'] = data['certification']['scope']
    itr = [x for x in data['staff'] if not x['is_worker']]
    workers = [x for x in data['staff'] if x['is_worker']]
    dates = generator.calculate_dates(data['certification']['audit_date'])
    resp = generator.select_responsible(itr)
    started = time.time()
    result = generate_iso_suot_package_v2(
        company, itr, dates, resp, product='iso_suot', workers=workers,
        suppliers=data['suppliers'], objects=data['objects']
    )
    assert time.time() - started < 10
    assert result['docs']
    assert not result.get('warnings')
    assert not any('Варта' in d['name'] for d in result['docs'])
    combined = '\n'.join(xml_text(d['bytes']) for d in result['docs'])
    assert 'Варта' not in combined
    assert '[Наименование]' not in combined
    assert '[Форма_собственности_сокращенная]' not in combined
    assert 'СТАРАЯ ОБЛАСТЬ КОМПАНИИ' not in combined
    assert 'Текущий ремонт и отделочные работы' in combined
    assert 'Лукашик Сергей Васильевич' in combined
    assert 'Волков Василий Анатольевич' in combined
    assert any('ИОТ Штукатур' in d['name'] for d in result['docs'])
    assert any('Карточка оценки поставщика' in d['name'] for d in result['docs'])
    assert not any('валидац' in d['name'].lower() for d in result['docs'])


def test_suot_report_has_current_scope_and_current_staff_count():
    data = sample_data()
    company = dict(data['company']); company['scope'] = data['certification']['scope']
    itr = [x for x in data['staff'] if not x['is_worker']]
    workers = [x for x in data['staff'] if x['is_worker']]
    result = generate_iso_suot_package_v2(
        company, itr, generator.calculate_dates('20.08.2026'), generator.select_responsible(itr),
        product='suot', workers=workers
    )
    report = next(d for d in result['docs'] if 'Отчет для анализа OH&S' in d['name'])
    source = next(d for d in result['docs'] if 'Исходная информация СУОТ' in d['name'])
    assert data['certification']['scope'] in xml_text(report['bytes'])
    source_xml = xml_text(source['bytes'])
    assert '4 чел.' in source_xml or '>4 чел.<' in source_xml
    assert '17 человек' not in source_xml


def test_company_att_itr_is_enriched_from_staff():
    att = {'itr': [{'fio':'Иванов Иван Иванович','position':'Главный инженер'}]}
    staff = [{
        'fio':'Иванов Иван Иванович','position':'Главный инженер',
        'education_full_text':'Высшее, БНТУ, диплом А №123',
        'trudovye_numbers':['ПК №123','Вкладыш №456'],
        'employment_periods':[{'start':'01.01.2010','end':'01.01.2020','position':'Прораб'}],
    }]
    merged = generator._merge_company_att_itr_from_staff(att, staff)
    p = merged['itr'][0]
    assert 'БНТУ' in p['education_full_text']
    assert p['trudovye_numbers'] == ['ПК №123','Вкладыш №456']
    assert p['employment_periods']


def test_iso_fast_archive_skips_ai_reconciliation(tmp_path):
    # text-only ZIP; if fast mode accidentally makes a reconciliation AI call, fail.
    zbuf = io.BytesIO()
    with zipfile.ZipFile(zbuf, 'w', zipfile.ZIP_DEFLATED) as z:
        z.writestr('Список сотрудников.txt', 'Иванов Иван Иванович | Директор\nПетров Петр Петрович | Штукатур')
        z.writestr('Перечень объектов.txt', 'Объект: ремонт офиса')
    old_company = server._extract_company_details
    old_person = server._reconcile_person_summary
    try:
        server._extract_company_details = lambda *a, **k: (_ for _ in ()).throw(AssertionError('AI company reconciliation called'))
        server._reconcile_person_summary = lambda *a, **k: (_ for _ in ()).throw(AssertionError('AI person reconciliation called'))
        result = server.extract_archive_with_vision(zbuf.getvalue(), 'исо.zip', 'dummy', product='iso_suot')
    finally:
        server._extract_company_details = old_company
        server._reconcile_person_summary = old_person
    assert 'Иванов' in result['text']
    assert 'ремонт офиса' in result['analysis_text']


def test_no_legacy_iso_ai_fallback(monkeypatch):
    import generator_iso_suot_templates as mod
    data = sample_data()
    called = {'legacy': False}
    monkeypatch.setattr(mod, 'generate_iso_suot_package_v2', lambda *a, **k: (_ for _ in ()).throw(RuntimeError('boom')))
    monkeypatch.setattr(generator, '_gen_iso', lambda *a, **k: called.__setitem__('legacy', True))
    result = generator.generate_package(data, 'dummy', 'iso')
    assert result.get('error')
    assert called['legacy'] is False
