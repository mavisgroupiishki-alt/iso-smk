import io
import zipfile

import generator
from generator_spk_templates import generate_spk_package_v2


def _xml_text(doc_bytes):
    with zipfile.ZipFile(io.BytesIO(doc_bytes)) as archive:
        return '\n'.join(
            archive.read(name).decode('utf-8', 'ignore')
            for name in archive.namelist()
            if name.startswith('word/') and name.endswith('.xml')
        )


def _document(result, marker):
    return next(doc for doc in result['docs'] if marker in doc['name'])


def test_spk_bisp_static_templates_use_package_dates_and_never_keep_sample_certificate():
    dates = generator.calculate_dates('17.09.2026')
    company = {
        'name': 'Тестовая организация', 'form': 'ООО', 'city': 'Минск',
        'address': 'г. Минск', 'director_fio': 'Иванов Иван Иванович',
        'director_position': 'Директор', 'phone': '+375291234567',
    }
    itr = [
        {'fio': 'Иванов Иван Иванович', 'position': 'Директор'},
        {'fio': 'Петров Петр Петрович', 'position': 'Главный инженер'},
        {'fio': 'Сидоров Сидор Сидорович', 'position': 'Производитель работ'},
    ]
    result = generate_spk_package_v2(
        company, itr, [], dates, generator.select_responsible(itr),
        variant='spk_bisp', spk_data={'measurement_tools': []},
    )

    documents = {doc['name']: _xml_text(doc['bytes']) for doc in result['docs']}
    combined = '\n'.join(documents.values())
    assert '27.05.2026' not in combined
    assert '12.06.2026' not in combined
    assert '05890263.1245-2021' not in combined

    assert dates['policy'] in documents[next(name for name in documents if '5 Положение о СПК' in name)]
    assert dates['goals'] in documents[next(name for name in documents if '6 Паспорт СПК' in name)]
    assert dates['reports'] in documents[next(name for name in documents if '8 Справка СИ' in name)]
    assert dates['policy'] in documents[next(name for name in documents if '5.2 Положение о входном контроле' in name)]
    assert dates['goals'] in documents[next(name for name in documents if 'График поверки СИ' in name)]
    assert 'ТРЕБУЕТ УТОЧНЕНИЯ: свидетельство о технической компетентности' in documents[next(name for name in documents if '6 Паспорт СПК' in name)]
