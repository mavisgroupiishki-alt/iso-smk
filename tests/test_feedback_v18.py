import io, re, zipfile

from generator import generate_package
from generator_company_att import calculate_person_experience


def _visible(docx_bytes: bytes) -> str:
    with zipfile.ZipFile(io.BytesIO(docx_bytes)) as z:
        xml = z.read('word/document.xml').decode('utf-8', errors='ignore')
    return ' '.join(re.findall(r'<w:t[^>]*>(.*?)</w:t>', xml))


def test_att_stage_uses_exact_labour_dates_without_global_ocr_yellow():
    person = {
        'fio': 'Иванов Иван Иванович',
        'position': 'Производитель работ',
        'needs_review': True,
        'confidence': 0.6,  # unrelated generic OCR confidence
        'employment_periods': [
            {'start': '01.02.2020', 'end': '31.12.2022', 'position': 'Производитель работ', 'employer': 'ООО Строй', 'confidence': 0.6},
            {'start': '01.01.2023', 'end': None, 'position': 'Производитель работ', 'employer': 'ООО Тест', 'is_current_employer': True, 'confidence': 0.7},
        ],
    }
    calculate_person_experience(person, {'name': 'Тест'}, '21.08.2026')
    assert person['stage_years']
    assert person['stage_years_here']
    assert person['stage_needs_review'] is False


def test_spk_si_uses_real_tools_and_verification():
    data = {
        'company': {'name': 'ТестСПК', 'form': 'ООО', 'director_fio': 'Иванов Иван Иванович', 'director_position': 'Директор', 'city': 'Минск', 'address': 'Минск', 'scope': 'общестрой'},
        'certification': {'standard': 'spk_stroy', 'audit_date': '21.08.2026'},
        'staff': [
            {'fio': 'Иванов Иван Иванович', 'position': 'Директор', 'role': 'director', 'is_worker': False},
            {'fio': 'Петров Петр Петрович', 'position': 'Главный инженер', 'is_worker': False, 'diploma_number': 'AB123', 'stage_years': '10 лет', 'trudovaya_number': 'ТК001'},
            {'fio': 'Сидоров Сидор Сидорович', 'position': 'Производитель работ', 'is_worker': False, 'diploma_number': 'CD456', 'stage_years': '8 лет', 'trudovaya_number': 'ТК002'},
        ],
        'spk': {
            'measurement_tools': [{'name': 'Нивелир', 'model': 'ATLAS KL24', 'factory_number': 'N123', 'range': '0-100 м', 'quantity': 1}],
            'verification_documents': [{'tool': 'Нивелир ATLAS KL24', 'number': 'ПВ-77', 'date': '01.08.2026', 'valid_until': '01.08.2027'}],
        },
    }
    result = generate_package(data, '', 'spk_stroy')
    si = next(d for d in result['docs'] if 'Справка СИ' in d['name'])
    text = _visible(si['bytes'])
    assert 'Нивелир' in text
    assert 'ATLAS KL24' in text
    assert 'ПВ-77' in text
    assert 'Рулетка измерительная' not in text


def test_spk_does_not_turn_foreman_into_chief_engineer():
    data = {
        'company': {'name': 'ТестСПК2', 'form': 'ООО', 'director_fio': 'Иванов Иван Иванович', 'director_position': 'Директор', 'city': 'Минск', 'address': 'Минск', 'scope': 'общестрой'},
        'certification': {'standard': 'spk_stroy', 'audit_date': '21.08.2026'},
        'staff': [
            {'fio': 'Иванов Иван Иванович', 'position': 'Директор', 'role': 'director', 'is_worker': False},
            {'fio': 'Сидоров Сидор Сидорович', 'position': 'Производитель работ', 'is_worker': False, 'diploma_number': 'CD456', 'stage_years': '8 лет', 'trudovaya_number': 'ТК002'},
        ],
        'spk': {'measurement_tools': []},
    }
    result = generate_package(data, '', 'spk_stroy')
    assert any('главный инженер' in w.lower() for w in result.get('warnings', []))
