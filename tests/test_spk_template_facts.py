import io
import json
import zipfile

import generator
from docx_review import collect_required_items, collect_review_tokens_and_items
from generator_spk_templates import _build_real_si_list, generate_spk_package_v2
import server


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
        variant='spk_bisp',
        spk_data={
            'measurement_tools': [
                {'name': 'Уровень электронный', 'model': 'ATLAS-400M', 'factory_number': 'A-1'},
                {'name': 'Теодолит оптический', 'factory_number': 'T-1'},
            ],
            'calibration_documents': [
                {'tool': 'Уровень электронный ATLAS-400M', 'number': 'К-1', 'date': '18.03.2026'},
            ],
        },
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
    schedule = documents[next(name for name in documents if 'График поверки СИ' in name)]
    assert dates['goals'] in schedule
    assert 'Уровень электронный' in schedule
    assert 'Теодолит оптический' not in schedule
    assert 'Плотномер динамический Д-51А' not in schedule
    assert 'ТРЕБУЕТ УТОЧНЕНИЯ: свидетельство о технической компетентности' in documents[next(name for name in documents if '6 Паспорт СПК' in name)]


def test_expert_date_flag_is_removed_after_the_user_supplies_the_date():
    data = {
        'certification': {'audit_date': '17.09.2026'},
        'review_items': [{'field': 'certification.audit_date', 'value': '', 'reason': 'дата не найдена'}],
        'flags': [{'type': 'warning', 'text': 'Дата выезда эксперта в исходных материалах не найдена'}],
    }
    items = collect_required_items(data, 'spk_bisp')
    _tokens, review_items = collect_review_tokens_and_items(data)

    assert not any(item['field'].startswith('flags[') for item in items)
    assert not any(item['field'] == 'certification.audit_date' for item in review_items)


def test_spk_chat_asks_for_missing_facts_before_generation():
    raw = json.dumps({
        'message': 'Карточка обновлена.',
        'questions': [],
        'data': {
            'certification': {'audit_date': '17.09.2026'},
            'spk': {'ttk': [{'code': 'ТТК-1', 'valid_until': ''}]},
            'review_items': [{'field': 'certification.audit_date', 'value': '', 'reason': 'дата не найдена'}],
            'flags': [{'type': 'warning', 'text': 'Дата выезда эксперта не найдена'}],
        },
    }, ensure_ascii=False)

    payload = json.loads(server._sanitize_ai_visible_response(raw, 'spk_bisp'))

    questions = ' '.join(payload['questions']).lower()
    assert 'помещен' in questions
    assert 'технической компетентности' in questions
    assert 'ттк' in questions
    assert payload['data']['review_items'] == []
    assert payload['data']['flags'] == []


def test_spk_itr_keeps_all_non_ptu_diplomas_and_workbook_numbers():
    dates = generator.calculate_dates('17.09.2026')
    company = {
        'name': 'Тестовая организация', 'form': 'ООО', 'city': 'Минск',
        'address': 'г. Минск', 'director_fio': 'Иванов Иван Иванович',
        'director_position': 'Директор',
    }
    itr = [
        {
            'fio': 'Иванов Иван Иванович', 'position': 'Директор',
            'diplomas': [
                {'number': 'В-100', 'institution': 'БГТУ', 'speciality': 'ПГС'},
                {'number': 'ПТУ-200', 'institution': 'ПТУ № 15', 'speciality': 'каменщик'},
                {'number': 'С-300', 'institution': 'БНТУ', 'speciality': 'строительство'},
            ],
            'trudovye_numbers': ['ПК № 1111111', 'Вкладыш № 2222222'],
        },
        {'fio': 'Петров Петр Петрович', 'position': 'Главный инженер'},
        {'fio': 'Сидоров Сидор Сидорович', 'position': 'Производитель работ'},
    ]
    result = generate_spk_package_v2(
        company, itr, [], dates, generator.select_responsible(itr), variant='spk_stroy',
    )
    text = _xml_text(_document(result, '2 Справка ИТР')['bytes'])

    assert 'В-100' in text
    assert 'С-300' in text
    assert 'ПТУ-200' not in text
    assert 'ПК № 1111111' in text
    assert 'Вкладыш № 2222222' in text


def test_spk_copy_list_is_preserved_as_measurement_tools():
    source = '''
    ПЕРЕЧЕНЬ КОПИЙ СПК
    Сведения по инструментам:
    - Нивелир (возможна аренда)
    - Рейка нивелирная
    - Плотномер динамический (возможна аренда)
    - Рулетка измерительная
    - Линейка измерительная
    - Уровень электронный строительный
    - Рейка контрольная
    - Штангенциркуль ШЦ
    - Угольник поверочный
    - Термометр (-35 +50)
    - Теодолит (возможна аренда)
    '''
    tools = server._extract_spk_tools_from_copy_list(source)
    names = [tool['name'] for tool in tools]

    assert len(names) == 11
    assert 'Нивелир' in names
    assert 'Теодолит' in names


def test_person_archive_folder_can_be_a_surname_or_role():
    personal_blocks = ['--- диплом.pdf ---\nДиплом', '--- трудовая.pdf ---\nТрудовая книжка']

    assert server._looks_like_person_folder('Белько', personal_blocks)
    assert server._looks_like_person_folder('Директор', personal_blocks)
    assert not server._looks_like_person_folder('Уставные', personal_blocks)


def test_spk_si_includes_copy_list_tools_without_inventing_verification():
    source = 'СВЕДЕНИЯ ПО ИНСТРУМЕНТАМ: Нивелир; Рейка нивелирная; Теодолит'
    rows, warnings = _build_real_si_list({
        'measurement_tools': server._extract_spk_tools_from_copy_list(source),
        'verification_documents': [],
        'calibration_documents': [],
    })
    by_name = {row['name']: row for row in rows}

    assert set(by_name) == {'Нивелир', 'Рейка нивелирная', 'Теодолит'}
    assert by_name['Теодолит']['verification'] == 'ТРЕБУЕТ УТОЧНЕНИЯ: поверка/калибровка'
    assert len(warnings) == 3
