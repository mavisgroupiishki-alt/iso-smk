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


def test_source_document_errors_do_not_appear_as_technical_package_warnings():
    _tokens, items = collect_review_tokens_and_items({
        'source_documents': [{
            'filename': 'калибровка.pdf',
            'needs_review': True,
            'review_reason': 'пустой результат',
        }],
        'review_items': [{
            'field': 'source_documents[0]',
            'value': 'ТРЕБУЕТ ПРОВЕРКИ ИСТОЧНИКА',
            'reason': 'пустой результат',
        }],
    })

    assert items == []


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


def test_spk_does_not_block_personnel_on_missing_ot_certificates():
    raw = json.dumps({
        'message': 'Мне не хватает сведений об удостоверениях по охране труда, чтобы закрыть вопрос с персоналом.',
        'questions': ['Пришлите фото удостоверений по охране труда сотрудников.'],
        'data': {
            'certification': {'standard': 'spk_stroy'},
            'staff': [{'fio': 'Иванов Иван Иванович', 'position': 'Директор'}],
            'spk': {'premises': [{'address': 'г. Минск'}], 'technical_competence': {'number': 'СПК-1'}},
        },
    }, ensure_ascii=False)

    payload = json.loads(server._sanitize_ai_visible_response(raw, 'spk_stroy'))

    assert not payload['questions']
    assert 'удостоверен' not in payload['message'].lower()


def test_spk_activity_profile_is_preserved_and_unknown_profile_needs_confirmation():
    raw = json.dumps({
        'message': 'Область обновлена.',
        'questions': [],
        'data': {
            'certification': {'standard': 'spk_stroy'},
            'spk': {'activity_profile': 'construction_metal'},
        },
    }, ensure_ascii=False)
    profile = json.loads(server._sanitize_ai_visible_response(raw, 'spk_stroy'))

    assert profile['data']['spk']['activity_profile'] == 'construction_metal'
    assert not any('Уточните область СПК' in question for question in profile['questions'])

    raw_without_profile = json.dumps({
        'message': 'Добавлены сведения о помещении.',
        'questions': [],
        'data': {
            'certification': {'standard': 'spk_stroy'},
            'spk': {'premises': [{'address': 'г. Минск'}]},
        },
    }, ensure_ascii=False)
    without_profile = json.loads(server._sanitize_ai_visible_response(raw_without_profile, 'spk_stroy'))

    # The client deep-merges this response into the saved company card. Omitting
    # the scalar must therefore retain a previously confirmed non-default profile.
    assert 'activity_profile' not in without_profile['data']['spk']
    assert not any('Уточните область СПК' in question for question in without_profile['questions'])

    raw_unknown = json.dumps({
        'message': 'Область обновлена.',
        'questions': [],
        'data': {
            'certification': {'standard': 'spk_stroy'},
            'spk': {'activity_profile': 'неизвестная область'},
        },
    }, ensure_ascii=False)
    unknown = json.loads(server._sanitize_ai_visible_response(raw_unknown, 'spk_stroy'))

    assert unknown['data']['spk']['activity_profile'] == 'construction'
    assert any('Уточните область СПК' in question for question in unknown['questions'])


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
                {'full_text': 'Диплом М-400, БГАС, организация строительства'},
                {'full_text': 'Диплом ПТУ-500, каменщик'},
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
    assert 'Диплом М-400, БГАС, организация строительства' in text
    assert 'ПТУ-200' not in text
    assert 'ПТУ-500' not in text
    assert 'ПК № 1111111' in text
    assert 'Вкладыш № 2222222' in text


def test_spk_itr_uses_manually_confirmed_bsc_attestation_details():
    dates = generator.calculate_dates('17.09.2026')
    company = {
        'name': 'Тестовая организация', 'form': 'ООО', 'city': 'Минск',
        'address': 'г. Минск', 'director_fio': 'Иванов Иван Иванович',
        'director_position': 'Директор',
    }
    itr = [{
        'fio': 'Иванов Иван Иванович', 'position': 'Директор',
        'attestat_number': 'АТ-12345', 'attestat_date_from': '12.09.2026',
    }]
    result = generate_spk_package_v2(
        company, itr, [], dates, generator.select_responsible(itr), variant='spk_stroy',
    )
    text = _xml_text(_document(result, '2 Справка ИТР')['bytes'])

    assert 'Протокол №АТ-12345 от 12.09.2026 г.' in text


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


def test_spk_person_summaries_become_structured_staff_without_losing_documents():
    archive_text = '''
=== 👤 Рощин ===
1) Рощин Александр Викторович
Должность/роль для СПК: Заместитель директора — главный инженер
Паспорт: не требуется для справки
Дипломы: Диплом АБ № 12345, БНТУ, промышленное и гражданское строительство; Диплом ПТУ № 9, каменщик
Трудовая книжка и вкладыши: ТК № 7654321; вкладыш № 654321
ПЕРИОДЫ РАБОТЫ:
- 01.01.2010 — по настоящее время | ООО «Тест» | главный инженер
'''

    staff = server._extract_spk_staff_from_person_summaries(archive_text)

    assert staff == [{
        'fio': 'Рощин Александр Викторович',
        'position': 'Заместитель директора — главный инженер',
        'diplomas': [
            {'full_text': 'Диплом АБ № 12345, БНТУ, промышленное и гражданское строительство'},
            {'full_text': 'Диплом ПТУ № 9, каменщик'},
        ],
        'trudovye_numbers': ['ТК № 7654321', 'вкладыш № 654321'],
        'source': 'archive_person_summary',
    }]


def test_spk_person_summary_accepts_dot_numbering_and_multiline_diplomas():
    archive_text = '''
1. Гринкевич Вадим Николаевич
Должность/роль для СПК: Директор
Образование:
- Диплом АБ № 12345, БНТУ, промышленное строительство
- Диплом СВ № 67890, БГТУ, менеджмент
Трудовая книжка и вкладыши: ТК № 7654321
ПЕРИОДЫ РАБОТЫ:
- 01.01.2010 — по настоящее время | ООО «Тест» | директор
'''

    staff = server._extract_spk_staff_from_person_summaries(archive_text)

    assert staff[0]['fio'] == 'Гринкевич Вадим Николаевич'
    assert [item['full_text'] for item in staff[0]['diplomas']] == [
        'Диплом АБ № 12345, БНТУ, промышленное строительство',
        'Диплом СВ № 67890, БГТУ, менеджмент',
    ]


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


def test_spk_si_keeps_standard_template_characteristics_until_a_document_changes_them():
    rows, _warnings = _build_real_si_list({
        'measurement_tools': [
            {'name': 'Линейка измерительная', 'quantity': 1},
            {'name': 'Рейка контрольная 3000 мм', 'quantity': 1},
            {'name': 'Шаблон сварщика УШС-2', 'quantity': 1},
        ],
    })
    by_name = {row['name']: row for row in rows}

    assert by_name['Линейка измерительная']['characteristics'] == 'Диапазон измерений: (0-1 000) мм'
    assert by_name['Рейка контрольная 3000 мм']['characteristics'] == 'Диапазон измерений: (0-3000) мм'
    assert by_name['Шаблон сварщика УШС-2']['characteristics'] == 'Диапазон измерений: 4-14 мм'

    changed, _warnings = _build_real_si_list({
        'measurement_tools': [{
            'name': 'Линейка измерительная',
            'range': 'Диапазон измерений: (0-2 000) мм',
        }],
    })
    assert changed[0]['characteristics'] == 'Диапазон измерений: (0-2 000) мм'


def test_spk_si_model_does_not_replace_the_template_range_without_documented_override():
    rows, _warnings = _build_real_si_list({
        'measurement_tools': [{
            'name': 'Линейка измерительная',
            'model': 'ЛМ-1',
        }],
    })

    assert rows[0]['characteristics'] == 'ЛМ-1; Диапазон измерений: (0-1 000) мм'


def test_spk_si_does_not_attach_a_document_for_a_different_factory_number():
    rows, warnings = _build_real_si_list({
        'measurement_tools': [{
            'name': 'Угольник поверочный',
            'factory_number': '111',
        }],
        'verification_documents': [{
            'tool': 'Угольник поверочный',
            'factory_number': '222',
            'number': 'ПВ-222',
        }],
    })

    assert rows[0]['verification'] == 'ТРЕБУЕТ УТОЧНЕНИЯ: поверка/калибровка'
    assert len(warnings) == 1


def test_spk_si_does_not_attach_leveling_staff_certificate_to_level():
    rows, warnings = _build_real_si_list({
        'measurement_tools': [
            {'name': 'Нивелир', 'factory_number': 'Н-1'},
            {'name': 'Рейка нивелирная', 'factory_number': 'Р-1'},
        ],
        'verification_documents': [{
            'tool': 'Рейка нивелирная', 'factory_number': 'Р-1', 'number': 'П-18', 'date': '01.03.2026',
        }],
    })
    by_name = {row['name']: row for row in rows}

    assert by_name['Нивелир']['verification'] == 'ТРЕБУЕТ УТОЧНЕНИЯ: поверка/калибровка'
    assert by_name['Рейка нивелирная']['verification'] == 'Поверка № П-18 от 01.03.2026'
    assert len(warnings) == 1


def test_spk_activity_profiles_change_all_scope_dependent_documents():
    dates = generator.calculate_dates('17.09.2026')
    company = {
        'name': 'Тестовая организация', 'form': 'ООО', 'city': 'Минск',
        'address': 'г. Минск', 'director_fio': 'Иванов Иван Иванович',
        'director_position': 'Директор',
    }
    itr = [
        {'fio': 'Иванов Иван Иванович', 'position': 'Директор'},
        {'fio': 'Петров Петр Петрович', 'position': 'Главный инженер'},
        {'fio': 'Сидоров Сидор Сидорович', 'position': 'Производитель работ'},
    ]

    def package_text(profile):
        result = generate_spk_package_v2(
            company, itr, [], dates, generator.select_responsible(itr),
            spk_data={'activity_profile': profile},
        )
        return '\n'.join(_xml_text(doc['bytes']) for doc in result['docs'])

    construction = package_text('construction')
    metal_only = package_text('metal_only')
    construction_metal = package_text('construction_metal')
    low_voltage = package_text('low_voltage_systems')

    assert 'строительно-монтажных работ' in construction
    assert 'производству металлоконструкций' in metal_only
    assert 'строительно-монтажных работ' not in metal_only
    assert 'стаж работы в области строительства' not in metal_only
    assert 'стаж работы в области производства металлоконструкций' in metal_only
    assert 'СМР' not in metal_only
    assert 'производству строительных работ, производству металлоконструкций' in construction_metal
    assert 'монтажу слаботочных систем' in low_voltage
    assert 'видеонаблюдения' in low_voltage
    assert 'строительно-монтажных работ' not in low_voltage
    assert 'стаж работы в области строительства' not in low_voltage
    assert 'стаж работы в области электромонтажных и слаботочных систем' in low_voltage
    assert 'СМР' not in low_voltage


def test_spk_scope_profiles_keep_confirmed_operational_job_title():
    dates = generator.calculate_dates('17.09.2026')
    company = {
        'name': 'Тестовая организация', 'form': 'ООО', 'city': 'Минск',
        'address': 'г. Минск', 'director_fio': 'Иванов Иван Иванович',
        'director_position': 'Директор',
    }
    itr = [
        {'fio': 'Иванов Иван Иванович', 'position': 'Директор'},
        {'fio': 'Петров Петр Петрович', 'position': 'Мастер производственного участка'},
    ]
    result = generate_spk_package_v2(
        company, itr, [], dates, generator.select_responsible(itr),
        spk_data={'activity_profile': 'metal_only'},
    )
    order = _xml_text(_document(result, '4.1 Приказ о СПК')['bytes'])
    si_order = _xml_text(_document(result, '4.3 Приказ о ТО СИ')['bytes'])

    assert 'Мастера производственного участка Петрова П.П.' in order
    assert 'Мастера производственного участка Петрова П.П.' in si_order
