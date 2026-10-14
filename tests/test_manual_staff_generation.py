import generator


def test_manual_employee_without_position_blocks_iso_suot_generation():
    data = {
        'company': {
            'name': 'МасКомпани', 'form': 'ООО', 'director_fio': 'Масюк Дмитрий Александрович',
            'scope': 'СМР',
        },
        'certification': {'standard': 'iso_suot', 'scope': 'СМР'},
        'dates': {'document_date': '13.10.2026'},
        'staff': [
            {'fio': 'Масюк Дмитрий Александрович', 'position': 'Директор', 'source': 'manual'},
            {'fio': 'Масюк Александр Михайлович', 'position': 'Требует уточнения', 'source': 'manual'},
        ],
    }

    assert 'должность сотрудника: Масюк Александр Михайлович' in generator._iso_suot_generation_missing_fields(
        data, 'iso_suot'
    )
