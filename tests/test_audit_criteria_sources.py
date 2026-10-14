from generator_iso_suot_templates import _role_audit_points_iso, _role_audit_points_suot


def test_iso_audit_criteria_follow_auditor_source_for_key_roles():
    assert _role_audit_points_iso('Директор') == (
        'п.п. 4.1–4.4, 5.1–5.3, 6.1–6.3, 7.1–7.5, 8.1, 8.2, 8.4–8.7, 9.1–9.3, 10.1–10.3'
    )
    assert _role_audit_points_iso('Инженер по охране труда') == (
        'п.п. 4.1–4.4, 5.2–5.3, 6.1, 6.2, 7.1.4, 7.1.6, 7.2–7.5, 10.1–10.3'
    )
    assert _role_audit_points_iso('Главный бухгалтер') == (
        'п.п. 4.1–4.4, 5.2–5.3, 6.1–6.3, 7.2–7.5, 8.1, 10.1–10.3'
    )


def test_suot_audit_criteria_follow_auditor_source_for_key_roles():
    assert _role_audit_points_suot('Директор') == (
        'п.п. 4.1–4.4, 5.1–5.4, 6.1–6.2, 7.1–7.5, 9.1–9.3, 10.1–10.3'
    )
    assert _role_audit_points_suot('Производитель работ') == (
        'п.п. 4.1–4.4, 7.1–7.5, 8.1–8.2, 10.1–10.3'
    )
    assert _role_audit_points_suot('Начальник ПТО') == (
        'п.п. 4.1, 4.2, 5.2, 5.3, 7.3, 7.4, 8.2, 10.3'
    )
