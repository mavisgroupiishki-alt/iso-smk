import json

import server


def test_ai_reply_with_trailing_prose_keeps_only_structured_message():
    raw = (
        '{"message":"Данные приняты.","questions":[],"data":'
        '{"company_attestation":{"readiness":"review"}}}\n'
        'Готово, можно продолжать.'
    )

    result = json.loads(server._sanitize_ai_visible_response(raw, 'company_att'))

    assert result['message'] == 'Данные приняты.'
    assert result['questions'] == []
    assert result['data']['company_attestation']['readiness'] == 'review'


def test_ai_reply_without_json_is_not_exposed_as_service_payload():
    assert server._extract_ai_json_object('обычный ответ') is None
