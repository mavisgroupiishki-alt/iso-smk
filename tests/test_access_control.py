import os
import tempfile
import http.client
import json
import socketserver
import threading
from pathlib import Path

import server


def _use_temp_auth_storage():
    temp = tempfile.TemporaryDirectory()
    root = Path(temp.name)
    previous = {
        'AUTH_DIR': server.AUTH_DIR,
        'USERS_FILE': server.USERS_FILE,
        'SESSIONS_FILE': server.SESSIONS_FILE,
        'IGOR_OWNER_PASSWORD': os.environ.get('IGOR_OWNER_PASSWORD'),
        'IGOR_OWNER_USERNAME': os.environ.get('IGOR_OWNER_USERNAME'),
        'IGOR_WORKSPACE_USERNAME': os.environ.get('IGOR_WORKSPACE_USERNAME'),
        'IGOR_WORKSPACE_PASSWORD': os.environ.get('IGOR_WORKSPACE_PASSWORD'),
        'IGOR_WORKSPACE_ROLE': os.environ.get('IGOR_WORKSPACE_ROLE'),
    }
    server.AUTH_DIR = root / 'auth'
    server.USERS_FILE = server.AUTH_DIR / 'users.json'
    server.SESSIONS_FILE = server.AUTH_DIR / 'sessions.json'
    os.environ['IGOR_OWNER_PASSWORD'] = 'owner-secret-password-123'
    os.environ['IGOR_OWNER_USERNAME'] = 'owner'
    return temp, previous


def _restore_auth_storage(temp, previous):
    server.AUTH_DIR = previous['AUTH_DIR']
    server.USERS_FILE = previous['USERS_FILE']
    server.SESSIONS_FILE = previous['SESSIONS_FILE']
    for name in ('IGOR_OWNER_PASSWORD', 'IGOR_OWNER_USERNAME',
                 'IGOR_WORKSPACE_USERNAME', 'IGOR_WORKSPACE_PASSWORD', 'IGOR_WORKSPACE_ROLE'):
        if previous[name] is None:
            os.environ.pop(name, None)
        else:
            os.environ[name] = previous[name]
    temp.cleanup()


def test_owner_can_create_and_revoke_operator_session():
    temp, previous = _use_temp_auth_storage()
    try:
        owner = server.auth_bootstrap_owner()
        assert owner['role'] == 'owner'

        owner_session = server.auth_login('owner', 'owner-secret-password-123')
        assert owner_session['user']['role'] == 'owner'

        operator = server.auth_create_user(
            owner_session['user'], 'operator-anna', 'operator-secret-password-456', 'operator'
        )
        assert operator == {'username': 'operator-anna', 'role': 'operator', 'active': True}

        operator_session = server.auth_login('operator-anna', 'operator-secret-password-456')
        assert server.auth_current_user(operator_session['session_id'])['username'] == 'operator-anna'

        server.auth_update_user(owner_session['user'], 'operator-anna', active=False)
        assert server.auth_current_user(operator_session['session_id']) is None
    finally:
        _restore_auth_storage(temp, previous)


def test_dedicated_service_accepts_only_its_configured_login():
    temp, previous = _use_temp_auth_storage()
    try:
        os.environ['IGOR_WORKSPACE_USERNAME'] = 'Настя'
        os.environ['IGOR_WORKSPACE_PASSWORD'] = 'operator-secret-password-456'
        os.environ['IGOR_WORKSPACE_ROLE'] = 'operator'

        account = server.auth_bootstrap_owner()
        assert account == {'username': 'настя', 'role': 'operator', 'active': True}
        assert server.auth_login('Настя', 'operator-secret-password-456')['user'] == account
        assert server.auth_login('Кристина', 'operator-secret-password-456') is None
        with __import__('pytest').raises(PermissionError):
            server.auth_create_user({'id': 'admin', 'role': 'owner'}, 'kristina', 'operator-secret-password-456')
    finally:
        _restore_auth_storage(temp, previous)


def test_operator_policy_allows_only_new_package_flow():
    assert server.auth_route_allowed('operator', 'POST', '/api/ai/chat')
    assert server.auth_route_allowed('operator', 'POST', '/api/generate')
    assert server.auth_route_allowed('operator', 'GET', '/api/task/abc123')

    assert server.auth_route_allowed('operator', 'GET', '/api/companies')
    assert server.auth_route_allowed('operator', 'GET', '/api/journal')
    assert not server.auth_route_allowed('operator', 'GET', '/api/knowledge/list')
    assert not server.auth_route_allowed('operator', 'POST', '/api/users/create')


def test_legacy_results_are_owner_only_and_new_results_are_isolated():
    owner = {'id': 'owner', 'role': 'owner'}
    operator = {'id': 'operator-anna', 'role': 'operator'}

    assert server.auth_owns_record(owner, {'owner_user_id': 'owner'})
    assert not server.auth_owns_record(owner, {'owner_user_id': 'operator-anna'})
    assert not server.auth_owns_record(operator, {'owner_user_id': 'owner'})
    assert server.auth_owns_record(operator, {'owner_user_id': 'operator-anna'})
    assert server.auth_owns_record(owner, {})
    assert not server.auth_owns_record(operator, {})


def test_workspace_storage_isolated_with_owner_only_legacy_recovery(tmp_path, monkeypatch):
    owner = {'id': 'admin', 'role': 'owner'}
    nastya = {'id': 'nastya', 'role': 'operator'}
    kristina = {'id': 'kristina', 'role': 'operator'}
    monkeypatch.setattr(server, 'KV_DIR', tmp_path / 'kv')
    monkeypatch.setattr(server, 'CO_DIR', tmp_path / 'companies')
    monkeypatch.setattr(server, 'JOURNAL_DIR', tmp_path / 'journal')
    server.KV_DIR.mkdir()
    server.CO_DIR.mkdir()
    server.JOURNAL_DIR.mkdir()

    key = 'igor:company:one'
    server.kv_set_for_user(nastya, key, 'nastya-data')
    server.kv_set_for_user(kristina, key, 'kristina-data')
    server.kv_set(key, 'legacy-admin-data')
    assert server.kv_get_for_user(nastya, key)['value'] == 'nastya-data'
    assert server.kv_get_for_user(kristina, key)['value'] == 'kristina-data'
    assert server.kv_get_for_user(owner, key)['value'] == 'legacy-admin-data'
    assert server.kv_list_for_user(nastya, 'igor:company:') == [key]
    assert server.kv_list_for_user(kristina, 'igor:company:') == [key]

    nastya_id = server.save_company({'name': 'Настя'}, nastya)
    kristina_id = server.save_company({'name': 'Кристина'}, kristina)
    assert [row['id'] for row in server.get_companies(nastya)] == [nastya_id]
    assert [row['id'] for row in server.get_companies(kristina)] == [kristina_id]
    assert server.get_companies(owner) == []

    legacy = server.save_journal({'orgName': 'Старый'})
    server.save_journal({'owner_user_id': nastya['id'], 'orgName': 'Настя'})
    assert [row['orgName'] for row in server.get_journal(nastya)] == ['Настя']
    assert [row['orgName'] for row in server.get_journal(owner)] == ['Старый']
    assert server.get_journal_entry(legacy, kristina) is None


def test_archive_queue_round_robin_between_workspaces():
    candidates = [
        ('a1', {'status': 'queued', 'queued_at': '2026-10-06T09:00:00', 'owner_user_id': 'nastya'}),
        ('a2', {'status': 'queued', 'queued_at': '2026-10-06T09:01:00', 'owner_user_id': 'nastya'}),
        ('b1', {'status': 'queued', 'queued_at': '2026-10-06T09:02:00', 'owner_user_id': 'kristina'}),
    ]
    assert server._archive_pick_next(candidates, 'nastya')[0] == 'b1'
    assert server._archive_pick_next(candidates, 'kristina')[0] == 'a1'


def test_http_layer_blocks_operator_from_history_and_revokes_live_session():
    temp, previous = _use_temp_auth_storage()
    httpd = socketserver.TCPServer(('127.0.0.1', 0), server.H)
    thread = threading.Thread(target=httpd.serve_forever, daemon=True)
    thread.start()
    try:
        port = httpd.server_address[1]
        conn = http.client.HTTPConnection('127.0.0.1', port, timeout=5)
        conn.request('GET', '/api/companies')
        assert conn.getresponse().status == 401

        server.auth_bootstrap_owner()
        conn.request('POST', '/api/auth/login', body=json.dumps({
            'username': 'owner', 'password': 'owner-secret-password-123'
        }), headers={'Content-Type': 'application/json'})
        response = conn.getresponse()
        assert response.status == 200
        response.read()
        owner_cookie = response.getheader('Set-Cookie').split(';', 1)[0]
        conn.request('POST', '/api/users/create', body=json.dumps({
            'username': 'operator-anna', 'password': 'operator-secret-password-456', 'role': 'operator'
        }), headers={'Content-Type': 'application/json', 'Cookie': owner_cookie})
        response = conn.getresponse()
        assert response.status == 200
        response.read()
        conn.request('POST', '/api/auth/login', body=json.dumps({
            'username': 'operator-anna', 'password': 'operator-secret-password-456'
        }), headers={'Content-Type': 'application/json'})
        response = conn.getresponse()
        assert response.status == 200
        response.read()
        cookie = response.getheader('Set-Cookie').split(';', 1)[0]

        conn.request('GET', '/api/companies', headers={'Cookie': cookie})
        assert conn.getresponse().status == 200
        conn.request('GET', '/api/task/not-owned', headers={'Cookie': cookie})
        assert conn.getresponse().status == 200

        conn.request('POST', '/api/users/update', body=json.dumps({
            'username': 'operator-anna', 'active': False
        }), headers={'Content-Type': 'application/json', 'Cookie': owner_cookie})
        response = conn.getresponse()
        assert response.status == 200
        response.read()
        conn.request('GET', '/api/task/not-owned', headers={'Cookie': cookie})
        assert conn.getresponse().status == 401
    finally:
        httpd.shutdown()
        httpd.server_close()
        _restore_auth_storage(temp, previous)


def test_http_workspaces_do_not_leak_through_direct_api_calls(tmp_path, monkeypatch):
    """The UI is not the security boundary: another signed-in user cannot read,
    delete, or poll a task from this workspace by constructing its URL."""
    temp, previous = _use_temp_auth_storage()
    previous_tasks = dict(server.TASKS)
    try:
        for attr, folder in (
            ('KV_DIR', 'kv'), ('CO_DIR', 'companies'), ('JOURNAL_DIR', 'journal'),
        ):
            path = tmp_path / folder
            path.mkdir()
            monkeypatch.setattr(server, attr, path)
        server.TASKS.clear()

        httpd = socketserver.TCPServer(('127.0.0.1', 0), server.H)
        thread = threading.Thread(target=httpd.serve_forever, daemon=True)
        thread.start()
        port = httpd.server_address[1]

        def request(method, path, payload=None, cookie=None):
            conn = http.client.HTTPConnection('127.0.0.1', port, timeout=5)
            headers = {'Content-Type': 'application/json'} if payload is not None else {}
            if cookie:
                headers['Cookie'] = cookie
            conn.request(method, path, body=(json.dumps(payload) if payload is not None else None), headers=headers)
            response = conn.getresponse()
            raw = response.read()
            return response.status, json.loads(raw.decode('utf-8') or '{}'), response.getheader('Set-Cookie')

        server.auth_bootstrap_owner()
        status, _body, session = request('POST', '/api/auth/login', {
            'username': 'owner', 'password': 'owner-secret-password-123',
        })
        assert status == 200
        owner_cookie = session.split(';', 1)[0]
        for username in ('nastya', 'kristina'):
            status, _body, _session = request('POST', '/api/users/create', {
                'username': username, 'password': f'{username}-secret-password-123', 'role': 'operator',
            }, cookie=owner_cookie)
            assert status == 200

        cookies = {}
        for username in ('nastya', 'kristina'):
            status, _body, session = request('POST', '/api/auth/login', {
                'username': username, 'password': f'{username}-secret-password-123',
            })
            assert status == 200
            cookies[username] = session.split(';', 1)[0]

        status, _body, _session = request(
            'POST', '/api/kv/set', {'key': 'igor:company:current', 'value': 'только Настя'}, cookie=cookies['nastya']
        )
        assert status == 200
        status, body, _session = request('GET', '/api/kv/get?key=igor:company:current', cookie=cookies['kristina'])
        assert status == 200
        assert body['value'] is None

        status, body, _session = request('POST', '/api/companies/save', {'name': 'Только Настя'}, cookie=cookies['nastya'])
        assert status == 200
        nastya_company_id = body['id']
        status, body, _session = request('GET', '/api/companies', cookie=cookies['kristina'])
        assert status == 200
        assert body == []
        status, body, _session = request('POST', '/api/companies/delete', {'id': nastya_company_id}, cookie=cookies['kristina'])
        assert status == 403
        assert body['success'] is False

        server.TASKS['only-nastya'] = {'owner_user_id': 'nastya', 'status': 'done', 'text': 'секретные данные'}
        status, body, _session = request('GET', '/api/task/only-nastya', cookie=cookies['kristina'])
        assert status == 200
        assert body == {'status': 'not_found'}
    finally:
        if 'httpd' in locals():
            httpd.shutdown()
            httpd.server_close()
        server.TASKS.clear()
        server.TASKS.update(previous_tasks)
        _restore_auth_storage(temp, previous)
