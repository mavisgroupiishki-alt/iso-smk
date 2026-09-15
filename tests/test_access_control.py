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
    for name in ('IGOR_OWNER_PASSWORD', 'IGOR_OWNER_USERNAME'):
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


def test_operator_policy_allows_only_new_package_flow():
    assert server.auth_route_allowed('operator', 'POST', '/api/ai/chat')
    assert server.auth_route_allowed('operator', 'POST', '/api/generate')
    assert server.auth_route_allowed('operator', 'GET', '/api/task/abc123')

    assert not server.auth_route_allowed('operator', 'GET', '/api/companies')
    assert not server.auth_route_allowed('operator', 'GET', '/api/journal')
    assert not server.auth_route_allowed('operator', 'GET', '/api/knowledge/list')
    assert not server.auth_route_allowed('operator', 'POST', '/api/users/create')


def test_legacy_results_are_owner_only_and_new_results_are_isolated():
    owner = {'id': 'owner', 'role': 'owner'}
    operator = {'id': 'operator-anna', 'role': 'operator'}

    assert server.auth_owns_record(owner, {'owner_user_id': 'owner'})
    assert server.auth_owns_record(owner, {'owner_user_id': 'operator-anna'})
    assert not server.auth_owns_record(operator, {'owner_user_id': 'owner'})
    assert server.auth_owns_record(operator, {'owner_user_id': 'operator-anna'})
    assert not server.auth_owns_record(operator, {})


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
        assert conn.getresponse().status == 403
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
