import importlib.util
import json
import os
import pathlib
import tempfile

ROOT = pathlib.Path(__file__).resolve().parents[1]
with tempfile.TemporaryDirectory() as td:
    os.environ['IGOR_DATA_DIR'] = td
    spec = importlib.util.spec_from_file_location('igor_server_v13', ROOT / 'server.py')
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)

    payload = {
        'version': 13,
        'data': {
            'company': {'name': 'Тест', 'unp': '123', 'bank_name': 'Банк'},
            'staff': [{'fio': 'Иванов', 'position': 'Прораб'}],
            'workers': [{'fio': 'Петров', 'profession': 'Маляр'}],
            'objects': [{'name': 'Объект'}],
            'suppliers': [{'name': 'Поставщик'}],
            'spk': {'measurement_tools': [{'name': 'Рулетка'}], 'ttk': [{'code': 'ТТК-1'}]},
        },
    }
    key = 'igor:company:test-v13'
    mod.kv_set(key, json.dumps(payload, ensure_ascii=False))
    restored = json.loads(mod.kv_get(key)['value'])
    assert restored['data']['staff'][0]['fio'] == 'Иванов'
    assert restored['data']['objects'][0]['name'] == 'Объект'
    assert restored['data']['spk']['measurement_tools'][0]['name'] == 'Рулетка'
    assert key in mod.kv_list('igor:company:')
    # Atomic writes must not leave temp files.
    assert not list(pathlib.Path(td).rglob('*.tmp-*'))
    mod.kv_delete(key)
    assert mod.kv_get(key) is None

print('V13 SERVER PERSISTENCE TEST PASSED')
