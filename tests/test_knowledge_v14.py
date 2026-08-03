import importlib.util
import json
import os
from pathlib import Path


def load_server(tmp_path):
    os.environ['IGOR_DATA_DIR'] = str(tmp_path / 'data')
    module_path = Path(__file__).resolve().parents[1] / 'server.py'
    spec = importlib.util.spec_from_file_location('igor_server_v14_test', module_path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_knowledge_crud_and_scope(tmp_path):
    server = load_server(tmp_path)
    rule = server.knowledge_create({
        'title': 'Рабочие Формы №2',
        'instruction': 'Заполнять рабочих по выбранному виду работ.',
        'scope': 'company_att',
        'author': 'Эксперт',
        'active': True,
        'source_text': 'регламент' * 100,
    })
    assert rule['active'] is True
    assert 'Рабочие Формы №2' in server.knowledge_context('company_att')
    assert server.knowledge_context('iso') == ''

    public = server.knowledge_public(rule)
    assert 'source_text' not in public
    assert public['source_length'] > 0

    updated = server.knowledge_update({
        'id': rule['id'],
        'instruction': 'Новое правило.',
        'active': False,
    })
    assert updated['version'] == 2
    assert updated['history']
    assert server.knowledge_context('company_att') == ''

    assert server.knowledge_delete(rule['id']) is True
    assert server.knowledge_list() == []


def test_scope_aliases(tmp_path):
    server = load_server(tmp_path)
    server.knowledge_create({'title':'Общее СПК','instruction':'Правило СПК','scope':'spk','active':True})
    server.knowledge_create({'title':'Только БИСП','instruction':'Правило БИСП','scope':'spk_bisp','active':True})
    stroy = server.knowledge_context('spk_stroy')
    bisp = server.knowledge_context('spk_bisp')
    assert 'Общее СПК' in stroy
    assert 'Только БИСП' not in stroy
    assert 'Общее СПК' in bisp and 'Только БИСП' in bisp


def test_generator_injects_knowledge(monkeypatch):
    module_path = Path(__file__).resolve().parents[1] / 'generator.py'
    spec = importlib.util.spec_from_file_location('igor_generator_v14_test', module_path)
    generator = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(generator)
    captured = {}

    class Response:
        def raise_for_status(self):
            return None
        def json(self):
            return {'choices':[{'message':{'content':'ok'}}]}

    def fake_post(*args, **kwargs):
        captured['payload'] = kwargs['json']
        return Response()

    monkeypatch.setattr(generator.req_lib, 'post', fake_post)
    generator._GENERATION_KNOWLEDGE_CONTEXT = 'Всегда применять правило №14.'
    assert generator.vibe_call([{'role':'user','content':'Создай документ'}], 'key', retries=1) == 'ok'
    messages = captured['payload']['messages']
    assert messages[0]['role'] == 'system'
    assert 'правило №14' in messages[0]['content']
