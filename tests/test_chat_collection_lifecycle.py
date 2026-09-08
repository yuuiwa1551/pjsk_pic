"""Exercise the real plugin handlers in the framework's text-before-tool order."""
import ast
import json
import unittest
from pathlib import Path
from types import SimpleNamespace


def handlers():
    tree = ast.parse((Path(__file__).resolve().parents[1] / 'main.py').read_text(encoding='utf-8'))
    plugin = next(n for n in tree.body if isinstance(n, ast.ClassDef) and n.name == 'PJSKPicPlugin')
    names = {'save_chat_image', 'mark_chat_collection_done', 'decorate_chat_collection'}
    methods = [n for n in plugin.body if isinstance(n, ast.AsyncFunctionDef) and n.name in names]
    for method in methods:
        method.decorator_list = []
    module = ast.Module(body=[ast.ImportFrom(module='__future__', names=[ast.alias(name='annotations')], level=0),
                             ast.ClassDef(name='Handlers', bases=[], keywords=[], body=methods, decorator_list=[])], type_ignores=[])
    namespace = {'json': json, 'Plain': lambda text: text,
                 'ResultContentType': SimpleNamespace(STREAMING_RESULT='stream', STREAMING_FINISH='finish')}
    exec(compile(ast.fix_missing_locations(module), '<real-plugin-handlers>', 'exec'), namespace)
    return namespace['Handlers']


class Event:
    def __init__(self):
        self.state = SimpleNamespace(agent_done=False, saved=[])
        self.extras = {'pjsk_chat_collection': self.state}
        self.sent = []
        self.result = None

    def get_extra(self, key):
        return self.extras.get(key)

    def set_extra(self, key, value):
        self.extras[key] = value

    def get_result(self):
        return self.result

    def output(self, kind='llm'):
        self.result = SimpleNamespace(result_content_type=kind, chain=['正文'], is_llm_result=lambda: kind == 'llm')

    def plain_result(self, text):
        return text

    async def send(self, result):
        self.sent.append(result)


class Service:
    async def save(self, state, ref, tags, reason):
        state.saved.append(ref)
        return {'ok': True}

    async def summary(self, state):
        return f'顺手收了 {len(state.saved)} 张' if state.saved else ''


class LifecycleTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.plugin = handlers()()
        self.plugin._chat_collection_allowed = lambda event: True
        self.plugin.chat_image_collection_service = Service()
        self.event = Event()

    async def test_text_before_tools_preserves_state_and_final_summary_once(self):
        for index in range(2):
            self.event.output()
            await self.plugin.decorate_chat_collection(self.event)
            self.assertIs(self.event.state, self.event.get_extra('pjsk_chat_collection'))
            response = await self.plugin.save_chat_image(self.event, str(index), [7], '明确角色')
            self.assertTrue(json.loads(response)['ok'])
            self.assertEqual(['正文'], self.event.result.chain)
        await self.plugin.mark_chat_collection_done(self.event, None)
        self.event.output()
        await self.plugin.decorate_chat_collection(self.event)
        await self.plugin.decorate_chat_collection(self.event)
        self.assertEqual(['正文', '\n顺手收了 2 张'], self.event.result.chain)
        self.assertIsNone(self.event.get_extra('pjsk_chat_collection'))

    async def test_stream_only_finishes_after_agent_done(self):
        self.event.output('stream')
        await self.plugin.decorate_chat_collection(self.event)
        self.assertIsNotNone(self.event.get_extra('pjsk_chat_collection'))
        await self.plugin.save_chat_image(self.event, 'one', [7], '')
        await self.plugin.mark_chat_collection_done(self.event, None)
        await self.plugin.decorate_chat_collection(self.event)
        self.assertEqual([], self.event.sent)
        self.event.output('finish')
        await self.plugin.decorate_chat_collection(self.event)
        await self.plugin.decorate_chat_collection(self.event)
        self.assertEqual(['顺手收了 1 张'], self.event.sent)

    async def test_final_without_saved_images_has_no_receipt(self):
        await self.plugin.mark_chat_collection_done(self.event, None)
        self.event.output()
        await self.plugin.decorate_chat_collection(self.event)
        self.assertEqual(['正文'], self.event.result.chain)
        self.assertEqual([], self.event.sent)
        self.assertIsNone(self.event.get_extra('pjsk_chat_collection'))
