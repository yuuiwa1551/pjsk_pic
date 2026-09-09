from __future__ import annotations

import asyncio
import base64
import importlib
import json
import shutil
import subprocess
import sys
import tempfile
import types
import unittest
from pathlib import Path


CORE_DIR = Path(__file__).resolve().parents[1] / "core"
PACKAGE = "pjsk_luna_test_core"
MEDIA_RESOLVER_PATHS: dict[str, str] = {}
MEDIA_RESOLVER_PAYLOADS: dict[str, bytes] = {}
MEDIA_RESOLVER_FAILURES: set[str] = set()
MEDIA_RESOLVER_CALLS: list[tuple[str, str, str]] = []


class FakeImage:
    def __init__(self, file: str | None, *, url: str = "", **_) -> None:
        self.url = url
        self.file = file or ""

    async def convert_to_file_path(self) -> str:
        return self.file or self.url

    async def convert_to_base64(self) -> str:
        return base64.b64encode(b"local-image").decode()


class FakeResolvedMediaData:
    def __init__(self, base64_data: str, mime_type: str) -> None:
        self.base64_data = base64_data
        self.mime_type = mime_type

    def to_data_url(self) -> str:
        return f"data:{self.mime_type};base64,{self.base64_data}"


class FakeMediaResolver:
    def __init__(self, media_ref: str, *, media_type: str = "file", **_) -> None:
        self.media_ref = str(media_ref)
        self.media_type = media_type

    def _record(self, operation: str) -> None:
        MEDIA_RESOLVER_CALLS.append((operation, self.media_ref, self.media_type))

    def _path(self) -> str:
        if self.media_ref in MEDIA_RESOLVER_FAILURES:
            raise OSError(f"unavailable media: {self.media_ref}")
        if self.media_ref in MEDIA_RESOLVER_PATHS:
            return MEDIA_RESOLVER_PATHS[self.media_ref]
        if self.media_ref.startswith(("http://", "https://", "data:")):
            raise OSError(f"unmapped remote media: {self.media_ref}")
        return self.media_ref

    async def to_path(self, **_) -> str:
        self._record("to_path")
        return self._path()

    async def to_base64_data(self, **_) -> FakeResolvedMediaData:
        self._record("to_base64_data")
        path = self._path()
        payload = MEDIA_RESOLVER_PAYLOADS.get(path, b"resolved-image")
        return FakeResolvedMediaData(base64.b64encode(payload).decode(), "image/png")

    async def to_data_url(self, **_) -> str:
        data = await self.to_base64_data()
        return data.to_data_url()


class TextPart:
    def __init__(self, text: str) -> None:
        self.text = text


class ImageURLPart:
    class ImageURL:
        def __init__(self, *, url: str, id: str = "") -> None:
            self.url = url
            self.id = id

    def __init__(self, *, image_url: "ImageURLPart.ImageURL") -> None:
        self.image_url = image_url


def install_stubs() -> None:
    api = types.ModuleType("astrbot.api")
    api.logger = types.SimpleNamespace(info=lambda *a, **k: None, warning=lambda *a, **k: None, error=lambda *a, **k: None)
    components = types.ModuleType("astrbot.api.message_components")
    components.Image = FakeImage
    agent = types.ModuleType("astrbot.core.agent.message")
    agent.ImageURLPart = ImageURLPart
    agent.TextPart = TextPart
    utils = types.ModuleType("astrbot.core.utils")
    utils.__path__ = []
    media_utils = types.ModuleType("astrbot.core.utils.media_utils")
    media_utils.MediaResolver = FakeMediaResolver
    astrbot = types.ModuleType("astrbot")
    astrbot.__path__ = []
    core = types.ModuleType("astrbot.core")
    core.__path__ = []
    sys.modules.update({
        "astrbot": astrbot,
        "astrbot.api": api,
        "astrbot.api.message_components": components,
        "astrbot.core": core,
        "astrbot.core.agent": types.ModuleType("astrbot.core.agent"),
        "astrbot.core.agent.message": agent,
        "astrbot.core.utils": utils,
        "astrbot.core.utils.media_utils": media_utils,
    })
    astrbot.api = api
    core.utils = utils
    utils.media_utils = media_utils


install_stubs()
pkg = types.ModuleType(PACKAGE)
pkg.__path__ = [str(CORE_DIR)]
sys.modules[PACKAGE] = pkg
message_images = importlib.import_module(f"{PACKAGE}.message_images")
chat_context = importlib.import_module(f"{PACKAGE}.chat_image_context")
collection_module = importlib.import_module(f"{PACKAGE}.chat_image_collection_service")
db_module = importlib.import_module(f"{PACKAGE}.db")
models = importlib.import_module(f"{PACKAGE}.models")


class MessageObj:
    def __init__(self, message_id: str, raw_message=None) -> None:
        self.message_id = message_id
        self.raw_message = raw_message


class Event:
    def __init__(self, components, *, message_id="m1", session="group:1", sender_id="u1", sender_name="Alice") -> None:
        self.unified_msg_origin = session
        self.message_obj = MessageObj(message_id, {"message": components})
        self._components = components
        self._extras = {}
        self._sender_id = sender_id
        self._sender_name = sender_name

    def get_messages(self):
        return self._components

    def set_extra(self, key, value):
        self._extras[key] = value

    def get_extra(self, key, default=None):
        return self._extras.get(key, default)

    def get_sender_id(self):
        return self._sender_id

    def get_sender_name(self):
        return self._sender_name


class Request:
    def __init__(self, *, image_urls=None, prompt="", contexts=None, extra=None) -> None:
        self.image_urls = list(image_urls or [])
        self.prompt = prompt
        self.contexts = contexts
        self.extra_user_content_parts = list(extra or [])


class FakeImporter:
    def __init__(self, root: Path, db) -> None:
        self.root = root
        self.db = db
        self.calls = 0

    async def import_local_file(self, path: Path, *, platform: str):
        self.calls += 1
        image_id = self.db.upsert_image(
            file_path=str(path), file_name=path.name, sha256=f"sha-{path.name}",
            width=100, height=100, format_="png",
        )
        return models.ImportedImage(image_id=image_id, file_path=path, sha256=f"sha-{path.name}", phash="", width=100, height=100, format="png")


class ChatImageContextTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        MEDIA_RESOLVER_PATHS.clear()
        MEDIA_RESOLVER_PAYLOADS.clear()
        MEDIA_RESOLVER_FAILURES.clear()
        MEDIA_RESOLVER_CALLS.clear()

    async def test_multi_image_refs_preserve_order_and_resolve_local_paths(self):
        event = Event([
            {"type": "image", "data": {"file": "/original/one.png"}},
            {"type": "image", "data": {"file": "/original/two.png"}},
        ])
        ctx = chat_context.ChatImageContext()
        ctx.capture(event)
        sources = event.get_extra("pjsk_gallery_image_sources")
        sources[0].metadata["resolved_path"] = "/resolved/one.png"
        sources[1].metadata["resolved_path"] = "/resolved/two.png"
        req = Request(image_urls=["/resolved/one.png", "/resolved/two.png"], prompt="请判断")
        found = await ctx.prepare(event, req, attach_originals=False)
        self.assertEqual([item.ref for item in sources], [item.ref for item in found])
        self.assertIn(f"image_ref：{sources[0].ref}、{sources[1].ref}", req.prompt)

    async def test_history_marker_adds_original_and_preserves_sender_metadata(self):
        event = Event([{"type": "image", "data": {"file": "/original/one.png"}}], sender_id="u99", sender_name="历史发送者")
        ctx = chat_context.ChatImageContext()
        ctx.capture(event)
        item = event.get_extra("pjsk_gallery_image_sources")[0]
        req = Request(prompt=f"上下文 [gallery_image:{item.ref}]")
        found = await ctx.prepare(event, req, attach_originals=True)
        self.assertEqual([item.ref], [x.ref for x in found])
        self.assertTrue(any(isinstance(x, ImageURLPart) and x.image_url.id == item.ref for x in req.extra_user_content_parts))
        self.assertEqual("u99", item.metadata["source_sender_id"])
        self.assertEqual("历史发送者", item.metadata["source_sender_name"])

    async def test_unknown_historical_http_and_data_images_are_preserved(self):
        locations = [
            "https://cdn.invalid/history.png",
            "data:image/png;base64," + base64.b64encode(b"history-image").decode(),
        ]
        MEDIA_RESOLVER_PATHS[locations[0]] = "/resolved/history.png"
        MEDIA_RESOLVER_PAYLOADS["/resolved/history.png"] = b"history-image"
        for location in locations:
            with self.subTest(location=location[:32]):
                event = Event([])
                ctx = chat_context.ChatImageContext()
                req = Request(contexts=[{
                    "role": "user",
                    "content": [
                        {"type": "text", "text": "历史图片"},
                        {"type": "image_url", "image_url": {"url": location}},
                    ],
                }])

                found = await ctx.prepare(event, req, attach_originals=True)

                self.assertEqual(1, len(found))
                self.assertEqual(location, found[0].location)
                self.assertEqual("historical_unknown", found[0].metadata["source_origin"])
                image_parts = [
                    part for part in req.contexts[0]["content"]
                    if part.get("type") == "image_url"
                ]
                expected_url = "data:image/png;base64," + base64.b64encode(b"history-image").decode()
                self.assertEqual([expected_url], [part["image_url"]["url"] for part in image_parts])

    async def test_resolved_path_is_preferred_for_history_original_data_url(self):
        event = Event([{"type": "image", "data": {"file": "/original/one.png"}}])
        ctx = chat_context.ChatImageContext()
        ctx.capture(event)
        item = event.get_extra("pjsk_gallery_image_sources")[0]
        item.metadata["resolved_path"] = "/resolved/one.png"
        MEDIA_RESOLVER_PAYLOADS["/resolved/one.png"] = b"resolved-image"
        req = Request(prompt=f"上下文 [gallery_image:{item.ref}]")

        found = await ctx.prepare(event, req, attach_originals=True)

        expected_url = "data:image/png;base64," + base64.b64encode(b"resolved-image").decode()
        self.assertEqual([item.ref], [x.ref for x in found])
        self.assertTrue(any(
            isinstance(part, ImageURLPart)
            and part.image_url.id == item.ref
            and part.image_url.url == expected_url
            for part in req.extra_user_content_parts
        ))
        self.assertIn(("to_path", "/resolved/one.png", "image"), MEDIA_RESOLVER_CALLS)
        self.assertIn(("to_base64_data", "/resolved/one.png", "image"), MEDIA_RESOLVER_CALLS)
        self.assertNotIn(("to_path", "/original/one.png", "image"), MEDIA_RESOLVER_CALLS)

    async def test_failed_remote_history_is_skipped_without_mutating_current_image_urls(self):
        bad = "https://bad.invalid/history.png"
        good = "https://cdn.invalid/history-good.png"
        current = "/current/image.png"
        MEDIA_RESOLVER_FAILURES.add(bad)
        MEDIA_RESOLVER_PATHS[good] = "/resolved/history-good.png"
        MEDIA_RESOLVER_PAYLOADS["/resolved/history-good.png"] = b"history-good"
        event = Event([])
        ctx = chat_context.ChatImageContext()
        req = Request(
            image_urls=[current],
            contexts=[{
                "role": "user",
                "content": [
                    {"type": "text", "text": "历史图片"},
                    {"type": "image_url", "image_url": {"url": bad}},
                    {"type": "image_url", "image_url": {"url": good}},
                ],
            }],
        )

        found = await ctx.prepare(event, req, attach_originals=True)

        self.assertEqual([current], req.image_urls)
        self.assertEqual({current, good}, {item.location for item in found})
        self.assertNotIn(bad, {item.location for item in found})
        self.assertNotIn(bad, req.prompt)
        context_images = [
            part["image_url"]["url"]
            for part in req.contexts[0]["content"]
            if part.get("type") == "image_url"
        ]
        self.assertNotIn(bad, context_images)
        self.assertIn("data:image/png;base64," + base64.b64encode(b"history-good").decode(), context_images)

    async def test_import_into_prefers_resolved_path_for_remote_image(self):
        class Importer:
            def __init__(self):
                self.local_paths = []
                self.remote_urls = []

            async def import_local_file(self, path: Path, *, platform: str):
                self.local_paths.append((path, platform))
                return "local"

            async def import_candidate(self, candidate):
                self.remote_urls.append(candidate.image_url)
                return "remote"

        remote = "https://cdn.invalid/original.png"
        item = message_images.MessageImage(
            FakeImage(None, url=remote),
            {"resolved_path": "/resolved/original.png"},
        )
        importer = Importer()

        result = await item.import_into(importer)

        self.assertEqual("local", result)
        self.assertEqual([(Path("/resolved/original.png"), "submission")], importer.local_paths)
        self.assertEqual([], importer.remote_urls)

    async def test_unknown_history_and_captured_marker_share_one_request(self):
        event = Event([{"type": "image", "data": {"file": "/original/one.png"}}])
        ctx = chat_context.ChatImageContext()
        ctx.capture(event)
        captured = event.get_extra("pjsk_gallery_image_sources")[0]
        unknown_location = "data:image/png;base64," + base64.b64encode(b"history-image").decode()
        req = Request(
            image_urls=[unknown_location],
            prompt=f"历史记录 [gallery_image:{captured.ref}]",
        )

        found = await ctx.prepare(event, req, attach_originals=True)

        self.assertEqual({captured.ref, next(item.ref for item in found if item.location == unknown_location)},
                         {item.ref for item in found})
        self.assertTrue(any(
            isinstance(part, ImageURLPart) and part.image_url.id == captured.ref
            for part in req.extra_user_content_parts
        ))

    async def test_forward_nodes_keep_original_sender_and_position(self):
        class Bot:
            async def call_action(self, name, **params):
                if name != "get_forward_msg":
                    raise AssertionError(name)
                return {"data": {"messages": [{"type": "node", "data": {
                    "message_id": "node-msg", "sender": {"user_id": "u-forward", "nickname": "转发者"},
                    "content": [{"type": "image", "data": {"url": "https://img.invalid/a.png"}},
                                 {"type": "image", "data": {"url": "https://img.invalid/b.png"}}],
                }}]}}

        event = Event([{"type": "forward", "data": {"id": "forward-1"}}])
        event.bot = Bot()
        result = await message_images.collect_submission_images(event)
        self.assertEqual(2, len(result.items))
        self.assertEqual([1, 2], [x.metadata["image_index"] for x in result.items])
        self.assertEqual({"u-forward"}, {x.metadata["source_sender_id"] for x in result.items})
        self.assertEqual({"转发者"}, {x.metadata["source_sender_name"] for x in result.items})


class CollectionAndDBTests(unittest.IsolatedAsyncioTestCase):
    async def test_candidates_reuse_id_80_aliases_and_known_pairings(self):
        with tempfile.TemporaryDirectory() as td:
            db = db_module.ImageIndexDB(Path(td) / "index.db")
            for index in range(79):
                db.get_or_create_tag(f"dummy-{index}")
            mafuyu_id = db.get_or_create_tag("朝比奈真冬", tag_type="character")
            self.assertEqual(80, mafuyu_id)
            db.add_alias("朝比奈真冬", "朝比奈まふゆ")
            db.add_alias("朝比奈真冬", "Mafuyu Asahina")
            pair_ids = {
                name: db.get_or_create_tag(name, tag_type="pairing")
                for name in ("杏豆", "遥实")
            }
            service = collection_module.ChatImageCollectionService(db, None, Path(td))
            state = await service.prepare([])
            candidates = service.candidate_tags()
            mafuyu = next(x for x in candidates if x["standard_name"] == "朝比奈真冬")
            self.assertEqual(80, mafuyu["tag_id"])
            self.assertEqual(80, db.resolve_tag("朝比奈まふゆ", allow_fuzzy=False).tag_id)
            self.assertEqual(80, db.resolve_tag("Mafuyu Asahina", allow_fuzzy=False).tag_id)
            self.assertEqual(1, len([x for x in candidates if x["tag_id"] == 80]))
            by_name = {x["name"]: x for x in candidates}
            self.assertEqual(set(by_name["杏豆"]["member_ids"]), {
                by_name["白石杏"]["tag_id"], by_name["小豆泽心羽"]["tag_id"]
            })
            self.assertEqual(set(by_name["遥实"]["member_ids"]), {
                by_name["桐谷遥"]["tag_id"], by_name["花里实乃理"]["tag_id"]
            })
            self.assertEqual(pair_ids["杏豆"], by_name["杏豆"]["tag_id"])
            self.assertEqual(pair_ids["遥实"], by_name["遥实"]["tag_id"])
            self.assertEqual(26 + 6 + 2, len(state.candidates))

    async def test_save_duplicate_call_summary_and_rejected_tag_not_success(self):
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            db = db_module.ImageIndexDB(root / "index.db")
            accepted_id = db.get_or_create_tag("白石杏", tag_type="character")
            rejected_id = db.get_or_create_tag("小豆泽心羽", tag_type="character")
            image_path = root / "image.png"
            image_path.write_bytes(b"image")
            image_id = db.upsert_image(file_path=str(image_path), file_name=image_path.name, sha256="seed", width=1, height=1, format_="png")
            with db._connect() as conn:
                conn.execute("INSERT INTO image_tags(image_id, tag_id, source_type, score, review_status, review_reason, created_at, updated_at) VALUES(?,?,?,?,?,?,?,?)",
                             (image_id, rejected_id, "manual", 1, "rejected", "拒绝", "now", "now"))
            result = db.commit_chat_collection_image(image_id=image_id, image_url="/image.png", author="sender",
                raw_tags=["白石杏", "小豆泽心羽"], extra_json={"source_message_id": "m1"},
                tag_ids=[accepted_id, rejected_id], tag_members={})
            self.assertEqual([accepted_id], result["tag_ids_accepted"])
            self.assertEqual([rejected_id], result["tag_ids_skipped"])
            self.assertTrue(result["changed"])
            importer = FakeImporter(root, db)
            service = collection_module.ChatImageCollectionService(db, importer, root)
            item = message_images.MessageImage(FakeImage(file=str(image_path)), {"session_id": "s", "source_message_id": "m1", "source_sender_name": "sender"})
            state = collection_module.ChatImageCollection(images={item.ref: item}, candidates=[
                {"tag_id": accepted_id, "name": "白石杏", "tag_type": "character"},
                {"tag_id": rejected_id, "name": "小豆泽心羽", "tag_type": "character"},
            ])
            first = await service.save(state, item.ref, [accepted_id], reason="测试")
            second = await service.save(state, item.ref, [accepted_id], reason="测试")
            self.assertTrue(first["ok"])
            self.assertTrue(first["changed"])
            self.assertFalse(second["changed"])
            self.assertEqual("顺手收了 1 张：白石杏", await service.summary(state))
            rejected = await service.save(state, item.ref, [rejected_id], reason="测试")
            self.assertFalse(rejected["ok"])
            self.assertFalse(rejected.get("changed", False))


class MarkerInstallerTests(unittest.TestCase):
    def test_marker_installer_matches_current_source_tree(self):
        repo = Path(__file__).resolve().parents[1]
        backup_source = Path(r"D:\astrbot\data\backups\pjsk-chat-markers-v0231-20260907")
        if not backup_source.is_dir():
            self.skipTest("live marker backup is unavailable")
        with tempfile.TemporaryDirectory() as td:
            root = Path(td) / "AstrBot"
            (root / "astrbot/builtin_stars/astrbot").mkdir(parents=True)
            (root / "data/plugins/plugin_upload_astrbot_plugin_group_chat_plus/utils").mkdir(parents=True)
            shutil.copy2(repo / "integrations" / "install_chat_image_markers.py", root / "install.py")
            # These are complete originals saved by the prior live install, copied read-only.
            shutil.copy2(backup_source / "group_chat_context.py", root / "astrbot/builtin_stars/astrbot/group_chat_context.py")
            shutil.copy2(backup_source / "group_image_handler.py", root / "data/plugins/plugin_upload_astrbot_plugin_group_chat_plus/utils/image_handler.py")
            shutil.copy2(backup_source / "group_chat_plus_main.py", root / "data/plugins/plugin_upload_astrbot_plugin_group_chat_plus/main.py")
            backup = Path(td) / "backup"
            completed = subprocess.run([sys.executable, str(root / "install.py"), str(root), str(backup)], capture_output=True, text=True)
            self.assertEqual(0, completed.returncode, completed.stderr)
            for path in [root / "astrbot/builtin_stars/astrbot/group_chat_context.py",
                         root / "data/plugins/plugin_upload_astrbot_plugin_group_chat_plus/utils/image_handler.py",
                         root / "data/plugins/plugin_upload_astrbot_plugin_group_chat_plus/main.py"]:
                self.assertIn("# pjsk-gallery-image-markers-v1", path.read_text(encoding="utf-8"))


if __name__ == "__main__":
    unittest.main()
