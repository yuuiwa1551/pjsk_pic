from __future__ import annotations

import asyncio
from collections import defaultdict, deque
from pathlib import Path

from astrbot.api import AstrBotConfig, logger
from astrbot.api.event import AstrMessageEvent, filter
from astrbot.api.event import filter as event_filter
from astrbot.api.star import Context, Star, StarTools
from astrbot.core.message.message_event_result import MessageChain

from .core import ImageIndexDB, LibraryIndexer, extract_query_from_text


class PJSKPicPlugin(Star):
    def __init__(self, context: Context, config: AstrBotConfig) -> None:
        super().__init__(context)
        self.config = config
        self.data_dir = StarTools.get_data_dir("astrbot_plugin_pjsk_pic")
        self.db = ImageIndexDB(self.data_dir / "image_index.db")
        self.indexer = LibraryIndexer(self.db)
        self.recent_by_session: dict[str, deque[int]] = defaultdict(
            lambda: deque(maxlen=self._dedupe_count()),
        )

    async def initialize(self) -> None:
        library_root = self._library_root()
        library_root.mkdir(parents=True, exist_ok=True)
        if self.config.get("scan_on_startup", True):
            try:
                result = await asyncio.to_thread(self.indexer.scan, library_root)
                logger.info(f"[PJSKPic] 启动扫描完成: {result}")
            except Exception as e:
                logger.error(f"[PJSKPic] 启动扫描失败: {e}", exc_info=True)

    def _library_root(self) -> Path:
        configured = str(self.config.get("library_root", "") or "").strip()
        if configured:
            return Path(configured).expanduser().resolve()
        return (self.data_dir / "library").resolve()

    def _dedupe_count(self) -> int:
        count = int(self.config.get("recent_dedupe_count", 20) or 20)
        return max(1, count)

    def _recent_queue(self, session_id: str) -> deque[int]:
        queue = self.recent_by_session.get(session_id)
        if queue is None or queue.maxlen != self._dedupe_count():
            queue = deque(list(queue or []), maxlen=self._dedupe_count())
            self.recent_by_session[session_id] = queue
        return queue

    async def _send_tag_image(
        self,
        event: AstrMessageEvent,
        raw_query: str,
        count: int = 1,
        silent_on_tool: bool = False,
    ) -> str | None:
        query = (raw_query or "").strip()
        if not query:
            if not silent_on_tool:
                await event.send(MessageChain().message("没看懂你要看什么图。"))
            return "empty_query"

        match = self.db.resolve_tag(
            query=query,
            allow_fuzzy=bool(self.config.get("allow_fuzzy_match", True)),
            candidate_limit=int(self.config.get("ambiguous_candidate_limit", 5) or 5),
        )

        if not match.matched:
            if match.candidates:
                msg = f"你想看的是不是：{'、'.join(match.candidates)}"
            else:
                msg = f"图库里还没有“{query}”这个 tag。"
            if not silent_on_tool:
                await event.send(MessageChain().message(msg))
            return "tag_not_found"

        send_count = max(1, min(int(count or 1), 3))
        sent = 0
        queue = self._recent_queue(event.unified_msg_origin)

        for _ in range(send_count):
            row = self.db.get_random_image_for_tag(match.tag_id, list(queue))
            if not row:
                if sent == 0:
                    await event.send(
                        MessageChain().message(f"“{match.tag_name}”这个 tag 目前没有可发送图片。"),
                    )
                    return "empty_tag"
                break

            image_path = Path(str(row["file_path"]))
            if not image_path.exists():
                logger.warning(f"[PJSKPic] 图片文件不存在，已跳过: {image_path}")
                continue

            await event.send(MessageChain().file_image(str(image_path)))
            queue.append(int(row["id"]))
            self.db.record_send_log(event.unified_msg_origin, int(row["id"]), str(match.tag_name))
            sent += 1

        if sent == 0:
            return "send_failed"
        return None

    @filter.regex(r"^(?:看看|看下|看一看|来张|来一张|发一张|来点).+")
    async def send_image_by_natural_language(self, event: AstrMessageEvent):
        query = extract_query_from_text(event.message_str)
        if not query:
            return
        result = await self._send_tag_image(event, query)
        if result is None:
            event.stop_event()

    @event_filter.llm_tool(name="send_local_image_by_tag")
    async def send_local_image_by_tag(
        self,
        event: AstrMessageEvent,
        tag: str,
        count: int = 1,
    ):
        """
        从本地图库按 tag 或别名随机发送图片。

        Args:
            tag(string): 想看的图片 tag、角色名或 tag 别名
            count(number): 发送图片数量，默认 1，当前最多 3
        """
        if not self.config.get("enable_llm_tool", True):
            return "该工具当前未启用。"
        await self._send_tag_image(event, tag, count=count, silent_on_tool=False)
        return None

    @filter.command_group("pjsk图库")
    async def pjsk_gallery(self):
        """PJSK 图片库管理命令。"""

    @pjsk_gallery.command("重扫")
    @filter.permission_type(filter.PermissionType.ADMIN)
    async def rescan_library(self, event: AstrMessageEvent):
        library_root = self._library_root()
        yield event.plain_result(f"开始扫描图库：{library_root}")
        result = await asyncio.to_thread(self.indexer.scan, library_root)
        yield event.plain_result(
            "扫描完成："
            f"扫描 {result['scanned']}，入库 {result['indexed']}，关联 {result['linked']}，"
            f"跳过 {result['skipped']}，失效 {result['missing_marked_inactive']}"
        )

    @pjsk_gallery.command("统计")
    @filter.permission_type(filter.PermissionType.ADMIN)
    async def show_stats(self, event: AstrMessageEvent):
        stats = self.db.get_stats()
        yield event.plain_result(
            f"图库统计：图片 {stats['images']} 张，tag {stats['tags']} 个，alias {stats['aliases']} 个。"
        )

    @pjsk_gallery.command("查看")
    @filter.permission_type(filter.PermissionType.ADMIN)
    async def show_tag_info(self, event: AstrMessageEvent, tag_name: str):
        count = self.db.count_images_for_tag(tag_name)
        aliases = self.db.list_aliases(tag_name)
        if count == 0 and not aliases and self.db.get_tag_id(tag_name) is None:
            yield event.plain_result(f"没有找到 tag：{tag_name}")
            return
        alias_text = "、".join(aliases) if aliases else "无"
        yield event.plain_result(f"tag：{tag_name}\n图片数：{count}\n别名：{alias_text}")

    @pjsk_gallery.command("别名添加")
    @filter.permission_type(filter.PermissionType.ADMIN)
    async def add_alias(self, event: AstrMessageEvent, tag_name: str, alias: str):
        ok, message = self.db.add_alias(tag_name, alias)
        if ok:
            yield event.plain_result(message)
        else:
            yield event.plain_result(f"添加失败：{message}")

    @pjsk_gallery.command("别名删除")
    @filter.permission_type(filter.PermissionType.ADMIN)
    async def remove_alias(self, event: AstrMessageEvent, tag_name: str, alias: str):
        ok, message = self.db.remove_alias(tag_name, alias)
        if ok:
            yield event.plain_result(message)
        else:
            yield event.plain_result(f"删除失败：{message}")

    @pjsk_gallery.command("别名查看")
    @filter.permission_type(filter.PermissionType.ADMIN)
    async def list_aliases(self, event: AstrMessageEvent, tag_name: str):
        aliases = self.db.list_aliases(tag_name)
        if not aliases:
            yield event.plain_result(f"tag “{tag_name}” 当前没有别名。")
            return
        yield event.plain_result(f"{tag_name} 的别名：{'、'.join(aliases)}")
