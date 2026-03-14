from __future__ import annotations

import asyncio
from collections import defaultdict, deque
from pathlib import Path

from astrbot.api import AstrBotConfig, logger
from astrbot.api.event import AstrMessageEvent, filter
from astrbot.api.event import filter as event_filter
from astrbot.api.star import Context, Star, StarTools
from astrbot.core.message.message_event_result import MessageChain

from .core import (
    CrawlService,
    ImageIndexDB,
    ImportedImageService,
    LibraryIndexer,
    ReviewService,
    extract_query_from_text,
)
from .core.webui import GalleryWebUI


class PJSKPicPlugin(Star):
    def __init__(self, context: Context, config: AstrBotConfig) -> None:
        super().__init__(context)
        self.config = config
        self.data_dir = StarTools.get_data_dir("astrbot_plugin_pjsk_pic")
        self.db = ImageIndexDB(self.data_dir / "image_index.db")
        self.indexer = LibraryIndexer(self.db)
        self.importer = ImportedImageService(self.db, self.data_dir, timeout_seconds=self._crawler_timeout())
        self.reviewer = ReviewService(context, self.db, config)
        self.crawl_service = CrawlService(
            db=self.db,
            importer=self.importer,
            reviewer=self.reviewer,
            config=config,
        )
        self.webui = GalleryWebUI(self.db, self.crawl_service)
        self.recent_by_session: dict[str, deque[int]] = defaultdict(
            lambda: deque(maxlen=self._dedupe_count()),
        )
        self._webui_registered = False

    async def initialize(self) -> None:
        library_root = self._library_root()
        library_root.mkdir(parents=True, exist_ok=True)
        if self.config.get("scan_on_startup", True):
            try:
                await asyncio.to_thread(self.indexer.scan, library_root)
            except Exception as exc:
                logger.error(f"[PJSKPic] 启动扫描失败: {exc}", exc_info=True)
        await self.crawl_service.start()
        if not self._webui_registered:
            self.webui.register(self.context)
            self._webui_registered = True

    async def terminate(self) -> None:
        await self.crawl_service.stop()

    def _library_root(self) -> Path:
        configured = str(self.config.get("library_root", "") or "").strip()
        if configured:
            return Path(configured).expanduser().resolve()
        return (self.data_dir / "library").resolve()

    def _dedupe_count(self) -> int:
        count = int(self.config.get("recent_dedupe_count", 20) or 20)
        return max(1, count)

    def _crawler_timeout(self) -> int:
        value = int(self.config.get("crawler_timeout_seconds", 20) or 20)
        return max(5, value)

    def _recent_queue(self, session_id: str) -> deque[int]:
        key = session_id or "default"
        queue = self.recent_by_session.get(key)
        if queue is None or queue.maxlen != self._dedupe_count():
            queue = deque(list(queue or []), maxlen=self._dedupe_count())
            self.recent_by_session[key] = queue
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
        queue = self._recent_queue(getattr(event, "unified_msg_origin", "default"))

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
                continue

            await event.send(MessageChain().file_image(str(image_path)))
            queue.append(int(row["id"]))
            self.db.record_send_log(
                getattr(event, "unified_msg_origin", "default"),
                int(row["id"]),
                str(match.tag_name),
            )
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
    async def send_local_image_by_tag(self, event: AstrMessageEvent, tag: str, count: int = 1):
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
            "图库统计："
            f"图片 {stats['images']} 张，tag {stats['tags']} 个，alias {stats['aliases']} 个，"
            f"采集任务 {stats['crawl_jobs']} 个，待处理审核 {stats['pending_reviews']} 个。"
        )

    @pjsk_gallery.command("查看")
    @filter.permission_type(filter.PermissionType.ADMIN)
    async def show_tag_info(self, event: AstrMessageEvent, tag_name: str):
        count = self.db.count_images_for_tag(tag_name)
        all_count = self.db.count_images_for_tag(tag_name, include_unapproved=True)
        aliases = self.db.list_aliases(tag_name)
        row = self.db.get_tag_row(tag_name)
        if count == 0 and not aliases and row is None:
            yield event.plain_result(f"没有找到 tag：{tag_name}")
            return
        alias_text = "、".join(aliases) if aliases else "无"
        character_text = "是" if row and int(row["is_character"]) == 1 else "否"
        yield event.plain_result(
            f"tag：{tag_name}\n"
            f"可发送图片数：{count}\n"
            f"全部图片数：{all_count}\n"
            f"角色 tag：{character_text}\n"
            f"别名：{alias_text}"
        )

    @pjsk_gallery.command("别名添加")
    @filter.permission_type(filter.PermissionType.ADMIN)
    async def add_alias(self, event: AstrMessageEvent, tag_name: str, alias: str):
        ok, message = self.db.add_alias(tag_name, alias)
        yield event.plain_result(message if ok else f"添加失败：{message}")

    @pjsk_gallery.command("别名删除")
    @filter.permission_type(filter.PermissionType.ADMIN)
    async def remove_alias(self, event: AstrMessageEvent, tag_name: str, alias: str):
        ok, message = self.db.remove_alias(tag_name, alias)
        yield event.plain_result(message if ok else f"删除失败：{message}")

    @pjsk_gallery.command("别名查看")
    @filter.permission_type(filter.PermissionType.ADMIN)
    async def list_aliases(self, event: AstrMessageEvent, tag_name: str):
        aliases = self.db.list_aliases(tag_name)
        if not aliases:
            yield event.plain_result(f"tag “{tag_name}” 当前没有别名。")
            return
        yield event.plain_result(f"{tag_name} 的别名：{'、'.join(aliases)}")

    @pjsk_gallery.command("角色标记")
    @filter.permission_type(filter.PermissionType.ADMIN)
    async def set_character_tag(self, event: AstrMessageEvent, tag_name: str, is_character_text: str):
        value = str(is_character_text or "").strip().lower()
        is_character = value in {"1", "true", "yes", "y", "是"}
        ok, message = self.db.set_tag_character(tag_name, is_character)
        yield event.plain_result(message if ok else f"设置失败：{message}")

    @pjsk_gallery.command("采集添加")
    @filter.permission_type(filter.PermissionType.ADMIN)
    async def add_crawl_job(self, event: AstrMessageEvent, platform: str, source_url: str, tags_csv: str = ""):
        tags = self._parse_csv_tags(tags_csv)
        try:
            job_id = await self.crawl_service.submit_job(platform, source_url, tags)
        except Exception as exc:
            yield event.plain_result(f"创建采集任务失败：{exc}")
            return
        yield event.plain_result(
            f"已创建采集任务 #{job_id}\n平台：{platform}\n链接：{source_url}\n标签：{('、'.join(tags) if tags else '自动提取')}"
        )

    @pjsk_gallery.command("采集列表")
    @filter.permission_type(filter.PermissionType.ADMIN)
    async def list_crawl_jobs(self, event: AstrMessageEvent):
        rows = self.db.list_crawl_jobs(limit=10)
        if not rows:
            yield event.plain_result("当前没有采集任务。")
            return
        lines = ["最近采集任务："]
        for row in rows:
            lines.append(
                f"#{row['id']} [{row['status']}] {row['platform']} {row['progress']}%\n"
                f"URL: {row['source_url']}\n"
                f"标签: {row['tags_text'] or '自动提取'}\n"
                f"结果: {row['result_summary'] or row['error_log'] or '-'}"
            )
        yield event.plain_result("\n\n".join(lines))

    @pjsk_gallery.command("采集重试")
    @filter.permission_type(filter.PermissionType.ADMIN)
    async def retry_crawl_job(self, event: AstrMessageEvent, job_id: int):
        ok, message = await self.crawl_service.retry_job(int(job_id))
        yield event.plain_result(message if ok else f"重试失败：{message}")

    @pjsk_gallery.command("审核列表")
    @filter.permission_type(filter.PermissionType.ADMIN)
    async def list_review_tasks(self, event: AstrMessageEvent, status: str = ""):
        wanted = str(status or "").strip() or None
        rows = self.db.list_review_tasks(status=wanted, limit=10)
        if not rows:
            yield event.plain_result("当前没有审核任务。")
            return
        lines = ["最近审核任务："]
        for row in rows:
            lines.append(
                f"#{row['id']} [{row['status']}] tag={row['tag_name']} image={row['image_id']}\n"
                f"原因: {row['reason'] or '-'}\n"
                f"文件: {row['file_path']}"
            )
        yield event.plain_result("\n\n".join(lines))

    @pjsk_gallery.command("审核通过")
    @filter.permission_type(filter.PermissionType.ADMIN)
    async def approve_review_task(self, event: AstrMessageEvent, review_id: int):
        ok, message = self.db.apply_manual_review(int(review_id), approved=True)
        yield event.plain_result(message if ok else f"处理失败：{message}")

    @pjsk_gallery.command("审核拒绝")
    @filter.permission_type(filter.PermissionType.ADMIN)
    async def reject_review_task(self, event: AstrMessageEvent, review_id: int):
        ok, message = self.db.apply_manual_review(int(review_id), approved=False)
        yield event.plain_result(message if ok else f"处理失败：{message}")

    @staticmethod
    def _parse_csv_tags(tags_csv: str) -> list[str]:
        if not tags_csv:
            return []
        raw = str(tags_csv).replace("，", ",").split(",")
        seen: set[str] = set()
        result: list[str] = []
        for item in raw:
            tag = item.strip()
            if not tag or tag in seen:
                continue
            seen.add(tag)
            result.append(tag)
        return result
