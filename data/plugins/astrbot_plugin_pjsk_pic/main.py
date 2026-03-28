from __future__ import annotations

import asyncio
import shutil
from collections import defaultdict, deque
from datetime import datetime
from pathlib import Path

from astrbot.api import AstrBotConfig, logger
from astrbot.api.event import AstrMessageEvent, filter
from astrbot.api.event import filter as event_filter
from astrbot.api.star import Context, Star, StarTools
from astrbot.core.message.message_event_result import MessageChain

from .core import (
    AutoCrawlService,
    CrawlTagRules,
    CrawlService,
    ImageIndexDB,
    ImportedImageService,
    LibraryIndexer,
    ReviewService,
    SubmissionNotifyService,
    SubmissionService,
    extract_query_from_text,
    parse_crawl_rule_text,
)
from .core.webui import GalleryWebUI


class PJSKPicPlugin(Star):
    OPEN_REVIEW_STATUSES = ("pending", "uncertain", "rejected")
    SENDABLE_REVIEW_STATUSES = {"approved", "manual_approved"}

    def __init__(self, context: Context, config: AstrBotConfig) -> None:
        super().__init__(context)
        self.config = config
        self.data_dir = StarTools.get_data_dir("astrbot_plugin_pjsk_pic")
        self.db = ImageIndexDB(self.data_dir / "image_index.db")
        self.indexer = LibraryIndexer(self.db)
        self.importer = ImportedImageService(
            self.db,
            self.data_dir,
            timeout_seconds=self._crawler_timeout(),
            enable_phash_dedupe=bool(self.config.get("enable_phash_dedupe", True)),
            phash_max_distance=int(self.config.get("phash_max_distance", 8) or 8),
        )
        self.reviewer = ReviewService(context, self.db, config)
        self.crawl_service = CrawlService(
            db=self.db,
            importer=self.importer,
            reviewer=self.reviewer,
            config=config,
        )
        self.auto_crawl_service = AutoCrawlService(
            db=self.db,
            crawl_service=self.crawl_service,
            config=config,
        )
        self.submission_service = SubmissionService(self.db, self.importer, self.reviewer)
        self.submission_notify_service = SubmissionNotifyService(context, config)
        self.webui = GalleryWebUI(self.db, self.crawl_service)
        self.recent_by_session: dict[str, deque[int]] = defaultdict(
            lambda: deque(maxlen=self._dedupe_count()),
        )

    async def initialize(self) -> None:
        library_root = self._library_root()
        library_root.mkdir(parents=True, exist_ok=True)
        if self.config.get("scan_on_startup", True):
            try:
                await asyncio.to_thread(self.indexer.scan, library_root)
            except Exception as exc:
                logger.error(f"[PJSKPic] 启动扫描失败: {exc}", exc_info=True)
        await self.crawl_service.start()
        await self.auto_crawl_service.start()
        if self._webui_enabled():
            try:
                await self.webui.start(
                    host=self._webui_host(),
                    port=self._webui_port(),
                    access_token=self._webui_access_token(),
                )
            except Exception as exc:
                logger.error(f"[PJSKPic] 独立 WebUI 启动失败: {exc}", exc_info=True)

    async def terminate(self) -> None:
        await self.webui.stop()
        await self.auto_crawl_service.stop()
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
        value = int(self.config.get("platform_request_timeout", self.config.get("crawler_timeout_seconds", 20)) or 20)
        return max(5, value)

    def _webui_enabled(self) -> bool:
        return bool(self.config.get("webui_enabled", True))

    def _webui_host(self) -> str:
        return str(self.config.get("webui_host", "0.0.0.0") or "0.0.0.0").strip() or "0.0.0.0"

    def _webui_port(self) -> int:
        value = int(self.config.get("webui_port", 9099) or 9099)
        return min(max(1, value), 65535)

    def _webui_access_token(self) -> str:
        return str(self.config.get("webui_access_token", "") or "").strip()

    def _submission_review_enabled(self) -> bool:
        return bool(self.config.get("submission_review_enabled", False))

    def _set_submission_review_enabled(self, enabled: bool) -> tuple[bool, str]:
        self.config["submission_review_enabled"] = bool(enabled)
        save_config = getattr(self.config, "save_config", None)
        if callable(save_config):
            try:
                save_config()
            except Exception as exc:
                logger.error(f"[PJSKPic] 保存投稿审核配置失败: {exc}", exc_info=True)
                return False, f"保存投稿审核配置失败：{exc}"
        return True, ""

    def _recent_queue(self, session_id: str) -> deque[int]:
        key = session_id or "default"
        queue = self.recent_by_session.get(key)
        if queue is None or queue.maxlen != self._dedupe_count():
            queue = deque(list(queue or []), maxlen=self._dedupe_count())
            self.recent_by_session[key] = queue
        return queue

    def _resolve_existing_tag_name(self, raw_query: str, *, allow_fuzzy: bool = False) -> tuple[str | None, str]:
        query = str(raw_query or "").strip()
        if not query:
            return None, ""
        match = self.db.resolve_tag(
            query=query,
            allow_fuzzy=allow_fuzzy,
            candidate_limit=int(self.config.get("ambiguous_candidate_limit", 5) or 5),
        )
        if match.matched and match.tag_name:
            return str(match.tag_name), str(match.match_type or "")
        return None, ""

    @staticmethod
    def _parse_alias_csv(alias_text: str) -> list[str]:
        if not alias_text:
            return []
        raw = (
            str(alias_text)
            .replace("，", ",")
            .replace("、", ",")
            .replace("；", ";")
        )
        items: list[str] = []
        seen: set[str] = set()
        for chunk in raw.replace(";", ",").split(","):
            alias = chunk.strip()
            normalized = alias.casefold()
            if not alias or normalized in seen:
                continue
            seen.add(normalized)
            items.append(alias)
        return items

    @staticmethod
    def _parse_shortcut_args(raw_message: str, command_names: set[str]) -> tuple[str, list[str]]:
        text = str(raw_message or "").strip()
        if not text:
            return "", []
        parts = text.split(maxsplit=1)
        head = parts[0].lstrip("/!！.。．").strip().lower()
        body = parts[1].strip() if len(parts) > 1 and head in command_names else text
        if not body:
            return "", []
        target, _, rest = body.partition(" ")
        aliases = PJSKPicPlugin._parse_alias_csv(rest.strip())
        return target.strip(), aliases

    @staticmethod
    def _parse_alias_command_args(raw_message: str) -> tuple[str, list[str]]:
        return PJSKPicPlugin._parse_shortcut_args(raw_message, {"alias", "别名"})

    def _batch_add_aliases(self, canonical_tag_name: str, aliases: list[str]) -> tuple[list[str], list[str]]:
        added: list[str] = []
        skipped: list[str] = []
        for alias in aliases:
            ok, message = self.db.add_alias(canonical_tag_name, alias)
            if ok:
                added.append(alias)
            else:
                skipped.append(f"{alias}（{message}）")
        return added, skipped

    def _batch_remove_aliases(self, canonical_tag_name: str, aliases: list[str]) -> tuple[list[str], list[str]]:
        removed: list[str] = []
        skipped: list[str] = []
        for alias in aliases:
            ok, message = self.db.remove_alias(canonical_tag_name, alias)
            if ok:
                removed.append(alias)
            else:
                skipped.append(f"{alias}（{message}）")
        return removed, skipped

    @staticmethod
    def _collect_display_tag_names(tags: list[dict], *, sendable_only: bool = False) -> list[str]:
        selected = tags
        if sendable_only:
            visible = [
                tag for tag in tags
                if str(tag.get("review_status") or "") in PJSKPicPlugin.SENDABLE_REVIEW_STATUSES
            ]
            if visible:
                selected = visible
        result: list[str] = []
        seen: set[str] = set()
        for tag in selected:
            name = str(tag.get("name") or "").strip()
            normalized = name.casefold()
            if not name or normalized in seen:
                continue
            seen.add(normalized)
            result.append(name)
        return result

    @staticmethod
    def _find_detail_image_path(detail: dict, *, prefer_active: bool = True) -> Path | None:
        candidates: list[str] = []
        image = dict(detail.get("image") or {})
        file_locations = list(detail.get("file_locations") or [])

        if prefer_active and image.get("is_active") and image.get("file_path"):
            candidates.append(str(image["file_path"]))
        for row in file_locations:
            if prefer_active and row.get("is_active") and row.get("file_path"):
                candidates.append(str(row["file_path"]))

        if image.get("file_path"):
            candidates.append(str(image["file_path"]))
        for row in file_locations:
            if row.get("file_path"):
                candidates.append(str(row["file_path"]))

        seen: set[str] = set()
        for candidate in candidates:
            normalized = str(candidate).strip()
            if not normalized or normalized in seen:
                continue
            seen.add(normalized)
            path = Path(normalized)
            if path.exists():
                return path
        return None

    @staticmethod
    def _build_source_brief_line(detail: dict | None) -> str:
        sources = list((detail or {}).get("sources") or [])
        submission_fallback = ""
        for source in sources:
            platform = str(source.get("platform") or "").strip().lower()
            extra = dict(source.get("extra") or {})
            if platform == "submission" or str(extra.get("source_kind") or "").strip() == "user_submission":
                submission_fallback = "来源：来自投稿"
                continue
            post_url = str(source.get("post_url") or "").strip()
            if post_url:
                return f"来源：{post_url}"
        return submission_fallback

    def _build_image_brief_text(self, image_id: int, *, matched_tag: str = "") -> str:
        detail = self.db.get_image_detail(int(image_id))
        tag_names = []
        if detail:
            tag_names = self._collect_display_tag_names(list(detail.get("tags") or []), sendable_only=True)
        if not tag_names and matched_tag:
            tag_names = [matched_tag]
        tag_text = "、".join(tag_names) if tag_names else "-"
        lines = [f"#{image_id}", f"tag：{tag_text}"]
        source_line = self._build_source_brief_line(detail)
        if source_line:
            lines.append(source_line)
        return "\n".join(lines)

    def _build_image_detail_text(self, detail: dict) -> str:
        image = dict(detail.get("image") or {})
        image_id = int(image.get("id") or 0)
        width = int(image.get("width") or 0)
        height = int(image.get("height") or 0)
        format_name = str(image.get("format") or "-").upper()
        status_text = "可发送" if int(image.get("is_active") or 0) == 1 else "已移出可发送列表"

        tags = list(detail.get("tags") or [])
        tag_segments = [
            f"{tag['name']}[{tag['review_status']}]"
            for tag in tags
            if str(tag.get("name") or "").strip()
        ]
        tag_text = "、".join(tag_segments) if tag_segments else "无"

        sources = list(detail.get("sources") or [])
        source_lines = []
        for source in sources[:3]:
            source_lines.append(
                f"- {source['platform']} / {source['author'] or '-'} / {source['post_url'] or '-'}"
            )
        if not source_lines:
            source_lines.append("- 无")

        file_locations = list(detail.get("file_locations") or [])
        location_lines = []
        for row in file_locations[:4]:
            state = "active" if row.get("is_active") else "inactive"
            location_lines.append(f"- [{row.get('storage_type')}/{state}] {row.get('file_path')}")
        if not location_lines:
            location_lines.append(f"- {image.get('file_path') or '-'}")

        return (
            f"图片：#{image_id}\n"
            f"状态：{status_text}\n"
            f"尺寸：{width}x{height}\n"
            f"格式：{format_name}\n"
            f"当前路径：{image.get('file_path') or '-'}\n"
            f"tag：{tag_text}\n"
            f"来源：\n" + "\n".join(source_lines) + "\n"
            f"文件位置：\n" + "\n".join(location_lines)
        )

    def _trash_root(self) -> Path:
        return (self.data_dir / "trash" / "images").resolve()

    def _build_trash_destination(self, image_id: int, current_path: Path) -> Path:
        stamp = datetime.now().strftime("%Y%m%d-%H%M%S")
        parent = self._trash_root() / str(image_id)
        parent.mkdir(parents=True, exist_ok=True)
        candidate = parent / f"{stamp}_{current_path.name}"
        index = 1
        while candidate.exists():
            candidate = parent / f"{stamp}_{index}_{current_path.name}"
            index += 1
        return candidate

    def _find_trash_path(self, detail: dict) -> Path | None:
        file_locations = list(detail.get("file_locations") or [])
        seen: set[str] = set()
        for row in file_locations:
            if str(row.get("storage_type") or "") != "trash":
                continue
            raw_path = str(row.get("file_path") or "").strip()
            if not raw_path or raw_path in seen:
                continue
            seen.add(raw_path)
            path = Path(raw_path)
            if path.exists():
                return path
        return None

    def _build_restore_destination(self, detail: dict, source_path: Path) -> Path:
        image = dict(detail.get("image") or {})
        image_id = int(image.get("id") or 0)
        file_locations = list(detail.get("file_locations") or [])

        candidate_paths: list[Path] = []
        current_path = str(image.get("file_path") or "").strip()
        if current_path and "/trash/" not in current_path.replace("\\", "/").lower():
            candidate_paths.append(Path(current_path))
        for row in file_locations:
            raw_path = str(row.get("file_path") or "").strip()
            if not raw_path:
                continue
            normalized = raw_path.replace("\\", "/").lower()
            if "/trash/" in normalized:
                continue
            candidate_paths.append(Path(raw_path))

        target = candidate_paths[0] if candidate_paths else (self.data_dir / "images" / "restored" / str(image_id) / source_path.name)
        target = target.resolve()
        target.parent.mkdir(parents=True, exist_ok=True)
        if target == source_path:
            target = (self.data_dir / "images" / "restored" / str(image_id) / source_path.name).resolve()
            target.parent.mkdir(parents=True, exist_ok=True)

        if not target.exists():
            return target

        stem = target.stem
        suffix = target.suffix
        stamp = datetime.now().strftime("%Y%m%d-%H%M%S")
        candidate = target.with_name(f"{stem}.restored-{stamp}{suffix}")
        index = 1
        while candidate.exists():
            candidate = target.with_name(f"{stem}.restored-{stamp}-{index}{suffix}")
            index += 1
        return candidate

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

            resolved_path = self.db.get_image_file_path(int(row["id"]))
            if not resolved_path:
                continue

            image_path = Path(resolved_path)
            if not image_path.exists():
                continue

            await event.send(MessageChain().file_image(str(image_path)))
            await event.send(
                MessageChain().message(
                    self._build_image_brief_text(
                        int(row["id"]),
                        matched_tag=str(match.tag_name),
                    ),
                ),
            )
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

    @filter.regex(r"^(?:看看|看下|看一看|看|来张|来一张|发一张|来点).+")
    async def send_image_by_natural_language(self, event: AstrMessageEvent):
        query = extract_query_from_text(event.message_str)
        if not query:
            return
        await self._send_tag_image(event, query, silent_on_tool=True)
        event.stop_event()

    def _parse_submission_request(self, raw_message: str):
        request = self.submission_service.parse_submission_text(raw_message)
        if request and request.tag_name:
            return request
        text = str(raw_message or "").strip()
        if not text:
            return None
        candidates: list[str] = [text]
        if " " in text:
            body = text.partition(" ")[2].strip()
            if body and body not in candidates:
                candidates.append(body)
        for candidate in candidates:
            fallback = self.submission_service.parse_submission_text(f"\u6295\u7A3F {candidate}")
            if fallback and fallback.tag_name:
                return fallback
        return None

    async def _handle_submission_event(self, event: AstrMessageEvent, *, missing_tag_reply: str | None = None) -> bool:
        request = self._parse_submission_request(event.message_str)
        if not request or not request.tag_name:
            if missing_tag_reply:
                await event.send(MessageChain().message(missing_tag_reply))
                event.stop_event()
            return False
        result = await self.submission_service.submit_from_event(
            event,
            request.tag_name,
            aliases=request.aliases,
            review_enabled=self._submission_review_enabled(),
        )
        if result.reply_message:
            await event.send(MessageChain().message(result.reply_message))
        if result.ok:
            await self.submission_notify_service.notify(event, result)
        event.stop_event()
        return bool(result.ok)

    @filter.command("\u6295\u7A3F", alias={"tg"})
    async def submit_image_by_user_command(self, event: AstrMessageEvent):
        await self._handle_submission_event(
            event,
            missing_tag_reply="\u8BF7\u5728\u6295\u7A3F\u547D\u4EE4\u540E\u63D0\u4F9B\u89D2\u8272 tag\uFF0C\u4F8B\u5982\uFF1A/tg \u521D\u97F3\u672A\u6765",
        )

    @filter.regex(r"^\s*(?:@.+?\(\d+\)\s+)*(?:[/!！.。．])?(?:投稿|tg)\s+.+$")
    async def submit_image_by_user(self, event: AstrMessageEvent):
        await self._handle_submission_event(event)

    @filter.command("alias", alias={"别名"})
    @filter.permission_type(filter.PermissionType.ADMIN)
    async def alias_shortcut(self, event: AstrMessageEvent):
        target_input, alias_values = self._parse_alias_command_args(event.message_str)
        if not target_input:
            yield event.plain_result("用法：/alias <tag或alias> [新alias1,新alias2]")
            return

        canonical_tag_name, match_type = self._resolve_existing_tag_name(target_input, allow_fuzzy=False)
        if not canonical_tag_name:
            yield event.plain_result(f"没有找到 tag 或 alias：{target_input}")
            return

        lines = [f"主 tag：{canonical_tag_name}"]
        if match_type == "exact_alias":
            lines.append(f"输入“{target_input}”命中 alias，已归并到主 tag。")

        if alias_values:
            added, skipped = self._batch_add_aliases(canonical_tag_name, alias_values)
            if added:
                lines.append("已添加别名：" + "、".join(added))
            if skipped:
                lines.append("以下别名未添加：" + "；".join(skipped[:10]))

        aliases = self.db.list_aliases(canonical_tag_name)
        lines.append("当前别名：" + ("、".join(aliases) if aliases else "无"))
        yield event.plain_result("\n".join(lines))

    @filter.command("unalias", alias={"删别名"})
    @filter.permission_type(filter.PermissionType.ADMIN)
    async def unalias_shortcut(self, event: AstrMessageEvent):
        target_input, alias_values = self._parse_shortcut_args(event.message_str, {"unalias", "删别名"})
        if not target_input or not alias_values:
            yield event.plain_result("用法：/unalias <tag或alias> <alias1,alias2>")
            return

        canonical_tag_name, match_type = self._resolve_existing_tag_name(target_input, allow_fuzzy=False)
        if not canonical_tag_name:
            yield event.plain_result(f"没有找到 tag 或 alias：{target_input}")
            return

        removed, skipped = self._batch_remove_aliases(canonical_tag_name, alias_values)
        lines = [f"主 tag：{canonical_tag_name}"]
        if match_type == "exact_alias":
            lines.append(f"输入“{target_input}”命中 alias，已归并到主 tag。")
        if removed:
            lines.append("已删除别名：" + "、".join(removed))
        if skipped:
            lines.append("以下别名未删除：" + "；".join(skipped[:10]))
        aliases = self.db.list_aliases(canonical_tag_name)
        lines.append("当前别名：" + ("、".join(aliases) if aliases else "无"))
        yield event.plain_result("\n".join(lines))

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
            f"采集任务 {stats['crawl_jobs']} 个，自动订阅 {stats['crawl_subscriptions']} 个，"
            f"待处理审核 {stats['pending_reviews']} 个。"
        )

    @pjsk_gallery.command("查看")
    @filter.permission_type(filter.PermissionType.ADMIN)
    async def show_tag_info(self, event: AstrMessageEvent, tag_name: str):
        canonical_tag_name, match_type = self._resolve_existing_tag_name(tag_name, allow_fuzzy=False)
        if not canonical_tag_name:
            yield event.plain_result(f"没有找到 tag：{tag_name}")
            return
        count = self.db.count_images_for_tag(canonical_tag_name)
        all_count = self.db.count_images_for_tag(canonical_tag_name, include_unapproved=True)
        aliases = self.db.list_aliases(canonical_tag_name)
        row = self.db.get_tag_row(canonical_tag_name)
        if count == 0 and not aliases and row is None:
            yield event.plain_result(f"没有找到 tag：{tag_name}")
            return
        alias_text = "、".join(aliases) if aliases else "无"
        character_text = "是" if row and int(row["is_character"]) == 1 else "否"
        lines = []
        if match_type == "exact_alias":
            lines.append(f"输入“{tag_name}”命中 alias，已归并到主 tag。")
        lines.append(
            f"tag：{canonical_tag_name}\n"
            f"可发送图片数：{count}\n"
            f"全部图片数：{all_count}\n"
            f"角色 tag：{character_text}\n"
            f"别名：{alias_text}"
        )
        yield event.plain_result("\n".join(lines))

    @pjsk_gallery.command("看图")
    @filter.permission_type(filter.PermissionType.ADMIN)
    async def show_image_detail(self, event: AstrMessageEvent, image_id: int):
        detail = self.db.get_image_detail(int(image_id))
        if not detail:
            yield event.plain_result(f"没有找到图片：#{int(image_id)}")
            return

        image_path = self._find_detail_image_path(detail, prefer_active=True)
        if image_path is None:
            image_path = self._find_trash_path(detail)
        if image_path is not None and image_path.exists():
            await event.send(MessageChain().file_image(str(image_path)))

        yield event.plain_result(self._build_image_detail_text(detail))

    @pjsk_gallery.command("别名添加")
    @filter.permission_type(filter.PermissionType.ADMIN)
    async def add_alias(self, event: AstrMessageEvent, tag_name: str, alias: str):
        canonical_tag_name, match_type = self._resolve_existing_tag_name(tag_name, allow_fuzzy=False)
        if not canonical_tag_name:
            yield event.plain_result(f"添加失败：没有找到 tag 或 alias：{tag_name}")
            return
        alias_values = self._parse_alias_csv(alias)
        if not alias_values:
            yield event.plain_result("添加失败：请提供至少一个 alias，可用逗号分隔多个别名。")
            return
        added, skipped = self._batch_add_aliases(canonical_tag_name, alias_values)
        lines = []
        if match_type == "exact_alias":
            lines.append(f"输入“{tag_name}”命中 alias，已归并到主 tag。")
        if added:
            lines.append("已添加别名：" + "、".join(added))
        if skipped:
            lines.append("以下别名未添加：" + "；".join(skipped[:10]))
        aliases = self.db.list_aliases(canonical_tag_name)
        lines.append(f"{canonical_tag_name} 当前别名：" + ("、".join(aliases) if aliases else "无"))
        yield event.plain_result("\n".join(lines))

    @pjsk_gallery.command("别名删除")
    @filter.permission_type(filter.PermissionType.ADMIN)
    async def remove_alias(self, event: AstrMessageEvent, tag_name: str, alias: str):
        canonical_tag_name, match_type = self._resolve_existing_tag_name(tag_name, allow_fuzzy=False)
        if not canonical_tag_name:
            yield event.plain_result(f"删除失败：没有找到 tag 或 alias：{tag_name}")
            return
        alias_values = self._parse_alias_csv(alias)
        if not alias_values:
            yield event.plain_result("删除失败：请提供至少一个 alias，可用逗号分隔多个别名。")
            return
        removed, skipped = self._batch_remove_aliases(canonical_tag_name, alias_values)
        lines = []
        if match_type == "exact_alias":
            lines.append(f"输入“{tag_name}”命中 alias，已归并到主 tag。")
        if removed:
            lines.append("已删除别名：" + "、".join(removed))
        if skipped:
            lines.append("以下别名未删除：" + "；".join(skipped[:10]))
        aliases = self.db.list_aliases(canonical_tag_name)
        lines.append(f"{canonical_tag_name} 当前别名：" + ("、".join(aliases) if aliases else "无"))
        yield event.plain_result("\n".join(lines))

    @pjsk_gallery.command("别名查看")
    @filter.permission_type(filter.PermissionType.ADMIN)
    async def list_aliases(self, event: AstrMessageEvent, tag_name: str):
        canonical_tag_name, match_type = self._resolve_existing_tag_name(tag_name, allow_fuzzy=False)
        if not canonical_tag_name:
            yield event.plain_result(f"没有找到 tag 或 alias：{tag_name}")
            return
        aliases = self.db.list_aliases(canonical_tag_name)
        lines = []
        if match_type == "exact_alias":
            lines.append(f"输入“{tag_name}”命中 alias，已归并到主 tag。")
        if not aliases:
            lines.append(f"tag “{canonical_tag_name}” 当前没有别名。")
            yield event.plain_result("\n".join(lines))
            return
        lines.append(f"{canonical_tag_name} 的别名：{'、'.join(aliases)}")
        yield event.plain_result("\n".join(lines))

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
        rules = parse_crawl_rule_text(tags_csv)
        try:
            job_id = await self.crawl_service.submit_job(
                platform,
                source_url,
                rules.manual_tags,
                include_tags=rules.include_tags,
                exclude_tags=rules.exclude_tags,
            )
        except Exception as exc:
            yield event.plain_result(f"创建采集任务失败：{exc}")
            return
        lines = [
            f"已创建采集任务 #{job_id}",
            f"平台：{platform}",
            f"链接：{source_url}",
            f"标签：{self._format_crawl_tags(rules.manual_tags, fallback='自动提取')}",
        ]
        if rules.include_tags:
            lines.append(f"包含采集：{self._format_crawl_tags(rules.include_tags)}")
        if rules.exclude_tags:
            lines.append(f"排除采集：{self._format_crawl_tags(rules.exclude_tags)}")
        yield event.plain_result("\n".join(lines))

    @pjsk_gallery.command("采集列表")
    @filter.permission_type(filter.PermissionType.ADMIN)
    async def list_crawl_jobs(self, event: AstrMessageEvent):
        rows = self.db.list_crawl_jobs(limit=10)
        if not rows:
            yield event.plain_result("当前没有采集任务。")
            return
        lines = ["最近采集任务："]
        for row in rows:
            job_rules = CrawlTagRules.from_db_row(row)
            rule_lines = [
                f"标签: {self._format_crawl_tags(job_rules.manual_tags, fallback='自动提取')}",
            ]
            if job_rules.include_tags:
                rule_lines.append(f"包含采集: {self._format_crawl_tags(job_rules.include_tags)}")
            if job_rules.exclude_tags:
                rule_lines.append(f"排除采集: {self._format_crawl_tags(job_rules.exclude_tags)}")
            if str(row["tag_match_mode"] or "exact") != "exact":
                rule_lines.append(f"标签匹配: {row['tag_match_mode']}")
            lines.append(
                "\n".join(
                    [
                        f"#{row['id']} [{row['status']}] {row['platform']} {row['progress']}%",
                        f"URL: {row['source_url']}",
                        *rule_lines,
                        f"结果: {row['result_summary'] or row['error_log'] or '-'}",
                    ]
                )
            )
        yield event.plain_result("\n\n".join(lines))

    @pjsk_gallery.command("自动采集状态")
    @filter.permission_type(filter.PermissionType.ADMIN)
    async def auto_crawl_status(self, event: AstrMessageEvent):
        stats = self.db.get_stats()
        rows = self.db.list_crawl_subscriptions(platform="pixiv", enabled_only=True, limit=200)
        yield event.plain_result(
            "Pixiv 自动采集状态：\n"
            f"已启用：{'是' if self.auto_crawl_service.enabled() else '否'}\n"
            f"已配置 refresh token：{'是' if self.auto_crawl_service.has_refresh_token() else '否'}\n"
            f"角色 tag 限定：{'是' if self.auto_crawl_service.character_only() else '否'}\n"
            f"检索词后缀：{self.config.get('pixiv_auto_crawl_query_suffix', 'user') or 'user'}\n"
            f"轮询间隔：{self.auto_crawl_service.interval_minutes()} 分钟\n"
            f"自动订阅数：{len(rows)} / 统计 {stats['crawl_subscriptions']}\n"
            f"单轮最多新任务：{self.auto_crawl_service.max_new_jobs_per_cycle()}\n"
            f"每个 tag 最多检查：{self.auto_crawl_service.max_results_per_tag()} 条结果"
        )

    @pjsk_gallery.command("自动采集列表")
    @filter.permission_type(filter.PermissionType.ADMIN)
    async def auto_crawl_list(self, event: AstrMessageEvent):
        rows = self.db.list_crawl_subscriptions(platform="pixiv", enabled_only=True, limit=20)
        if not rows:
            yield event.plain_result("当前没有启用中的 Pixiv 自动采集订阅。")
            return
        lines = ["当前 Pixiv 自动采集订阅："]
        for row in rows:
            lines.append(
                "\n".join(
                    [
                        f"#{row['id']} {row['tag_name']}",
                        f"query: {row['query_text'] or '-'}",
                        f"last_seen: {row['last_seen_source_uid'] or '-'}",
                        f"last_checked: {row['last_checked_at'] or '-'}",
                        f"last_success: {row['last_success_at'] or '-'}",
                        f"last_error: {row['last_error'] or '-'}",
                    ]
                )
            )
        yield event.plain_result("\n\n".join(lines))

    @pjsk_gallery.command("自动采集执行")
    @filter.permission_type(filter.PermissionType.ADMIN)
    async def run_auto_crawl_once(self, event: AstrMessageEvent):
        summary = await self.auto_crawl_service.run_once(force=True)
        yield event.plain_result(
            "Pixiv 自动采集执行完成：\n"
            f"订阅 {summary['subscriptions']} 个，检查 {summary['checked']} 个，"
            f"命中过滤 {summary['matched']} 个，入队 {summary['queued']} 个，"
            f"已存在跳过 {summary['skipped_existing']} 个，"
            f"过滤跳过 {summary['skipped_filtered']} 个，错误 {summary['errors']} 个。"
        )

    @pjsk_gallery.command("采集重试")
    @filter.permission_type(filter.PermissionType.ADMIN)
    async def retry_crawl_job(self, event: AstrMessageEvent, job_id: int):
        ok, message = await self.crawl_service.retry_job(int(job_id))
        yield event.plain_result(message if ok else f"重试失败：{message}")

    @pjsk_gallery.command("审核列表")
    @filter.permission_type(filter.PermissionType.ADMIN)
    async def list_review_tasks(self, event: AstrMessageEvent, status: str = ""):
        status_text = str(status or "").strip().lower()
        statuses = list(self.OPEN_REVIEW_STATUSES) if not status_text else None
        wanted = None if not status_text or status_text in {"all", "全部"} else status_text
        rows = self.db.list_review_tasks(status=wanted, statuses=statuses, limit=10)
        if not rows:
            yield event.plain_result("当前没有审核任务。")
            return
        lines = ["当前待处理审核任务：" if statuses else "最近审核任务："]
        for row in rows:
            lines.append(
                f"#{row['id']} [{row['status']}] tag={row['tag_name']} image={row['image_id']}\n"
                f"原因: {row['reason'] or '-'}\n"
                f"文件: {row['file_path']}"
            )
        yield event.plain_result("\n\n".join(lines))

    @pjsk_gallery.command("审核查看")
    @filter.permission_type(filter.PermissionType.ADMIN)
    async def show_review_task(self, event: AstrMessageEvent, review_id: int = 0):
        task = self.db.get_review_task(int(review_id)) if int(review_id or 0) > 0 else None
        if task is None:
            rows = self.db.list_review_tasks(statuses=self.OPEN_REVIEW_STATUSES, limit=1)
            task = rows[0] if rows else None
        if task is None:
            yield event.plain_result("当前没有待处理审核图片。")
            return

        image_path = self.db.get_image_file_path(int(task["image_id"])) or str(task["file_path"] or "")
        if image_path and Path(image_path).exists():
            await event.send(MessageChain().file_image(str(image_path)))

        yield event.plain_result(
            f"审核任务 #{task['id']}\n"
            f"状态：{task['status']}\n"
            f"tag：{task['tag_name']}\n"
            f"image_id：{task['image_id']}\n"
            f"来源：{task['source_type'] or '-'}\n"
            f"原因：{task['reason'] or '-'}\n"
            f"通过：/pjsk图库 审核通过 {task['id']}\n"
            f"拒绝：/pjsk图库 审核拒绝 {task['id']}"
        )

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

    @pjsk_gallery.command("投稿审核状态")
    @filter.permission_type(filter.PermissionType.ADMIN)
    async def submission_review_status(self, event: AstrMessageEvent):
        enabled = self._submission_review_enabled()
        yield event.plain_result(
            "投稿审核当前状态："
            + ("开启\n新投稿会进入审核链路。" if enabled else "关闭\n新投稿会默认直接入库并可参与发图。")
        )

    @pjsk_gallery.command("投稿审核开启")
    @filter.permission_type(filter.PermissionType.ADMIN)
    async def enable_submission_review(self, event: AstrMessageEvent):
        ok, message = self._set_submission_review_enabled(True)
        if not ok:
            yield event.plain_result(message)
            return
        yield event.plain_result("投稿审核已开启；后续新投稿会进入审核链路。")

    @pjsk_gallery.command("投稿审核关闭")
    @filter.permission_type(filter.PermissionType.ADMIN)
    async def disable_submission_review(self, event: AstrMessageEvent):
        ok, message = self._set_submission_review_enabled(False)
        if not ok:
            yield event.plain_result(message)
            return
        yield event.plain_result("投稿审核已关闭；后续新投稿将默认直接入库并可参与发图。")

    @pjsk_gallery.command("删图")
    @filter.permission_type(filter.PermissionType.ADMIN)
    async def delete_image(self, event: AstrMessageEvent, image_id: int):
        image_id = int(image_id)
        detail = self.db.get_image_detail(image_id)
        if not detail:
            yield event.plain_result(f"删图失败：没有找到图片 #{image_id}")
            return

        image = dict(detail.get("image") or {})
        if int(image.get("is_active") or 0) != 1:
            yield event.plain_result(f"图片 #{image_id} 当前已不在可发送状态；可用 /pjsk图库 看图 {image_id} 查看详情。")
            return

        current_path = self._find_detail_image_path(detail, prefer_active=True)
        tag_names = self._collect_display_tag_names(list(detail.get("tags") or []), sendable_only=False)
        tag_text = "、".join(tag_names) if tag_names else "-"

        if current_path is None:
            ok, message = self.db.trash_image(image_id, trash_path=None)
            yield event.plain_result(
                (f"已仅在数据库中禁用图片 #{image_id}\n"
                 f"tag：{tag_text}\n"
                 f"原因：原文件不存在，无法移入回收站。")
                if ok else f"删图失败：{message}"
            )
            return

        trash_path = self._build_trash_destination(image_id, current_path)
        try:
            await asyncio.to_thread(shutil.move, str(current_path), str(trash_path))
        except Exception as exc:
            yield event.plain_result(f"删图失败：移动到回收站失败：{exc}")
            return

        ok, message = self.db.trash_image(image_id, trash_path=str(trash_path))
        if not ok:
            try:
                if trash_path.exists():
                    trash_path.parent.mkdir(parents=True, exist_ok=True)
                    await asyncio.to_thread(shutil.move, str(trash_path), str(current_path))
            except Exception:
                logger.error(f"[PJSKPic] 删图回滚失败: image_id={image_id}, trash={trash_path}, original={current_path}", exc_info=True)
            yield event.plain_result(f"删图失败：{message}")
            return

        yield event.plain_result(
            f"已删除图片 #{image_id}\n"
            f"tag：{tag_text}\n"
            f"原路径：{current_path}\n"
            f"回收站：{trash_path}"
        )

    @pjsk_gallery.command("恢复图")
    @filter.permission_type(filter.PermissionType.ADMIN)
    async def restore_image(self, event: AstrMessageEvent, image_id: int):
        image_id = int(image_id)
        detail = self.db.get_image_detail(image_id)
        if not detail:
            yield event.plain_result(f"恢复失败：没有找到图片 #{image_id}")
            return

        image = dict(detail.get("image") or {})
        if int(image.get("is_active") or 0) == 1:
            yield event.plain_result(f"图片 #{image_id} 当前已经是可发送状态。")
            return

        trash_path = self._find_trash_path(detail)
        if trash_path is None:
            yield event.plain_result(f"恢复失败：图片 #{image_id} 在回收站中没有找到可恢复文件。")
            return

        restore_path = self._build_restore_destination(detail, trash_path)
        try:
            await asyncio.to_thread(shutil.move, str(trash_path), str(restore_path))
        except Exception as exc:
            yield event.plain_result(f"恢复失败：移动回原位置失败：{exc}")
            return

        ok, message = self.db.restore_image(image_id, restored_path=str(restore_path), trash_path=str(trash_path))
        if not ok:
            try:
                if restore_path.exists():
                    trash_path.parent.mkdir(parents=True, exist_ok=True)
                    await asyncio.to_thread(shutil.move, str(restore_path), str(trash_path))
            except Exception:
                logger.error(f"[PJSKPic] 恢复图片回滚失败: image_id={image_id}, trash={trash_path}, restored={restore_path}", exc_info=True)
            yield event.plain_result(f"恢复失败：{message}")
            return

        yield event.plain_result(
            f"已恢复图片 #{image_id}\n"
            f"恢复路径：{restore_path}\n"
            f"可用命令：/pjsk图库 看图 {image_id}"
        )

    @pjsk_gallery.command("面板地址")
    @filter.permission_type(filter.PermissionType.ADMIN)
    async def show_webui_address(self, event: AstrMessageEvent):
        if not self._webui_enabled():
            yield event.plain_result("独立 WebUI 当前已禁用。")
            return
        urls = self.webui.get_access_urls()
        if not urls:
            yield event.plain_result("独立 WebUI 当前未启动。")
            return
        lines = ["PJSK 独立 WebUI 地址：", *urls]
        if self._webui_access_token():
            lines.append("当前已启用访问令牌。")
        else:
            lines.append("当前未配置访问令牌；若开放局域网访问，请注意安全。")
        yield event.plain_result("\n".join(lines))

    @staticmethod
    def _format_crawl_tags(tags: list[str], *, fallback: str = "-") -> str:
        return "、".join(str(tag).strip() for tag in tags if str(tag).strip()) or fallback
