from __future__ import annotations

import re
import unicodedata
from dataclasses import dataclass, field
from pathlib import Path

from astrbot.api import logger
from astrbot.api.message_components import Image, Reply

from .db import ImageIndexDB
from .importer import ImportedImageService
from .matcher import normalize_tag_name
from .review_service import ReviewService

SUBMISSION_PATTERN = re.compile(r"(?:^|[\s])(?:[/!\uFF01.\u3002\uFF0E])?(?:\u6295\u7A3F|tg)\s+(?P<body>.+?)\s*$", re.IGNORECASE)
ALIAS_PATTERN = re.compile(
    r"(?P<tag>.+?)\s+(?:\u522B\u540D|alias(?:es)?)\s*(?:[:\uFF1A=]\s*|\s+)(?P<aliases>.+)$",
    re.IGNORECASE,
)

APPROVED_REVIEW_STATUSES = {"approved", "manual_approved"}
PENDING_REVIEW_STATUSES = {"pending", "uncertain"}
REJECTED_REVIEW_STATUSES = {"rejected", "manual_rejected"}


@dataclass(slots=True)
class SubmissionRequest:
    tag_name: str
    aliases: list[str] = field(default_factory=list)


@dataclass(slots=True)
class SubmissionImageResult:
    ok: bool
    image_index: int
    image_id: int | None = None
    review_id: int | None = None
    review_status: str = ""
    similar_image_ids: list[int] = field(default_factory=list)
    error_message: str = ""


@dataclass(slots=True)
class SubmissionResult:
    ok: bool
    reply_message: str
    input_tag_name: str = ""
    tag_name: str = ""
    resolved_from_alias: bool = False
    created_tag: bool = False
    aliases: list[str] = field(default_factory=list)
    skipped_aliases: list[str] = field(default_factory=list)
    items: list[SubmissionImageResult] = field(default_factory=list)
    sender_id: str = ""
    sender_name: str = ""
    platform_name: str = ""
    session_id: str = ""
    message_id: str = ""

    @property
    def image_count(self) -> int:
        return len(self.items)

    @property
    def processed_count(self) -> int:
        return sum(1 for item in self.items if item.ok)

    @property
    def approved_count(self) -> int:
        return sum(1 for item in self.items if item.ok and item.review_status in APPROVED_REVIEW_STATUSES)

    @property
    def pending_count(self) -> int:
        return sum(1 for item in self.items if item.ok and item.review_status in PENDING_REVIEW_STATUSES)

    @property
    def rejected_count(self) -> int:
        return sum(1 for item in self.items if item.ok and item.review_status in REJECTED_REVIEW_STATUSES)

    @property
    def failure_count(self) -> int:
        return sum(1 for item in self.items if not item.ok)

    @property
    def image_ids(self) -> list[int]:
        return [int(item.image_id) for item in self.items if item.ok and item.image_id is not None]

    @property
    def review_ids(self) -> list[int]:
        return [int(item.review_id) for item in self.items if item.review_id is not None]

    @property
    def image_id(self) -> int | None:
        ids = self.image_ids
        return ids[0] if ids else None

    @property
    def review_id(self) -> int | None:
        ids = self.review_ids
        return ids[0] if ids else None

    @property
    def review_status(self) -> str:
        statuses = {str(item.review_status) for item in self.items if item.ok and item.review_status}
        if not statuses:
            return ""
        if len(statuses) == 1:
            return next(iter(statuses))
        return "mixed"


class SubmissionService:
    def __init__(self, db: ImageIndexDB, importer: ImportedImageService, reviewer: ReviewService) -> None:
        self.db = db
        self.importer = importer
        self.reviewer = reviewer

    @staticmethod
    def extract_tag_from_text(text: str) -> str:
        parsed = SubmissionService.parse_submission_text(text)
        return parsed.tag_name if parsed else ""

    @staticmethod
    def parse_submission_text(text: str) -> SubmissionRequest | None:
        normalized = re.sub(r"\s+", " ", str(text or "")).strip()
        if not normalized:
            return None

        match = SUBMISSION_PATTERN.search(normalized)
        if not match:
            return None

        body = match.group("body").strip()
        if not body:
            return SubmissionRequest(tag_name="")

        alias_match = ALIAS_PATTERN.fullmatch(body)
        if not alias_match:
            return SubmissionRequest(tag_name=body)

        tag_name = alias_match.group("tag").strip()
        aliases = SubmissionService._split_aliases(alias_match.group("aliases"), tag_name)
        return SubmissionRequest(tag_name=tag_name, aliases=aliases)

    @staticmethod
    def _split_aliases(alias_text: str, tag_name: str = "") -> list[str]:
        text = unicodedata.normalize("NFKC", str(alias_text or "")).strip()
        if not text:
            return []

        raw_items = re.split(r"[,\uFF0C\u3001/|;\uFF1B]+", text)
        if len(raw_items) == 1 and " " in text:
            whitespace_items = [item for item in re.split(r"\s+", text) if item]
            if len(whitespace_items) > 1:
                raw_items = whitespace_items

        tag_normalized = normalize_tag_name(tag_name)
        seen: set[str] = set()
        result: list[str] = []
        for item in raw_items:
            alias = str(item or "").strip()
            normalized = normalize_tag_name(alias)
            if not alias or not normalized or normalized == tag_normalized or normalized in seen:
                continue
            seen.add(normalized)
            result.append(alias)
        return result

    async def submit_from_event(
        self,
        event,
        tag_name: str,
        aliases: list[str] | None = None,
        *,
        review_enabled: bool = True,
    ) -> SubmissionResult:
        input_tag_name = str(tag_name or "").strip()
        if not input_tag_name:
            return SubmissionResult(
                ok=False,
                reply_message="请在投稿命令后提供角色 tag，例如：投稿 初音未来",
            )

        canonical_tag_name, resolved_from_alias = self._resolve_target_tag(input_tag_name)
        normalized_aliases = self._normalize_submission_aliases(
            aliases or [],
            input_tag_name=input_tag_name,
            canonical_tag_name=canonical_tag_name,
        )

        image_paths = await self._extract_image_paths(event)
        if not image_paths:
            return SubmissionResult(
                ok=False,
                reply_message="投稿时请至少附带 1 张图片，或回复一条带图消息后发送 tg <tag>。",
                input_tag_name=input_tag_name,
                tag_name=canonical_tag_name,
                resolved_from_alias=resolved_from_alias,
            )

        sender_id = self._safe_call(event, "get_sender_id")
        sender_name = self._safe_call(event, "get_sender_name")
        platform_name = self._safe_call(event, "get_platform_name")
        unified_origin = str(getattr(event, "unified_msg_origin", "") or "")
        message_obj = getattr(event, "message_obj", None)
        message_id = str(getattr(message_obj, "message_id", "") or "")

        tag_exists = self.db.get_tag_row(canonical_tag_name) is not None
        tag_id: int | None = None
        tag_ready = False
        created_tag = False
        aliases_applied = False
        added_aliases: list[str] = []
        skipped_aliases: list[str] = []
        items: list[SubmissionImageResult] = []

        for image_index, image_path in enumerate(image_paths, start=1):
            try:
                imported = await self.importer.import_local_file(image_path, platform="submission")

                if not tag_ready:
                    tag_id = self.db.get_or_create_tag(canonical_tag_name, is_character=True)
                    tag_ready = True
                    created_tag = not tag_exists

                if not aliases_applied:
                    added_aliases, skipped_aliases = self._apply_aliases(canonical_tag_name, normalized_aliases)
                    aliases_applied = True

                if review_enabled:
                    decision = await self.reviewer.review_image_for_tag(imported.file_path, canonical_tag_name)
                    review_status = decision.status
                    review_score = decision.confidence
                    review_reason = decision.reason
                    review_raw_result = decision.raw_result
                else:
                    review_status = "approved"
                    review_score = 1.0
                    review_reason = "投稿审核已关闭，直接入库"
                    review_raw_result = ""

                self.db.link_image_tag(
                    imported.image_id,
                    int(tag_id),
                    source_type="submission:user",
                    review_status=review_status,
                    score=review_score,
                    review_reason=review_reason,
                )

                review_id: int | None = None
                if review_enabled and (
                    review_status in {"pending", "uncertain", "rejected"}
                    or self.reviewer.is_character_tag(canonical_tag_name)
                ):
                    review_id = self.db.create_review_task(
                        imported.image_id,
                        int(tag_id),
                        review_status,
                        model_result=review_raw_result,
                        reason=review_reason,
                    )

                post_url = f"submission://{platform_name or 'unknown'}/{message_id or imported.sha256}"
                image_url = f"{post_url}/{imported.sha256}"
                author = str(sender_name or sender_id or "unknown")

                raw_tags = self._build_raw_tags(
                    canonical_tag_name,
                    input_tag_name,
                    normalized_aliases,
                    resolved_from_alias=resolved_from_alias,
                )

                self.db.upsert_source(
                    image_id=imported.image_id,
                    platform="submission",
                    post_url=post_url,
                    image_url=image_url,
                    author=author,
                    raw_tags=raw_tags,
                    extra_json={
                        "source_kind": "user_submission",
                        "sender_id": str(sender_id or ""),
                        "sender_name": str(sender_name or ""),
                        "platform_name": str(platform_name or ""),
                        "session_id": unified_origin,
                        "message_id": message_id,
                        "submission_input_tag": input_tag_name,
                        "submission_tag": canonical_tag_name,
                        "submission_aliases": normalized_aliases,
                        "linked_aliases": added_aliases,
                        "resolved_from_alias": resolved_from_alias,
                        "input_tag_resolved_to": canonical_tag_name if resolved_from_alias else "",
                        "image_index": image_index,
                        "image_count": len(image_paths),
                        "similar_image_ids": imported.similar_image_ids,
                    },
                )

                items.append(
                    SubmissionImageResult(
                        ok=True,
                        image_index=image_index,
                        image_id=imported.image_id,
                        review_id=review_id,
                        review_status=review_status,
                        similar_image_ids=list(imported.similar_image_ids),
                    ),
                )
            except Exception as exc:
                logger.error(
                    f"[PJSKPic] 投稿处理失败: tag={input_tag_name}, image_index={image_index}, path={image_path}, error={exc}",
                    exc_info=True,
                )
                items.append(
                    SubmissionImageResult(
                        ok=False,
                        image_index=image_index,
                        error_message=str(exc),
                    ),
                )

        result = SubmissionResult(
            ok=any(item.ok for item in items),
            reply_message="",
            input_tag_name=input_tag_name,
            tag_name=canonical_tag_name,
            resolved_from_alias=resolved_from_alias,
            created_tag=created_tag,
            aliases=list(added_aliases),
            skipped_aliases=list(skipped_aliases),
            items=items,
            sender_id=str(sender_id or ""),
            sender_name=str(sender_name or ""),
            platform_name=str(platform_name or ""),
            session_id=unified_origin,
            message_id=message_id,
        )
        result.reply_message = self._build_reply_message(result)
        return result

    def _resolve_target_tag(self, input_tag_name: str) -> tuple[str, bool]:
        match = self.db.resolve_tag(input_tag_name, allow_fuzzy=False, candidate_limit=1)
        if match.matched and match.tag_name:
            return str(match.tag_name), bool(match.match_type == "exact_alias")
        return input_tag_name, False

    def _normalize_submission_aliases(
        self,
        aliases: list[str],
        *,
        input_tag_name: str,
        canonical_tag_name: str,
    ) -> list[str]:
        input_normalized = normalize_tag_name(input_tag_name)
        canonical_normalized = normalize_tag_name(canonical_tag_name)
        seen: set[str] = set()
        result: list[str] = []
        for alias in aliases:
            clean_alias = str(alias or "").strip()
            normalized = normalize_tag_name(clean_alias)
            if (
                not clean_alias
                or not normalized
                or normalized in seen
                or normalized == input_normalized
                or normalized == canonical_normalized
            ):
                continue
            seen.add(normalized)
            result.append(clean_alias)
        return result

    def _apply_aliases(self, tag_name: str, aliases: list[str]) -> tuple[list[str], list[str]]:
        added_aliases: list[str] = []
        skipped_aliases: list[str] = []
        for alias in aliases:
            ok, message = self.db.add_alias(tag_name, alias)
            if ok:
                added_aliases.append(alias)
            else:
                skipped_aliases.append(f"{alias}（{message}）")
        return added_aliases, skipped_aliases

    @staticmethod
    def _build_raw_tags(
        canonical_tag_name: str,
        input_tag_name: str,
        aliases: list[str],
        *,
        resolved_from_alias: bool,
    ) -> list[str]:
        items = [canonical_tag_name]
        if resolved_from_alias:
            items.append(input_tag_name)
        items.extend(aliases)
        seen: set[str] = set()
        result: list[str] = []
        for item in items:
            clean_item = str(item or "").strip()
            normalized = normalize_tag_name(clean_item)
            if not clean_item or not normalized or normalized in seen:
                continue
            seen.add(normalized)
            result.append(clean_item)
        return result

    def _build_reply_message(self, result: SubmissionResult) -> str:
        if result.image_count == 1 and result.processed_count == 1 and result.failure_count == 0:
            return self._build_single_reply(result)
        return self._build_batch_reply(result)

    def _build_single_reply(self, result: SubmissionResult) -> str:
        item = result.items[0]
        lines = [f"投稿已收录：{result.tag_name}"]
        if result.resolved_from_alias:
            lines.append(f"输入 tag「{result.input_tag_name}」已自动归并到主 tag。")
        if result.created_tag:
            lines.append("主 tag 原本不存在，已自动创建。")
        if result.aliases:
            lines.append("已补充别名：" + "、".join(result.aliases))
        if result.skipped_aliases:
            lines.append("以下别名未处理：" + "；".join(result.skipped_aliases[:10]))
        lines.append(f"image_id：{item.image_id}")
        lines.append("审核结果：" + self._review_status_text(item))
        if item.similar_image_ids:
            lines.append("检测到疑似重复图片：" + "、".join(str(image_id) for image_id in item.similar_image_ids[:10]))
        return "\n".join(lines)

    def _build_batch_reply(self, result: SubmissionResult) -> str:
        lines = [f"投稿处理完成：{result.tag_name}"]
        if result.resolved_from_alias:
            lines.append(f"输入 tag：{result.input_tag_name}（已自动归并到主 tag）")
        if result.created_tag:
            lines.append("主 tag 原本不存在，已自动创建。")
        if result.aliases:
            lines.append("已补充别名：" + "、".join(result.aliases))
        if result.skipped_aliases:
            lines.append("以下别名未处理：" + "；".join(result.skipped_aliases[:10]))

        lines.append(
            f"共 {result.image_count} 张：已处理 {result.processed_count}，"
            f"已通过 {result.approved_count}，待人工复核 {result.pending_count}，"
            f"拒绝 {result.rejected_count}，失败 {result.failure_count}。"
        )

        successful_items = [item for item in result.items if item.ok]
        failed_items = [item for item in result.items if not item.ok]

        if successful_items:
            lines.append("处理明细：")
            for item in successful_items[:10]:
                detail = f"#{item.image_index} image_id={item.image_id}，审核：{self._review_status_text(item)}"
                if item.similar_image_ids:
                    detail += f"，疑似重复：{'、'.join(str(image_id) for image_id in item.similar_image_ids[:5])}"
                lines.append(detail)
            if len(successful_items) > 10:
                lines.append(f"……其余 {len(successful_items) - 10} 张已省略。")

        if failed_items:
            lines.append("失败明细：")
            for item in failed_items[:10]:
                detail = item.error_message or "未知错误"
                lines.append(f"#{item.image_index} 失败：{detail}")
            if len(failed_items) > 10:
                lines.append(f"……其余 {len(failed_items) - 10} 张失败记录已省略。")

        if result.processed_count <= 0:
            lines.insert(0, "本次投稿未能成功处理任何图片。")

        return "\n".join(lines)

    @staticmethod
    def _review_status_text(item: SubmissionImageResult) -> str:
        if item.review_status in APPROVED_REVIEW_STATUSES:
            return "已通过，可直接参与发图。"
        if item.review_status in PENDING_REVIEW_STATUSES:
            return f"待人工复核（review_id：{item.review_id or '-'}）。"
        if item.review_status in REJECTED_REVIEW_STATUSES:
            return f"{item.review_status}（review_id：{item.review_id or '-'}）。"
        if item.review_status:
            return item.review_status
        return "未知"

    async def _extract_image_paths(self, event) -> list[Path]:
        message_obj = getattr(event, "message_obj", None)
        message_chain = getattr(message_obj, "message", None)
        if not message_chain:
            return []

        direct_images = await self._collect_image_paths_from_chain(message_chain)
        if direct_images:
            return direct_images

        for component in message_chain:
            if not isinstance(component, Reply):
                continue
            reply_chain = getattr(component, "chain", None)
            reply_images = await self._collect_image_paths_from_chain(reply_chain, include_reply=True)
            if reply_images:
                return reply_images
        return []

    async def _collect_image_paths_from_chain(
        self,
        message_chain,
        *,
        include_reply: bool = False,
    ) -> list[Path]:
        result: list[Path] = []
        seen: set[str] = set()
        for component in message_chain or []:
            if not isinstance(component, Image):
                if include_reply and isinstance(component, Reply):
                    nested_paths = await self._collect_image_paths_from_chain(
                        getattr(component, "chain", None),
                        include_reply=True,
                    )
                    for path in nested_paths:
                        normalized = str(path)
                        if normalized in seen:
                            continue
                        seen.add(normalized)
                        result.append(path)
                continue

            try:
                image_path = await component.convert_to_file_path()
            except Exception as exc:
                logger.warning(f"[PJSKPic] 投稿图片提取失败: {exc}")
                continue
            if not image_path:
                continue
            path = Path(str(image_path))
            normalized = str(path)
            if path.exists() and normalized not in seen:
                seen.add(normalized)
                result.append(path)
        return result

    @staticmethod
    def _safe_call(obj, method_name: str) -> str:
        method = getattr(obj, method_name, None)
        if callable(method):
            try:
                value = method()
                return str(value or "")
            except Exception:
                return ""
        return ""
