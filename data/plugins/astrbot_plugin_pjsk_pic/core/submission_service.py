from __future__ import annotations

import re
import unicodedata
from dataclasses import dataclass, field
from pathlib import Path

from astrbot.api import logger
from astrbot.api.message_components import Image

from .db import ImageIndexDB
from .importer import ImportedImageService
from .matcher import normalize_tag_name
from .review_service import ReviewService

SUBMISSION_PATTERN = re.compile("(?:^|[\\s])(?:[/!\uFF01.\u3002\uFF0E])?(?:\u6295\u7A3F|tg)\\s+(?P<body>.+?)\\s*$", re.IGNORECASE)
ALIAS_PATTERN = re.compile(
    "(?P<tag>.+?)\\s+(?:\u522B\u540D|alias(?:es)?)\\s*(?:[:\uFF1A=]\\s*|\\s+)(?P<aliases>.+)$",
    re.IGNORECASE,
)


@dataclass(slots=True)
class SubmissionRequest:
    tag_name: str
    aliases: list[str] = field(default_factory=list)


@dataclass(slots=True)
class SubmissionResult:
    ok: bool
    reply_message: str
    tag_name: str = ""
    image_id: int | None = None
    review_id: int | None = None
    aliases: list[str] = field(default_factory=list)
    sender_id: str = ""
    sender_name: str = ""
    platform_name: str = ""
    session_id: str = ""
    message_id: str = ""
    review_status: str = ""


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

        raw_items = re.split("[,\uFF0C\u3001/|;\uFF1B]+", text)
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

    async def submit_from_event(self, event, tag_name: str, aliases: list[str] | None = None) -> SubmissionResult:
        clean_tag = str(tag_name or "").strip()
        if not clean_tag:
            return SubmissionResult(
                ok=False,
                reply_message="\u8BF7\u5728\u6295\u7A3F\u547D\u4EE4\u540E\u63D0\u4F9B\u89D2\u8272 tag\uFF0C\u4F8B\u5982\uFF1A\u6295\u7A3F \u521D\u97F3\u672A\u6765",
            )

        normalized_aliases: list[str] = []
        seen_aliases: set[str] = set()
        for alias in aliases or []:
            clean_alias = str(alias or "").strip()
            normalized = normalize_tag_name(clean_alias)
            if not clean_alias or not normalized or normalized in seen_aliases:
                continue
            seen_aliases.add(normalized)
            normalized_aliases.append(clean_alias)

        image_paths = await self._extract_image_paths(event)
        if not image_paths:
            return SubmissionResult(ok=False, reply_message="\u6295\u7A3F\u65F6\u8BF7\u9644\u5E26 1 \u5F20\u56FE\u7247\u3002")
        if len(image_paths) > 1:
            return SubmissionResult(
                ok=False,
                reply_message="\u5F53\u524D\u6295\u7A3F\u4EC5\u652F\u6301\u5355\u56FE\uFF0C\u8BF7\u4E00\u6B21\u53EA\u53D1 1 \u5F20\u56FE\u7247\u3002",
            )

        imported = await self.importer.import_local_file(image_paths[0], platform="submission")
        tag_existed = self.db.get_tag_row(clean_tag) is not None
        tag_id = self.db.get_or_create_tag(clean_tag, is_character=True)

        added_aliases: list[str] = []
        skipped_aliases: list[str] = []
        for alias in normalized_aliases:
            ok, message = self.db.add_alias(clean_tag, alias)
            if ok:
                added_aliases.append(alias)
            else:
                skipped_aliases.append(f"{alias}\uFF08{message}\uFF09")

        decision = await self.reviewer.review_image_for_tag(imported.file_path, clean_tag)

        self.db.link_image_tag(
            imported.image_id,
            tag_id,
            source_type="submission:user",
            review_status=decision.status,
            score=decision.confidence,
            review_reason=decision.reason,
        )

        review_id: int | None = None
        if decision.status in {"pending", "uncertain", "rejected"} or self.reviewer.is_character_tag(clean_tag):
            review_id = self.db.create_review_task(
                imported.image_id,
                tag_id,
                decision.status,
                model_result=decision.raw_result,
                reason=decision.reason,
            )

        sender_id = self._safe_call(event, "get_sender_id")
        sender_name = self._safe_call(event, "get_sender_name")
        platform_name = self._safe_call(event, "get_platform_name")
        unified_origin = str(getattr(event, "unified_msg_origin", "") or "")
        message_obj = getattr(event, "message_obj", None)
        message_id = str(getattr(message_obj, "message_id", "") or "")

        post_url = f"submission://{platform_name or 'unknown'}/{message_id or imported.sha256}"
        image_url = f"{post_url}/{imported.sha256}"
        author = str(sender_name or sender_id or "unknown")

        self.db.upsert_source(
            image_id=imported.image_id,
            platform="submission",
            post_url=post_url,
            image_url=image_url,
            author=author,
            raw_tags=[clean_tag, *normalized_aliases],
            extra_json={
                "source_kind": "user_submission",
                "sender_id": str(sender_id or ""),
                "sender_name": str(sender_name or ""),
                "platform_name": str(platform_name or ""),
                "session_id": unified_origin,
                "message_id": message_id,
                "submission_tag": clean_tag,
                "submission_aliases": normalized_aliases,
                "linked_aliases": added_aliases,
                "similar_image_ids": imported.similar_image_ids,
            },
        )

        lines = [f"\u6295\u7A3F\u5DF2\u6536\u5F55\uFF1A{clean_tag}"]
        if not tag_existed:
            lines.append("\u4E3B tag \u539F\u672C\u4E0D\u5B58\u5728\uFF0C\u5DF2\u81EA\u52A8\u521B\u5EFA\u3002")
        if added_aliases:
            lines.append("\u5DF2\u8865\u5145\u522B\u540D\uFF1A" + "\u3001".join(added_aliases))
        if skipped_aliases:
            lines.append("\u4EE5\u4E0B\u522B\u540D\u672A\u5904\u7406\uFF1A" + "\uFF1B".join(skipped_aliases[:10]))
        lines.append(f"image_id\uFF1A{imported.image_id}")
        if decision.status == "approved":
            lines.append("\u5BA1\u6838\u7ED3\u679C\uFF1A\u5DF2\u901A\u8FC7\uFF0C\u53EF\u76F4\u63A5\u53C2\u4E0E\u53D1\u56FE\u3002")
        elif decision.status in {"pending", "uncertain"}:
            lines.append(f"\u5BA1\u6838\u7ED3\u679C\uFF1A\u5F85\u4EBA\u5DE5\u590D\u6838\uFF08review_id\uFF1A{review_id or '-'}\uFF09\u3002")
        else:
            lines.append(f"\u5BA1\u6838\u7ED3\u679C\uFF1A{decision.status}\uFF08review_id\uFF1A{review_id or '-'}\uFF09\u3002")

        if imported.similar_image_ids:
            lines.append("\u68C0\u6D4B\u5230\u7591\u4F3C\u91CD\u590D\u56FE\u7247\uFF1A" + "\u3001".join(str(item) for item in imported.similar_image_ids[:10]))
        return SubmissionResult(
            ok=True,
            reply_message="\\n".join(lines),
            tag_name=clean_tag,
            image_id=imported.image_id,
            review_id=review_id,
            aliases=list(added_aliases),
            sender_id=str(sender_id or ""),
            sender_name=str(sender_name or ""),
            platform_name=str(platform_name or ""),
            session_id=unified_origin,
            message_id=message_id,
            review_status=decision.status,
        )

    async def _extract_image_paths(self, event) -> list[Path]:
        message_obj = getattr(event, "message_obj", None)
        message_chain = getattr(message_obj, "message", None)
        if not message_chain:
            return []

        result: list[Path] = []
        for component in message_chain:
            if not isinstance(component, Image):
                continue
            try:
                image_path = await component.convert_to_file_path()
            except Exception as exc:
                logger.warning(f"[PJSKPic] \u6295\u7A3F\u56FE\u7247\u63D0\u53D6\u5931\u8D25: {exc}")
                continue
            if not image_path:
                continue
            path = Path(str(image_path))
            if path.exists():
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
