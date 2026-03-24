from __future__ import annotations

import re
from pathlib import Path

from astrbot.api import logger
from astrbot.api.message_components import Image

from .db import ImageIndexDB
from .importer import ImportedImageService
from .review_service import ReviewService

SUBMISSION_PATTERN = re.compile(r"(?:^|[\s])(?:[/!！.。．])?(?:投稿|tg)\s+(.+?)\s*$", re.IGNORECASE)


class SubmissionService:
    def __init__(self, db: ImageIndexDB, importer: ImportedImageService, reviewer: ReviewService) -> None:
        self.db = db
        self.importer = importer
        self.reviewer = reviewer

    @staticmethod
    def extract_tag_from_text(text: str) -> str:
        normalized = re.sub(r"\s+", " ", str(text or "")).strip()
        if not normalized:
            return ""
        match = SUBMISSION_PATTERN.search(normalized)
        return match.group(1).strip() if match else ""

    async def submit_from_event(self, event, tag_name: str) -> tuple[bool, str]:
        clean_tag = str(tag_name or "").strip()
        if not clean_tag:
            return False, "请在投稿命令后提供角色 tag，例如：投稿 初音未来"

        image_paths = await self._extract_image_paths(event)
        if not image_paths:
            return False, "投稿时请附带 1 张图片。"
        if len(image_paths) > 1:
            return False, "当前投稿仅支持单图，请一次只发 1 张图片。"

        imported = await self.importer.import_local_file(image_paths[0], platform="submission")
        tag_id = self.db.get_or_create_tag(clean_tag, is_character=True)
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
            raw_tags=[clean_tag],
            extra_json={
                "source_kind": "user_submission",
                "sender_id": str(sender_id or ""),
                "sender_name": str(sender_name or ""),
                "platform_name": str(platform_name or ""),
                "session_id": unified_origin,
                "message_id": message_id,
                "submission_tag": clean_tag,
                "similar_image_ids": imported.similar_image_ids,
            },
        )

        lines = [f"投稿已收录：{clean_tag}", f"image_id：{imported.image_id}"]
        if decision.status == "approved":
            lines.append("审核结果：已通过，可直接参与发图。")
        elif decision.status in {"pending", "uncertain"}:
            lines.append(f"审核结果：待人工复核（review_id：{review_id or '-'}）。")
        else:
            lines.append(f"审核结果：{decision.status}（review_id：{review_id or '-'}）。")

        if imported.similar_image_ids:
            lines.append("检测到疑似重复图片：" + "、".join(str(item) for item in imported.similar_image_ids[:10]))
        return True, "\n".join(lines)

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
                logger.warning(f"[PJSKPic] 投稿图片提取失败: {exc}")
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
