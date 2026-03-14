from __future__ import annotations

import json
import re
from pathlib import Path

from astrbot.api import logger

from .db import ImageIndexDB
from .models import ReviewDecision

GENERIC_NON_CHARACTER_KEYWORDS = {
    "插画", "壁纸", "头像", "风景", "景色", "摄影", "约稿", "摸鱼", "练习",
    "单人", "双人", "多人", "竖图", "横图", "官图", "截图", "漫画", "同人",
    "fanart", "art", "illustration", "wallpaper", "commission", "pixiv", "lofter", "twitter",
}


class ReviewService:
    def __init__(self, context, db: ImageIndexDB, config) -> None:
        self.context = context
        self.db = db
        self.config = config

    def looks_like_character_tag(self, tag_name: str) -> bool:
        text = (tag_name or "").strip()
        if not text or len(text) > 30:
            return False
        lowered = text.lower()
        if any(keyword in lowered for keyword in GENERIC_NON_CHARACTER_KEYWORDS):
            return False
        if any(ch in text for ch in "/|,，+&"):
            return False
        return len(text.strip("#＃ ")) >= 2

    def is_character_tag(self, tag_name: str) -> bool:
        row = self.db.get_tag_row(tag_name)
        if row and int(row["is_character"]) == 1:
            return True
        if not self.config.get("guess_character_tags", True):
            return False
        return self.looks_like_character_tag(tag_name)

    async def review_image_for_tag(self, image_path: Path, tag_name: str) -> ReviewDecision:
        if not self.is_character_tag(tag_name):
            if self.config.get("approve_non_character_tags", True):
                return ReviewDecision(status="approved", confidence=1.0, reason="非角色 tag 默认通过")
            return ReviewDecision(status="pending", confidence=0.0, reason="非角色 tag，等待人工审核")

        if not self.config.get("enable_auto_review", False):
            return ReviewDecision(status="pending", confidence=0.0, reason="未启用自动审核")

        provider_id = str(self.config.get("review_provider_id", "") or "").strip()
        if not provider_id:
            return ReviewDecision(status="pending", confidence=0.0, reason="未配置 review_provider_id")

        prompt = self._build_prompt(tag_name)
        try:
            response = await self.context.llm_generate(
                chat_provider_id=provider_id,
                prompt=prompt,
                image_urls=[str(image_path)],
            )
            text = getattr(response, "completion_text", "") or getattr(response, "_completion_text", "") or ""
            parsed = self._parse_review_response(text)
        except Exception as exc:
            logger.error(f"[PJSKPic] 自动审核失败: {exc}", exc_info=True)
            return ReviewDecision(status="uncertain", confidence=0.0, reason=f"模型调用失败: {exc}")

        threshold = float(self.config.get("review_confidence_threshold", 0.78) or 0.78)
        is_match = parsed.get("is_match")
        confidence = float(parsed.get("confidence", 0.0) or 0.0)
        reason = str(parsed.get("reason", "")).strip() or "模型未给出原因"
        if is_match is True and confidence >= threshold:
            status = "approved"
        elif is_match is False and confidence >= threshold:
            status = "rejected"
        else:
            status = "uncertain"
        return ReviewDecision(status=status, confidence=confidence, reason=reason, raw_result=text)

    @staticmethod
    def _build_prompt(tag_name: str) -> str:
        return (
            "你是图片角色审核器。请判断这张图是否主要表现目标角色。\n"
            f"目标角色 tag: {tag_name}\n"
            "只输出 JSON，不要输出多余文字，格式严格如下：\n"
            '{"is_match": true/false, "confidence": 0.0-1.0, "reason": "简短中文理由"}'
        )

    @staticmethod
    def _parse_review_response(text: str) -> dict[str, object]:
        candidates = re.findall(r"\{[\s\S]*?\}", text or "")
        for raw in candidates:
            try:
                data = json.loads(raw)
                if isinstance(data, dict):
                    return data
            except json.JSONDecodeError:
                continue
        lowered = (text or "").lower()
        if "true" in lowered or "yes" in lowered:
            return {"is_match": True, "confidence": 0.6, "reason": text[:200]}
        if "false" in lowered or "no" in lowered:
            return {"is_match": False, "confidence": 0.6, "reason": text[:200]}
        return {"is_match": None, "confidence": 0.0, "reason": (text or "").strip()[:200]}
