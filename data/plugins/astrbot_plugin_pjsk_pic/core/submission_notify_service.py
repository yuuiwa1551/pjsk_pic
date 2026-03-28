from __future__ import annotations

import re
from pathlib import Path

from astrbot.api import logger
from astrbot.api.star import Context
from astrbot.core.message.message_event_result import MessageChain

from .db import ImageIndexDB
from .submission_service import SubmissionResult


class SubmissionNotifyService:
    def __init__(self, context: Context, db: ImageIndexDB, config) -> None:
        self.context = context
        self.db = db
        self.config = config

    def is_enabled(self) -> bool:
        return bool(self.config.get("submission_notify_enabled", True))

    @staticmethod
    def _private_origin(platform: str, user_id: str) -> str:
        return f"{platform}:FriendMessage:{user_id}"

    @staticmethod
    def _looks_like_unified_origin(value: str) -> bool:
        return bool(value and value.count(":") >= 2 and "Message" in value)

    def _event_platform(self, event, result: SubmissionResult) -> str:
        if result.platform_name:
            return result.platform_name
        get_platform_name = getattr(event, "get_platform_name", None)
        if callable(get_platform_name):
            try:
                value = str(get_platform_name() or "").strip()
                if value:
                    return value
            except Exception:
                pass
        unified_origin = str(getattr(event, "unified_msg_origin", "") or "")
        if ":" in unified_origin:
            return unified_origin.split(":", 1)[0]
        return "aiocqhttp"

    def _config_targets(self) -> list[str]:
        raw = str(self.config.get("submission_notify_targets", "") or "").strip()
        if not raw:
            return []
        return [item.strip() for item in re.split(r"[\r\n,，;；]+", raw) if item.strip()]

    def resolve_targets(self, event, result: SubmissionResult) -> list[str]:
        if not self.is_enabled():
            return []

        platform = self._event_platform(event, result)
        current_origin = str(getattr(event, "unified_msg_origin", "") or "")
        resolved: list[str] = []
        seen: set[str] = set()
        raw_targets: list[str] = []

        if bool(self.config.get("submission_notify_use_astr_admins", True)):
            admin_ids = self.context.get_config().get("admins_id", []) or []
            raw_targets.extend(str(item).strip() for item in admin_ids if str(item).strip())

        raw_targets.extend(self._config_targets())

        for item in raw_targets:
            if not item:
                continue
            target = item if self._looks_like_unified_origin(item) else self._private_origin(platform, item)
            if not target or target == current_origin or target in seen:
                continue
            seen.add(target)
            resolved.append(target)
        return resolved

    def preview_image_paths(self, result: SubmissionResult, *, limit: int = 3) -> list[Path]:
        paths: list[Path] = []
        seen: set[str] = set()
        max_items = max(1, int(limit or 1))
        for image_id in result.image_ids:
            file_path = self.db.get_image_file_path(int(image_id))
            if not file_path:
                continue
            path = Path(file_path)
            normalized = str(path)
            if not path.exists() or normalized in seen:
                continue
            seen.add(normalized)
            paths.append(path)
            if len(paths) >= max_items:
                break
        return paths

    @staticmethod
    def build_message(result: SubmissionResult) -> str:
        review_ids = result.review_ids
        lines = ["[PJSKPic] 收到新投稿", f"主 tag：{result.tag_name or '-'}"]
        if result.resolved_from_alias and result.input_tag_name:
            lines.append(f"输入 tag：{result.input_tag_name}（已自动归并）")
        lines.append(
            f"图片：{result.image_count} 张（已处理 {result.processed_count} / 已通过 {result.approved_count} / "
            f"待审 {result.pending_count} / 拒绝 {result.rejected_count} / 失败 {result.failure_count}）",
        )
        if result.aliases:
            lines.append("新增别名：" + "、".join(result.aliases))
        if result.sender_name or result.sender_id:
            lines.append(f"投稿人：{result.sender_name or result.sender_id}")
        if result.platform_name:
            lines.append(f"来源平台：{result.platform_name}")
        if result.session_id:
            lines.append(f"来源会话：{result.session_id}")
        if result.message_id:
            lines.append(f"原消息：{result.message_id}")
        if review_ids:
            preview = "、".join(str(review_id) for review_id in review_ids[:10])
            lines.append(f"待处理 review_id：{preview}")
            if len(review_ids) == 1:
                lines.append(f"处理命令：/pjsk图库 审核通过 {review_ids[0]}；/pjsk图库 审核拒绝 {review_ids[0]}")
            else:
                lines.append("可先用：/pjsk图库 审核查看 <review_id>")
        return "\n".join(lines)

    async def notify(self, event, result: SubmissionResult) -> int:
        if not result.ok:
            return 0

        targets = self.resolve_targets(event, result)
        if not targets:
            return 0

        text = self.build_message(result)
        preview_paths = self.preview_image_paths(result)
        sent = 0
        for target in targets:
            try:
                for image_path in preview_paths:
                    await self.context.send_message(target, MessageChain().file_image(str(image_path)))
                await self.context.send_message(target, MessageChain().message(text))
                sent += 1
            except Exception as exc:
                logger.warning(f"[PJSKPic] 投稿通知发送失败: target={target}, error={exc}")
        return sent
