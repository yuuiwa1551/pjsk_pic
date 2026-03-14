from __future__ import annotations

import re

DEFAULT_BLACKLIST = {
    "图片", "图", "插画", "壁纸", "头像", "约稿", "摸鱼", "练习",
    "pixiv", "twitter", "x", "小红书", "红薯", "lofter",
    "illustration", "fanart", "art", "image", "images",
}

PLATFORM_TAG_BLACKLIST = {
    "pixiv": {"original", "1000users入り", "5000users入り", "r-18", "r18"},
    "x": {"x", "twitter"},
    "xiaohongshu": {"小红书", "薯队长", "求推荐", "求扩列"},
}


class TagCleaner:
    def __init__(self, config) -> None:
        self.config = config

    def clean_tags(self, tags: list[str], *, platform: str = "") -> list[str]:
        blacklist = {item.lower() for item in DEFAULT_BLACKLIST}
        platform_blacklist = {item.lower() for item in PLATFORM_TAG_BLACKLIST.get(platform, set())}
        custom = self._custom_blacklist()
        blacklist.update(platform_blacklist)
        blacklist.update(custom)

        result: list[str] = []
        seen: set[str] = set()
        for raw in tags:
            for item in self._split_tag(raw):
                tag = self._normalize_tag(item)
                if not tag:
                    continue
                lowered = tag.lower()
                if lowered in blacklist:
                    continue
                if self._looks_like_noise(tag):
                    continue
                if lowered in seen:
                    continue
                seen.add(lowered)
                result.append(tag)
        return result

    def _custom_blacklist(self) -> set[str]:
        value = self.config.get("tag_blacklist", [])
        if isinstance(value, str):
            items = value.replace("，", ",").split(",")
        elif isinstance(value, list):
            items = value
        else:
            items = []
        return {str(item).strip().lower() for item in items if str(item).strip()}

    @staticmethod
    def _split_tag(value: str) -> list[str]:
        text = str(value or "").strip().strip("#＃")
        if not text:
            return []
        return [item.strip() for item in re.split(r"[,，/|·]+", text) if item.strip()]

    @staticmethod
    def _normalize_tag(value: str) -> str:
        tag = re.sub(r"\s+", " ", str(value or "")).strip()
        tag = tag.strip("#＃")
        return tag[:60]

    @staticmethod
    def _looks_like_noise(tag: str) -> bool:
        lowered = tag.lower()
        if len(tag) <= 1:
            return True
        if lowered.isdigit():
            return True
        if re.fullmatch(r"[0-9a-zA-Z_\-]{1,3}", lowered):
            return True
        if "http" in lowered:
            return True
        return False
