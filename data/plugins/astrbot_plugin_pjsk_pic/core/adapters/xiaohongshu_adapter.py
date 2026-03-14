from __future__ import annotations

import re

from .common import BaseCrawlAdapter

NOTE_ID_PATTERN = re.compile(r"/(?:explore|discovery/item|note)/([0-9a-zA-Z]+)")
XHS_IMAGE_PATTERN = re.compile(r"https://(?:sns-webpic-qc|sns-img-qc|ci)\.[^\s\"'<>\\]+")


class XiaohongshuAdapter(BaseCrawlAdapter):
    def __init__(self, config: dict | None = None) -> None:
        super().__init__("xiaohongshu", config=config)

    def cookie_string(self) -> str:
        return str(self.config.get("xiaohongshu_cookie_string", "") or "").strip()

    def extract_image_urls(self, html: str, base_url: str) -> list[str]:
        ordered = super().extract_image_urls(html, base_url)
        seen = set(ordered)
        for raw in XHS_IMAGE_PATTERN.findall(html):
            cleaned = raw.replace("\\u002F", "/").replace("\\/", "/").replace("&amp;", "&")
            if cleaned not in seen:
                seen.add(cleaned)
                ordered.append(cleaned)
        return ordered

    def extract_raw_tags(self, html: str) -> list[str]:
        ordered = super().extract_raw_tags(html)
        seen = {item.lower() for item in ordered}
        for tag in re.findall(r'"tagName"\s*:\s*"([^"]+)"', html):
            self.push_tag(ordered, seen, tag)
        return ordered[:40]

    def extract_author(self, html: str) -> str:
        for pattern in (r'"nickname"\s*:\s*"([^"]+)"', r'"userName"\s*:\s*"([^"]+)"'):
            match = re.search(pattern, html)
            if match:
                return match.group(1)
        return super().extract_author(html)

    def extract_title(self, html: str) -> str:
        for pattern in (r'"title"\s*:\s*"([^"]+)"', r'"desc"\s*:\s*"([^"]+)"'):
            match = re.search(pattern, html)
            if match:
                return match.group(1)
        return super().extract_title(html)

    def extract_source_uid(self, final_url: str, html: str) -> str:
        match = NOTE_ID_PATTERN.search(final_url) or re.search(r'"noteId"\s*:\s*"([^"]+)"', html)
        return match.group(1) if match else ""

    def build_extra(self, source_url: str, final_url: str, image_url: str, html: str, title: str) -> dict[str, object]:
        extra = super().build_extra(source_url, final_url, image_url, html, title)
        extra["source_id"] = self.extract_source_uid(final_url, html)
        extra["adapter"] = "xiaohongshu"
        extra["is_cover"] = bool(re.search(r'"cover"\s*:\s*"[^"]+"', html))
        return extra
