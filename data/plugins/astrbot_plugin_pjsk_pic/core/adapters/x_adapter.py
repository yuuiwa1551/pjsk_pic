from __future__ import annotations

import re

from .common import BaseCrawlAdapter

TWEET_ID_PATTERN = re.compile(r"/status/(\d+)")
MEDIA_PATTERN = re.compile(r"https://pbs\.twimg\.com/media/[^\s\"'<>\\]+")


class XAdapter(BaseCrawlAdapter):
    def __init__(self, config: dict | None = None) -> None:
        super().__init__("x", config=config)

    def cookie_string(self) -> str:
        return str(self.config.get("x_cookie_string", "") or "").strip()

    def extract_image_urls(self, html: str, base_url: str) -> list[str]:
        ordered = super().extract_image_urls(html, base_url)
        seen = set(ordered)
        for raw in MEDIA_PATTERN.findall(html):
            cleaned = raw.replace("\\u002F", "/").replace("\\/", "/").replace("&amp;", "&")
            if cleaned not in seen:
                seen.add(cleaned)
                ordered.append(cleaned)
        return ordered

    def extract_raw_tags(self, html: str) -> list[str]:
        ordered = super().extract_raw_tags(html)
        seen = {item.lower() for item in ordered}
        for match in re.findall(r'"hashtags?"\s*:\s*\[(.*?)\]', html):
            for tag in re.findall(r'"text"\s*:\s*"([^"]+)"', match):
                self.push_tag(ordered, seen, tag)
        return ordered[:40]

    def extract_author(self, html: str) -> str:
        for pattern in (
            r'"screen_name"\s*:\s*"([^"]+)"',
            r'"author"\s*:\s*"([^"]+)"',
            r'"identifier"\s*:\s*"@?([^"]+)"',
        ):
            match = re.search(pattern, html)
            if match:
                return match.group(1)
        return super().extract_author(html)

    def extract_title(self, html: str) -> str:
        meta_map = self.parse_meta_map(html)
        for key in ("og:description", "twitter:description", "description"):
            value = meta_map.get(key, "")
            if value:
                return value
        return super().extract_title(html)

    def extract_source_uid(self, final_url: str, html: str) -> str:
        match = TWEET_ID_PATTERN.search(final_url) or re.search(r'"rest_id"\s*:\s*"(\d+)"', html)
        return match.group(1) if match else ""

    def build_extra(self, source_url: str, final_url: str, image_url: str, html: str, title: str) -> dict[str, object]:
        extra = super().build_extra(source_url, final_url, image_url, html, title)
        extra["source_id"] = self.extract_source_uid(final_url, html)
        extra["adapter"] = "x"
        return extra
