from __future__ import annotations

import re
import urllib.parse

from .common import BaseCrawlAdapter

PIXIV_ID_PATTERN = re.compile(r"(?:artworks/|illust_id=)(\d+)")
PIXIV_IMAGE_PATTERN = re.compile(r"https://i\.pximg\.net/[^\s\"'<>\\]+")


class PixivAdapter(BaseCrawlAdapter):
    def __init__(self, config: dict | None = None) -> None:
        super().__init__("pixiv", config=config)

    def image_request_headers(self, source_url: str, image_url: str) -> dict[str, str]:
        return {"Referer": "https://www.pixiv.net/"}

    def extract_image_urls(self, html: str, base_url: str) -> list[str]:
        ordered = super().extract_image_urls(html, base_url)
        seen = set(ordered)
        for raw in PIXIV_IMAGE_PATTERN.findall(html):
            cleaned = raw.replace("\\/", "/").replace("\\u002F", "/").replace("\\u0026", "&")
            cleaned = cleaned.replace("\\", "")
            if "/img-master/" in cleaned:
                cleaned = cleaned.replace("/img-master/", "/img-original/")
                cleaned = re.sub(r"_master1200(?=\.)", "", cleaned)
            if cleaned not in seen:
                seen.add(cleaned)
                ordered.append(cleaned)
        return ordered

    def extract_raw_tags(self, html: str) -> list[str]:
        ordered = super().extract_raw_tags(html)
        seen = {item.lower() for item in ordered}
        for match in re.findall(r'"tag"\s*:\s*"([^"]+)"', html):
            self.push_tag(ordered, seen, match)
        return ordered[:40]

    def extract_author(self, html: str) -> str:
        for pattern in (r'"userName"\s*:\s*"([^"]+)"', r'"name"\s*:\s*"([^"]+)"'):
            match = re.search(pattern, html)
            if match:
                return match.group(1)
        return super().extract_author(html)

    def extract_title(self, html: str) -> str:
        for pattern in (r'"illustTitle"\s*:\s*"([^"]+)"', r'"title"\s*:\s*"([^"]+)"'):
            match = re.search(pattern, html)
            if match:
                return match.group(1)
        return super().extract_title(html)

    def extract_source_uid(self, final_url: str, html: str) -> str:
        match = PIXIV_ID_PATTERN.search(final_url) or re.search(r'"illustId"\s*:\s*"?(\\d+)"?', html)
        return match.group(1) if match else ""

    def build_extra(self, source_url: str, final_url: str, image_url: str, html: str, title: str) -> dict[str, object]:
        extra = super().build_extra(source_url, final_url, image_url, html, title)
        extra["source_id"] = self.extract_source_uid(final_url, html)
        extra["adapter"] = "pixiv"
        return extra
