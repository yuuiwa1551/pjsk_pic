from __future__ import annotations

import asyncio
import re
import urllib.parse
import urllib.request
from dataclasses import dataclass
from html import unescape

from .models import CrawlCandidate

DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
    "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
}

DIRECT_IMAGE_SUFFIXES = {".jpg", ".jpeg", ".png", ".webp", ".gif", ".bmp"}
META_PATTERN = re.compile(r"<meta\s+([^>]*?)>", re.IGNORECASE | re.DOTALL)
ATTR_PATTERN = re.compile(r"([\w:-]+)\s*=\s*([\"'])(.*?)\2", re.IGNORECASE | re.DOTALL)
IMG_PATTERN = re.compile(r"<img\s+[^>]*src\s*=\s*([\"'])(.*?)\1", re.IGNORECASE | re.DOTALL)
TITLE_PATTERN = re.compile(r"<title[^>]*>(.*?)</title>", re.IGNORECASE | re.DOTALL)
HASHTAG_PATTERN = re.compile(r"[#＃]([0-9A-Za-z_\-\u4e00-\u9fff\u3040-\u30ff]{1,40})")


@dataclass
class FetchResult:
    final_url: str
    content_type: str
    body: bytes


class GenericCrawlAdapter:
    def __init__(self, platform: str) -> None:
        self.platform = platform

    async def fetch_candidates(self, source_url: str, *, max_candidates: int = 8, timeout_seconds: int = 20) -> list[CrawlCandidate]:
        result = await asyncio.to_thread(self._fetch_url, source_url, timeout_seconds)
        content_type = (result.content_type or "").lower()
        if self._looks_like_direct_image(result.final_url) or content_type.startswith("image/"):
            return [self._build_direct_candidate(source_url, result.final_url)]

        html = self._decode_html(result.body)
        image_urls = self._extract_image_urls(html, result.final_url)[: max(1, max_candidates)]
        raw_tags = self._extract_raw_tags(html)
        author = self._extract_author(html)
        title = self._extract_title(html)
        candidates: list[CrawlCandidate] = []
        for image_url in image_urls:
            extra = {"title": title}
            headers = self._image_request_headers(source_url, image_url)
            if headers:
                extra["request_headers"] = headers
            candidates.append(
                CrawlCandidate(
                    platform=self.platform,
                    post_url=source_url,
                    image_url=image_url,
                    raw_tags=raw_tags,
                    author=author,
                    title=title,
                    extra=extra,
                )
            )
        return candidates

    def _fetch_url(self, url: str, timeout_seconds: int) -> FetchResult:
        request = urllib.request.Request(url, headers=DEFAULT_HEADERS)
        with urllib.request.urlopen(request, timeout=timeout_seconds) as response:
            body = response.read()
            content_type = response.headers.get("Content-Type", "")
            final_url = response.geturl()
        return FetchResult(final_url=final_url, content_type=content_type, body=body)

    def _build_direct_candidate(self, source_url: str, image_url: str) -> CrawlCandidate:
        extra: dict[str, object] = {}
        headers = self._image_request_headers(source_url, image_url)
        if headers:
            extra["request_headers"] = headers
        return CrawlCandidate(
            platform=self.platform,
            post_url=source_url,
            image_url=image_url,
            raw_tags=[],
            author="",
            title="",
            extra=extra,
        )

    def _image_request_headers(self, source_url: str, image_url: str) -> dict[str, str]:
        host = urllib.parse.urlparse(image_url).netloc.lower()
        if self.platform == "pixiv" or "pximg.net" in host:
            return {"Referer": "https://www.pixiv.net/"}
        if source_url and urllib.parse.urlparse(source_url).netloc:
            return {"Referer": source_url}
        return {}

    @staticmethod
    def _decode_html(body: bytes) -> str:
        for encoding in ("utf-8", "utf-8-sig", "gb18030", "big5"):
            try:
                return body.decode(encoding)
            except UnicodeDecodeError:
                continue
        return body.decode("utf-8", errors="ignore")

    @staticmethod
    def _looks_like_direct_image(url: str) -> bool:
        path = urllib.parse.urlparse(url).path.lower()
        return any(path.endswith(suffix) for suffix in DIRECT_IMAGE_SUFFIXES)

    def _extract_image_urls(self, html: str, base_url: str) -> list[str]:
        ordered: list[str] = []
        seen: set[str] = set()
        meta_map = self._parse_meta_map(html)
        for key in ("og:image", "twitter:image", "twitter:image:src"):
            value = meta_map.get(key, "")
            if value:
                self._push_url(ordered, seen, urllib.parse.urljoin(base_url, value))
        for match in IMG_PATTERN.finditer(html):
            value = match.group(2).strip()
            if value:
                self._push_url(ordered, seen, urllib.parse.urljoin(base_url, value))
        return ordered

    def _extract_raw_tags(self, html: str) -> list[str]:
        meta_map = self._parse_meta_map(html)
        ordered: list[str] = []
        seen: set[str] = set()
        keywords = meta_map.get("keywords", "")
        if keywords:
            for raw in re.split(r"[,，/|]", keywords):
                self._push_tag(ordered, seen, raw)
        for match in HASHTAG_PATTERN.finditer(html):
            self._push_tag(ordered, seen, match.group(1))
        return ordered[:20]

    def _extract_author(self, html: str) -> str:
        meta_map = self._parse_meta_map(html)
        for key in ("author", "article:author", "og:site_name"):
            value = meta_map.get(key, "")
            if value:
                return value.strip()
        return ""

    def _extract_title(self, html: str) -> str:
        match = TITLE_PATTERN.search(html)
        if not match:
            return ""
        return re.sub(r"\s+", " ", unescape(match.group(1))).strip()[:200]

    def _parse_meta_map(self, html: str) -> dict[str, str]:
        result: dict[str, str] = {}
        for meta_match in META_PATTERN.finditer(html):
            attrs = self._parse_attrs(meta_match.group(1))
            key = (attrs.get("property") or attrs.get("name") or "").strip().lower()
            value = (attrs.get("content") or "").strip()
            if key and value and key not in result:
                result[key] = unescape(value)
        return result

    @staticmethod
    def _parse_attrs(text: str) -> dict[str, str]:
        attrs: dict[str, str] = {}
        for key, _, value in ATTR_PATTERN.findall(text):
            attrs[key.lower()] = unescape(value)
        return attrs

    @staticmethod
    def _push_url(target: list[str], seen: set[str], value: str) -> None:
        parsed = urllib.parse.urlparse(value)
        if parsed.scheme not in {"http", "https"}:
            return
        normalized = value.strip()
        if not normalized or normalized in seen:
            return
        seen.add(normalized)
        target.append(normalized)

    @staticmethod
    def _push_tag(target: list[str], seen: set[str], raw: str) -> None:
        tag = unescape(raw or "").strip().strip("#＃")
        tag = re.sub(r"\s+", " ", tag)
        if not tag or len(tag) > 40:
            return
        lowered = tag.lower()
        if lowered in seen:
            return
        seen.add(lowered)
        target.append(tag)


class CrawlAdapterFactory:
    SUPPORTED_PLATFORMS = {"pixiv", "x", "xiaohongshu", "lofter", "generic"}

    @classmethod
    def normalize_platform(cls, platform: str) -> str:
        value = (platform or "").strip().lower()
        aliases = {"twitter": "x", "xhs": "xiaohongshu", "rednote": "xiaohongshu"}
        return aliases.get(value, value)

    @classmethod
    def supports(cls, platform: str) -> bool:
        return cls.normalize_platform(platform) in cls.SUPPORTED_PLATFORMS

    @classmethod
    def create(cls, platform: str) -> GenericCrawlAdapter:
        normalized = cls.normalize_platform(platform)
        if normalized not in cls.SUPPORTED_PLATFORMS:
            raise ValueError(f"暂不支持的平台：{platform}")
        return GenericCrawlAdapter(normalized)
