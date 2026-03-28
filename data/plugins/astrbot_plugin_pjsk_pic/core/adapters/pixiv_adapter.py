from __future__ import annotations

import asyncio
import re
import urllib.parse

from .common import BaseCrawlAdapter
from ..models import CrawlCandidate
from ..pixiv_app_api import PixivAppAPIError, fetch_illust_detail

PIXIV_ID_PATTERN = re.compile(r"(?:artworks/|illust_id=)(\d+)")
PIXIV_IMAGE_PATTERN = re.compile(r"https://i\.pximg\.net/[^\s\"'<>\\]+")


class PixivAdapter(BaseCrawlAdapter):
    def __init__(self, config: dict | None = None) -> None:
        super().__init__("pixiv", config=config)

    async def fetch_candidates(self, source_url: str, *, max_candidates: int = 8, timeout_seconds: int = 20) -> list[CrawlCandidate]:
        source_uid = self.extract_source_uid(source_url, "")
        refresh_token = self.refresh_token()
        if source_uid and refresh_token:
            return await asyncio.to_thread(
                self._fetch_candidates_via_app_api,
                source_url,
                source_uid,
                max_candidates,
                timeout_seconds,
            )
        return await super().fetch_candidates(
            source_url,
            max_candidates=max_candidates,
            timeout_seconds=timeout_seconds,
        )

    def refresh_token(self) -> str:
        return str(self.config.get("pixiv_refresh_token", "") or "").strip()

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
        match = PIXIV_ID_PATTERN.search(final_url) or re.search(r'"illustId"\s*:\s*"?(\d+)"?', html)
        return match.group(1) if match else ""

    def build_extra(self, source_url: str, final_url: str, image_url: str, html: str, title: str) -> dict[str, object]:
        extra = super().build_extra(source_url, final_url, image_url, html, title)
        extra["source_id"] = self.extract_source_uid(final_url, html)
        extra["adapter"] = "pixiv"
        return extra

    def _fetch_candidates_via_app_api(
        self,
        source_url: str,
        illust_id: str,
        max_candidates: int,
        timeout_seconds: int,
    ) -> list[CrawlCandidate]:
        try:
            illust = fetch_illust_detail(
                illust_id,
                refresh_token=self.refresh_token(),
                timeout_seconds=timeout_seconds,
            )
        except PixivAppAPIError as exc:
            raise RuntimeError(f"Pixiv refresh token 鉴权采集失败：{exc}") from exc

        title = str(illust.get("title", "") or "").strip()
        user = illust.get("user")
        author = ""
        if isinstance(user, dict):
            author = str(user.get("name", "") or "").strip()

        raw_tags: list[str] = []
        translated_tags: list[str] = []
        tag_container = illust.get("tags")
        if isinstance(tag_container, list):
            seen_tags: set[str] = set()
            seen_translated: set[str] = set()
            for item in tag_container:
                if not isinstance(item, dict):
                    continue
                tag_name = str(item.get("name", "") or "").strip()
                translated_name = str(item.get("translated_name", "") or "").strip()
                lowered = tag_name.lower()
                if tag_name and lowered not in seen_tags:
                    seen_tags.add(lowered)
                    raw_tags.append(tag_name)
                lowered_translated = translated_name.lower()
                if translated_name and lowered_translated not in seen_translated and lowered_translated not in seen_tags:
                    seen_translated.add(lowered_translated)
                    translated_tags.append(translated_name)

        all_image_urls = self._extract_api_image_urls(illust)
        image_urls = all_image_urls[: max(1, max_candidates)]
        if not image_urls:
            raise RuntimeError("Pixiv App API 未返回可下载原图 URL")

        normalized_post_url = f"https://www.pixiv.net/artworks/{illust_id}"
        page_count = len(all_image_urls)
        candidates: list[CrawlCandidate] = []
        for index, image_url in enumerate(image_urls, start=1):
            candidates.append(
                CrawlCandidate(
                    platform=self.platform,
                    post_url=source_url,
                    normalized_post_url=normalized_post_url,
                    source_uid=str(illust.get("id", illust_id) or illust_id),
                    image_url=image_url,
                    raw_tags=list(raw_tags),
                    author=author,
                    title=title,
                    extra={
                        "adapter": "pixiv",
                        "via": "pixiv_app_api",
                        "source_id": str(illust.get("id", illust_id) or illust_id),
                        "translated_tags": translated_tags,
                        "page_index": index,
                        "page_count": page_count,
                        "request_headers": self.image_request_headers(normalized_post_url, image_url),
                    },
                )
            )
        return candidates

    @staticmethod
    def _extract_api_image_urls(illust: dict) -> list[str]:
        ordered: list[str] = []
        seen: set[str] = set()

        meta_pages = illust.get("meta_pages")
        if isinstance(meta_pages, list):
            for page in meta_pages:
                if not isinstance(page, dict):
                    continue
                image_urls = page.get("image_urls")
                if not isinstance(image_urls, dict):
                    continue
                original_url = str(image_urls.get("original", "") or "").strip()
                if original_url:
                    BaseCrawlAdapter.push_url(ordered, seen, original_url)

        meta_single_page = illust.get("meta_single_page")
        if isinstance(meta_single_page, dict):
            original_url = str(meta_single_page.get("original_image_url", "") or "").strip()
            if original_url:
                BaseCrawlAdapter.push_url(ordered, seen, original_url)

        image_urls = illust.get("image_urls")
        if isinstance(image_urls, dict):
            original_url = str(image_urls.get("large", "") or image_urls.get("medium", "") or "").strip()
            if original_url:
                BaseCrawlAdapter.push_url(ordered, seen, original_url)

        return ordered
