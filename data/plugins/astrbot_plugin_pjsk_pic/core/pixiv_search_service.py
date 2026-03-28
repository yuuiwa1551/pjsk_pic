from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import Any

from .pixiv_app_api import PixivAppAPIError, extract_offset_from_next_url, search_illusts


@dataclass
class PixivSearchHit:
    illust_id: str
    post_url: str
    title: str = ""
    author: str = ""
    raw_tags: list[str] | None = None
    translated_tags: list[str] | None = None


class PixivSearchService:
    def __init__(self, config) -> None:
        self.config = config

    def refresh_token(self) -> str:
        return str(self.config.get("pixiv_refresh_token", "") or "").strip()

    def query_suffix(self) -> str:
        return str(self.config.get("pixiv_auto_crawl_query_suffix", "user") or "user").strip()

    def build_query(self, tag_name: str, *, suffix: str | None = None) -> str:
        parts = [str(tag_name or "").strip()]
        suffix_text = str(self.query_suffix() if suffix is None else suffix).strip()
        if suffix_text:
            parts.append(suffix_text)
        return " ".join(part for part in parts if part)

    async def search_tag(
        self,
        tag_name: str,
        *,
        max_results: int = 30,
        max_pages: int = 3,
        timeout_seconds: int = 20,
    ) -> list[PixivSearchHit]:
        return await asyncio.to_thread(
            self._search_tag_sync,
            tag_name,
            max_results,
            max_pages,
            timeout_seconds,
        )

    def _search_tag_sync(
        self,
        tag_name: str,
        max_results: int,
        max_pages: int,
        timeout_seconds: int,
    ) -> list[PixivSearchHit]:
        refresh_token = self.refresh_token()
        if not refresh_token:
            raise PixivAppAPIError("??? Pixiv refresh token")

        query = self.build_query(tag_name)
        results: list[PixivSearchHit] = []
        offset: int | None = None
        remaining_pages = max(1, int(max_pages or 1))
        wanted = max(1, int(max_results or 1))

        while remaining_pages > 0 and len(results) < wanted:
            payload = search_illusts(
                query,
                refresh_token=refresh_token,
                search_target="partial_match_for_tags",
                sort="date_desc",
                search_ai_type=0,
                offset=offset,
                timeout_seconds=timeout_seconds,
            )
            illusts = payload.get("illusts") or []
            if not illusts:
                break

            for illust in illusts:
                hit = self._build_hit(illust)
                if hit is None:
                    continue
                results.append(hit)
                if len(results) >= wanted:
                    break

            next_url = str(payload.get("next_url", "") or "").strip()
            offset = extract_offset_from_next_url(next_url)
            if offset is None:
                break
            remaining_pages -= 1
        return results[:wanted]

    @staticmethod
    def _build_hit(illust: dict[str, Any]) -> PixivSearchHit | None:
        illust_id = str(illust.get("id", "") or "").strip()
        if not illust_id:
            return None

        title = str(illust.get("title", "") or "").strip()
        user = illust.get("user")
        author = ""
        if isinstance(user, dict):
            author = str(user.get("name", "") or "").strip()

        raw_tags: list[str] = []
        translated_tags: list[str] = []
        tag_container = illust.get("tags")
        if isinstance(tag_container, list):
            seen_raw: set[str] = set()
            seen_translated: set[str] = set()
            for item in tag_container:
                if not isinstance(item, dict):
                    continue
                tag_name = str(item.get("name", "") or "").strip()
                translated_name = str(item.get("translated_name", "") or "").strip()
                lowered_raw = tag_name.casefold()
                lowered_translated = translated_name.casefold()
                if tag_name and lowered_raw not in seen_raw:
                    seen_raw.add(lowered_raw)
                    raw_tags.append(tag_name)
                if translated_name and lowered_translated not in seen_translated and lowered_translated not in seen_raw:
                    seen_translated.add(lowered_translated)
                    translated_tags.append(translated_name)

        return PixivSearchHit(
            illust_id=illust_id,
            post_url=f"https://www.pixiv.net/artworks/{illust_id}",
            title=title,
            author=author,
            raw_tags=raw_tags,
            translated_tags=translated_tags,
        )
