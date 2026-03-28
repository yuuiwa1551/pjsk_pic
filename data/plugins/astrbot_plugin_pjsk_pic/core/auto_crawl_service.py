from __future__ import annotations

import asyncio
from datetime import datetime, timedelta

from astrbot.api import logger

from .matcher import normalize_tag_name
from .pixiv_search_service import PixivSearchHit, PixivSearchService


class AutoCrawlService:
    def __init__(self, *, db, crawl_service, config) -> None:
        self.db = db
        self.crawl_service = crawl_service
        self.config = config
        self.search_service = PixivSearchService(config)
        self._task: asyncio.Task | None = None
        self._stop_event = asyncio.Event()
        self._run_lock = asyncio.Lock()

    def enabled(self) -> bool:
        return bool(self.config.get("pixiv_auto_crawl_enabled", False))

    def has_refresh_token(self) -> bool:
        return bool(self.search_service.refresh_token())

    def character_only(self) -> bool:
        return bool(self.config.get("pixiv_auto_crawl_character_only", True))

    def interval_minutes(self) -> int:
        return max(5, int(self.config.get("pixiv_auto_crawl_interval_minutes", 60) or 60))

    def max_results_per_tag(self) -> int:
        return max(1, int(self.config.get("pixiv_auto_crawl_max_results_per_tag", 30) or 30))

    def max_pages_per_tag(self) -> int:
        return max(1, int(self.config.get("pixiv_auto_crawl_max_pages_per_tag", 3) or 3))

    def max_new_jobs_per_cycle(self) -> int:
        return max(1, int(self.config.get("pixiv_auto_crawl_max_new_jobs_per_cycle", 30) or 30))

    def timeout_seconds(self) -> int:
        return max(5, int(self.config.get("platform_request_timeout", self.config.get("crawler_timeout_seconds", 20)) or 20))

    async def start(self) -> None:
        self._stop_event.clear()
        if not self.enabled():
            logger.info("[PJSKPic] Pixiv ???????")
            return
        if not self.has_refresh_token():
            logger.warning("[PJSKPic] Pixiv ?????????? refresh token")
            return
        if self._task is None or self._task.done():
            self._task = asyncio.create_task(self._loop(), name="pjsk-pic-auto-crawl")

    async def stop(self) -> None:
        self._stop_event.set()
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
            self._task = None

    async def run_once(self, *, force: bool = False) -> dict[str, int]:
        async with self._run_lock:
            summary = {
                "subscriptions": 0,
                "checked": 0,
                "queued": 0,
                "matched": 0,
                "skipped_existing": 0,
                "skipped_filtered": 0,
                "errors": 0,
            }
            if not self.enabled() or not self.has_refresh_token():
                return summary

            self._sync_subscriptions()
            subscriptions = self.db.list_crawl_subscriptions(platform="pixiv", enabled_only=True)
            summary["subscriptions"] = len(subscriptions)
            remaining_jobs = self.max_new_jobs_per_cycle()

            for row in subscriptions:
                if remaining_jobs <= 0:
                    break
                if not force and not self._is_due(row):
                    continue
                summary["checked"] += 1
                try:
                    result = await self._process_subscription(row, remaining_jobs=remaining_jobs)
                except Exception as exc:
                    summary["errors"] += 1
                    self.db.update_crawl_subscription_state(
                        int(row["id"]),
                        last_error=str(exc),
                    )
                    logger.warning(f"[PJSKPic] Pixiv ?????? #{row['id']} ????: {exc}", exc_info=True)
                    continue
                for key in ("queued", "matched", "skipped_existing", "skipped_filtered"):
                    summary[key] += int(result.get(key, 0) or 0)
                remaining_jobs -= int(result.get("queued", 0) or 0)
            return summary

    async def _loop(self) -> None:
        while not self._stop_event.is_set():
            try:
                await self.run_once()
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.warning(f"[PJSKPic] Pixiv ????????: {exc}", exc_info=True)
            try:
                await asyncio.wait_for(self._stop_event.wait(), timeout=self.interval_minutes() * 60)
            except asyncio.TimeoutError:
                continue

    def _sync_subscriptions(self) -> None:
        tags = self.db.list_tags_for_auto_crawl(character_only=self.character_only())
        enabled_normalized: set[str] = set()
        for row in tags:
            tag_name = str(row["name"])
            tag_id = int(row["id"])
            query_text = self.search_service.build_query(tag_name)
            self.db.upsert_crawl_subscription(
                platform="pixiv",
                tag_id=tag_id,
                tag_name=tag_name,
                query_text=query_text,
                enabled=True,
            )
            enabled_normalized.add(normalize_tag_name(tag_name))
        self.db.disable_missing_crawl_subscriptions(platform="pixiv", keep_normalized_tags=enabled_normalized)

    def _is_due(self, row) -> bool:
        last_checked = str(row["last_checked_at"] or "").strip()
        if not last_checked:
            return True
        try:
            last_dt = datetime.fromisoformat(last_checked)
        except ValueError:
            return True
        return datetime.utcnow() - last_dt >= timedelta(minutes=self.interval_minutes())

    async def _process_subscription(self, row, *, remaining_jobs: int) -> dict[str, int]:
        tag_name = str(row["tag_name"] or "").strip()
        if not tag_name:
            return {"queued": 0, "matched": 0, "skipped_existing": 0, "skipped_filtered": 0}

        hits = await self.search_service.search_tag(
            tag_name,
            max_results=self.max_results_per_tag(),
            max_pages=self.max_pages_per_tag(),
            timeout_seconds=self.timeout_seconds(),
        )
        newest_source_uid = hits[0].illust_id if hits else str(row["last_seen_source_uid"] or "")
        last_seen_source_uid = str(row["last_seen_source_uid"] or "").strip()

        pending_hits: list[PixivSearchHit] = []
        matched = 0
        skipped_filtered = 0
        skipped_existing = 0

        for hit in hits:
            if last_seen_source_uid and hit.illust_id == last_seen_source_uid:
                break
            if not self._matches_target_tag(tag_name, hit):
                skipped_filtered += 1
                continue
            matched += 1
            if self.db.has_source_post_url(hit.post_url, platform="pixiv"):
                skipped_existing += 1
                continue
            pending_hits.append(hit)
            if len(pending_hits) >= remaining_jobs:
                break

        queued = 0
        for hit in reversed(pending_hits):
            await self.crawl_service.submit_job(
                "pixiv",
                hit.post_url,
                [tag_name],
                include_tags=[tag_name],
                exclude_tags=[],
                match_mode="partial",
            )
            queued += 1

        self.db.update_crawl_subscription_state(
            int(row["id"]),
            last_checked_at=datetime.utcnow().isoformat(timespec="seconds"),
            last_success_at=datetime.utcnow().isoformat(timespec="seconds"),
            last_error="",
            last_seen_source_uid=newest_source_uid,
            query_text=self.search_service.build_query(tag_name),
        )
        return {
            "queued": queued,
            "matched": matched,
            "skipped_existing": skipped_existing,
            "skipped_filtered": skipped_filtered,
        }

    @staticmethod
    def _matches_target_tag(tag_name: str, hit: PixivSearchHit) -> bool:
        target = normalize_tag_name(tag_name)
        if not target:
            return False
        candidates = [*(hit.raw_tags or []), *(hit.translated_tags or [])]
        seen: set[str] = set()
        for tag in candidates:
            normalized = normalize_tag_name(tag)
            if not normalized or normalized in seen:
                continue
            seen.add(normalized)
            if target in normalized or normalized in target:
                return True
        return False
