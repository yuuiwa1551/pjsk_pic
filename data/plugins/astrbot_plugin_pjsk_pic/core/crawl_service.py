from __future__ import annotations

import asyncio
from difflib import SequenceMatcher
from typing import Iterable

from astrbot.api import logger

from .crawl_adapter import CrawlAdapterFactory
from .crawl_tag_rules import CrawlTagRules
from .db import ImageIndexDB
from .importer import ImportedImageService
from .matcher import normalize_tag_name
from .review_service import ReviewService
from .tag_cleaner import TagCleaner


class CrawlService:
    def __init__(self, *, db: ImageIndexDB, importer: ImportedImageService, reviewer: ReviewService, config) -> None:
        self.db = db
        self.importer = importer
        self.reviewer = reviewer
        self.config = config
        self.tag_cleaner = TagCleaner(config)
        self._queue: asyncio.Queue[int] = asyncio.Queue()
        self._queued_ids: set[int] = set()
        self._worker_task: asyncio.Task | None = None
        self._stop_event = asyncio.Event()

    def _keep_primary_tags_only(self) -> bool:
        return bool(self.config.get("crawl_keep_primary_tags_only", True))

    async def start(self) -> None:
        self.db.reset_running_jobs()
        self._stop_event.clear()
        if self._worker_task is None or self._worker_task.done():
            self._worker_task = asyncio.create_task(self._worker_loop(), name="pjsk-pic-crawl-worker")
        for job_id in self.db.get_pending_job_ids():
            await self._enqueue_job(job_id)

    async def stop(self) -> None:
        self._stop_event.set()
        if self._worker_task:
            self._worker_task.cancel()
            try:
                await self._worker_task
            except asyncio.CancelledError:
                pass
            self._worker_task = None

    async def submit_job(
        self,
        platform: str,
        source_url: str,
        tags: list[str],
        *,
        include_tags: list[str] | None = None,
        exclude_tags: list[str] | None = None,
        match_mode: str = "exact",
    ) -> int:
        normalized_platform = CrawlAdapterFactory.normalize_platform(platform)
        if not CrawlAdapterFactory.supports(normalized_platform):
            raise ValueError(f"暂不支持的平台：{platform}")
        job_id = self.db.create_crawl_job(
            normalized_platform,
            source_url,
            tags,
            include_tags=include_tags,
            exclude_tags=exclude_tags,
            match_mode=match_mode,
        )
        await self._enqueue_job(job_id)
        return job_id

    async def retry_job(self, job_id: int) -> tuple[bool, str]:
        row = self.db.get_crawl_job(job_id)
        if not row:
            return False, f"采集任务不存在：{job_id}"
        self.db.update_crawl_job(job_id, status="retry", progress=0, error_log="")
        await self._enqueue_job(job_id)
        return True, f"已重新入队采集任务 #{job_id}"

    async def _enqueue_job(self, job_id: int) -> None:
        if job_id in self._queued_ids:
            return
        self._queued_ids.add(job_id)
        await self._queue.put(job_id)

    async def _worker_loop(self) -> None:
        while not self._stop_event.is_set():
            try:
                job_id = await asyncio.wait_for(self._queue.get(), timeout=1.0)
            except asyncio.TimeoutError:
                continue

            self._queued_ids.discard(job_id)
            try:
                await self._process_job(job_id)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.error(f"[PJSKPic] 采集任务 #{job_id} 执行异常: {exc}", exc_info=True)
                self.db.update_crawl_job(job_id, status="failed", error_log=str(exc), progress=0)
            finally:
                self._queue.task_done()

    async def _process_job(self, job_id: int) -> None:
        row = self.db.get_crawl_job(job_id)
        if not row:
            return

        attempt_count = self.db.increment_crawl_job_attempt(job_id)
        platform = str(row["platform"])
        source_url = str(row["source_url"])
        job_rules = CrawlTagRules.from_db_row(row)
        default_rules = CrawlTagRules.from_config(self.config)
        manual_tags = job_rules.manual_tags
        include_tags = self._normalized_rule_tags([*default_rules.include_tags, *job_rules.include_tags])
        exclude_tags = self._normalized_rule_tags([*default_rules.exclude_tags, *job_rules.exclude_tags])
        match_mode = str(row["tag_match_mode"] or "exact").strip().lower() or "exact"
        adapter = CrawlAdapterFactory.create(platform, config=self.config)
        max_candidates = max(1, int(self.config.get("crawler_max_candidates", 6) or 6))
        timeout_seconds = max(5, int(self.config.get("platform_request_timeout", self.config.get("crawler_timeout_seconds", 20)) or 20))
        retry_times = max(1, int(self.config.get("platform_retry_times", 2) or 2))

        self.db.update_crawl_job(job_id, status="running", progress=5, error_log="", result_summary="", attempt_count=attempt_count)
        candidates: list = []
        last_error = ""
        for _ in range(retry_times):
            try:
                candidates = await adapter.fetch_candidates(
                    source_url,
                    max_candidates=max_candidates,
                    timeout_seconds=timeout_seconds,
                )
                if candidates:
                    break
            except Exception as exc:
                last_error = str(exc)
        if not candidates:
            self.db.update_crawl_job(job_id, status="failed", progress=0, error_log=last_error or "未解析到可下载图片")
            return

        imported_count = 0
        tag_links = 0
        pending_reviews = 0
        approved_links = 0
        rejected_links = 0
        skipped_without_tags = 0
        skipped_by_include = 0
        skipped_by_exclude = 0
        similar_hits = 0
        failed_candidates = 0
        candidate_errors: list[str] = []

        for index, candidate in enumerate(candidates, start=1):
            progress = 10 + int(index / max(1, len(candidates)) * 80)
            self.db.update_crawl_job(job_id, progress=min(progress, 95))
            try:
                translated_tags: list[str] = []
                if isinstance(candidate.extra, dict):
                    translated = candidate.extra.get("translated_tags")
                    if isinstance(translated, list):
                        translated_tags = [str(item) for item in translated]
                candidate_tags = self.tag_cleaner.normalize_tags(
                    [*candidate.raw_tags, *translated_tags],
                    drop_noise=False,
                )
                filter_reason = self._match_filter_reason(
                    candidate_tags,
                    include_tags=include_tags,
                    exclude_tags=exclude_tags,
                    match_mode=match_mode,
                )
                if filter_reason == "exclude":
                    skipped_by_exclude += 1
                    continue
                if filter_reason == "include":
                    skipped_by_include += 1
                    continue

                imported = await self.importer.import_candidate(candidate)
                imported_count += 1
                if imported.similar_image_ids:
                    similar_hits += 1
                self.db.upsert_source(
                    image_id=imported.image_id,
                    platform=platform,
                    post_url=candidate.normalized_post_url or candidate.post_url,
                    image_url=candidate.image_url,
                    author=candidate.author,
                    raw_tags=candidate.raw_tags,
                    extra_json={
                        "title": candidate.title,
                        "source_uid": candidate.source_uid,
                        "similar_image_ids": imported.similar_image_ids,
                        **(candidate.extra or {}),
                    },
                )

                if self._keep_primary_tags_only():
                    tags = self._canonicalize_primary_tags(
                        manual_tags=manual_tags,
                        include_tags=include_tags,
                        raw_tags=[*candidate.raw_tags, *translated_tags],
                    )
                else:
                    tags = self.tag_cleaner.clean_tags(
                        self._merge_tags(manual_tags, [*candidate.raw_tags, *translated_tags]),
                        platform=platform,
                    )
                    tags = self._collapse_similar_tags(tags, preferred_tags=[*manual_tags, *include_tags])
                if not tags:
                    skipped_without_tags += 1
                    continue

                for tag_name in tags[: max(1, int(self.config.get("max_tags_per_image", 12) or 12))]:
                    tag_id = self.db.get_or_create_tag(tag_name)
                    decision = await self.reviewer.review_image_for_tag(imported.file_path, tag_name)
                    self.db.link_image_tag(
                        imported.image_id,
                        tag_id,
                        source_type=f"crawl:{platform}",
                        review_status=decision.status,
                        score=decision.confidence,
                        review_reason=decision.reason,
                    )
                    tag_links += 1
                    if decision.status in {"pending", "uncertain", "rejected"}:
                        pending_reviews += 1
                        self.db.create_review_task(
                            imported.image_id,
                            tag_id,
                            decision.status,
                            model_result=decision.raw_result,
                            reason=decision.reason,
                        )
                    elif self.reviewer.is_character_tag(tag_name):
                        self.db.create_review_task(
                            imported.image_id,
                            tag_id,
                            decision.status,
                            model_result=decision.raw_result,
                            reason=decision.reason,
                        )
                    if decision.status in {"approved", "manual_approved"}:
                        approved_links += 1
                    if decision.status in {"rejected", "manual_rejected"}:
                        rejected_links += 1
            except Exception as exc:
                failed_candidates += 1
                message = f"候选图 #{index} 处理失败: {exc}"
                if len(candidate_errors) < 3:
                    candidate_errors.append(message)
                logger.warning(f"[PJSKPic] 采集任务 #{job_id} {message}", exc_info=True)
                continue

        if imported_count == 0 and failed_candidates > 0:
            self.db.update_crawl_job(
                job_id,
                status="failed",
                progress=0,
                error_log="；".join(candidate_errors) or "候选图片处理失败",
                result_summary=f"候选图 {len(candidates)} 张，全部处理失败",
            )
            return

        summary = (
            f"图片 {imported_count} 张，标签关联 {tag_links} 条，"
            f"通过 {approved_links}，待复核 {pending_reviews}，拒绝 {rejected_links}"
        )
        if skipped_without_tags:
            summary += f"，无 tag 图片 {skipped_without_tags}"
        if skipped_by_include:
            summary += f"，include 跳过 {skipped_by_include}"
        if skipped_by_exclude:
            summary += f"，exclude 跳过 {skipped_by_exclude}"
        if similar_hits:
            summary += f"，疑似重复 {similar_hits}"
        if failed_candidates:
            summary += f"，失败 {failed_candidates}"
        self.db.update_crawl_job(
            job_id,
            status="completed",
            progress=100,
            result_summary=summary,
            error_log="；".join(candidate_errors) if candidate_errors else "",
        )

    @staticmethod
    def _merge_tags(manual_tags: Iterable[str], raw_tags: Iterable[str]) -> list[str]:
        result: list[str] = []
        seen: set[str] = set()
        for source in (manual_tags, raw_tags):
            for item in source:
                tag = str(item or "").strip()
                key = tag.lower()
                if not tag or key in seen:
                    continue
                seen.add(key)
                result.append(tag)
        return result

    def _normalized_rule_tags(self, tags: Iterable[str]) -> set[str]:
        normalized = self.tag_cleaner.normalize_tags(list(tags), drop_noise=False)
        return {normalize_tag_name(tag) for tag in normalized if tag}

    def _canonicalize_primary_tags(
        self,
        *,
        manual_tags: Iterable[str],
        include_tags: Iterable[str],
        raw_tags: Iterable[str],
    ) -> list[str]:
        result: list[str] = []
        seen: set[str] = set()

        def append_if_missing(tag_name: str) -> None:
            normalized = normalize_tag_name(tag_name)
            if not normalized or normalized in seen:
                return
            seen.add(normalized)
            result.append(tag_name)

        for tag in self.tag_cleaner.normalize_tags(list(manual_tags), drop_noise=False):
            canonical = self._canonicalize_explicit_tag(tag)
            if canonical:
                append_if_missing(canonical)

        for tag in self.tag_cleaner.normalize_tags(list(include_tags), drop_noise=False):
            canonical = self._canonicalize_existing_character_tag(tag)
            if canonical:
                append_if_missing(canonical)

        for tag in self.tag_cleaner.normalize_tags(list(raw_tags), drop_noise=False):
            canonical = self._canonicalize_existing_character_tag(tag)
            if canonical:
                append_if_missing(canonical)

        return result

    def _canonicalize_explicit_tag(self, tag_name: str) -> str | None:
        match = self.db.resolve_tag(tag_name, allow_fuzzy=False)
        if match.matched and match.tag_name:
            return str(match.tag_name)
        normalized = self.tag_cleaner.normalize_tags([tag_name], drop_noise=False)
        return normalized[0] if normalized else None

    def _canonicalize_existing_character_tag(self, tag_name: str) -> str | None:
        match = self.db.resolve_tag(tag_name, allow_fuzzy=False)
        if not match.matched or not match.tag_name:
            return None
        row = self.db.get_tag_row(str(match.tag_name))
        if not row or int(row["is_character"] or 0) != 1:
            return None
        return str(match.tag_name)

    @classmethod
    def _collapse_similar_tags(cls, tags: list[str], *, preferred_tags: Iterable[str]) -> list[str]:
        if not tags:
            return []

        normalized_preferred: list[str] = []
        seen_preferred: set[str] = set()
        for tag in preferred_tags:
            normalized = normalize_tag_name(str(tag))
            if not normalized or normalized in seen_preferred:
                continue
            seen_preferred.add(normalized)
            normalized_preferred.append(normalized)
        if not normalized_preferred:
            return tags

        consumed_indexes: set[int] = set()
        chosen_indexes: set[int] = set()
        for target in normalized_preferred:
            matches: list[tuple[float, int]] = []
            for index, tag in enumerate(tags):
                score = cls._tag_similarity_score(tag, target)
                if score < 0.72:
                    continue
                matches.append((score, index))
            if len(matches) <= 1:
                continue
            matches.sort(key=lambda item: (-item[0], item[1]))
            winner = matches[0][1]
            chosen_indexes.add(winner)
            for _, index in matches:
                consumed_indexes.add(index)

        if not consumed_indexes:
            return tags

        result: list[str] = []
        for index, tag in enumerate(tags):
            if index in consumed_indexes and index not in chosen_indexes:
                continue
            result.append(tag)
        return result

    @staticmethod
    def _tag_similarity_score(left: str, right: str) -> float:
        normalized_left = normalize_tag_name(str(left))
        normalized_right = normalize_tag_name(str(right))
        if not normalized_left or not normalized_right:
            return 0.0
        if normalized_left == normalized_right:
            return 1.0
        shorter = min(len(normalized_left), len(normalized_right))
        longer = max(len(normalized_left), len(normalized_right))
        if normalized_left in normalized_right or normalized_right in normalized_left:
            return 0.88 + (shorter / max(1, longer)) * 0.12
        return SequenceMatcher(None, normalized_left, normalized_right).ratio()

    @staticmethod
    def _match_filter_reason(
        candidate_tags: Iterable[str],
        *,
        include_tags: set[str],
        exclude_tags: set[str],
        match_mode: str = "exact",
    ) -> str | None:
        candidate_set = {normalize_tag_name(str(tag)) for tag in candidate_tags if str(tag).strip()}
        candidate_set.discard("")
        if exclude_tags and CrawlService._rule_set_matches(candidate_set, exclude_tags, match_mode=match_mode):
            return "exclude"
        if include_tags and not CrawlService._rule_set_matches(candidate_set, include_tags, match_mode=match_mode):
            return "include"
        return None

    @staticmethod
    def _rule_set_matches(candidate_set: set[str], rule_tags: set[str], *, match_mode: str = "exact") -> bool:
        if not candidate_set or not rule_tags:
            return False
        if match_mode != "partial":
            return bool(candidate_set.intersection(rule_tags))
        for candidate in candidate_set:
            for rule in rule_tags:
                if not candidate or not rule:
                    continue
                if candidate == rule or candidate in rule or rule in candidate:
                    return True
        return False
