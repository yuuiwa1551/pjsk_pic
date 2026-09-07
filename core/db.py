from __future__ import annotations

from collections import Counter
from contextlib import contextmanager
import json
import re
import sqlite3
import threading
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Iterator

from .matcher import normalize_tag_name
from .models import APPROVED_STATUSES, MatchResult
from .phash import hamming_distance
from .tag_policy import TAG_STATUSES, TAG_TYPES, normalize_tag_status, normalize_tag_type

IMAGE_TAG_STATUS_PRIORITY = {
    "manual_approved": 5,
    "approved": 4,
    "pending": 3,
    "uncertain": 2,
    "manual_rejected": 1,
    "rejected": 0,
}


def split_status_filter(value: str | Iterable[str] | None) -> list[str]:
    if value is None:
        return []
    if isinstance(value, str):
        raw_items = re.split(r"[,，\s]+", value)
    else:
        raw_items = [str(item) for item in value]
    result: list[str] = []
    seen: set[str] = set()
    for raw in raw_items:
        text = str(raw or "").strip()
        if not text or text in seen:
            continue
        seen.add(text)
        result.append(text)
    return result


def utcnow_str() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


class ImageIndexDB:
    def __init__(self, db_path: Path) -> None:
        self.db_path = Path(db_path).expanduser().resolve()
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.RLock()
        self._init_db()

    @contextmanager
    def _connect(self) -> Iterator[sqlite3.Connection]:
        conn = sqlite3.connect(self.db_path, timeout=30)
        conn.row_factory = sqlite3.Row
        try:
            conn.execute("PRAGMA busy_timeout=30000;")
            conn.execute("PRAGMA temp_store=MEMORY;")
        except sqlite3.OperationalError:
            pass
        try:
            yield conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    def _table_columns(self, conn: sqlite3.Connection, table: str) -> set[str]:
        rows = conn.execute(f"PRAGMA table_info({table})").fetchall()
        return {str(row['name']) for row in rows}

    def _ensure_column(self, conn: sqlite3.Connection, table: str, column_name: str, ddl_suffix: str) -> None:
        if column_name not in self._table_columns(conn, table):
            conn.execute(f"ALTER TABLE {table} ADD COLUMN {column_name} {ddl_suffix}")

    def _init_db(self) -> None:
        with self._lock, self._connect() as conn:
            conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS images (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    file_path TEXT NOT NULL UNIQUE,
                    file_name TEXT NOT NULL,
                    sha256 TEXT NOT NULL,
                    phash TEXT DEFAULT '',
                    width INTEGER DEFAULT 0,
                    height INTEGER DEFAULT 0,
                    format TEXT DEFAULT '',
                    is_active INTEGER DEFAULT 1,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS tags (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    name TEXT NOT NULL UNIQUE,
                    normalized_name TEXT NOT NULL UNIQUE,
                    is_character INTEGER DEFAULT 0,
                    tag_type TEXT NOT NULL DEFAULT 'other',
                    status TEXT NOT NULL DEFAULT 'active',
                    created_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS tag_aliases (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    tag_id INTEGER NOT NULL,
                    alias TEXT NOT NULL,
                    normalized_alias TEXT NOT NULL UNIQUE,
                    created_at TEXT NOT NULL,
                    FOREIGN KEY(tag_id) REFERENCES tags(id)
                );

                CREATE TABLE IF NOT EXISTS image_tags (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    image_id INTEGER NOT NULL,
                    tag_id INTEGER NOT NULL,
                    source_type TEXT NOT NULL,
                    score REAL DEFAULT 1.0,
                    review_status TEXT DEFAULT 'approved',
                    review_reason TEXT DEFAULT '',
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    UNIQUE(image_id, tag_id, source_type),
                    FOREIGN KEY(image_id) REFERENCES images(id),
                    FOREIGN KEY(tag_id) REFERENCES tags(id)
                );

                CREATE TABLE IF NOT EXISTS image_files (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    image_id INTEGER NOT NULL,
                    file_path TEXT NOT NULL UNIQUE,
                    file_name TEXT NOT NULL,
                    storage_type TEXT NOT NULL DEFAULT 'library',
                    is_active INTEGER DEFAULT 1,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    FOREIGN KEY(image_id) REFERENCES images(id)
                );

                CREATE TABLE IF NOT EXISTS sources (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    image_id INTEGER NOT NULL,
                    platform TEXT NOT NULL,
                    post_url TEXT NOT NULL,
                    image_url TEXT NOT NULL,
                    author TEXT DEFAULT '',
                    raw_tags TEXT DEFAULT '[]',
                    extra_json TEXT DEFAULT '{}',
                    created_at TEXT NOT NULL,
                    UNIQUE(image_id, image_url),
                    FOREIGN KEY(image_id) REFERENCES images(id)
                );

                CREATE TABLE IF NOT EXISTS crawl_jobs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    platform TEXT NOT NULL,
                    source_url TEXT NOT NULL,
                    source_context_json TEXT DEFAULT '{}',
                    tags_text TEXT DEFAULT '',
                    include_tags_text TEXT DEFAULT '',
                    exclude_tags_text TEXT DEFAULT '',
                    tag_match_mode TEXT DEFAULT 'exact',
                    origin TEXT NOT NULL DEFAULT 'manual',
                    priority INTEGER NOT NULL DEFAULT 20,
                    status TEXT NOT NULL DEFAULT 'pending',
                    progress INTEGER DEFAULT 0,
                    error_log TEXT DEFAULT '',
                    result_summary TEXT DEFAULT '',
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS crawl_subscriptions (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    platform TEXT NOT NULL,
                    tag_id INTEGER DEFAULT 0,
                    tag_name TEXT NOT NULL,
                    normalized_tag TEXT NOT NULL,
                    query_text TEXT DEFAULT '',
                    enabled INTEGER DEFAULT 1,
                    last_seen_source_uid TEXT DEFAULT '',
                    last_checked_at TEXT DEFAULT '',
                    last_success_at TEXT DEFAULT '',
                    last_error TEXT DEFAULT '',
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    UNIQUE(platform, normalized_tag),
                    FOREIGN KEY(tag_id) REFERENCES tags(id)
                );

                CREATE TABLE IF NOT EXISTS tag_proposals (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    proposed_name TEXT NOT NULL,
                    normalized_name TEXT NOT NULL,
                    aliases_json TEXT DEFAULT '[]',
                    submitter_id TEXT DEFAULT '',
                    submitter_name TEXT DEFAULT '',
                    platform_name TEXT DEFAULT '',
                    session_id TEXT DEFAULT '',
                    message_id TEXT DEFAULT '',
                    occurrence_count INTEGER DEFAULT 1,
                    status TEXT NOT NULL DEFAULT 'pending',
                    resolved_tag_id INTEGER DEFAULT 0,
                    reason TEXT DEFAULT '',
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    FOREIGN KEY(resolved_tag_id) REFERENCES tags(id)
                );

                CREATE TABLE IF NOT EXISTS crawl_subscription_terms (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    subscription_id INTEGER NOT NULL,
                    query_term TEXT NOT NULL,
                    normalized_term TEXT NOT NULL,
                    query_text TEXT DEFAULT '',
                    position INTEGER DEFAULT 0,
                    enabled INTEGER DEFAULT 1,
                    last_seen_source_uid TEXT DEFAULT '',
                    last_checked_at TEXT DEFAULT '',
                    last_success_at TEXT DEFAULT '',
                    last_error TEXT DEFAULT '',
                    scan_offset TEXT DEFAULT '',
                    scan_high_watermark TEXT DEFAULT '',
                    scan_target_source_uid TEXT DEFAULT '',
                    saturated INTEGER DEFAULT 0,
                    saturated_at TEXT DEFAULT '',
                    saturated_reason TEXT DEFAULT '',
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    UNIQUE(subscription_id, normalized_term),
                    FOREIGN KEY(subscription_id) REFERENCES crawl_subscriptions(id)
                );

                CREATE TABLE IF NOT EXISTS crawl_discoveries (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    platform TEXT NOT NULL,
                    source_uid TEXT NOT NULL,
                    post_url TEXT NOT NULL,
                    source_context_json TEXT DEFAULT '{}',
                    tags_text TEXT DEFAULT '',
                    status TEXT NOT NULL DEFAULT 'pending',
                    crawl_job_id INTEGER DEFAULT 0,
                    last_error TEXT DEFAULT '',
                    discovered_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    UNIQUE(platform, source_uid)
                );

                CREATE TABLE IF NOT EXISTS crawl_provider_states (
                    platform TEXT PRIMARY KEY,
                    status TEXT NOT NULL DEFAULT 'active',
                    paused_category TEXT DEFAULT '',
                    paused_reason TEXT DEFAULT '',
                    paused_at TEXT DEFAULT '',
                    last_checked_at TEXT DEFAULT '',
                    last_success_at TEXT DEFAULT '',
                    last_error TEXT DEFAULT '',
                    updated_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS pixiv_backfill_tasks (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    tag_id INTEGER DEFAULT 0,
                    tag_name TEXT NOT NULL,
                    normalized_tag TEXT NOT NULL,
                    tag_text TEXT DEFAULT '',
                    query_terms_json TEXT DEFAULT '[]',
                    include_tags_text TEXT DEFAULT '',
                    exclude_tags_text TEXT DEFAULT '',
                    max_pages INTEGER DEFAULT 20,
                    max_results INTEGER DEFAULT 200,
                    max_new_jobs INTEGER DEFAULT 100,
                    status TEXT NOT NULL DEFAULT 'pending',
                    current_query_text TEXT DEFAULT '',
                    current_page INTEGER DEFAULT 0,
                    current_offset TEXT DEFAULT '',
                    scanned INTEGER DEFAULT 0,
                    matched INTEGER DEFAULT 0,
                    queued INTEGER DEFAULT 0,
                    skipped_existing INTEGER DEFAULT 0,
                    skipped_rejected INTEGER DEFAULT 0,
                    skipped_filtered INTEGER DEFAULT 0,
                    skipped_duplicate INTEGER DEFAULT 0,
                    error_log TEXT DEFAULT '',
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    completed_at TEXT DEFAULT ''
                );

                CREATE TABLE IF NOT EXISTS xhs_backfill_tasks (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    tag_id INTEGER DEFAULT 0,
                    tag_name TEXT NOT NULL,
                    normalized_tag TEXT NOT NULL,
                    tag_text TEXT DEFAULT '',
                    query_terms_json TEXT DEFAULT '[]',
                    match_terms_json TEXT DEFAULT '[]',
                    max_pages INTEGER DEFAULT 10,
                    max_results INTEGER DEFAULT 200,
                    max_new_jobs INTEGER DEFAULT 50,
                    status TEXT NOT NULL DEFAULT 'pending',
                    current_query_index INTEGER DEFAULT 0,
                    current_query_text TEXT DEFAULT '',
                    next_page INTEGER DEFAULT 1,
                    scanned INTEGER DEFAULT 0,
                    detailed INTEGER DEFAULT 0,
                    matched INTEGER DEFAULT 0,
                    queued INTEGER DEFAULT 0,
                    skipped_existing INTEGER DEFAULT 0,
                    skipped_rejected INTEGER DEFAULT 0,
                    skipped_filtered INTEGER DEFAULT 0,
                    skipped_duplicate INTEGER DEFAULT 0,
                    failed_details INTEGER DEFAULT 0,
                    error_log TEXT DEFAULT '',
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    completed_at TEXT DEFAULT ''
                );

                CREATE TABLE IF NOT EXISTS review_tasks (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    image_id INTEGER NOT NULL,
                    tag_id INTEGER NOT NULL,
                    status TEXT NOT NULL DEFAULT 'pending',
                    model_result TEXT DEFAULT '',
                    manual_result TEXT DEFAULT '',
                    reason TEXT DEFAULT '',
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    FOREIGN KEY(image_id) REFERENCES images(id),
                    FOREIGN KEY(tag_id) REFERENCES tags(id)
                );

                CREATE TABLE IF NOT EXISTS send_logs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    session_id TEXT NOT NULL,
                    image_id INTEGER NOT NULL,
                    matched_tag TEXT NOT NULL,
                    sent_at TEXT NOT NULL,
                    FOREIGN KEY(image_id) REFERENCES images(id)
                );

                CREATE TABLE IF NOT EXISTS platform_tag_terms (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    tag_id INTEGER NOT NULL,
                    platform TEXT NOT NULL,
                    term TEXT NOT NULL,
                    normalized_term TEXT NOT NULL,
                    term_type TEXT NOT NULL DEFAULT 'both',
                    source TEXT NOT NULL DEFAULT 'manual_review',
                    confidence REAL DEFAULT 1.0,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    UNIQUE(platform, normalized_term),
                    FOREIGN KEY(tag_id) REFERENCES tags(id)
                );

                CREATE TABLE IF NOT EXISTS llm_image_review_runs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    image_id INTEGER NOT NULL,
                    platform TEXT NOT NULL DEFAULT '',
                    mode TEXT NOT NULL DEFAULT 'shadow',
                    provider_id TEXT NOT NULL DEFAULT '',
                    prompt_version TEXT NOT NULL DEFAULT 'v1',
                    input_fingerprint TEXT NOT NULL UNIQUE,
                    image_sha256 TEXT NOT NULL DEFAULT '',
                    candidates_json TEXT NOT NULL DEFAULT '[]',
                    status TEXT NOT NULL DEFAULT 'pending',
                    attempt_count INTEGER NOT NULL DEFAULT 0,
                    decision TEXT NOT NULL DEFAULT '',
                    quality_json TEXT NOT NULL DEFAULT '{}',
                    selected_tags_json TEXT NOT NULL DEFAULT '[]',
                    result_json TEXT NOT NULL DEFAULT '{}',
                    raw_result TEXT NOT NULL DEFAULT '',
                    reason TEXT NOT NULL DEFAULT '',
                    error_log TEXT NOT NULL DEFAULT '',
                    error_history_json TEXT NOT NULL DEFAULT '[]',
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    completed_at TEXT NOT NULL DEFAULT '',
                    FOREIGN KEY(image_id) REFERENCES images(id)
                );

                CREATE TABLE IF NOT EXISTS rejected_sources (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    platform TEXT NOT NULL,
                    post_url TEXT NOT NULL,
                    normalized_post_url TEXT NOT NULL,
                    source_uid TEXT DEFAULT '',
                    image_id INTEGER DEFAULT 0,
                    reason TEXT DEFAULT '',
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    UNIQUE(platform, normalized_post_url)
                );

                CREATE TABLE IF NOT EXISTS tag_merge_identity_candidates (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    source_tag_id INTEGER NOT NULL,
                    target_tag_id INTEGER NOT NULL,
                    status TEXT NOT NULL DEFAULT 'pending',
                    score REAL DEFAULT 0,
                    reasons_json TEXT DEFAULT '[]',
                    evidence_json TEXT DEFAULT '{}',
                    llm_result_json TEXT DEFAULT '{}',
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    UNIQUE(source_tag_id, target_tag_id),
                    FOREIGN KEY(source_tag_id) REFERENCES tags(id),
                    FOREIGN KEY(target_tag_id) REFERENCES tags(id)
                );

                CREATE TABLE IF NOT EXISTS image_similarity_ignores (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    image_id_low INTEGER NOT NULL,
                    image_id_high INTEGER NOT NULL,
                    reason TEXT DEFAULT '',
                    created_at TEXT NOT NULL,
                    UNIQUE(image_id_low, image_id_high),
                    FOREIGN KEY(image_id_low) REFERENCES images(id),
                    FOREIGN KEY(image_id_high) REFERENCES images(id)
                );

                CREATE INDEX IF NOT EXISTS idx_images_active ON images(is_active);
                CREATE TABLE IF NOT EXISTS xhs_backfill_items (
                    task_id INTEGER NOT NULL,
                    note_id TEXT NOT NULL,
                    outcome TEXT NOT NULL,
                    crawl_job_id INTEGER DEFAULT 0,
                    PRIMARY KEY(task_id, note_id)
                );
                CREATE INDEX IF NOT EXISTS idx_images_sha256 ON images(sha256);
                CREATE INDEX IF NOT EXISTS idx_image_files_image_id ON image_files(image_id);
                CREATE INDEX IF NOT EXISTS idx_image_files_active ON image_files(is_active);
                CREATE INDEX IF NOT EXISTS idx_image_tags_tag_id ON image_tags(tag_id);
                CREATE INDEX IF NOT EXISTS idx_image_tags_review_status ON image_tags(review_status);
                CREATE INDEX IF NOT EXISTS idx_sources_platform ON sources(platform);
                CREATE INDEX IF NOT EXISTS idx_sources_image_platform ON sources(image_id, platform);
                CREATE INDEX IF NOT EXISTS idx_sources_platform_post_url ON sources(platform, post_url);
                CREATE INDEX IF NOT EXISTS idx_crawl_jobs_status ON crawl_jobs(status);
                CREATE INDEX IF NOT EXISTS idx_crawl_jobs_platform_source_url ON crawl_jobs(platform, source_url);
                CREATE INDEX IF NOT EXISTS idx_crawl_subscriptions_platform ON crawl_subscriptions(platform, enabled);
                CREATE INDEX IF NOT EXISTS idx_crawl_subscription_terms_subscription ON crawl_subscription_terms(subscription_id, enabled, position);
                CREATE INDEX IF NOT EXISTS idx_crawl_discoveries_status ON crawl_discoveries(platform, status, id);
                CREATE INDEX IF NOT EXISTS idx_pixiv_backfill_tasks_status ON pixiv_backfill_tasks(status, updated_at);
                CREATE INDEX IF NOT EXISTS idx_xhs_backfill_tasks_status ON xhs_backfill_tasks(status, updated_at);
                CREATE INDEX IF NOT EXISTS idx_review_tasks_status ON review_tasks(status);
                CREATE INDEX IF NOT EXISTS idx_review_tasks_status_image ON review_tasks(status, image_id, id);
                CREATE INDEX IF NOT EXISTS idx_review_tasks_image_status ON review_tasks(image_id, status);
                CREATE INDEX IF NOT EXISTS idx_llm_image_review_runs_status ON llm_image_review_runs(status, id);
                CREATE INDEX IF NOT EXISTS idx_llm_image_review_runs_image ON llm_image_review_runs(image_id, id);
                CREATE INDEX IF NOT EXISTS idx_llm_image_review_runs_created ON llm_image_review_runs(created_at, status);
                CREATE INDEX IF NOT EXISTS idx_send_logs_session_id ON send_logs(session_id);
                CREATE INDEX IF NOT EXISTS idx_platform_tag_terms_tag_id ON platform_tag_terms(tag_id, platform);
                CREATE INDEX IF NOT EXISTS idx_rejected_sources_platform ON rejected_sources(platform, normalized_post_url);
                CREATE INDEX IF NOT EXISTS idx_tag_merge_identity_candidates_status ON tag_merge_identity_candidates(status, updated_at);
                CREATE INDEX IF NOT EXISTS idx_image_similarity_ignores_low ON image_similarity_ignores(image_id_low);
                CREATE INDEX IF NOT EXISTS idx_image_similarity_ignores_high ON image_similarity_ignores(image_id_high);
                CREATE INDEX IF NOT EXISTS idx_tag_proposals_status ON tag_proposals(status, updated_at, id);
                CREATE INDEX IF NOT EXISTS idx_tag_proposals_normalized ON tag_proposals(normalized_name, status);
                """
            )
            try:
                conn.execute("PRAGMA journal_mode=DELETE;")
            except sqlite3.OperationalError:
                pass

            tag_columns_before_migration = self._table_columns(conn, 'tags')
            had_tag_type = 'tag_type' in tag_columns_before_migration

            self._ensure_column(conn, 'images', 'phash', "TEXT DEFAULT ''")
            self._ensure_column(conn, 'tags', 'is_character', 'INTEGER DEFAULT 0')
            self._ensure_column(conn, 'tags', 'tag_type', "TEXT NOT NULL DEFAULT 'other'")
            self._ensure_column(conn, 'tags', 'status', "TEXT NOT NULL DEFAULT 'active'")
            self._ensure_column(conn, 'image_tags', 'score', 'REAL DEFAULT 1.0')
            self._ensure_column(conn, 'image_tags', 'review_status', "TEXT DEFAULT 'approved'")
            self._ensure_column(conn, 'image_tags', 'review_reason', "TEXT DEFAULT ''")
            self._ensure_column(conn, 'image_tags', 'updated_at', "TEXT DEFAULT ''")
            self._ensure_column(conn, 'crawl_jobs', 'attempt_count', 'INTEGER DEFAULT 0')
            self._ensure_column(conn, 'crawl_jobs', 'source_context_json', "TEXT DEFAULT '{}'")
            self._ensure_column(conn, 'crawl_jobs', 'include_tags_text', "TEXT DEFAULT ''")
            self._ensure_column(conn, 'crawl_jobs', 'exclude_tags_text', "TEXT DEFAULT ''")
            self._ensure_column(conn, 'crawl_jobs', 'tag_match_mode', "TEXT DEFAULT 'exact'")
            self._ensure_column(conn, 'crawl_jobs', 'origin', "TEXT NOT NULL DEFAULT 'manual'")
            self._ensure_column(conn, 'crawl_jobs', 'priority', 'INTEGER NOT NULL DEFAULT 20')
            conn.execute(
                'CREATE INDEX IF NOT EXISTS idx_crawl_jobs_status_priority '
                'ON crawl_jobs(status, priority, id)'
            )
            self._ensure_column(conn, 'crawl_subscriptions', 'tag_id', 'INTEGER DEFAULT 0')
            self._ensure_column(conn, 'crawl_subscriptions', 'query_text', "TEXT DEFAULT ''")
            self._ensure_column(conn, 'crawl_subscriptions', 'enabled', 'INTEGER DEFAULT 1')
            self._ensure_column(conn, 'crawl_subscriptions', 'last_seen_source_uid', "TEXT DEFAULT ''")
            self._ensure_column(conn, 'crawl_subscriptions', 'last_checked_at', "TEXT DEFAULT ''")
            self._ensure_column(conn, 'crawl_subscriptions', 'last_success_at', "TEXT DEFAULT ''")
            self._ensure_column(conn, 'crawl_subscriptions', 'last_error', "TEXT DEFAULT ''")
            self._ensure_column(conn, 'crawl_discoveries', 'source_context_json', "TEXT DEFAULT '{}'")
            self._ensure_column(conn, 'crawl_subscription_terms', 'scan_offset', "TEXT DEFAULT ''")
            self._ensure_column(conn, 'crawl_subscription_terms', 'scan_high_watermark', "TEXT DEFAULT ''")
            self._ensure_column(conn, 'crawl_subscription_terms', 'scan_target_source_uid', "TEXT DEFAULT ''")
            self._ensure_column(conn, 'crawl_subscription_terms', 'saturated', 'INTEGER DEFAULT 0')
            self._ensure_column(conn, 'crawl_subscription_terms', 'saturated_at', "TEXT DEFAULT ''")
            self._ensure_column(conn, 'crawl_subscription_terms', 'saturated_reason', "TEXT DEFAULT ''")
            self._ensure_column(conn, 'xhs_backfill_tasks', 'page_snapshot_json', "TEXT DEFAULT ''")
            self._ensure_column(conn, 'xhs_backfill_tasks', 'page_item_index', 'INTEGER DEFAULT 0')
            self._ensure_column(conn, 'xhs_backfill_tasks', 'page_size', 'INTEGER DEFAULT 20')
            self._ensure_column(
                conn,
                'llm_image_review_runs',
                'error_history_json',
                "TEXT NOT NULL DEFAULT '[]'",
            )
            if not had_tag_type:
                conn.execute(
                    "UPDATE tags SET tag_type = CASE WHEN is_character = 1 THEN 'character' ELSE 'other' END"
                )
            else:
                conn.execute(
                    """
                    UPDATE tags
                    SET tag_type = CASE WHEN is_character = 1 THEN 'character' ELSE 'other' END
                    WHERE tag_type IS NULL OR tag_type = '' OR tag_type NOT IN ('character', 'pairing', 'theme', 'other')
                    """
                )
            conn.execute(
                "UPDATE tags SET status = 'active' WHERE status IS NULL OR status = '' OR status NOT IN ('active', 'pending', 'archived')"
            )
            conn.execute("UPDATE tags SET is_character = CASE WHEN tag_type = 'character' THEN 1 ELSE 0 END")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_tags_governance ON tags(status, tag_type, name)")
            self._ensure_file_locations_initialized(conn)

    @staticmethod
    def _infer_storage_type(file_path: str) -> str:
        normalized = str(file_path or "").replace("\\", "/").lower()
        if "/trash/" in normalized:
            return "trash"
        if "/images/restored/" in normalized:
            return "restored"
        if "/images/imported/" in normalized:
            return "imported"
        return "library"

    def _ensure_file_locations_initialized(self, conn: sqlite3.Connection) -> None:
        location_count = int(conn.execute("SELECT COUNT(*) AS c FROM image_files").fetchone()["c"])
        if location_count > 0:
            return
        rows = conn.execute(
            "SELECT id, file_path, file_name, is_active, created_at, updated_at FROM images",
        ).fetchall()
        for row in rows:
            conn.execute(
                """
                INSERT OR IGNORE INTO image_files(image_id, file_path, file_name, storage_type, is_active, created_at, updated_at)
                VALUES(?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    int(row["id"]),
                    str(row["file_path"]),
                    str(row["file_name"]),
                    self._infer_storage_type(str(row["file_path"])),
                    int(row["is_active"] or 0),
                    str(row["created_at"]),
                    str(row["updated_at"]),
                ),
            )

    def _upsert_file_location(
        self,
        conn: sqlite3.Connection,
        *,
        image_id: int,
        file_path: str,
        file_name: str,
        storage_type: str,
        now: str,
    ) -> None:
        row = conn.execute(
            "SELECT id FROM image_files WHERE file_path = ? LIMIT 1",
            (file_path,),
        ).fetchone()
        if row:
            conn.execute(
                """
                UPDATE image_files
                SET image_id = ?, file_name = ?, storage_type = ?, is_active = 1, updated_at = ?
                WHERE id = ?
                """,
                (image_id, file_name, storage_type, now, row["id"]),
            )
            return
        conn.execute(
            """
            INSERT INTO image_files(image_id, file_path, file_name, storage_type, is_active, created_at, updated_at)
            VALUES(?, ?, ?, ?, 1, ?, ?)
            """,
            (image_id, file_path, file_name, storage_type, now, now),
        )

    def _sync_image_file_state(
        self,
        conn: sqlite3.Connection,
        image_id: int,
        *,
        preferred_path: str | None = None,
        now: str | None = None,
    ) -> None:
        now = now or utcnow_str()
        image_row = conn.execute(
            "SELECT file_path FROM images WHERE id = ? LIMIT 1",
            (image_id,),
        ).fetchone()
        current_path = str(image_row["file_path"]) if image_row and image_row["file_path"] else ""
        locations = conn.execute(
            """
            SELECT file_path, file_name
            FROM image_files
            WHERE image_id = ? AND is_active = 1
            ORDER BY updated_at DESC, id DESC
            """,
            (image_id,),
        ).fetchall()

        chosen: sqlite3.Row | None = None
        if current_path:
            for row in locations:
                if str(row["file_path"]) == current_path and Path(str(row["file_path"])).exists():
                    chosen = row
                    break
        if chosen is None and preferred_path:
            for row in locations:
                if str(row["file_path"]) == preferred_path and Path(str(row["file_path"])).exists():
                    chosen = row
                    break
        if chosen is None:
            for row in locations:
                try:
                    if Path(str(row["file_path"])).exists():
                        chosen = row
                        break
                except OSError:
                    continue
        if chosen is None and locations:
            chosen = locations[0]

        if chosen is not None:
            conn.execute(
                """
                UPDATE images
                SET file_path = ?, file_name = ?, is_active = 1, updated_at = ?
                WHERE id = ?
                """,
                (str(chosen["file_path"]), str(chosen["file_name"]), now, image_id),
            )
            return

        conn.execute(
            "UPDATE images SET is_active = 0, updated_at = ? WHERE id = ?",
            (now, image_id),
        )

    def _set_preferred_image_variant(
        self,
        conn: sqlite3.Connection,
        *,
        image_id: int,
        file_path: str,
        file_name: str,
        sha256: str,
        phash: str,
        width: int,
        height: int,
        format_: str,
        now: str,
    ) -> None:
        conn.execute(
            "UPDATE image_files SET is_active = CASE WHEN file_path = ? THEN 1 ELSE 0 END, updated_at = ? WHERE image_id = ?",
            (file_path, now, image_id),
        )
        conn.execute(
            """
            UPDATE images
            SET file_path = ?, file_name = ?, sha256 = ?, phash = ?, width = ?, height = ?, format = ?, is_active = 1, updated_at = ?
            WHERE id = ?
            """,
            (file_path, file_name, sha256, phash, width, height, format_, now, image_id),
        )

    def upsert_image(
        self,
        *,
        file_path: str,
        file_name: str,
        sha256: str,
        width: int,
        height: int,
        format_: str,
        phash: str = '',
        storage_type: str = "library",
    ) -> int:
        now = utcnow_str()
        with self._lock, self._connect() as conn:
            row = conn.execute(
                "SELECT image_id FROM image_files WHERE file_path = ? LIMIT 1",
                (file_path,),
            ).fetchone()
            if row:
                image_id = int(row["image_id"])
                existing = conn.execute(
                    "SELECT phash, width, height, format FROM images WHERE id = ? LIMIT 1",
                    (image_id,),
                ).fetchone()
                next_phash = phash or str(existing["phash"] or "") if existing else phash
                next_width = int(width or (existing["width"] if existing else 0) or 0)
                next_height = int(height or (existing["height"] if existing else 0) or 0)
                next_format = format_ or str(existing["format"] or "") if existing else format_
                conn.execute(
                    """
                    UPDATE images
                    SET file_name = ?, sha256 = ?, phash = ?, width = ?, height = ?, format = ?, is_active = 1, updated_at = ?
                    WHERE id = ?
                    """,
                    (file_name, sha256, next_phash, next_width, next_height, next_format, now, image_id),
                )
                self._upsert_file_location(
                    conn,
                    image_id=image_id,
                    file_path=file_path,
                    file_name=file_name,
                    storage_type=storage_type,
                    now=now,
                )
                self._sync_image_file_state(conn, image_id, preferred_path=file_path, now=now)
                return image_id

            existing = conn.execute(
                """
                SELECT id, phash, width, height, format
                FROM images
                WHERE sha256 = ?
                ORDER BY id ASC
                LIMIT 1
                """,
                (sha256,),
            ).fetchone()
            if existing:
                image_id = int(existing["id"])
                next_phash = phash or str(existing["phash"] or "")
                next_width = int(width or existing["width"] or 0)
                next_height = int(height or existing["height"] or 0)
                next_format = format_ or str(existing["format"] or "")
                conn.execute(
                    """
                    UPDATE images
                    SET phash = ?, width = ?, height = ?, format = ?, is_active = 1, updated_at = ?
                    WHERE id = ?
                    """,
                    (next_phash, next_width, next_height, next_format, now, image_id),
                )
                self._upsert_file_location(
                    conn,
                    image_id=image_id,
                    file_path=file_path,
                    file_name=file_name,
                    storage_type=storage_type,
                    now=now,
                )
                self._sync_image_file_state(conn, image_id, preferred_path=file_path, now=now)
                return image_id

            cursor = conn.execute(
                """
                INSERT INTO images(file_path, file_name, sha256, phash, width, height, format, is_active, created_at, updated_at)
                VALUES(?, ?, ?, ?, ?, ?, ?, 1, ?, ?)
                """,
                (file_path, file_name, sha256, phash, width, height, format_, now, now),
            )
            image_id = int(cursor.lastrowid)
            self._upsert_file_location(
                conn,
                image_id=image_id,
                file_path=file_path,
                file_name=file_name,
                storage_type=storage_type,
                now=now,
            )
            return image_id

    def get_active_image_by_sha256(self, sha256: str) -> sqlite3.Row | None:
        digest = str(sha256 or '').strip()
        if not digest:
            return None
        with self._lock, self._connect() as conn:
            return conn.execute(
                """
                SELECT id, file_path, file_name, sha256, phash, width, height, format
                FROM images
                WHERE sha256 = ? AND is_active = 1
                ORDER BY id ASC
                LIMIT 1
                """,
                (digest,),
            ).fetchone()

    def find_similar_images_by_phash(self, phash: str, *, max_distance: int = 8, limit: int = 10) -> list[sqlite3.Row]:
        if not phash:
            return []
        with self._lock, self._connect() as conn:
            rows = conn.execute(
                """
                SELECT id, file_path, file_name, sha256, phash, width, height, format, updated_at
                FROM images
                WHERE is_active = 1 AND phash != ''
                ORDER BY id DESC
                """
            ).fetchall()
        matches: list[tuple[int, sqlite3.Row]] = []
        for row in rows:
            distance = hamming_distance(phash, str(row["phash"] or ""))
            if distance <= max_distance:
                matches.append((distance, row))
        matches.sort(key=lambda item: (item[0], -int(item[1]["id"])))
        return [row for _, row in matches[:limit]]

    @staticmethod
    def _similarity_pair(image_id1: int, image_id2: int) -> tuple[int, int]:
        left = int(image_id1 or 0)
        right = int(image_id2 or 0)
        return (left, right) if left <= right else (right, left)

    def add_similarity_ignore(self, image_id1: int, image_id2: int, reason: str = "") -> tuple[bool, str]:
        low, high = self._similarity_pair(image_id1, image_id2)
        if low <= 0 or high <= 0:
            return False, "图片 ID 必须是正整数。"
        if low == high:
            return False, "不能忽略同一张图片与自身的重复关系。"
        reason_text = str(reason or "").strip()
        now = utcnow_str()
        with self._lock, self._connect() as conn:
            rows = conn.execute(
                "SELECT id FROM images WHERE id IN (?, ?)",
                (low, high),
            ).fetchall()
            if len(rows) != 2:
                return False, f"图片不存在或不完整：#{low} / #{high}"
            existing = conn.execute(
                """
                SELECT id
                FROM image_similarity_ignores
                WHERE image_id_low = ? AND image_id_high = ?
                LIMIT 1
                """,
                (low, high),
            ).fetchone()
            if existing:
                conn.execute(
                    """
                    UPDATE image_similarity_ignores
                    SET reason = ?, created_at = ?
                    WHERE id = ?
                    """,
                    (reason_text, now, int(existing["id"])),
                )
                return True, f"已更新重复忽略：#{low} <-> #{high}"
            conn.execute(
                """
                INSERT INTO image_similarity_ignores(image_id_low, image_id_high, reason, created_at)
                VALUES(?, ?, ?, ?)
                """,
                (low, high, reason_text, now),
            )
        return True, f"已忽略疑似重复关系：#{low} <-> #{high}"

    def remove_similarity_ignore(self, image_id1: int, image_id2: int) -> tuple[bool, str]:
        low, high = self._similarity_pair(image_id1, image_id2)
        if low <= 0 or high <= 0 or low == high:
            return False, "图片 ID 无效。"
        with self._lock, self._connect() as conn:
            cursor = conn.execute(
                "DELETE FROM image_similarity_ignores WHERE image_id_low = ? AND image_id_high = ?",
                (low, high),
            )
        if int(cursor.rowcount or 0) <= 0:
            return False, f"没有找到重复忽略记录：#{low} <-> #{high}"
        return True, f"已恢复疑似重复提示：#{low} <-> #{high}"

    def list_similarity_ignores(self, image_id: int | None = None, *, limit: int = 50) -> list[sqlite3.Row]:
        resolved_limit = max(1, min(int(limit or 50), 200))
        with self._lock, self._connect() as conn:
            if image_id and int(image_id) > 0:
                return conn.execute(
                    """
                    SELECT id, image_id_low, image_id_high, reason, created_at
                    FROM image_similarity_ignores
                    WHERE image_id_low = ? OR image_id_high = ?
                    ORDER BY id DESC
                    LIMIT ?
                    """,
                    (int(image_id), int(image_id), resolved_limit),
                ).fetchall()
            return conn.execute(
                """
                SELECT id, image_id_low, image_id_high, reason, created_at
                FROM image_similarity_ignores
                ORDER BY id DESC
                LIMIT ?
                """,
                (resolved_limit,),
            ).fetchall()

    def filter_ignored_similar_image_ids(self, image_id: int, similar_image_ids: Iterable[int]) -> list[int]:
        base_id = int(image_id or 0)
        if base_id <= 0:
            return [int(item) for item in similar_image_ids if int(item or 0) > 0]
        candidates = []
        seen: set[int] = set()
        pairs: list[tuple[int, int, int]] = []
        for raw in similar_image_ids:
            other_id = int(raw or 0)
            if other_id <= 0 or other_id == base_id or other_id in seen:
                continue
            seen.add(other_id)
            low, high = self._similarity_pair(base_id, other_id)
            candidates.append(other_id)
            pairs.append((other_id, low, high))
        if not pairs:
            return []
        clauses: list[str] = []
        params: list[int] = []
        for _, low, high in pairs:
            clauses.append("(image_id_low = ? AND image_id_high = ?)")
            params.extend([low, high])
        with self._lock, self._connect() as conn:
            ignored = {
                (int(row["image_id_low"]), int(row["image_id_high"]))
                for row in conn.execute(
                    f"""
                    SELECT image_id_low, image_id_high
                    FROM image_similarity_ignores
                    WHERE {' OR '.join(clauses)}
                    """,
                    params,
                ).fetchall()
            }
        return [other_id for other_id, low, high in pairs if (low, high) not in ignored]

    def get_image_row(self, image_id: int) -> sqlite3.Row | None:
        with self._lock, self._connect() as conn:
            return conn.execute("SELECT * FROM images WHERE id = ? LIMIT 1", (image_id,)).fetchone()

    def attach_image_variant(
        self,
        image_id: int,
        *,
        file_path: str,
        file_name: str,
        sha256: str,
        phash: str,
        width: int,
        height: int,
        format_: str,
        storage_type: str = "imported",
        make_primary: bool = False,
    ) -> int:
        now = utcnow_str()
        with self._lock, self._connect() as conn:
            row = conn.execute("SELECT id FROM images WHERE id = ? LIMIT 1", (image_id,)).fetchone()
            if not row:
                raise ValueError(f"image not found: {image_id}")
            self._upsert_file_location(
                conn,
                image_id=image_id,
                file_path=file_path,
                file_name=file_name,
                storage_type=storage_type,
                now=now,
            )
            if make_primary:
                self._set_preferred_image_variant(
                    conn,
                    image_id=image_id,
                    file_path=file_path,
                    file_name=file_name,
                    sha256=sha256,
                    phash=phash,
                    width=width,
                    height=height,
                    format_=format_,
                    now=now,
                )
            else:
                conn.execute(
                    "UPDATE image_files SET is_active = 0, updated_at = ? WHERE image_id = ? AND file_path = ?",
                    (now, image_id, file_path),
                )
                self._sync_image_file_state(conn, image_id, now=now)
        return image_id

    @staticmethod
    def _preferred_image_tag_status(*statuses: str) -> str:
        valid = [str(item or "").strip() for item in statuses if str(item or "").strip()]
        if not valid:
            return "approved"
        return max(valid, key=lambda item: (IMAGE_TAG_STATUS_PRIORITY.get(item, -1), item))

    def merge_images(
        self,
        primary_image_id: int,
        duplicate_image_id: int,
        *,
        preferred_file_path: str | None = None,
        preferred_file_name: str | None = None,
        preferred_sha256: str | None = None,
        preferred_phash: str | None = None,
        preferred_width: int | None = None,
        preferred_height: int | None = None,
        preferred_format: str | None = None,
    ) -> tuple[bool, str]:
        if primary_image_id == duplicate_image_id:
            return False, "cannot merge the same image"

        now = utcnow_str()
        with self._lock, self._connect() as conn:
            primary = conn.execute("SELECT * FROM images WHERE id = ? LIMIT 1", (primary_image_id,)).fetchone()
            duplicate = conn.execute("SELECT * FROM images WHERE id = ? LIMIT 1", (duplicate_image_id,)).fetchone()
            if not primary or not duplicate:
                return False, "image_not_found"

            conn.execute(
                "UPDATE image_files SET image_id = ?, updated_at = ? WHERE image_id = ?",
                (primary_image_id, now, duplicate_image_id),
            )

            duplicate_tags = conn.execute(
                """
                SELECT tag_id, source_type, score, review_status, review_reason, created_at
                FROM image_tags
                WHERE image_id = ?
                """,
                (duplicate_image_id,),
            ).fetchall()
            for row in duplicate_tags:
                existing = conn.execute(
                    """
                    SELECT id, score, review_status, review_reason, created_at
                    FROM image_tags
                    WHERE image_id = ? AND tag_id = ? AND source_type = ?
                    LIMIT 1
                    """,
                    (primary_image_id, int(row["tag_id"]), str(row["source_type"])),
                ).fetchone()
                merged_status = self._preferred_image_tag_status(
                    str(row["review_status"] or ""),
                    str(existing["review_status"] or "") if existing else "",
                )
                merged_score = max(
                    float(row["score"] or 0.0),
                    float(existing["score"] or 0.0) if existing else 0.0,
                )
                merged_reason = (
                    str(existing["review_reason"] or "") if existing and str(existing["review_reason"] or "").strip()
                    else str(row["review_reason"] or "")
                )
                if existing:
                    conn.execute(
                        """
                        UPDATE image_tags
                        SET score = ?, review_status = ?, review_reason = ?, updated_at = ?
                        WHERE id = ?
                        """,
                        (merged_score, merged_status, merged_reason, now, int(existing["id"])),
                    )
                else:
                    conn.execute(
                        """
                        INSERT INTO image_tags(image_id, tag_id, source_type, score, review_status, review_reason, created_at, updated_at)
                        VALUES(?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            primary_image_id,
                            int(row["tag_id"]),
                            str(row["source_type"]),
                            merged_score,
                            merged_status,
                            merged_reason,
                            str(row["created_at"] or now),
                            now,
                        ),
                    )
            conn.execute("DELETE FROM image_tags WHERE image_id = ?", (duplicate_image_id,))

            duplicate_sources = conn.execute("SELECT * FROM sources WHERE image_id = ?", (duplicate_image_id,)).fetchall()
            for row in duplicate_sources:
                existing = conn.execute(
                    "SELECT id FROM sources WHERE image_id = ? AND image_url = ? LIMIT 1",
                    (primary_image_id, str(row["image_url"])),
                ).fetchone()
                if existing:
                    continue
                conn.execute(
                    """
                    INSERT INTO sources(image_id, platform, post_url, image_url, author, raw_tags, extra_json, created_at)
                    VALUES(?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        primary_image_id,
                        str(row["platform"]),
                        str(row["post_url"]),
                        str(row["image_url"]),
                        str(row["author"] or ""),
                        str(row["raw_tags"] or "[]"),
                        str(row["extra_json"] or "{}"),
                        str(row["created_at"] or now),
                    ),
                )
            conn.execute("DELETE FROM sources WHERE image_id = ?", (duplicate_image_id,))

            duplicate_reviews = conn.execute("SELECT * FROM review_tasks WHERE image_id = ?", (duplicate_image_id,)).fetchall()
            for row in duplicate_reviews:
                existing = conn.execute(
                    "SELECT id FROM review_tasks WHERE image_id = ? AND tag_id = ? LIMIT 1",
                    (primary_image_id, int(row["tag_id"])),
                ).fetchone()
                if existing:
                    conn.execute("DELETE FROM review_tasks WHERE id = ?", (int(row["id"]),))
                else:
                    conn.execute(
                        "UPDATE review_tasks SET image_id = ?, updated_at = ? WHERE id = ?",
                        (primary_image_id, now, int(row["id"])),
                    )

            conn.execute("UPDATE send_logs SET image_id = ? WHERE image_id = ?", (primary_image_id, duplicate_image_id))

            preferred_path = str(preferred_file_path or "").strip() or str(primary["file_path"])
            preferred_name = str(preferred_file_name or "").strip() or str(primary["file_name"])
            preferred_sha = str(preferred_sha256 or "").strip() or str(primary["sha256"])
            preferred_ph = str(preferred_phash or "").strip() or str(primary["phash"] or "")
            width = int(preferred_width if preferred_width is not None else int(primary["width"] or 0))
            height = int(preferred_height if preferred_height is not None else int(primary["height"] or 0))
            format_name = str(preferred_format or "").strip() or str(primary["format"] or "")

            duplicate_current_path = str(duplicate["file_path"] or "").strip()
            if preferred_path and preferred_path == duplicate_current_path and preferred_path != str(primary["file_path"] or ""):
                placeholder_path = f"{preferred_path}#merged-{duplicate_image_id}"
                conn.execute(
                    "UPDATE images SET file_path = ?, updated_at = ? WHERE id = ?",
                    (placeholder_path, now, duplicate_image_id),
                )

            self._set_preferred_image_variant(
                conn,
                image_id=primary_image_id,
                file_path=preferred_path,
                file_name=preferred_name,
                sha256=preferred_sha,
                phash=preferred_ph,
                width=width,
                height=height,
                format_=format_name,
                now=now,
            )
            conn.execute(
                "UPDATE image_files SET is_active = 0, updated_at = ? WHERE image_id = ? AND file_path != ?",
                (now, primary_image_id, preferred_path),
            )
            conn.execute(
                """
                UPDATE images
                SET is_active = 0, updated_at = ?
                WHERE id = ?
                """,
                (now, duplicate_image_id),
            )
        return True, f"merged {duplicate_image_id} -> {primary_image_id}"

    def mark_missing_files_inactive(self, library_root: str, seen_paths: set[str]) -> int:
        root = str(Path(library_root).resolve())
        count = 0
        with self._lock, self._connect() as conn:
            rows = conn.execute(
                """
                SELECT id, image_id, file_path
                FROM image_files
                WHERE storage_type = 'library' AND file_path LIKE ?
                """,
                (f'{root}%',),
            ).fetchall()
            affected_image_ids: set[int] = set()
            for row in rows:
                if row['file_path'] not in seen_paths:
                    conn.execute(
                        "UPDATE image_files SET is_active = 0, updated_at = ? WHERE id = ?",
                        (utcnow_str(), row['id']),
                    )
                    affected_image_ids.add(int(row["image_id"]))
                    count += 1
            for image_id in affected_image_ids:
                self._sync_image_file_state(conn, image_id)
        return count

    def get_or_create_tag(
        self,
        tag_name: str,
        is_character: bool | None = None,
        *,
        tag_type: str | None = None,
        status: str | None = None,
    ) -> int:
        normalized = normalize_tag_name(tag_name)
        if not normalized:
            raise ValueError('tag 不能为空')
        requested_type = normalize_tag_type(tag_type)
        if tag_type is not None and requested_type is None:
            raise ValueError(f'不支持的 tag 类型：{tag_type}')
        if requested_type is None and is_character is True:
            requested_type = 'character'
        requested_status = normalize_tag_status(status)
        if status is not None and requested_status is None:
            raise ValueError(f'不支持的 tag 状态：{status}')
        now = utcnow_str()
        with self._lock, self._connect() as conn:
            row = conn.execute('SELECT id, is_character, tag_type, status FROM tags WHERE normalized_name = ?', (normalized,)).fetchone()
            if row:
                updates: list[str] = []
                params: list[Any] = []
                if requested_type is not None and str(row['tag_type'] or '') != requested_type:
                    updates.extend(['tag_type = ?', 'is_character = ?'])
                    params.extend([requested_type, 1 if requested_type == 'character' else 0])
                if requested_status is not None and str(row['status'] or '') != requested_status:
                    updates.append('status = ?')
                    params.append(requested_status)
                if updates:
                    params.append(int(row['id']))
                    conn.execute(f"UPDATE tags SET {', '.join(updates)} WHERE id = ?", params)
                return int(row['id'])
            resolved_type = requested_type or 'other'
            resolved_status = requested_status or 'active'
            cursor = conn.execute(
                'INSERT INTO tags(name, normalized_name, is_character, tag_type, status, created_at) VALUES(?, ?, ?, ?, ?, ?)',
                (tag_name.strip(), normalized, 1 if resolved_type == 'character' else 0, resolved_type, resolved_status, now),
            )
            return int(cursor.lastrowid)

    def create_or_get_tag(
        self,
        tag_name: str,
        is_character: bool | None = None,
        *,
        tag_type: str | None = None,
        status: str | None = None,
    ) -> tuple[bool, dict[str, Any]]:
        tag_text = str(tag_name or '').strip()
        normalized = normalize_tag_name(tag_text)
        if not tag_text or not normalized:
            return False, {'message': 'tag 不能为空'}
        requested_type = normalize_tag_type(tag_type)
        if tag_type is not None and requested_type is None:
            return False, {'message': f'不支持的 tag 类型：{tag_type}'}
        if requested_type is None and is_character is True:
            requested_type = 'character'
        requested_status = normalize_tag_status(status)
        if status is not None and requested_status is None:
            return False, {'message': f'不支持的 tag 状态：{status}'}
        now = utcnow_str()
        with self._lock, self._connect() as conn:
            row = conn.execute('SELECT * FROM tags WHERE normalized_name = ? LIMIT 1', (normalized,)).fetchone()
            created = False
            if row:
                updates: list[str] = []
                params: list[Any] = []
                if requested_type is not None and str(row['tag_type'] or '') != requested_type:
                    updates.extend(['tag_type = ?', 'is_character = ?'])
                    params.extend([requested_type, 1 if requested_type == 'character' else 0])
                if requested_status is not None and str(row['status'] or '') != requested_status:
                    updates.append('status = ?')
                    params.append(requested_status)
                if updates:
                    params.append(int(row['id']))
                    conn.execute(f"UPDATE tags SET {', '.join(updates)} WHERE id = ?", params)
                    row = conn.execute('SELECT * FROM tags WHERE id = ? LIMIT 1', (int(row['id']),)).fetchone()
            else:
                resolved_type = requested_type or 'other'
                resolved_status = requested_status or 'active'
                cursor = conn.execute(
                    'INSERT INTO tags(name, normalized_name, is_character, tag_type, status, created_at) VALUES(?, ?, ?, ?, ?, ?)',
                    (tag_text, normalized, 1 if resolved_type == 'character' else 0, resolved_type, resolved_status, now),
                )
                row = conn.execute('SELECT * FROM tags WHERE id = ? LIMIT 1', (int(cursor.lastrowid),)).fetchone()
                created = True
        if not row:
            return False, {'message': 'tag 创建失败'}
        return True, {
            'message': ('已新增主 tag' if created else '已复用已有主 tag'),
            'tag': {
                'id': int(row['id']),
                'name': str(row['name']),
                'is_character': bool(int(row['is_character'] or 0)),
                'tag_type': str(row['tag_type'] or 'other'),
                'status': str(row['status'] or 'active'),
            },
            'created': created,
        }

    def set_tag_character(self, tag_name: str, is_character: bool) -> tuple[bool, str]:
        return self.set_tag_type(tag_name, 'character' if is_character else 'other')

    def set_tag_type(self, tag_name: str, tag_type: str) -> tuple[bool, str]:
        resolved_type = normalize_tag_type(tag_type)
        if resolved_type is None:
            return False, f'不支持的 tag 类型：{tag_type}'
        tag_id = self.get_tag_id(tag_name)
        if tag_id is None:
            return False, f'tag 不存在：{tag_name}'
        with self._lock, self._connect() as conn:
            row = conn.execute('SELECT name FROM tags WHERE id = ? LIMIT 1', (tag_id,)).fetchone()
            conn.execute(
                'UPDATE tags SET tag_type = ?, is_character = ? WHERE id = ?',
                (resolved_type, 1 if resolved_type == 'character' else 0, tag_id),
            )
            if resolved_type != 'character':
                conn.execute('UPDATE crawl_subscriptions SET enabled = 0, updated_at = ? WHERE tag_id = ?', (utcnow_str(), tag_id))
        canonical_name = str(row['name']) if row else tag_name
        return True, f'已将 {canonical_name} 类型设置为：{resolved_type}'

    def set_tag_status(self, tag_name: str, status: str) -> tuple[bool, str]:
        resolved_status = normalize_tag_status(status)
        if resolved_status is None:
            return False, f'不支持的 tag 状态：{status}'
        tag_id = self.get_tag_id(tag_name)
        if tag_id is None:
            return False, f'tag 不存在：{tag_name}'
        with self._lock, self._connect() as conn:
            row = conn.execute('SELECT name FROM tags WHERE id = ? LIMIT 1', (tag_id,)).fetchone()
            conn.execute('UPDATE tags SET status = ? WHERE id = ?', (resolved_status, tag_id))
            if resolved_status != 'active':
                conn.execute('UPDATE crawl_subscriptions SET enabled = 0, updated_at = ? WHERE tag_id = ?', (utcnow_str(), tag_id))
        canonical_name = str(row['name']) if row else tag_name
        return True, f'已将 {canonical_name} 状态设置为：{resolved_status}'

    def get_tag_row(self, tag_name: str) -> sqlite3.Row | None:
        normalized = normalize_tag_name(tag_name)
        with self._lock, self._connect() as conn:
            return conn.execute('SELECT * FROM tags WHERE normalized_name = ?', (normalized,)).fetchone()

    def get_tag_row_by_id(self, tag_id: int) -> sqlite3.Row | None:
        with self._lock, self._connect() as conn:
            return conn.execute('SELECT * FROM tags WHERE id = ?', (tag_id,)).fetchone()

    @staticmethod
    def _proposal_row_to_dict(row: sqlite3.Row | None) -> dict[str, Any] | None:
        if not row:
            return None
        try:
            aliases = json.loads(row['aliases_json'] or '[]')
        except Exception:
            aliases = []
        if not isinstance(aliases, list):
            aliases = []
        return {
            'id': int(row['id']),
            'proposed_name': str(row['proposed_name'] or ''),
            'normalized_name': str(row['normalized_name'] or ''),
            'aliases': [str(item) for item in aliases if str(item).strip()],
            'submitter_id': str(row['submitter_id'] or ''),
            'submitter_name': str(row['submitter_name'] or ''),
            'platform_name': str(row['platform_name'] or ''),
            'session_id': str(row['session_id'] or ''),
            'message_id': str(row['message_id'] or ''),
            'occurrence_count': int(row['occurrence_count'] or 1),
            'status': str(row['status'] or 'pending'),
            'resolved_tag_id': int(row['resolved_tag_id'] or 0),
            'reason': str(row['reason'] or ''),
            'created_at': str(row['created_at'] or ''),
            'updated_at': str(row['updated_at'] or ''),
        }

    def create_or_increment_tag_proposal(
        self,
        proposed_name: str,
        *,
        aliases: Iterable[str] = (),
        submitter_id: str = '',
        submitter_name: str = '',
        platform_name: str = '',
        session_id: str = '',
        message_id: str = '',
    ) -> dict[str, Any]:
        name = str(proposed_name or '').strip()
        normalized = normalize_tag_name(name)
        if not name or not normalized:
            raise ValueError('提案 tag 不能为空')
        now = utcnow_str()
        requested_aliases = self._dedupe_terms(aliases)
        with self._lock, self._connect() as conn:
            existing = conn.execute(
                "SELECT * FROM tag_proposals WHERE normalized_name = ? AND status = 'pending' ORDER BY id DESC LIMIT 1",
                (normalized,),
            ).fetchone()
            if existing:
                try:
                    existing_aliases = json.loads(existing['aliases_json'] or '[]')
                except Exception:
                    existing_aliases = []
                merged_aliases = self._dedupe_terms([*(existing_aliases if isinstance(existing_aliases, list) else []), *requested_aliases])
                conn.execute(
                    """
                    UPDATE tag_proposals
                    SET proposed_name = ?, aliases_json = ?, submitter_id = ?, submitter_name = ?,
                        platform_name = ?, session_id = ?, message_id = ?,
                        occurrence_count = occurrence_count + 1, updated_at = ?
                    WHERE id = ?
                    """,
                    (
                        name,
                        json.dumps(merged_aliases, ensure_ascii=False),
                        str(submitter_id or ''),
                        str(submitter_name or ''),
                        str(platform_name or ''),
                        str(session_id or ''),
                        str(message_id or ''),
                        now,
                        int(existing['id']),
                    ),
                )
                row = conn.execute('SELECT * FROM tag_proposals WHERE id = ?', (int(existing['id']),)).fetchone()
            else:
                cursor = conn.execute(
                    """
                    INSERT INTO tag_proposals(
                        proposed_name, normalized_name, aliases_json,
                        submitter_id, submitter_name, platform_name, session_id, message_id,
                        occurrence_count, status, resolved_tag_id, reason, created_at, updated_at
                    )
                    VALUES(?, ?, ?, ?, ?, ?, ?, ?, 1, 'pending', 0, '', ?, ?)
                    """,
                    (
                        name,
                        normalized,
                        json.dumps(requested_aliases, ensure_ascii=False),
                        str(submitter_id or ''),
                        str(submitter_name or ''),
                        str(platform_name or ''),
                        str(session_id or ''),
                        str(message_id or ''),
                        now,
                        now,
                    ),
                )
                row = conn.execute('SELECT * FROM tag_proposals WHERE id = ?', (int(cursor.lastrowid),)).fetchone()
        result = self._proposal_row_to_dict(row)
        if not result:
            raise RuntimeError('tag 提案记录失败')
        return result

    def list_tag_proposals(self, *, status: str = 'pending', limit: int = 30) -> list[dict[str, Any]]:
        normalized_status = str(status or '').strip().lower()
        if normalized_status not in {'pending', 'approved', 'rejected'}:
            normalized_status = 'pending'
        with self._lock, self._connect() as conn:
            rows = conn.execute(
                'SELECT * FROM tag_proposals WHERE status = ? ORDER BY updated_at DESC, id DESC LIMIT ?',
                (normalized_status, max(1, min(200, int(limit or 30)))),
            ).fetchall()
        return [item for row in rows if (item := self._proposal_row_to_dict(row)) is not None]

    def get_tag_proposal(self, proposal_id: int) -> dict[str, Any] | None:
        with self._lock, self._connect() as conn:
            row = conn.execute('SELECT * FROM tag_proposals WHERE id = ? LIMIT 1', (int(proposal_id),)).fetchone()
        return self._proposal_row_to_dict(row)

    def approve_tag_proposal(self, proposal_id: int, tag_type: str) -> tuple[bool, dict[str, Any]]:
        resolved_type = normalize_tag_type(tag_type)
        if resolved_type is None:
            return False, {'message': f'不支持的 tag 类型：{tag_type}'}
        now = utcnow_str()
        with self._lock, self._connect() as conn:
            proposal = conn.execute('SELECT * FROM tag_proposals WHERE id = ? LIMIT 1', (int(proposal_id),)).fetchone()
            if not proposal:
                return False, {'message': f'tag 提案不存在：#{int(proposal_id)}'}
            if str(proposal['status'] or '') != 'pending':
                return False, {'message': f'tag 提案 #{int(proposal_id)} 已处理：{proposal["status"]}'}
            existing, match_type = self._resolve_tag_exact_conn(conn, str(proposal['proposed_name'] or ''))
            if existing and match_type == 'exact_alias':
                return False, {'message': f'提案名已经是 {existing["name"]} 的 alias，请使用 tag提案归并。'}
            created = False
            if existing:
                tag_id = int(existing['id'])
                conn.execute(
                    'UPDATE tags SET tag_type = ?, is_character = ?, status = ? WHERE id = ?',
                    (resolved_type, 1 if resolved_type == 'character' else 0, 'active', tag_id),
                )
                tag_name = str(existing['name'])
            else:
                cursor = conn.execute(
                    'INSERT INTO tags(name, normalized_name, is_character, tag_type, status, created_at) VALUES(?, ?, ?, ?, ?, ?)',
                    (
                        str(proposal['proposed_name'] or '').strip(),
                        str(proposal['normalized_name'] or ''),
                        1 if resolved_type == 'character' else 0,
                        resolved_type,
                        'active',
                        now,
                    ),
                )
                tag_id = int(cursor.lastrowid)
                tag_name = str(proposal['proposed_name'] or '').strip()
                created = True
            conn.execute(
                "UPDATE tag_proposals SET status = 'approved', resolved_tag_id = ?, reason = ?, updated_at = ? WHERE id = ?",
                (tag_id, f'approved_as:{resolved_type}', now, int(proposal_id)),
            )
        return True, {
            'message': f'已通过 tag 提案 #{int(proposal_id)}：{tag_name}',
            'tag_id': tag_id,
            'tag_name': tag_name,
            'tag_type': resolved_type,
            'created': created,
        }

    def merge_tag_proposal(self, proposal_id: int, target_tag_name: str) -> tuple[bool, dict[str, Any]]:
        now = utcnow_str()
        with self._lock, self._connect() as conn:
            proposal = conn.execute('SELECT * FROM tag_proposals WHERE id = ? LIMIT 1', (int(proposal_id),)).fetchone()
            if not proposal:
                return False, {'message': f'tag 提案不存在：#{int(proposal_id)}'}
            if str(proposal['status'] or '') != 'pending':
                return False, {'message': f'tag 提案 #{int(proposal_id)} 已处理：{proposal["status"]}'}
            target, _ = self._resolve_tag_exact_conn(conn, target_tag_name)
            if not target:
                return False, {'message': f'目标主 tag 不存在：{target_tag_name}'}
            if str(target['status'] or 'active') != 'active':
                return False, {'message': f'目标主 tag 未启用：{target["name"]}'}
            target_id = int(target['id'])
            proposal_name = str(proposal['proposed_name'] or '').strip()
            existing, _ = self._resolve_tag_exact_conn(conn, proposal_name)
            alias_added = False
            if existing:
                if int(existing['id']) != target_id:
                    return False, {'message': f'提案名已被其他主 tag 使用：{existing["name"]}'}
            elif normalize_tag_name(proposal_name) != str(target['normalized_name'] or ''):
                ok, message = self._insert_alias_conn(conn, tag_id=target_id, alias=proposal_name, now=now)
                if not ok:
                    return False, {'message': message}
                alias_added = True
            conn.execute(
                "UPDATE tag_proposals SET status = 'approved', resolved_tag_id = ?, reason = ?, updated_at = ? WHERE id = ?",
                (target_id, 'merged_to_existing', now, int(proposal_id)),
            )
        return True, {
            'message': f'已将 tag 提案 #{int(proposal_id)} 归并到：{target["name"]}',
            'tag_id': target_id,
            'tag_name': str(target['name']),
            'alias_added': alias_added,
        }

    def reject_tag_proposal(self, proposal_id: int, reason: str = '') -> tuple[bool, str]:
        with self._lock, self._connect() as conn:
            proposal = conn.execute('SELECT * FROM tag_proposals WHERE id = ? LIMIT 1', (int(proposal_id),)).fetchone()
            if not proposal:
                return False, f'tag 提案不存在：#{int(proposal_id)}'
            if str(proposal['status'] or '') != 'pending':
                return False, f'tag 提案 #{int(proposal_id)} 已处理：{proposal["status"]}'
            conn.execute(
                "UPDATE tag_proposals SET status = 'rejected', reason = ?, updated_at = ? WHERE id = ?",
                (str(reason or '').strip() or '管理员拒绝', utcnow_str(), int(proposal_id)),
            )
        return True, f'已拒绝 tag 提案 #{int(proposal_id)}：{proposal["proposed_name"]}'

    @staticmethod
    def _resolve_tag_exact_conn(conn: sqlite3.Connection, query: str) -> tuple[sqlite3.Row | None, str]:
        normalized = normalize_tag_name(query)
        if not normalized:
            return None, ''
        exact_tag = conn.execute('SELECT * FROM tags WHERE normalized_name = ? LIMIT 1', (normalized,)).fetchone()
        if exact_tag:
            return exact_tag, 'exact_tag'
        exact_alias = conn.execute(
            """
            SELECT t.*
            FROM tag_aliases a
            JOIN tags t ON t.id = a.tag_id
            WHERE a.normalized_alias = ?
            LIMIT 1
            """,
            (normalized,),
        ).fetchone()
        if exact_alias:
            return exact_alias, 'exact_alias'
        return None, ''

    @staticmethod
    def _get_alias_usage_conn(conn: sqlite3.Connection, normalized_alias: str) -> sqlite3.Row | None:
        return conn.execute(
            """
            SELECT a.id, a.tag_id, a.alias, t.name AS tag_name
            FROM tag_aliases a
            JOIN tags t ON t.id = a.tag_id
            WHERE a.normalized_alias = ?
            LIMIT 1
            """,
            (normalized_alias,),
        ).fetchone()

    def _insert_alias_conn(
        self,
        conn: sqlite3.Connection,
        *,
        tag_id: int,
        alias: str,
        now: str | None = None,
    ) -> tuple[bool, str]:
        alias_text = str(alias or '').strip()
        if not alias_text:
            return False, 'alias 不能为空'
        target = conn.execute('SELECT id, name, normalized_name FROM tags WHERE id = ? LIMIT 1', (tag_id,)).fetchone()
        if not target:
            return False, f'tag 不存在：{tag_id}'

        normalized_alias = normalize_tag_name(alias_text)
        if not normalized_alias:
            return False, 'alias 不能为空'
        if normalized_alias == str(target['normalized_name'] or ''):
            return False, f'alias 不能与主 tag 相同：{alias_text}'

        conflict_tag = conn.execute(
            'SELECT id, name FROM tags WHERE normalized_name = ? LIMIT 1',
            (normalized_alias,),
        ).fetchone()
        if conflict_tag:
            if int(conflict_tag['id']) == int(tag_id):
                return False, f'alias 不能与主 tag 相同：{alias_text}'
            return False, f'alias 与现有 tag 冲突：{conflict_tag["name"]}'

        alias_usage = self._get_alias_usage_conn(conn, normalized_alias)
        if alias_usage:
            if int(alias_usage['tag_id']) == int(tag_id):
                return False, f'alias 已存在：{alias_text}'
            return False, f'alias 已被 {alias_usage["tag_name"]} 使用：{alias_text}'

        conn.execute(
            'INSERT INTO tag_aliases(tag_id, alias, normalized_alias, created_at) VALUES(?, ?, ?, ?)',
            (tag_id, alias_text, normalized_alias, now or utcnow_str()),
        )
        return True, f'已添加别名：{target["name"]} -> {alias_text}'

    @staticmethod
    def _preferred_review_task_status(*statuses: str) -> str:
        valid = [str(item or '').strip() for item in statuses if str(item or '').strip()]
        if not valid:
            return 'pending'
        return max(valid, key=lambda item: (IMAGE_TAG_STATUS_PRIORITY.get(item, -1), item))

    def _merge_tag_into_conn(
        self,
        conn: sqlite3.Connection,
        *,
        target_id: int,
        target_name: str,
        source_id: int,
        now: str,
    ) -> dict[str, Any]:
        target = conn.execute('SELECT * FROM tags WHERE id = ? LIMIT 1', (target_id,)).fetchone()
        source = conn.execute('SELECT * FROM tags WHERE id = ? LIMIT 1', (source_id,)).fetchone()
        if not target or not source:
            raise ValueError('tag_not_found')
        if int(target['id']) == int(source['id']):
            return {
                'source_name': str(source['name']),
                'image_links_migrated': 0,
                'review_tasks_migrated': 0,
                'review_tasks_merged': 0,
                'aliases_migrated': 0,
                'aliases_skipped': [],
                'source_name_alias_added': False,
                'subscriptions_migrated': 0,
                'subscriptions_merged': 0,
                'subscriptions_removed': 0,
            }

        image_links_migrated = 0
        source_image_tags = conn.execute(
            """
            SELECT id, image_id, source_type, score, review_status, review_reason, created_at
            FROM image_tags
            WHERE tag_id = ?
            """,
            (source_id,),
        ).fetchall()
        for row in source_image_tags:
            existing = conn.execute(
                """
                SELECT id, score, review_status, review_reason
                FROM image_tags
                WHERE image_id = ? AND tag_id = ? AND source_type = ?
                LIMIT 1
                """,
                (int(row['image_id']), target_id, str(row['source_type'])),
            ).fetchone()
            merged_status = self._preferred_image_tag_status(
                str(existing['review_status'] or '') if existing else '',
                str(row['review_status'] or ''),
            )
            merged_score = max(
                float(existing['score'] or 0.0) if existing else 0.0,
                float(row['score'] or 0.0),
            )
            merged_reason = (
                str(existing['review_reason'] or '') if existing and str(existing['review_reason'] or '').strip()
                else str(row['review_reason'] or '')
            )
            if existing:
                conn.execute(
                    """
                    UPDATE image_tags
                    SET score = ?, review_status = ?, review_reason = ?, updated_at = ?
                    WHERE id = ?
                    """,
                    (merged_score, merged_status, merged_reason, now, int(existing['id'])),
                )
                conn.execute('DELETE FROM image_tags WHERE id = ?', (int(row['id']),))
            else:
                conn.execute(
                    """
                    UPDATE image_tags
                    SET tag_id = ?, review_status = ?, review_reason = ?, score = ?, updated_at = ?
                    WHERE id = ?
                    """,
                    (target_id, merged_status, merged_reason, merged_score, now, int(row['id'])),
                )
            image_links_migrated += 1

        review_tasks_migrated = 0
        review_tasks_merged = 0
        source_reviews = conn.execute(
            """
            SELECT id, image_id, status, model_result, manual_result, reason, created_at
            FROM review_tasks
            WHERE tag_id = ?
            """,
            (source_id,),
        ).fetchall()
        for row in source_reviews:
            existing = conn.execute(
                """
                SELECT id, status, model_result, manual_result, reason
                FROM review_tasks
                WHERE image_id = ? AND tag_id = ?
                ORDER BY id DESC
                LIMIT 1
                """,
                (int(row['image_id']), target_id),
            ).fetchone()
            merged_status = self._preferred_review_task_status(
                str(existing['status'] or '') if existing else '',
                str(row['status'] or ''),
            )
            merged_model_result = (
                str(existing['model_result'] or '') if existing and str(existing['model_result'] or '').strip()
                else str(row['model_result'] or '')
            )
            merged_manual_result = (
                str(existing['manual_result'] or '') if existing and str(existing['manual_result'] or '').strip()
                else str(row['manual_result'] or '')
            )
            merged_reason = (
                str(existing['reason'] or '') if existing and str(existing['reason'] or '').strip()
                else str(row['reason'] or '')
            )
            if existing:
                conn.execute(
                    """
                    UPDATE review_tasks
                    SET status = ?, model_result = ?, manual_result = ?, reason = ?, updated_at = ?
                    WHERE id = ?
                    """,
                    (merged_status, merged_model_result, merged_manual_result, merged_reason, now, int(existing['id'])),
                )
                conn.execute('DELETE FROM review_tasks WHERE id = ?', (int(row['id']),))
                review_tasks_merged += 1
            else:
                conn.execute(
                    """
                    UPDATE review_tasks
                    SET tag_id = ?, status = ?, model_result = ?, manual_result = ?, reason = ?, updated_at = ?
                    WHERE id = ?
                    """,
                    (target_id, merged_status, merged_model_result, merged_manual_result, merged_reason, now, int(row['id'])),
                )
                review_tasks_migrated += 1

        source_alias_rows = conn.execute(
            'SELECT alias FROM tag_aliases WHERE tag_id = ? ORDER BY id ASC',
            (source_id,),
        ).fetchall()
        conn.execute('DELETE FROM tag_aliases WHERE tag_id = ?', (source_id,))
        aliases_migrated = 0
        aliases_skipped: list[str] = []
        for row in source_alias_rows:
            alias = str(row['alias'] or '').strip()
            if not alias:
                continue
            ok, message = self._insert_alias_conn(conn, tag_id=target_id, alias=alias, now=now)
            if ok:
                aliases_migrated += 1
            else:
                aliases_skipped.append(f'{alias}（{message}）')

        source_platform_terms = conn.execute(
            'SELECT platform, term, term_type, source, confidence FROM platform_tag_terms WHERE tag_id = ? ORDER BY id ASC',
            (source_id,),
        ).fetchall()
        conn.execute('DELETE FROM platform_tag_terms WHERE tag_id = ?', (source_id,))
        for row in source_platform_terms:
            self._upsert_platform_term_conn(
                conn,
                tag_id=target_id,
                platform=str(row['platform'] or ''),
                term=str(row['term'] or ''),
                term_type=str(row['term_type'] or 'both'),
                source=str(row['source'] or 'manual_review'),
                confidence=float(row['confidence'] or 0.0),
                now=now,
            )

        source_name_alias_added = False
        subscriptions_migrated = 0
        subscriptions_merged = 0
        subscriptions_removed = 0
        source_subscription_rows = conn.execute(
            'SELECT * FROM crawl_subscriptions WHERE tag_id = ? ORDER BY id ASC',
            (source_id,),
        ).fetchall()
        target_normalized = str(target['normalized_name'] or normalize_tag_name(str(target['name'] or '')))
        for row in source_subscription_rows:
            platform_text = str(row['platform'] or '').strip()
            target_subscription = conn.execute(
                """
                SELECT *
                FROM crawl_subscriptions
                WHERE platform = ? AND normalized_tag = ?
                ORDER BY id DESC
                LIMIT 1
                """,
                (platform_text, target_normalized),
            ).fetchone()
            if target_subscription and int(target_subscription['id']) != int(row['id']):
                merged_query_text = str(target_subscription['query_text'] or '').strip() or str(row['query_text'] or '').strip()
                merged_enabled = bool(int(target_subscription['enabled'] or 0)) or bool(int(row['enabled'] or 0))
                merged_last_seen_source_uid = str(target_subscription['last_seen_source_uid'] or '').strip() or str(row['last_seen_source_uid'] or '').strip()
                merged_last_checked_at = max(
                    str(target_subscription['last_checked_at'] or '').strip(),
                    str(row['last_checked_at'] or '').strip(),
                )
                merged_last_success_at = max(
                    str(target_subscription['last_success_at'] or '').strip(),
                    str(row['last_success_at'] or '').strip(),
                )
                merged_last_error = str(target_subscription['last_error'] or '').strip() or str(row['last_error'] or '').strip()
                conn.execute(
                    """
                    UPDATE crawl_subscriptions
                    SET tag_id = ?, tag_name = ?, normalized_tag = ?, query_text = ?, enabled = ?,
                        last_seen_source_uid = ?, last_checked_at = ?, last_success_at = ?, last_error = ?, updated_at = ?
                    WHERE id = ?
                    """,
                    (
                        target_id,
                        target_name,
                        target_normalized,
                        merged_query_text,
                        1 if merged_enabled else 0,
                        merged_last_seen_source_uid,
                        merged_last_checked_at,
                        merged_last_success_at,
                        merged_last_error,
                        now,
                        int(target_subscription['id']),
                    ),
                )
                self._merge_crawl_subscription_terms_conn(
                    conn,
                    source_subscription_id=int(row['id']),
                    target_subscription_id=int(target_subscription['id']),
                    now=now,
                )
                conn.execute('DELETE FROM crawl_subscriptions WHERE id = ?', (int(row['id']),))
                subscriptions_removed += 1
                subscriptions_merged += 1
            else:
                conn.execute(
                    """
                    UPDATE crawl_subscriptions
                    SET tag_id = ?, tag_name = ?, normalized_tag = ?, updated_at = ?
                    WHERE id = ?
                    """,
                    (target_id, target_name, target_normalized, now, int(row['id'])),
                )
                subscriptions_migrated += 1
        conn.execute('DELETE FROM tags WHERE id = ?', (source_id,))

        ok, _ = self._insert_alias_conn(conn, tag_id=target_id, alias=str(source['name']), now=now)
        source_name_alias_added = ok

        if str(source['tag_type'] or 'other') == 'character' and str(target['tag_type'] or 'other') != 'character':
            conn.execute(
                "UPDATE tags SET tag_type = 'character', is_character = 1 WHERE id = ?",
                (target_id,),
            )

        return {
            'source_name': str(source['name']),
            'image_links_migrated': image_links_migrated,
            'review_tasks_migrated': review_tasks_migrated,
            'review_tasks_merged': review_tasks_merged,
            'aliases_migrated': aliases_migrated,
            'aliases_skipped': aliases_skipped,
            'source_name_alias_added': source_name_alias_added,
            'subscriptions_migrated': subscriptions_migrated,
            'subscriptions_merged': subscriptions_merged,
            'subscriptions_removed': subscriptions_removed,
        }

    def link_image_tag(self, image_id: int, tag_id: int, source_type: str = 'directory', review_status: str = 'approved', score: float = 1.0, review_reason: str = '') -> None:
        now = utcnow_str()
        with self._lock, self._connect() as conn:
            conn.execute(
                """
                INSERT INTO image_tags(image_id, tag_id, source_type, score, review_status, review_reason, created_at, updated_at)
                VALUES(?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(image_id, tag_id, source_type)
                DO UPDATE SET
                    score = excluded.score,
                    review_status = excluded.review_status,
                    review_reason = excluded.review_reason,
                    updated_at = excluded.updated_at
                """,
                (image_id, tag_id, source_type, score, review_status, review_reason, now, now),
            )

    def update_image_tag_review(self, image_id: int, tag_id: int, status: str, reason: str = '', source_type_prefix: str | None = None) -> None:
        sql = 'UPDATE image_tags SET review_status = ?, review_reason = ?, updated_at = ? WHERE image_id = ? AND tag_id = ?'
        params: list[Any] = [status, reason, utcnow_str(), image_id, tag_id]
        if source_type_prefix:
            sql += ' AND source_type LIKE ?'
            params.append(f'{source_type_prefix}%')
        with self._lock, self._connect() as conn:
            conn.execute(sql, params)

    def add_alias(self, tag_name: str, alias: str) -> tuple[bool, str]:
        tag_name = tag_name.strip()
        alias = alias.strip()
        if not tag_name or not alias:
            return False, 'tag \u548c alias \u90FD\u4E0D\u80FD\u4E3A\u7A7A\u3002'
        tag_id = self.get_tag_id(tag_name)
        if tag_id is None:
            return False, f'tag \u4E0D\u5B58\u5728\uFF1A{tag_name}'
        with self._lock, self._connect() as conn:
            return self._insert_alias_conn(conn, tag_id=tag_id, alias=alias, now=utcnow_str())

    def remove_alias(self, tag_name: str, alias: str) -> tuple[bool, str]:
        tag_id = self.get_tag_id(tag_name.strip())
        if tag_id is None:
            return False, f'tag 不存在：{tag_name}'
        normalized = normalize_tag_name(alias)
        with self._lock, self._connect() as conn:
            cursor = conn.execute('DELETE FROM tag_aliases WHERE tag_id = ? AND normalized_alias = ?', (tag_id, normalized))
            if cursor.rowcount <= 0:
                return False, f'别名不存在：{alias}'
        return True, f'已删除别名：{tag_name} -> {alias}'

    def list_aliases(self, tag_name: str) -> list[str]:
        tag_id = self.get_tag_id(tag_name.strip())
        if tag_id is None:
            return []
        with self._lock, self._connect() as conn:
            rows = conn.execute('SELECT alias FROM tag_aliases WHERE tag_id = ? ORDER BY alias ASC', (tag_id,)).fetchall()
            return [str(row['alias']) for row in rows]


    @staticmethod
    def _normalize_platform_term_type(term_type: str) -> str:
        text = str(term_type or 'both').strip().lower()
        if text not in {'query', 'match', 'both'}:
            return 'both'
        return text

    @classmethod
    def _platform_term_type_union(cls, left: str, right: str) -> str:
        left_normalized = cls._normalize_platform_term_type(left)
        right_normalized = cls._normalize_platform_term_type(right)
        if left_normalized == right_normalized:
            return left_normalized
        return 'both'

    @classmethod
    def _platform_term_type_matches(cls, stored_term_type: str, expected_types: set[str]) -> bool:
        normalized = cls._normalize_platform_term_type(stored_term_type)
        if normalized == 'both':
            return bool({'query', 'match'} & expected_types)
        return normalized in expected_types

    @staticmethod
    def _looks_like_platform_term(term: str) -> bool:
        text = str(term or '').strip()
        normalized = normalize_tag_name(text)
        if not text or not normalized or len(text) > 60 or len(normalized) <= 1:
            return False
        lowered = text.lower()
        blacklist = {
            'pixiv', 'illustration', 'fanart', 'art', 'image', 'images',
            '插画', '图片', '图', '壁纸', '漫画', '约稿', '头像',
            '女の子', '女孩子', '世界计划', 'プロセカ', 'projectsekai', 'prsk_fa',
            'プロジェクトセカイ', 'プロジェクトセカイカラフルステージ', 'pjsk',
            'ニーゴ', '25時、ナイトコードで。', '25时，在夜之电台',
        }
        if lowered in blacklist:
            return False
        if lowered.isdigit() or 'http' in lowered:
            return False
        if 'users入り' in lowered or 'bookmarks' in lowered:
            return False
        if any(ch in text for ch in '/|,，'):
            return False
        return True

    @staticmethod
    def _resolve_platform_term_exact_conn(conn: sqlite3.Connection, platform: str, query: str) -> sqlite3.Row | None:
        normalized = normalize_tag_name(query)
        if not normalized:
            return None
        return conn.execute(
            """
            SELECT p.id, p.tag_id, p.term, p.term_type, p.source, p.confidence,
                   t.name AS tag_name
            FROM platform_tag_terms p
            JOIN tags t ON t.id = p.tag_id
            WHERE p.platform = ? AND p.normalized_term = ?
            LIMIT 1
            """,
            (str(platform or '').strip().lower(), normalized),
        ).fetchone()

    def _upsert_platform_term_conn(
        self,
        conn: sqlite3.Connection,
        *,
        tag_id: int,
        platform: str,
        term: str,
        term_type: str = 'both',
        source: str = 'manual_review',
        confidence: float = 1.0,
        now: str | None = None,
    ) -> tuple[bool, str]:
        platform_text = str(platform or '').strip().lower()
        term_text = str(term or '').strip()
        normalized_term = normalize_tag_name(term_text)
        normalized_term_type = self._normalize_platform_term_type(term_type)
        if not platform_text:
            return False, 'platform 不能为空'
        if not term_text or not normalized_term:
            return False, 'platform term 不能为空'

        target = conn.execute(
            'SELECT id, name, normalized_name FROM tags WHERE id = ? LIMIT 1',
            (tag_id,),
        ).fetchone()
        if not target:
            return False, f'tag 不存在：{tag_id}'
        explicit_platform_opt_in = platform_text == 'xiaohongshu'
        if normalized_term == str(target['normalized_name'] or '') and not explicit_platform_opt_in:
            return False, f'{platform_text} term 与主 tag 相同，无需单独保存'
        alias_exists = conn.execute(
            'SELECT 1 FROM tag_aliases WHERE tag_id = ? AND normalized_alias = ? LIMIT 1',
            (tag_id, normalized_term),
        ).fetchone()
        if alias_exists and not explicit_platform_opt_in:
            return False, f'{platform_text} term 已被 alias 覆盖：{term_text}'

        existing = conn.execute(
            'SELECT id, tag_id, term_type FROM platform_tag_terms WHERE platform = ? AND normalized_term = ? LIMIT 1',
            (platform_text, normalized_term),
        ).fetchone()
        current_time = now or utcnow_str()
        if existing:
            if int(existing['tag_id']) != int(tag_id):
                conflict = conn.execute('SELECT name FROM tags WHERE id = ? LIMIT 1', (int(existing['tag_id']),)).fetchone()
                conflict_name = str(conflict['name']) if conflict else str(existing['tag_id'])
                return False, f'{platform_text} term 已映射到其他 tag：{conflict_name}'
            merged_type = self._platform_term_type_union(str(existing['term_type'] or ''), normalized_term_type)
            conn.execute(
                """
                UPDATE platform_tag_terms
                SET term = ?, term_type = ?, source = ?, confidence = ?, updated_at = ?
                WHERE id = ?
                """,
                (term_text, merged_type, str(source or 'manual_review'), float(confidence or 0.0), current_time, int(existing['id'])),
            )
            return True, f'已更新 {platform_text} term：{target["name"]} -> {term_text}'

        conn.execute(
            """
            INSERT INTO platform_tag_terms(
                tag_id, platform, term, normalized_term, term_type, source, confidence, created_at, updated_at
            )
            VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                int(tag_id),
                platform_text,
                term_text,
                normalized_term,
                normalized_term_type,
                str(source or 'manual_review'),
                float(confidence or 0.0),
                current_time,
                current_time,
            ),
        )
        return True, f'已添加 {platform_text} term：{target["name"]} -> {term_text}'

    def add_platform_term(
        self,
        tag_name: str,
        term: str,
        *,
        platform: str = 'pixiv',
        term_type: str = 'both',
        source: str = 'manual_review',
        confidence: float = 1.0,
    ) -> tuple[bool, str]:
        tag_row = self.get_tag_row(tag_name)
        if not tag_row:
            return False, f'tag 不存在：{tag_name}'
        with self._lock, self._connect() as conn:
            return self._upsert_platform_term_conn(
                conn,
                tag_id=int(tag_row['id']),
                platform=platform,
                term=term,
                term_type=term_type,
                source=source,
                confidence=confidence,
                now=utcnow_str(),
            )

    def resolve_platform_term(self, platform: str, query: str) -> MatchResult:
        normalized = normalize_tag_name(query)
        if not normalized:
            return MatchResult(matched=False)
        with self._lock, self._connect() as conn:
            row = self._resolve_platform_term_exact_conn(conn, platform, query)
        if not row:
            return MatchResult(matched=False)
        return MatchResult(
            matched=True,
            tag_id=int(row['tag_id']),
            tag_name=str(row['tag_name']),
            match_type=f'platform:{str(platform or '').strip().lower()}',
        )

    def list_platform_terms(
        self,
        *,
        tag_name: str = '',
        tag_id: int | None = None,
        platform: str = '',
        term_types: Iterable[str] | None = None,
        limit: int = 100,
        offset: int = 0,
        keyword: str = '',
    ) -> list[sqlite3.Row]:
        resolved_tag_id = tag_id
        if resolved_tag_id is None and tag_name:
            resolved_tag_id = self.get_tag_id(tag_name)
            if resolved_tag_id is None:
                return []
        sql = """
            SELECT p.id, p.tag_id, p.platform, p.term, p.normalized_term, p.term_type, p.source, p.confidence,
                   p.created_at, p.updated_at, t.name AS tag_name
            FROM platform_tag_terms p
            JOIN tags t ON t.id = p.tag_id
        """
        clauses: list[str] = []
        params: list[Any] = []
        if resolved_tag_id is not None:
            clauses.append('p.tag_id = ?')
            params.append(int(resolved_tag_id))
        if platform:
            clauses.append('p.platform = ?')
            params.append(str(platform).strip().lower())
        keyword_text = str(keyword or '').strip()
        normalized_keyword = normalize_tag_name(keyword_text)
        if keyword_text:
            keyword_clauses: list[str] = []
            keyword_params: list[Any] = []
            if normalized_keyword:
                keyword_clauses.extend(['p.normalized_term LIKE ?', 't.normalized_name LIKE ?'])
                keyword_params.extend([f'%{normalized_keyword}%', f'%{normalized_keyword}%'])
            keyword_clauses.extend(['p.term LIKE ?', 't.name LIKE ?'])
            keyword_params.extend([f'%{keyword_text}%', f'%{keyword_text}%'])
            clauses.append('(' + ' OR '.join(keyword_clauses) + ')')
            params.extend(keyword_params)
        normalized_term_types = {
            self._normalize_platform_term_type(item)
            for item in (term_types or [])
            if str(item or '').strip()
        }
        if clauses:
            sql += ' WHERE ' + ' AND '.join(clauses)
        sql += ' ORDER BY p.confidence DESC, p.term ASC'
        with self._lock, self._connect() as conn:
            rows = conn.execute(sql, params).fetchall()
        if normalized_term_types:
            rows = [row for row in rows if self._platform_term_type_matches(str(row['term_type'] or ''), normalized_term_types)]
        resolved_offset = max(0, int(offset or 0))
        resolved_limit = max(1, int(limit or 100))
        return rows[resolved_offset : resolved_offset + resolved_limit]

    def update_platform_term(
        self,
        term_id: int,
        *,
        tag_name: str = '',
        term: str = '',
        term_type: str = '',
        source: str = '',
        confidence: float | None = None,
    ) -> tuple[bool, str]:
        resolved_term_id = int(term_id or 0)
        if resolved_term_id <= 0:
            return False, 'term_id 无效'
        with self._lock, self._connect() as conn:
            row = conn.execute(
                """
                SELECT p.id, p.tag_id, p.platform, p.term, p.normalized_term, p.term_type, p.source, p.confidence,
                       t.name AS tag_name
                FROM platform_tag_terms p
                JOIN tags t ON t.id = p.tag_id
                WHERE p.id = ?
                LIMIT 1
                """,
                (resolved_term_id,),
            ).fetchone()
            if not row:
                return False, 'platform_term_not_found'

            if tag_name:
                target_row, _ = self._resolve_tag_exact_conn(conn, tag_name)
                if not target_row:
                    return False, f'tag 不存在：{tag_name}'
                if int(target_row['id']) != int(row['tag_id']):
                    return False, '暂不支持直接修改主 tag，请删除后重建'

            term_text = str(term or row['term'] or '').strip()
            normalized_term = normalize_tag_name(term_text)
            if not term_text or not normalized_term:
                return False, 'Pixiv 词不能为空'
            if normalized_term != str(row['normalized_term'] or ''):
                return False, '暂不支持直接修改词本身，请删除后重建'

            normalized_term_type = self._normalize_platform_term_type(term_type or str(row['term_type'] or 'both'))
            source_text = str(source or row['source'] or 'manual_review').strip() or 'manual_review'
            if confidence is None:
                confidence_value = float(row['confidence'] or 0.0)
            else:
                confidence_value = float(confidence or 0.0)
            current_time = utcnow_str()
            conn.execute(
                """
                UPDATE platform_tag_terms
                SET term = ?, term_type = ?, source = ?, confidence = ?, updated_at = ?
                WHERE id = ?
                """,
                (term_text, normalized_term_type, source_text, confidence_value, current_time, resolved_term_id),
            )
            return True, f'已更新 {str(row["platform"] or "")} term：{str(row["tag_name"])} -> {term_text}'

    def remove_platform_term(self, term_id: int) -> tuple[bool, str]:
        resolved_term_id = int(term_id or 0)
        if resolved_term_id <= 0:
            return False, 'term_id 无效'
        with self._lock, self._connect() as conn:
            row = conn.execute(
                """
                SELECT p.id, p.platform, p.term, t.name AS tag_name
                FROM platform_tag_terms p
                JOIN tags t ON t.id = p.tag_id
                WHERE p.id = ?
                LIMIT 1
                """,
                (resolved_term_id,),
            ).fetchone()
            if not row:
                return False, 'platform_term_not_found'
            conn.execute('DELETE FROM platform_tag_terms WHERE id = ?', (resolved_term_id,))
            return True, f'已删除 {str(row["platform"] or "")} term：{str(row["tag_name"])} -> {str(row["term"] or "")}'

    def get_platform_terms_for_tag(
        self,
        *,
        tag_name: str,
        platform: str = 'pixiv',
        purpose: str = 'query',
        include_aliases: bool = True,
        include_primary: bool = True,
    ) -> list[str]:
        row = self.get_tag_row(tag_name)
        if not row:
            return []
        expected_types = {'query'} if str(purpose or 'query').strip().lower() == 'query' else {'match'}
        resolved: list[str] = []
        seen: set[str] = set()

        def append_term(value: str) -> None:
            text = str(value or '').strip()
            normalized = normalize_tag_name(text)
            if not text or not normalized or normalized in seen:
                return
            seen.add(normalized)
            resolved.append(text)

        for term_row in self.list_platform_terms(
            tag_id=int(row['id']),
            platform=platform,
            term_types=expected_types,
            limit=100,
        ):
            append_term(str(term_row['term']))
        if include_aliases:
            for alias in self.list_aliases(str(row['name'])):
                append_term(alias)
        if include_primary:
            append_term(str(row['name']))
        return resolved

    def suggest_platform_terms_for_tag(
        self,
        *,
        tag_name: str,
        platform: str = 'pixiv',
        limit: int = 15,
    ) -> list[dict[str, Any]]:
        tag_row = self.get_tag_row(tag_name)
        if not tag_row:
            return []
        existing_terms = {
            normalize_tag_name(str(tag_row['name'])),
            *(normalize_tag_name(alias) for alias in self.list_aliases(str(tag_row['name']))),
            *(normalize_tag_name(str(row['term'])) for row in self.list_platform_terms(tag_id=int(tag_row['id']), platform=platform, limit=200)),
        }
        counter: Counter[str] = Counter()
        display_terms: dict[str, str] = {}
        with self._lock, self._connect() as conn:
            rows = conn.execute(
                """
                SELECT s.raw_tags, s.extra_json
                FROM sources s
                JOIN image_tags it ON it.image_id = s.image_id
                JOIN images i ON i.id = s.image_id
                WHERE s.platform = ?
                  AND it.tag_id = ?
                  AND it.review_status IN ('approved', 'manual_approved')
                  AND i.is_active = 1
                """,
                (str(platform or '').strip().lower(), int(tag_row['id'])),
            ).fetchall()
        for row in rows:
            raw_terms = json.loads(row['raw_tags'] or '[]') if row['raw_tags'] else []
            extra = json.loads(row['extra_json'] or '{}') if row['extra_json'] else {}
            translated_terms = extra.get('translated_tags') if isinstance(extra, dict) else []
            if not isinstance(translated_terms, list):
                translated_terms = []
            for value in [*raw_terms, *translated_terms]:
                text = str(value or '').strip()
                normalized = normalize_tag_name(text)
                if (
                    not text
                    or not normalized
                    or normalized in existing_terms
                    or not self._looks_like_platform_term(text)
                ):
                    continue
                counter[normalized] += 1
                display_terms.setdefault(normalized, text)
        suggestions = sorted(counter.items(), key=lambda item: (-item[1], display_terms.get(item[0], item[0])))
        return [
            {
                'term': display_terms.get(normalized, normalized),
                'count': int(count),
                'normalized_term': normalized,
            }
            for normalized, count in suggestions[: max(1, int(limit or 1))]
        ]

    def list_unresolved_platform_terms(
        self,
        *,
        platform: str = 'pixiv',
        keyword: str = '',
        limit: int = 50,
    ) -> list[dict[str, Any]]:
        platform_text = str(platform or 'pixiv').strip().lower() or 'pixiv'
        keyword_text = str(keyword or '').strip()
        normalized_keyword = normalize_tag_name(keyword_text)
        with self._lock, self._connect() as conn:
            existing_terms = {
                str(row['normalized_term'] or '')
                for row in conn.execute(
                    'SELECT normalized_term FROM platform_tag_terms WHERE platform = ?',
                    (platform_text,),
                ).fetchall()
                if str(row['normalized_term'] or '').strip()
            }
            existing_terms.update(
                str(row['normalized_name'] or '')
                for row in conn.execute('SELECT normalized_name FROM tags').fetchall()
                if str(row['normalized_name'] or '').strip()
            )
            existing_terms.update(
                str(row['normalized_alias'] or '')
                for row in conn.execute('SELECT normalized_alias FROM tag_aliases').fetchall()
                if str(row['normalized_alias'] or '').strip()
            )
            rows = conn.execute(
                """
                SELECT s.post_url, s.author, s.raw_tags, s.extra_json
                FROM sources s
                JOIN images i ON i.id = s.image_id
                WHERE s.platform = ?
                  AND i.is_active = 1
                ORDER BY s.id DESC
                """,
                (platform_text,),
            ).fetchall()

        counter: Counter[str] = Counter()
        display_terms: dict[str, str] = {}
        sample_post_urls: dict[str, list[str]] = {}
        sample_authors: dict[str, list[str]] = {}

        for row in rows:
            try:
                raw_terms = json.loads(row['raw_tags'] or '[]') if row['raw_tags'] else []
            except Exception:
                raw_terms = []
            try:
                extra = json.loads(row['extra_json'] or '{}') if row['extra_json'] else {}
            except Exception:
                extra = {}
            translated_terms = extra.get('translated_tags') if isinstance(extra, dict) else []
            if not isinstance(translated_terms, list):
                translated_terms = []

            row_seen: set[str] = set()
            for value in [*raw_terms, *translated_terms]:
                text = str(value or '').strip()
                normalized = normalize_tag_name(text)
                if (
                    not text
                    or not normalized
                    or normalized in row_seen
                    or normalized in existing_terms
                    or not self._looks_like_platform_term(text)
                ):
                    continue
                if keyword_text and keyword_text.lower() not in text.lower():
                    if not normalized_keyword or normalized_keyword not in normalized:
                        continue
                row_seen.add(normalized)
                counter[normalized] += 1
                display_terms.setdefault(normalized, text)

                post_url = str(row['post_url'] or '').strip()
                if post_url:
                    bucket = sample_post_urls.setdefault(normalized, [])
                    if post_url not in bucket and len(bucket) < 3:
                        bucket.append(post_url)
                author = str(row['author'] or '').strip()
                if author:
                    bucket = sample_authors.setdefault(normalized, [])
                    if author not in bucket and len(bucket) < 3:
                        bucket.append(author)

        suggestions = sorted(counter.items(), key=lambda item: (-item[1], display_terms.get(item[0], item[0])))
        return [
            {
                'term': display_terms.get(normalized, normalized),
                'normalized_term': normalized,
                'count': int(count),
                'sample_post_urls': sample_post_urls.get(normalized, []),
                'sample_authors': sample_authors.get(normalized, []),
            }
            for normalized, count in suggestions[: max(1, int(limit or 1))]
        ]

    @staticmethod
    def _dedupe_terms(values: Iterable[str]) -> list[str]:
        seen: set[str] = set()
        result: list[str] = []
        for raw in values:
            text = str(raw or '').strip()
            normalized = normalize_tag_name(text)
            if not text or not normalized or normalized in seen:
                continue
            seen.add(normalized)
            result.append(text)
        return result

    def _collect_platform_term_usage_conn(
        self,
        conn: sqlite3.Connection,
        *,
        platform: str,
        terms: Iterable[str],
    ) -> dict[str, dict[str, Any]]:
        normalized_terms = {
            normalize_tag_name(item)
            for item in terms
            if normalize_tag_name(item)
        }
        if not normalized_terms:
            return {}
        rows = conn.execute(
            """
            SELECT s.post_url, s.author, s.raw_tags, s.extra_json
            FROM sources s
            JOIN images i ON i.id = s.image_id
            WHERE s.platform = ?
              AND i.is_active = 1
            ORDER BY s.id DESC
            """,
            (str(platform or 'pixiv').strip().lower() or 'pixiv',),
        ).fetchall()
        usage: dict[str, dict[str, Any]] = {
            normalized: {
                'count': 0,
                'sample_post_urls': [],
                'sample_authors': [],
            }
            for normalized in normalized_terms
        }
        for row in rows:
            try:
                raw_terms = json.loads(row['raw_tags'] or '[]') if row['raw_tags'] else []
            except Exception:
                raw_terms = []
            try:
                extra = json.loads(row['extra_json'] or '{}') if row['extra_json'] else {}
            except Exception:
                extra = {}
            translated_terms = extra.get('translated_tags') if isinstance(extra, dict) else []
            if not isinstance(translated_terms, list):
                translated_terms = []
            row_seen: set[str] = set()
            for value in [*raw_terms, *translated_terms]:
                normalized = normalize_tag_name(str(value or '').strip())
                if not normalized or normalized in row_seen or normalized not in normalized_terms:
                    continue
                row_seen.add(normalized)
                payload = usage[normalized]
                payload['count'] = int(payload.get('count', 0) or 0) + 1
                post_url = str(row['post_url'] or '').strip()
                if post_url and post_url not in payload['sample_post_urls'] and len(payload['sample_post_urls']) < 3:
                    payload['sample_post_urls'].append(post_url)
                author = str(row['author'] or '').strip()
                if author and author not in payload['sample_authors'] and len(payload['sample_authors']) < 3:
                    payload['sample_authors'].append(author)
        return usage

    def _build_review_alias_term_plan_conn(
        self,
        conn: sqlite3.Connection,
        *,
        platform: str,
        target_row: sqlite3.Row,
        term: str,
    ) -> dict[str, Any]:
        platform_text = str(platform or 'pixiv').strip().lower() or 'pixiv'
        term_text = str(term or '').strip()
        target_id = int(target_row['id'])
        target_name = str(target_row['name'])
        target_normalized = str(target_row['normalized_name'] or normalize_tag_name(target_name))
        normalized_term = normalize_tag_name(term_text)
        if not term_text or not normalized_term:
            return {'status': 'skipped', 'term': term_text, 'message': '空词'}
        if not self._looks_like_platform_term(term_text):
            return {'status': 'skipped', 'term': term_text, 'message': '看起来不是适合沉淀的平台角色词'}
        if normalized_term == target_normalized:
            return {'status': 'mapped', 'term': term_text, 'tag_name': target_name, 'action': 'already'}

        source_row, source_match_type = self._resolve_tag_exact_conn(conn, term_text)
        if source_row and int(source_row['id']) != target_id and source_match_type == 'exact_alias':
            return {
                'status': 'skipped',
                'term': term_text,
                'message': f'alias 已被 {str(source_row["name"])} 使用',
            }

        platform_match = self._resolve_platform_term_exact_conn(conn, platform_text, term_text)
        if platform_match and int(platform_match['tag_id']) != target_id:
            conflict = conn.execute('SELECT name FROM tags WHERE id = ? LIMIT 1', (int(platform_match['tag_id']),)).fetchone()
            conflict_name = str(conflict['name']) if conflict else str(platform_match['tag_id'])
            return {
                'status': 'skipped',
                'term': term_text,
                'message': f'{platform_text} term 已映射到其他 tag：{conflict_name}',
            }

        source_tag_id = int(source_row['id']) if source_row else 0
        action = 'add'
        if source_row and source_tag_id == target_id:
            action = 'already'
        elif source_row and source_match_type == 'exact_tag':
            action = 'merge_tag'
        elif platform_match:
            action = 'already'
        return {
            'status': 'mapped',
            'term': term_text,
            'tag_name': target_name,
            'action': action,
            'source_tag_id': source_tag_id,
            'source_match_type': source_match_type,
            'platform_already': bool(platform_match),
        }

    def _apply_review_alias_term_conn(
        self,
        conn: sqlite3.Connection,
        *,
        platform: str,
        target_row: sqlite3.Row,
        term: str,
        now: str,
    ) -> dict[str, Any]:
        plan = self._build_review_alias_term_plan_conn(conn, platform=platform, target_row=target_row, term=term)
        if plan.get('status') != 'mapped':
            return plan

        platform_text = str(platform or 'pixiv').strip().lower() or 'pixiv'
        target_id = int(target_row['id'])
        target_name = str(target_row['name'])
        action = str(plan.get('action') or '')

        if action == 'merge_tag':
            if not bool(plan.get('platform_already')):
                ok, message = self._upsert_platform_term_conn(
                    conn,
                    tag_id=target_id,
                    platform=platform_text,
                    term=str(plan['term']),
                    term_type='both',
                    source='manual_review',
                    confidence=1.0,
                    now=now,
                )
                if not ok and '已被 alias 覆盖' not in message and '与主 tag 相同' not in message:
                    return {'status': 'skipped', 'term': str(plan['term']), 'message': message}
            merge_result = self._merge_tag_into_conn(
                conn,
                target_id=target_id,
                target_name=target_name,
                source_id=int(plan.get('source_tag_id') or 0),
                now=now,
            )
            plan['action'] = 'merge_tag'
            plan['merged_tag'] = str(merge_result.get('source_name') or '')
            plan['merge_result'] = merge_result
            return plan

        if action == 'add':
            if not bool(plan.get('platform_already')):
                ok, message = self._upsert_platform_term_conn(
                    conn,
                    tag_id=target_id,
                    platform=platform_text,
                    term=str(plan['term']),
                    term_type='both',
                    source='manual_review',
                    confidence=1.0,
                    now=now,
                )
                if ok:
                    plan['platform_term_added'] = True
                elif '已被 alias 覆盖' not in message and '与主 tag 相同' not in message:
                    return {'status': 'skipped', 'term': str(plan['term']), 'message': message}
            ok, message = self._insert_alias_conn(conn, tag_id=target_id, alias=str(plan['term']), now=now)
            if ok:
                plan['action'] = 'alias_added'
                plan['alias_added'] = True
            elif '已存在' in message or '不能与主 tag 相同' in message:
                plan['action'] = 'already'
            else:
                return {'status': 'skipped', 'term': str(plan['term']), 'message': message}
        return plan

    def _preview_review_term_mappings_conn(
        self,
        conn: sqlite3.Connection,
        *,
        platform: str,
        requested_terms: Iterable[str],
        target_row: sqlite3.Row,
    ) -> tuple[list[dict[str, Any]], list[str]]:
        mapped_terms: list[dict[str, Any]] = []
        skipped_terms: list[str] = []
        for term in requested_terms:
            plan = self._build_review_alias_term_plan_conn(conn, platform=platform, target_row=target_row, term=term)
            if plan.get('status') != 'mapped':
                skipped_terms.append(f'{term}（{str(plan.get("message") or "无法沉淀")}）')
                continue
            mapped_terms.append(
                {
                    'term': str(plan['term']),
                    'tag_name': str(plan['tag_name']),
                    'action': str(plan.get('action') or 'add'),
                    'source_tag_id': int(plan.get('source_tag_id') or 0),
                }
            )
        return mapped_terms, skipped_terms

    def preview_batch_image_review(
        self,
        items: Iterable[dict[str, Any]],
        *,
        platform: str = 'pixiv',
        reject_unselected: bool = True,
    ) -> tuple[bool, dict[str, Any]]:
        platform_text = str(platform or 'pixiv').strip().lower() or 'pixiv'
        normalized_items: list[dict[str, Any]] = []
        seen_image_ids: set[int] = set()
        for raw in items:
            if not isinstance(raw, dict):
                continue
            image_id = int(raw.get('image_id', 0) or 0)
            if image_id <= 0 or image_id in seen_image_ids:
                continue
            seen_image_ids.add(image_id)
            normalized_items.append(
                {
                    'image_id': image_id,
                    'selected_tag_names': self._dedupe_terms(raw.get('selected_tag_names', []) or []),
                    'source_terms': self._dedupe_terms(raw.get('source_terms', []) or []),
                }
            )
        if not normalized_items:
            return False, {'message': '请至少选择一张图片。'}

        preview_items: list[dict[str, Any]] = []
        totals = {
            'images': 0,
            'approved_tag_links': 0,
            'rejected_tag_links': 0,
            'mapped_terms': 0,
            'skipped_terms': 0,
            'errors': 0,
        }
        with self._lock, self._connect() as conn:
            for item in normalized_items:
                image_id = int(item['image_id'])
                requested_tags = list(item['selected_tag_names'])
                requested_terms = list(item['source_terms'])
                if not requested_tags:
                    preview_items.append({'image_id': image_id, 'status': 'error', 'message': '请至少选择一个归入主 tag。'})
                    totals['errors'] += 1
                    continue
                image = conn.execute('SELECT id FROM images WHERE id = ? AND is_active = 1 LIMIT 1', (image_id,)).fetchone()
                if not image:
                    preview_items.append({'image_id': image_id, 'status': 'error', 'message': f'图片不存在：{image_id}'})
                    totals['errors'] += 1
                    continue
                source_exists = conn.execute(
                    'SELECT 1 FROM sources WHERE image_id = ? AND platform = ? LIMIT 1',
                    (image_id, platform_text),
                ).fetchone()
                if not source_exists:
                    preview_items.append({'image_id': image_id, 'status': 'error', 'message': f'图片 #{image_id} 没有 {platform_text} 来源记录。'})
                    totals['errors'] += 1
                    continue
                canonical_tag_name = requested_tags[0]
                alias_terms = self._dedupe_terms([*requested_tags[1:], *requested_terms])
                target_row, _ = self._resolve_tag_exact_conn(conn, canonical_tag_name)
                if not target_row:
                    preview_items.append({'image_id': image_id, 'status': 'error', 'message': f'归入主 tag 不存在：{canonical_tag_name}'})
                    totals['errors'] += 1
                    continue
                selected_ids = {int(target_row['id'])}
                selected_names = [str(target_row['name'])]
                mapped_terms, skipped_terms = self._preview_review_term_mappings_conn(
                    conn,
                    platform=platform_text,
                    requested_terms=alias_terms,
                    target_row=target_row,
                )
                merged_source_ids = {
                    int(term.get('source_tag_id') or 0)
                    for term in mapped_terms
                    if str(term.get('action') or '') == 'merge_tag' and int(term.get('source_tag_id') or 0) > 0
                }
                tasks = conn.execute(
                    """
                    SELECT rt.tag_id, t.name AS tag_name
                    FROM review_tasks rt
                    JOIN tags t ON t.id = rt.tag_id
                    WHERE rt.image_id = ?
                    ORDER BY rt.id DESC
                    """,
                    (image_id,),
                ).fetchall()
                rejected_names = [
                    str(row['tag_name'])
                    for row in tasks
                    if reject_unselected and int(row['tag_id']) not in selected_ids and int(row['tag_id']) not in merged_source_ids
                ]
                preview_items.append(
                    {
                        'image_id': image_id,
                        'status': 'ok',
                        'approved_tags': selected_names,
                        'rejected_tags': rejected_names,
                        'mapped_terms': mapped_terms,
                        'skipped_terms': skipped_terms,
                        'selected_source_terms': alias_terms,
                    }
                )
                totals['images'] += 1
                totals['approved_tag_links'] += len(selected_names)
                totals['rejected_tag_links'] += len(rejected_names)
                totals['mapped_terms'] += len(mapped_terms)
                totals['skipped_terms'] += len(skipped_terms)
        return True, {
            'message': f'已生成 {totals["images"]} 张图片的批量审核预览',
            'items': preview_items,
            'totals': totals,
        }

    def apply_batch_image_review(
        self,
        items: Iterable[dict[str, Any]],
        *,
        platform: str = 'pixiv',
        reject_unselected: bool = True,
    ) -> tuple[bool, dict[str, Any]]:
        normalized_items: list[dict[str, Any]] = []
        seen_image_ids: set[int] = set()
        for raw in items:
            if not isinstance(raw, dict):
                continue
            image_id = int(raw.get('image_id', 0) or 0)
            if image_id <= 0 or image_id in seen_image_ids:
                continue
            seen_image_ids.add(image_id)
            normalized_items.append(
                {
                    'image_id': image_id,
                    'selected_tag_names': self._dedupe_terms(raw.get('selected_tag_names', []) or []),
                    'source_terms': self._dedupe_terms(raw.get('source_terms', []) or []),
                    'reason': str(raw.get('reason', '') or '').strip(),
                }
            )
        if not normalized_items:
            return False, {'message': '请至少选择一张图片。'}

        results: list[dict[str, Any]] = []
        success_count = 0
        failure_count = 0
        mapped_terms_count = 0
        for item in normalized_items:
            ok, result = self.apply_image_review(
                int(item['image_id']),
                selected_tag_names=item['selected_tag_names'],
                source_terms=item['source_terms'],
                platform=platform,
                reason=item['reason'],
                reject_unselected=reject_unselected,
            )
            payload = {'ok': ok, 'image_id': int(item['image_id'])}
            if isinstance(result, dict):
                payload.update(result)
            else:
                payload['message'] = str(result)
            results.append(payload)
            if ok:
                success_count += 1
                mapped_terms_count += len(payload.get('mapped_terms', []) or [])
            else:
                failure_count += 1
        return success_count > 0, {
            'message': f'批量审核完成：成功 {success_count} 张，失败 {failure_count} 张',
            'items': results,
            'success_count': success_count,
            'failure_count': failure_count,
            'mapped_terms_count': mapped_terms_count,
        }

    def preview_batch_platform_terms(
        self,
        *,
        tag_name: str,
        terms: Iterable[str],
        platform: str = 'pixiv',
        term_type: str = 'both',
    ) -> tuple[bool, dict[str, Any]]:
        requested_terms = self._dedupe_terms(terms)
        if not requested_terms:
            return False, {'message': '请至少提供一个 Pixiv 词。'}
        platform_text = str(platform or 'pixiv').strip().lower() or 'pixiv'
        normalized_term_type = self._normalize_platform_term_type(term_type)
        target_row = self.get_tag_row(tag_name)
        if not target_row:
            return False, {'message': f'tag 不存在：{tag_name}'}

        with self._lock, self._connect() as conn:
            usage = self._collect_platform_term_usage_conn(conn, platform=platform_text, terms=requested_terms)
            items: list[dict[str, Any]] = []
            summary = {'add': 0, 'update': 0, 'already': 0, 'conflict': 0, 'invalid': 0}
            for term in requested_terms:
                normalized = normalize_tag_name(term)
                payload = usage.get(normalized, {'count': 0, 'sample_post_urls': [], 'sample_authors': []})
                item = {
                    'term': term,
                    'count': int(payload.get('count', 0) or 0),
                    'sample_post_urls': list(payload.get('sample_post_urls', []) or []),
                    'sample_authors': list(payload.get('sample_authors', []) or []),
                    'target_tag': str(target_row['name']),
                    'status': '',
                    'message': '',
                }
                if not self._looks_like_platform_term(term):
                    item['status'] = 'invalid'
                    item['message'] = '看起来不是适合沉淀的平台角色词'
                    summary['invalid'] += 1
                    items.append(item)
                    continue
                existing = conn.execute(
                    """
                    SELECT p.id, p.tag_id, p.term_type, p.source, p.confidence, t.name AS tag_name
                    FROM platform_tag_terms p
                    JOIN tags t ON t.id = p.tag_id
                    WHERE p.platform = ? AND p.normalized_term = ?
                    LIMIT 1
                    """,
                    (platform_text, normalized),
                ).fetchone()
                if existing and int(existing['tag_id']) != int(target_row['id']):
                    item['status'] = 'conflict'
                    item['message'] = f'已映射到其他 tag：{str(existing["tag_name"])}'
                    summary['conflict'] += 1
                elif existing:
                    merged_type = self._platform_term_type_union(str(existing['term_type'] or ''), normalized_term_type)
                    if merged_type != str(existing['term_type'] or 'both'):
                        item['status'] = 'update'
                        item['message'] = f'将扩展 term_type：{str(existing["term_type"])} -> {merged_type}'
                        summary['update'] += 1
                    else:
                        item['status'] = 'already'
                        item['message'] = '当前目标 tag 已存在该平台词'
                        summary['already'] += 1
                else:
                    item['status'] = 'add'
                    item['message'] = '将新增到目标 tag'
                    summary['add'] += 1
                items.append(item)

        return True, {
            'message': f'已生成 {len(items)} 个 Pixiv 词的批量映射预览',
            'target_tag': str(target_row['name']),
            'items': items,
            'summary': summary,
        }

    def apply_batch_platform_terms(
        self,
        *,
        tag_name: str,
        terms: Iterable[str],
        platform: str = 'pixiv',
        term_type: str = 'both',
        source: str = 'manual_review',
        confidence: float = 1.0,
    ) -> tuple[bool, dict[str, Any]]:
        requested_terms = self._dedupe_terms(terms)
        if not requested_terms:
            return False, {'message': '请至少提供一个 Pixiv 词。'}
        target_row = self.get_tag_row(tag_name)
        if not target_row:
            return False, {'message': f'tag 不存在：{tag_name}'}
        normalized_term_type = self._normalize_platform_term_type(term_type)
        results: list[dict[str, Any]] = []
        summary = {'added': 0, 'updated': 0, 'failed': 0}
        with self._lock, self._connect() as conn:
            for term in requested_terms:
                if not self._looks_like_platform_term(term):
                    results.append({'term': term, 'ok': False, 'message': '看起来不是适合沉淀的平台角色词'})
                    summary['failed'] += 1
                    continue
                existing = conn.execute(
                    """
                    SELECT id, tag_id, term_type
                    FROM platform_tag_terms
                    WHERE platform = ? AND normalized_term = ?
                    LIMIT 1
                    """,
                    (str(platform or 'pixiv').strip().lower() or 'pixiv', normalize_tag_name(term)),
                ).fetchone()
                ok, message = self._upsert_platform_term_conn(
                    conn,
                    tag_id=int(target_row['id']),
                    platform=platform,
                    term=term,
                    term_type=normalized_term_type,
                    source=source,
                    confidence=confidence,
                    now=utcnow_str(),
                )
                results.append({'term': term, 'ok': ok, 'message': message})
                if ok:
                    if existing and int(existing['tag_id']) == int(target_row['id']):
                        summary['updated'] += 1
                    else:
                        summary['added'] += 1
                else:
                    summary['failed'] += 1
        return summary['added'] + summary['updated'] > 0, {
            'message': f'批量平台词提交完成：新增 {summary["added"]}，更新 {summary["updated"]}，失败 {summary["failed"]}',
            'target_tag': str(target_row['name']),
            'items': results,
            'summary': summary,
        }

    def list_tag_merge_candidates(
        self,
        *,
        keyword: str = '',
        limit: int = 50,
    ) -> list[dict[str, Any]]:
        keyword_text = str(keyword or '').strip()
        with self._lock, self._connect() as conn:
            tag_rows = conn.execute(
                """
                SELECT t.id, t.name, t.normalized_name, t.is_character, t.tag_type, t.status,
                       COUNT(DISTINCT CASE WHEN it.review_status IN ('approved', 'manual_approved') AND i.is_active = 1 THEN i.id END) AS image_count
                FROM tags t
                LEFT JOIN image_tags it ON it.tag_id = t.id
                LEFT JOIN images i ON i.id = it.image_id
                WHERE t.tag_type = 'character' AND t.status = 'active'
                GROUP BY t.id, t.name, t.normalized_name, t.is_character, t.tag_type, t.status
                ORDER BY image_count DESC, t.name ASC
                LIMIT 240
                """
            ).fetchall()
            candidates: dict[tuple[int, int], dict[str, Any]] = {}
            for target_row in tag_rows:
                target_id = int(target_row['id'])
                target_name = str(target_row['name'])
                terms: list[tuple[str, str, int]] = []
                for platform_row in self.list_platform_terms(tag_id=target_id, platform='pixiv', limit=80):
                    terms.append((str(platform_row['term'] or ''), 'platform_term', 8))
                seen_term_keys: set[str] = set()
                for term, reason, weight in terms:
                    normalized_term = normalize_tag_name(term)
                    if not normalized_term or normalized_term in seen_term_keys:
                        continue
                    seen_term_keys.add(normalized_term)
                    matched_row, match_type = self._resolve_tag_exact_conn(conn, term)
                    if not matched_row or int(matched_row['id']) == target_id:
                        continue
                    source_id = int(matched_row['id'])
                    source_name = str(matched_row['name'])
                    key = (source_id, target_id)
                    entry = candidates.setdefault(
                        key,
                        {
                            'source_tag': source_name,
                            'target_tag': target_name,
                            'source_tag_id': source_id,
                            'target_tag_id': target_id,
                            'source_is_character': bool(matched_row['is_character']),
                            'target_is_character': bool(target_row['is_character']),
                            'source_image_count': int(conn.execute(
                                """
                                SELECT COUNT(DISTINCT i.id) AS c
                                FROM image_tags it
                                JOIN images i ON i.id = it.image_id
                                WHERE it.tag_id = ? AND i.is_active = 1 AND it.review_status IN ('approved', 'manual_approved')
                                """,
                                (source_id,),
                            ).fetchone()['c']),
                            'target_image_count': int(target_row['image_count'] or 0),
                            'score': 0,
                            'reasons': [],
                            'example_terms': [],
                        },
                    )
                    entry['score'] += int(weight)
                    reason_text = f'{reason}:{match_type or "exact"}'
                    if reason_text not in entry['reasons']:
                        entry['reasons'].append(reason_text)
                    if term not in entry['example_terms']:
                        entry['example_terms'].append(term)
            items = list(candidates.values())
        if keyword_text:
            keyword_normalized = normalize_tag_name(keyword_text)
            items = [
                item
                for item in items
                if keyword_text in str(item['source_tag'])
                or keyword_text in str(item['target_tag'])
                or any(keyword_text in str(term) for term in item.get('example_terms', []))
                or keyword_normalized in normalize_tag_name(str(item['source_tag']))
                or keyword_normalized in normalize_tag_name(str(item['target_tag']))
            ]
        items.sort(
            key=lambda item: (
                -int(item.get('score', 0) or 0),
                -int(item.get('target_is_character', False)),
                -int(item.get('target_image_count', 0) or 0),
                int(item.get('source_image_count', 0) or 0),
                str(item.get('source_tag', '')),
            )
        )
        return items[: max(1, int(limit or 1))]

    def list_tag_identity_scan_inputs(
        self,
        *,
        platform: str = 'pixiv',
        limit: int = 260,
    ) -> list[dict[str, Any]]:
        platform_text = str(platform or 'pixiv').strip().lower() or 'pixiv'
        resolved_limit = max(1, int(limit or 1))
        with self._lock, self._connect() as conn:
            tag_rows = conn.execute(
                """
                SELECT t.id, t.name, t.normalized_name, t.is_character, t.tag_type, t.status,
                       COUNT(DISTINCT CASE WHEN it.review_status IN ('approved', 'manual_approved') AND i.is_active = 1 THEN i.id END) AS image_count
                FROM tags t
                LEFT JOIN image_tags it ON it.tag_id = t.id
                LEFT JOIN images i ON i.id = it.image_id
                WHERE t.tag_type = 'character' AND t.status = 'active'
                GROUP BY t.id, t.name, t.normalized_name, t.is_character, t.tag_type, t.status
                ORDER BY image_count DESC, t.name ASC
                LIMIT ?
                """,
                (resolved_limit,),
            ).fetchall()
            result: list[dict[str, Any]] = []
            for tag_row in tag_rows:
                tag_id = int(tag_row['id'])
                alias_rows = conn.execute(
                    'SELECT alias, normalized_alias FROM tag_aliases WHERE tag_id = ? ORDER BY alias ASC',
                    (tag_id,),
                ).fetchall()
                platform_rows = conn.execute(
                    """
                    SELECT term, normalized_term, term_type, source, confidence
                    FROM platform_tag_terms
                    WHERE tag_id = ? AND platform = ?
                    ORDER BY confidence DESC, term ASC
                    LIMIT 120
                    """,
                    (tag_id, platform_text),
                ).fetchall()
                result.append(
                    {
                        'id': tag_id,
                        'name': str(tag_row['name']),
                        'normalized_name': str(tag_row['normalized_name'] or ''),
                        'is_character': bool(tag_row['is_character']),
                        'tag_type': str(tag_row['tag_type'] or 'character'),
                        'status': str(tag_row['status'] or 'active'),
                        'image_count': int(tag_row['image_count'] or 0),
                        'aliases': [str(row['alias']) for row in alias_rows],
                        'platform_terms': [
                            {
                                'term': str(row['term'] or ''),
                                'normalized_term': str(row['normalized_term'] or ''),
                                'term_type': str(row['term_type'] or 'both'),
                                'source': str(row['source'] or ''),
                                'confidence': float(row['confidence'] or 0.0),
                            }
                            for row in platform_rows
                        ],
                        # 来源图片的原始 Pixiv tag 仅作为图片元数据，不能作为角色身份依据。
                        'history_terms': [],
                    }
                )
        return result

    @staticmethod
    def _parse_candidate_json(value: str, fallback: Any) -> Any:
        try:
            parsed = json.loads(value or '')
        except Exception:
            return fallback
        return parsed if parsed is not None else fallback

    def upsert_tag_identity_candidate(
        self,
        *,
        source_tag_id: int,
        target_tag_id: int,
        score: float,
        reasons: list[str],
        evidence: dict[str, Any],
        llm_result: dict[str, Any] | None = None,
        status: str = 'pending',
    ) -> dict[str, Any]:
        source_id = int(source_tag_id or 0)
        target_id = int(target_tag_id or 0)
        if source_id <= 0 or target_id <= 0 or source_id == target_id:
            raise ValueError('source_tag_id / target_tag_id 无效')
        normalized_status = str(status or 'pending').strip().lower() or 'pending'
        if normalized_status not in {'pending', 'ignored'}:
            normalized_status = 'pending'
        now = utcnow_str()
        with self._lock, self._connect() as conn:
            conn.execute(
                """
                INSERT INTO tag_merge_identity_candidates(
                    source_tag_id, target_tag_id, status, score, reasons_json,
                    evidence_json, llm_result_json, created_at, updated_at
                )
                VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(source_tag_id, target_tag_id) DO UPDATE SET
                    status = CASE
                        WHEN tag_merge_identity_candidates.status = 'ignored' AND excluded.status = 'pending'
                        THEN tag_merge_identity_candidates.status
                        ELSE excluded.status
                    END,
                    score = excluded.score,
                    reasons_json = excluded.reasons_json,
                    evidence_json = excluded.evidence_json,
                    llm_result_json = excluded.llm_result_json,
                    updated_at = excluded.updated_at
                """,
                (
                    source_id,
                    target_id,
                    normalized_status,
                    float(score or 0),
                    json.dumps(reasons or [], ensure_ascii=False),
                    json.dumps(evidence or {}, ensure_ascii=False),
                    json.dumps(llm_result or {}, ensure_ascii=False),
                    now,
                    now,
                ),
            )
            row = conn.execute(
                """
                SELECT c.*, s.name AS source_tag, t.name AS target_tag
                FROM tag_merge_identity_candidates c
                JOIN tags s ON s.id = c.source_tag_id
                JOIN tags t ON t.id = c.target_tag_id
                WHERE c.source_tag_id = ? AND c.target_tag_id = ?
                LIMIT 1
                """,
                (source_id, target_id),
            ).fetchone()
        return self._build_tag_identity_candidate_payload(row) if row else {}

    def list_tag_identity_candidates(
        self,
        *,
        status: str = 'pending',
        keyword: str = '',
        limit: int = 80,
    ) -> list[dict[str, Any]]:
        status_text = str(status or 'pending').strip().lower()
        keyword_text = str(keyword or '').strip()
        normalized_keyword = normalize_tag_name(keyword_text)
        sql = """
            SELECT c.*, s.name AS source_tag, t.name AS target_tag,
                   s.is_character AS source_is_character, t.is_character AS target_is_character
            FROM tag_merge_identity_candidates c
            JOIN tags s ON s.id = c.source_tag_id
            JOIN tags t ON t.id = c.target_tag_id
        """
        clauses: list[str] = []
        params: list[Any] = []
        if status_text:
            clauses.append('c.status = ?')
            params.append(status_text)
        if keyword_text:
            like_text = f'%{keyword_text}%'
            like_normalized = f'%{normalized_keyword}%'
            clauses.append(
                """
                (
                    s.name LIKE ?
                    OR t.name LIKE ?
                    OR s.normalized_name LIKE ?
                    OR t.normalized_name LIKE ?
                    OR c.reasons_json LIKE ?
                    OR c.evidence_json LIKE ?
                )
                """
            )
            params.extend([like_text, like_text, like_normalized, like_normalized, like_text, like_text])
        if clauses:
            sql += ' WHERE ' + ' AND '.join(clauses)
        sql += ' ORDER BY c.score DESC, c.updated_at DESC, c.id DESC LIMIT ?'
        params.append(max(1, int(limit or 1)))
        with self._lock, self._connect() as conn:
            rows = conn.execute(sql, params).fetchall()
        return [self._build_tag_identity_candidate_payload(row) for row in rows]

    def ignore_tag_identity_candidate(self, candidate_id: int) -> tuple[bool, str]:
        resolved_id = int(candidate_id or 0)
        if resolved_id <= 0:
            return False, 'candidate_id 无效'
        with self._lock, self._connect() as conn:
            row = conn.execute(
                'SELECT id FROM tag_merge_identity_candidates WHERE id = ? LIMIT 1',
                (resolved_id,),
            ).fetchone()
            if not row:
                return False, '候选不存在'
            conn.execute(
                "UPDATE tag_merge_identity_candidates SET status = 'ignored', updated_at = ? WHERE id = ?",
                (utcnow_str(), resolved_id),
            )
        return True, '已忽略该归并候选'

    def mark_stale_tag_identity_candidates(self, active_pairs: set[tuple[int, int]]) -> int:
        normalized_pairs = {(int(source), int(target)) for source, target in active_pairs if int(source) > 0 and int(target) > 0}
        now = utcnow_str()
        changed = 0
        with self._lock, self._connect() as conn:
            pending_rows = conn.execute(
                """
                SELECT id, source_tag_id, target_tag_id
                FROM tag_merge_identity_candidates
                WHERE status = 'pending'
                """
            ).fetchall()
            for row in pending_rows:
                pair = (int(row['source_tag_id']), int(row['target_tag_id']))
                if pair in normalized_pairs:
                    continue
                conn.execute(
                    "UPDATE tag_merge_identity_candidates SET status = 'stale', updated_at = ? WHERE id = ?",
                    (now, int(row['id'])),
                )
                changed += 1
        return changed

    def get_pixiv_query_terms_for_tag(self, tag_name: str) -> list[str]:
        row = self.get_tag_row(tag_name)
        if not row:
            return []
        tag_id = int(row['id'])
        terms: list[str] = []
        seen: set[str] = set()

        def append_term(value: str) -> None:
            text = str(value or '').strip()
            normalized = normalize_tag_name(text)
            if not text or not normalized or normalized in seen:
                return
            seen.add(normalized)
            terms.append(text)

        with self._lock, self._connect() as conn:
            rows = conn.execute(
                """
                SELECT term, term_type, confidence
                FROM platform_tag_terms
                WHERE tag_id = ? AND platform = ?
                ORDER BY
                    CASE term_type WHEN 'query' THEN 0 WHEN 'both' THEN 1 ELSE 2 END,
                    confidence DESC,
                    term ASC
                LIMIT 80
                """,
                (tag_id, 'pixiv'),
            ).fetchall()
        for term_row in rows:
            term_type = self._normalize_platform_term_type(str(term_row['term_type'] or 'both'))
            if self._platform_term_type_matches(term_type, {'query'}):
                append_term(str(term_row['term'] or ''))
        if not terms:
            primary_cjk = set(re.findall(r"[\u3400-\u4dbf\u4e00-\u9fff\uf900-\ufaff]", str(row['name'] or '')))
            for suggestion in self.suggest_platform_terms_for_tag(
                tag_name=str(row['name']),
                platform='pixiv',
                limit=20,
            ):
                term = str(suggestion.get('term', '') or '').strip()
                term_cjk = set(re.findall(r"[\u3400-\u4dbf\u4e00-\u9fff\uf900-\ufaff]", term))
                if len(primary_cjk & term_cjk) >= 2:
                    append_term(term)
                if len(terms) >= 5:
                    break
        if not terms:
            append_term(str(row['name']))
        return terms

    def _build_tag_identity_candidate_payload(self, row: Any) -> dict[str, Any]:
        if not row:
            return {}
        reasons = self._parse_candidate_json(str(row['reasons_json'] or '[]'), [])
        evidence = self._parse_candidate_json(str(row['evidence_json'] or '{}'), {})
        llm_result = self._parse_candidate_json(str(row['llm_result_json'] or '{}'), {})
        return {
            'id': int(row['id']),
            'source_tag_id': int(row['source_tag_id']),
            'target_tag_id': int(row['target_tag_id']),
            'source_tag': str(row['source_tag'] or ''),
            'target_tag': str(row['target_tag'] or ''),
            'source_is_character': bool(row['source_is_character']) if 'source_is_character' in row.keys() else True,
            'target_is_character': bool(row['target_is_character']) if 'target_is_character' in row.keys() else True,
            'status': str(row['status'] or 'pending'),
            'score': float(row['score'] or 0.0),
            'reasons': reasons if isinstance(reasons, list) else [],
            'evidence': evidence if isinstance(evidence, dict) else {},
            'llm_result': llm_result if isinstance(llm_result, dict) else {},
            'created_at': str(row['created_at'] or ''),
            'updated_at': str(row['updated_at'] or ''),
        }

    def preview_merge_tags(
        self,
        *,
        target_tag_name: str,
        source_tag_names: Iterable[str],
    ) -> tuple[bool, dict[str, Any]]:
        requested_sources = self._dedupe_terms(source_tag_names)
        if not requested_sources:
            return False, {'message': '请至少提供一个来源 tag。'}
        with self._lock, self._connect() as conn:
            target_row, target_match_type = self._resolve_tag_exact_conn(conn, target_tag_name)
            if not target_row:
                return False, {'message': f'目标 tag 不存在：{target_tag_name}'}
            target_id = int(target_row['id'])
            target_name = str(target_row['name'])
            target_aliases = [str(row['alias']) for row in conn.execute('SELECT alias FROM tag_aliases WHERE tag_id = ? ORDER BY alias ASC', (target_id,)).fetchall()]
            items: list[dict[str, Any]] = []
            totals = {
                'image_links': 0,
                'image_link_collisions': 0,
                'review_tasks': 0,
                'review_task_collisions': 0,
                'aliases': 0,
                'platform_terms': 0,
                'subscriptions': 0,
            }
            for source_text in requested_sources:
                source_row, source_match_type = self._resolve_tag_exact_conn(conn, source_text)
                if not source_row:
                    items.append({'source_tag': source_text, 'status': 'error', 'message': f'来源 tag 不存在：{source_text}'})
                    continue
                source_id = int(source_row['id'])
                source_name = str(source_row['name'])
                if source_id == target_id:
                    items.append({'source_tag': source_name, 'status': 'skip', 'message': '来源 tag 与目标 tag 相同'})
                    continue
                image_links = int(conn.execute('SELECT COUNT(*) AS c FROM image_tags WHERE tag_id = ?', (source_id,)).fetchone()['c'])
                image_link_collisions = int(
                    conn.execute(
                        """
                        SELECT COUNT(*) AS c
                        FROM image_tags s
                        JOIN image_tags t
                          ON t.image_id = s.image_id
                         AND t.tag_id = ?
                         AND t.source_type = s.source_type
                        WHERE s.tag_id = ?
                        """,
                        (target_id, source_id),
                    ).fetchone()['c']
                )
                review_tasks = int(conn.execute('SELECT COUNT(*) AS c FROM review_tasks WHERE tag_id = ?', (source_id,)).fetchone()['c'])
                review_task_collisions = int(
                    conn.execute(
                        """
                        SELECT COUNT(*) AS c
                        FROM review_tasks s
                        JOIN review_tasks t
                          ON t.image_id = s.image_id
                         AND t.tag_id = ?
                        WHERE s.tag_id = ?
                        """,
                        (target_id, source_id),
                    ).fetchone()['c']
                )
                alias_rows = conn.execute('SELECT alias FROM tag_aliases WHERE tag_id = ? ORDER BY alias ASC', (source_id,)).fetchall()
                platform_rows = conn.execute('SELECT term FROM platform_tag_terms WHERE tag_id = ? ORDER BY id ASC', (source_id,)).fetchall()
                subscription_rows = conn.execute(
                    'SELECT platform, query_text, enabled FROM crawl_subscriptions WHERE tag_id = ? ORDER BY platform ASC',
                    (source_id,),
                ).fetchall()
                item = {
                    'source_tag': source_name,
                    'source_match_type': source_match_type,
                    'target_tag': target_name,
                    'target_match_type': target_match_type,
                    'source_is_character': bool(source_row['is_character']),
                    'target_is_character': bool(target_row['is_character']),
                    'image_links': image_links,
                    'image_link_collisions': image_link_collisions,
                    'review_tasks': review_tasks,
                    'review_task_collisions': review_task_collisions,
                    'aliases': [str(row['alias']) for row in alias_rows[:10]],
                    'alias_count': len(alias_rows),
                    'platform_terms': [str(row['term']) for row in platform_rows[:10]],
                    'platform_term_count': len(platform_rows),
                    'subscriptions': [
                        {
                            'platform': str(row['platform'] or ''),
                            'query_text': str(row['query_text'] or ''),
                            'enabled': bool(row['enabled']),
                        }
                        for row in subscription_rows
                    ],
                    'subscription_count': len(subscription_rows),
                    'status': 'ok',
                    'message': '可执行归并',
                }
                totals['image_links'] += image_links
                totals['image_link_collisions'] += image_link_collisions
                totals['review_tasks'] += review_tasks
                totals['review_task_collisions'] += review_task_collisions
                totals['aliases'] += len(alias_rows)
                totals['platform_terms'] += len(platform_rows)
                totals['subscriptions'] += len(subscription_rows)
                items.append(item)
        return True, {
            'message': f'已生成 {len(items)} 个来源 tag 的归并预览',
            'target_tag': target_name,
            'target_aliases': target_aliases,
            'items': items,
            'totals': totals,
        }

    def get_tag_id(self, tag_name: str) -> int | None:
        normalized = normalize_tag_name(tag_name)
        with self._lock, self._connect() as conn:
            row = conn.execute('SELECT id FROM tags WHERE normalized_name = ?', (normalized,)).fetchone()
            return int(row['id']) if row else None

    def get_stats(self) -> dict[str, int]:
        with self._lock, self._connect() as conn:
            images_count = conn.execute('SELECT COUNT(*) AS c FROM images WHERE is_active = 1').fetchone()['c']
            tags_count = conn.execute(
                """
                SELECT COUNT(*) AS c FROM tags t
                WHERE EXISTS (
                    SELECT 1 FROM image_tags it
                    JOIN images i ON i.id = it.image_id
                    WHERE it.tag_id = t.id
                      AND i.is_active = 1
                      AND it.review_status IN ('approved', 'manual_approved')
                )
                """
            ).fetchone()['c']
            alias_count = conn.execute('SELECT COUNT(*) AS c FROM tag_aliases').fetchone()['c']
            job_count = conn.execute('SELECT COUNT(*) AS c FROM crawl_jobs').fetchone()['c']
            subscription_count = conn.execute("SELECT COUNT(*) AS c FROM crawl_subscriptions WHERE enabled = 1").fetchone()['c']
            review_count = conn.execute("SELECT COUNT(*) AS c FROM review_tasks WHERE status IN ('pending', 'uncertain')").fetchone()['c']
        return {
            'images': int(images_count),
            'tags': int(tags_count),
            'aliases': int(alias_count),
            'crawl_jobs': int(job_count),
            'crawl_subscriptions': int(subscription_count),
            'pending_reviews': int(review_count),
        }

    def get_tag_governance_snapshot(self) -> dict[str, Any]:
        with self._lock, self._connect() as conn:
            tag_rows = conn.execute(
                """
                SELECT t.id, t.name, t.normalized_name, t.is_character, t.tag_type, t.status, t.created_at,
                       (SELECT COUNT(DISTINCT it.image_id)
                        FROM image_tags it WHERE it.tag_id = t.id) AS image_link_count,
                       (SELECT COUNT(DISTINCT it.image_id)
                        FROM image_tags it
                        WHERE it.tag_id = t.id
                          AND it.review_status IN ('approved', 'manual_approved')) AS approved_count,
                       (SELECT COUNT(DISTINCT it.image_id)
                        FROM image_tags it
                        WHERE it.tag_id = t.id
                          AND it.review_status NOT IN ('rejected', 'manual_rejected')) AS non_rejected_link_count,
                       (SELECT COUNT(*) FROM review_tasks rt
                        WHERE rt.tag_id = t.id AND rt.status IN ('pending', 'uncertain')) AS open_review_count,
                       (SELECT COUNT(*) FROM review_tasks rt
                        WHERE rt.tag_id = t.id
                          AND rt.status NOT IN ('rejected', 'manual_rejected')) AS review_dependency_count,
                       (SELECT COUNT(*) FROM tag_aliases a WHERE a.tag_id = t.id) AS alias_count,
                       (SELECT COUNT(*) FROM platform_tag_terms p WHERE p.tag_id = t.id) AS platform_term_count,
                       (SELECT COUNT(*) FROM platform_tag_terms p
                        WHERE p.tag_id = t.id AND p.platform = 'pixiv'
                          AND p.term_type IN ('query', 'both')) AS pixiv_query_term_count,
                       (SELECT COUNT(*) FROM crawl_subscriptions cs
                        WHERE cs.tag_id = t.id AND cs.enabled = 1) AS enabled_subscription_count,
                       (SELECT COUNT(*) FROM tag_merge_identity_candidates mic
                        WHERE mic.source_tag_id = t.id OR mic.target_tag_id = t.id) AS identity_candidate_count,
                       (SELECT COUNT(*) FROM tag_proposals tp
                        WHERE tp.resolved_tag_id = t.id
                           OR (tp.status = 'pending' AND tp.normalized_name = t.normalized_name)
                       ) AS proposal_dependency_count
                FROM tags t
                ORDER BY t.name COLLATE NOCASE ASC
                """
            ).fetchall()
            alias_rows = conn.execute(
                """
                SELECT a.id, a.alias, a.normalized_alias, a.tag_id, t.name AS tag_name
                FROM tag_aliases a
                JOIN tags t ON t.id = a.tag_id
                ORDER BY t.name ASC, a.alias ASC
                """
            ).fetchall()
            pending_proposals = int(
                conn.execute("SELECT COUNT(*) AS c FROM tag_proposals WHERE status = 'pending'").fetchone()['c']
            )

        aliases_by_tag: dict[int, list[str]] = {}
        aliases: list[dict[str, Any]] = []
        for row in alias_rows:
            tag_id = int(row['tag_id'])
            alias = str(row['alias'] or '')
            aliases_by_tag.setdefault(tag_id, []).append(alias)
            aliases.append(
                {
                    'id': int(row['id']),
                    'alias': alias,
                    'normalized_alias': str(row['normalized_alias'] or ''),
                    'tag_id': tag_id,
                    'tag_name': str(row['tag_name'] or ''),
                }
            )

        tags: list[dict[str, Any]] = []
        type_counts = {item: 0 for item in sorted(TAG_TYPES)}
        status_counts = {item: 0 for item in sorted(TAG_STATUSES)}
        for row in tag_rows:
            tag_type = normalize_tag_type(str(row['tag_type'] or ''), default='other') or 'other'
            status = normalize_tag_status(str(row['status'] or ''), default='active') or 'active'
            type_counts[tag_type] += 1
            status_counts[status] += 1
            image_link_count = int(row['image_link_count'] or 0)
            approved_count = int(row['approved_count'] or 0)
            non_rejected_link_count = int(row['non_rejected_link_count'] or 0)
            open_review_count = int(row['open_review_count'] or 0)
            review_dependency_count = int(row['review_dependency_count'] or 0)
            alias_count = int(row['alias_count'] or 0)
            platform_term_count = int(row['platform_term_count'] or 0)
            enabled_subscription_count = int(row['enabled_subscription_count'] or 0)
            identity_candidate_count = int(row['identity_candidate_count'] or 0)
            proposal_dependency_count = int(row['proposal_dependency_count'] or 0)
            safe_cleanup = (
                tag_type == 'other'
                and status == 'active'
                and approved_count == 0
                and non_rejected_link_count == 0
                and open_review_count == 0
                and review_dependency_count == 0
                and alias_count == 0
                and platform_term_count == 0
                and enabled_subscription_count == 0
                and identity_candidate_count == 0
                and proposal_dependency_count == 0
            )
            protected_other = tag_type == 'other' and (
                status != 'active'
                or any(
                    (
                        approved_count,
                        non_rejected_link_count,
                        open_review_count,
                        review_dependency_count,
                        alias_count,
                        platform_term_count,
                        enabled_subscription_count,
                        identity_candidate_count,
                        proposal_dependency_count,
                    )
                )
            )
            tags.append(
                {
                    'id': int(row['id']),
                    'name': str(row['name'] or ''),
                    'normalized_name': str(row['normalized_name'] or ''),
                    'is_character': bool(int(row['is_character'] or 0)),
                    'tag_type': tag_type,
                    'status': status,
                    'created_at': str(row['created_at'] or ''),
                    'image_link_count': image_link_count,
                    'approved_count': approved_count,
                    'non_rejected_link_count': non_rejected_link_count,
                    'open_review_count': open_review_count,
                    'review_dependency_count': review_dependency_count,
                    'alias_count': alias_count,
                    'platform_term_count': platform_term_count,
                    'pixiv_query_term_count': int(row['pixiv_query_term_count'] or 0),
                    'enabled_subscription_count': enabled_subscription_count,
                    'identity_candidate_count': identity_candidate_count,
                    'proposal_dependency_count': proposal_dependency_count,
                    'aliases': aliases_by_tag.get(int(row['id']), []),
                    'safe_cleanup': safe_cleanup,
                    'protected_other': protected_other,
                }
            )
        return {
            'totals': {
                'tags': len(tags),
                'aliases': len(aliases),
                'pending_proposals': pending_proposals,
                'type_counts': type_counts,
                'status_counts': status_counts,
            },
            'tags': tags,
            'aliases': aliases,
        }

    def count_images_for_tag(self, tag_name: str, include_unapproved: bool = False) -> int:
        tag_id = self.get_tag_id(tag_name)
        if tag_id is None:
            return 0
        status_sql = '' if include_unapproved else " AND it.review_status IN ('approved', 'manual_approved')"
        with self._lock, self._connect() as conn:
            row = conn.execute(
                f"""
                SELECT COUNT(DISTINCT i.id) AS c
                FROM image_tags it
                JOIN images i ON i.id = it.image_id
                WHERE it.tag_id = ? AND i.is_active = 1 {status_sql}
                """,
                (tag_id,),
            ).fetchone()
            return int(row['c'])

    def resolve_tag(self, query: str, allow_fuzzy: bool = True, candidate_limit: int = 5) -> MatchResult:
        normalized = normalize_tag_name(query)
        if not normalized:
            return MatchResult(matched=False)
        with self._lock, self._connect() as conn:
            exact_tag = conn.execute('SELECT t.id, t.name FROM tags t WHERE t.normalized_name = ? LIMIT 1', (normalized,)).fetchone()
            if exact_tag:
                return MatchResult(matched=True, tag_id=int(exact_tag['id']), tag_name=str(exact_tag['name']), match_type='exact_tag')
            exact_alias = conn.execute(
                'SELECT t.id, t.name FROM tag_aliases a JOIN tags t ON t.id = a.tag_id WHERE a.normalized_alias = ? LIMIT 1',
                (normalized,),
            ).fetchone()
            if exact_alias:
                return MatchResult(matched=True, tag_id=int(exact_alias['id']), tag_name=str(exact_alias['name']), match_type='exact_alias')
            if not allow_fuzzy:
                return MatchResult(matched=False)
            candidates = conn.execute(
                """
                SELECT DISTINCT t.id, t.name, COUNT(DISTINCT i.id) AS image_count
                FROM tags t
                LEFT JOIN tag_aliases a ON a.tag_id = t.id
                LEFT JOIN image_tags it ON it.tag_id = t.id AND it.review_status IN ('approved', 'manual_approved')
                LEFT JOIN images i ON i.id = it.image_id AND i.is_active = 1
                WHERE t.normalized_name LIKE ? OR a.normalized_alias LIKE ?
                GROUP BY t.id, t.name
                ORDER BY image_count DESC, t.name ASC
                LIMIT ?
                """,
                (f'%{normalized}%', f'%{normalized}%', candidate_limit + 1),
            ).fetchall()
        if not candidates:
            return MatchResult(matched=False)
        if len(candidates) == 1:
            row = candidates[0]
            return MatchResult(matched=True, tag_id=int(row['id']), tag_name=str(row['name']), match_type='fuzzy')
        return MatchResult(matched=False, candidates=[str(row['name']) for row in candidates[:candidate_limit]])

    def merge_tags(self, target_tag_name: str, source_tag_names: Iterable[str]) -> tuple[bool, dict[str, Any]]:
        requested_sources: list[str] = []
        seen_sources: set[str] = set()
        for raw in source_tag_names:
            text = str(raw or '').strip()
            normalized = normalize_tag_name(text)
            if not text or not normalized or normalized in seen_sources:
                continue
            seen_sources.add(normalized)
            requested_sources.append(text)
        if not requested_sources:
            return False, {'message': '请至少提供一个来源 tag。'}

        now = utcnow_str()
        with self._lock, self._connect() as conn:
            target_row, target_match_type = self._resolve_tag_exact_conn(conn, target_tag_name)
            if not target_row:
                return False, {'message': f'目标 tag 不存在：{target_tag_name}'}
            target_id = int(target_row['id'])
            target_name = str(target_row['name'])
            target_normalized = str(target_row['normalized_name'] or '')
            summary: dict[str, Any] = {
                'message': '',
                'target_tag': target_name,
                'target_match_type': target_match_type,
                'merged_tags': [],
                'aliases_added': [],
                'skipped': [],
                'image_links_migrated': 0,
                'review_tasks_migrated': 0,
                'review_tasks_merged': 0,
                'aliases_migrated': 0,
                'subscriptions_migrated': 0,
                'subscriptions_merged': 0,
                'subscriptions_removed': 0,
                'aliases_skipped': [],
            }

            for source_text in requested_sources:
                source_row, _ = self._resolve_tag_exact_conn(conn, source_text)
                if not source_row:
                    ok, message = self._insert_alias_conn(conn, tag_id=target_id, alias=source_text, now=now)
                    if ok:
                        summary['aliases_added'].append(source_text)
                    else:
                        summary['skipped'].append(f'{source_text}（{message}）')
                    continue

                source_id = int(source_row['id'])
                source_normalized = str(source_row['normalized_name'] or '')
                if source_id == target_id or source_normalized == target_normalized:
                    summary['skipped'].append(f'{source_text}（已归并到 {target_name}）')
                    continue

                result = self._merge_tag_into_conn(
                    conn,
                    target_id=target_id,
                    target_name=target_name,
                    source_id=source_id,
                    now=now,
                )
                summary['merged_tags'].append(result['source_name'])
                summary['image_links_migrated'] += int(result['image_links_migrated'])
                summary['review_tasks_migrated'] += int(result['review_tasks_migrated'])
                summary['review_tasks_merged'] += int(result['review_tasks_merged'])
                summary['aliases_migrated'] += int(result['aliases_migrated'])
                summary['subscriptions_migrated'] += int(result.get('subscriptions_migrated') or 0)
                summary['subscriptions_merged'] += int(result.get('subscriptions_merged') or 0)
                summary['subscriptions_removed'] += int(result['subscriptions_removed'])
                summary['aliases_skipped'].extend(list(result.get('aliases_skipped') or []))

            merged_count = len(summary['merged_tags'])
            alias_count = len(summary['aliases_added'])
            if merged_count == 0 and alias_count == 0:
                summary['message'] = '没有发生可执行的 tag 变更。'
                return False, summary
            summary['message'] = f'已归并到主 tag：{target_name}'
            return True, summary

    def switch_primary_tag(self, tag_name_or_alias: str, new_primary_name: str) -> tuple[bool, dict[str, Any]]:
        new_name = str(new_primary_name or '').strip()
        if not new_name:
            return False, {'message': '新主 tag 不能为空。'}
        now = utcnow_str()
        with self._lock, self._connect() as conn:
            current_row, match_type = self._resolve_tag_exact_conn(conn, tag_name_or_alias)
            if not current_row:
                return False, {'message': f'没有找到 tag 或 alias：{tag_name_or_alias}'}
            current_id = int(current_row['id'])
            current_name = str(current_row['name'])
            current_normalized = str(current_row['normalized_name'] or '')
            new_normalized = normalize_tag_name(new_name)
            if not new_normalized:
                return False, {'message': '新主 tag 不能为空。'}
            if new_normalized == current_normalized and new_name == current_name:
                return False, {'message': '新主 tag 与当前主 tag 相同。'}

            conflict_tag = conn.execute(
                'SELECT id, name FROM tags WHERE normalized_name = ? LIMIT 1',
                (new_normalized,),
            ).fetchone()
            if conflict_tag and int(conflict_tag['id']) != current_id:
                return False, {'message': f'新主 tag 与现有 tag 冲突：{conflict_tag["name"]}'}

            alias_usage = self._get_alias_usage_conn(conn, new_normalized)
            if alias_usage and int(alias_usage['tag_id']) != current_id:
                return False, {'message': f'新主 tag 已被 {alias_usage["tag_name"]} 的 alias 使用：{new_name}'}

            conn.execute(
                'DELETE FROM tag_aliases WHERE tag_id = ? AND normalized_alias = ?',
                (current_id, new_normalized),
            )
            conn.execute(
                'UPDATE tags SET name = ?, normalized_name = ? WHERE id = ?',
                (new_name, new_normalized, current_id),
            )

            old_name_promoted_to_alias = False
            if current_normalized != new_normalized:
                ok, _ = self._insert_alias_conn(conn, tag_id=current_id, alias=current_name, now=now)
                old_name_promoted_to_alias = ok

            return True, {
                'message': f'已切换主 tag：{current_name} -> {new_name}',
                'tag_id': current_id,
                'old_name': current_name,
                'new_name': new_name,
                'match_type': match_type,
                'old_name_promoted_to_alias': old_name_promoted_to_alias,
            }

    def get_random_image_for_tag(self, tag_id: int, excluded_image_ids: list[int] | None = None) -> sqlite3.Row | None:
        excluded_image_ids = excluded_image_ids or []
        approved_placeholder = ','.join('?' for _ in APPROVED_STATUSES)
        approved_params: tuple[Any, ...] = tuple(APPROVED_STATUSES)
        with self._lock, self._connect() as conn:
            if excluded_image_ids:
                placeholders = ','.join('?' for _ in excluded_image_ids)
                row = conn.execute(
                    f"""
                    SELECT DISTINCT i.id, i.file_path, i.file_name
                    FROM image_tags it
                    JOIN images i ON i.id = it.image_id
                    WHERE it.tag_id = ?
                      AND i.is_active = 1
                      AND it.review_status IN ({approved_placeholder})
                      AND i.id NOT IN ({placeholders})
                    ORDER BY RANDOM()
                    LIMIT 1
                    """,
                    (tag_id, *approved_params, *excluded_image_ids),
                ).fetchone()
                if row:
                    return row
            return conn.execute(
                f"""
                SELECT DISTINCT i.id, i.file_path, i.file_name
                FROM image_tags it
                JOIN images i ON i.id = it.image_id
                WHERE it.tag_id = ? AND i.is_active = 1
                  AND it.review_status IN ({approved_placeholder})
                ORDER BY RANDOM()
                LIMIT 1
                """,
                (tag_id, *approved_params),
            ).fetchone()

    def get_image_file_path(self, image_id: int) -> str | None:
        with self._lock, self._connect() as conn:
            self._sync_image_file_state(conn, image_id)
            row = conn.execute(
                "SELECT file_path FROM images WHERE id = ? AND is_active = 1",
                (image_id,),
            ).fetchone()
            return str(row["file_path"]) if row and row["file_path"] else None

    def record_send_log(self, session_id: str, image_id: int, matched_tag: str) -> None:
        with self._lock, self._connect() as conn:
            conn.execute('INSERT INTO send_logs(session_id, image_id, matched_tag, sent_at) VALUES(?, ?, ?, ?)', (session_id, image_id, matched_tag, utcnow_str()))

    def upsert_source(self, image_id: int, platform: str, post_url: str, image_url: str, author: str = '', raw_tags: list[str] | None = None, extra_json: dict[str, Any] | None = None) -> None:
        with self._lock, self._connect() as conn:
            conn.execute(
                """
                INSERT OR REPLACE INTO sources(image_id, platform, post_url, image_url, author, raw_tags, extra_json, created_at)
                VALUES(?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (image_id, platform, post_url, image_url, author, json.dumps(raw_tags or [], ensure_ascii=False), json.dumps(extra_json or {}, ensure_ascii=False), utcnow_str()),
            )

    def commit_crawl_image(
        self,
        *,
        image_id: int,
        platform: str,
        post_url: str,
        image_url: str,
        author: str,
        raw_tags: list[str],
        extra_json: dict[str, Any],
        tag_reviews: list[dict[str, Any]],
    ) -> None:
        now = utcnow_str()
        with self._lock, self._connect() as conn:
            conn.execute(
                """
                INSERT OR REPLACE INTO sources(
                    image_id, platform, post_url, image_url, author, raw_tags, extra_json, created_at
                )
                VALUES(?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    int(image_id),
                    str(platform or '').strip().lower(),
                    str(post_url or '').strip(),
                    str(image_url or '').strip(),
                    str(author or '').strip(),
                    json.dumps(raw_tags or [], ensure_ascii=False),
                    json.dumps(extra_json or {}, ensure_ascii=False),
                    now,
                ),
            )
            for item in tag_reviews:
                tag_id = int(item['tag_id'])
                source_type = str(item.get('source_type', '') or '')
                status = str(item.get('status', 'pending') or 'pending')
                score = float(item.get('score', 0.0) or 0.0)
                reason = str(item.get('reason', '') or '')
                model_result = str(item.get('model_result', '') or '')
                conn.execute(
                    """
                    INSERT INTO image_tags(
                        image_id, tag_id, source_type, score, review_status,
                        review_reason, created_at, updated_at
                    )

                    VALUES(?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(image_id, tag_id, source_type)
                    DO UPDATE SET
                        score = excluded.score,
                        review_status = excluded.review_status,
                        review_reason = excluded.review_reason,
                        updated_at = excluded.updated_at
                    """,
                    (
                        int(image_id),
                        tag_id,
                        source_type,
                        score,
                        status,
                        reason,
                        now,
                        now,
                    ),
                )
                if not bool(item.get('create_review_task', False)):
                    continue
                existing = conn.execute(
                    """
                    SELECT id FROM review_tasks
                    WHERE image_id = ? AND tag_id = ?
                    ORDER BY id DESC LIMIT 1
                    """,
                    (int(image_id), tag_id),
                ).fetchone()
                if existing:
                    conn.execute(
                        """
                        UPDATE review_tasks
                        SET status = ?, model_result = ?, reason = ?, updated_at = ?
                        WHERE id = ?
                        """,
                        (status, model_result, reason, now, int(existing['id'])),
                    )
                else:
                    conn.execute(
                        """
                        INSERT INTO review_tasks(
                            image_id, tag_id, status, model_result, manual_result,
                            reason, created_at, updated_at
                        )
                        VALUES(?, ?, ?, ?, '', ?, ?, ?)
                        """,
                        (int(image_id), tag_id, status, model_result, reason, now, now),
                    )

    def commit_chat_collection_image(self, *, image_id: int, image_url: str, author: str,
                                     raw_tags: list[str], extra_json: dict[str, Any],
                                     tag_ids: list[int], reason: str = '',
                                     tag_members: dict[int, list[int]] | None = None) -> dict[str, Any]:
        now = utcnow_str()
        accepted = []
        changed = []
        source_changed = False
        with self._lock, self._connect() as conn:
            for tag_id in tag_ids:
                tag = conn.execute('SELECT status FROM tags WHERE id=?', (tag_id,)).fetchone()
                rejected = conn.execute(
                    "SELECT 1 FROM image_tags WHERE image_id=? AND tag_id=? "
                    "AND review_status IN ('rejected','manual_rejected') LIMIT 1",
                    (image_id, tag_id),
                ).fetchone()
                if tag and tag['status'] == 'active' and not rejected:
                    accepted.append(tag_id)
            accepted = [tag_id for tag_id in accepted
                        if set((tag_members or {}).get(tag_id, [])).issubset(accepted)]
            has_character = any(conn.execute(
                "SELECT 1 FROM tags WHERE id=? AND tag_type='character'", (tag_id,)
            ).fetchone() for tag_id in accepted)
            if not has_character:
                accepted = []
            for tag_id in accepted:
                existing = conn.execute(
                    "SELECT 1 FROM image_tags WHERE image_id=? AND tag_id=? "
                    "AND review_status IN ('approved','manual_approved') LIMIT 1",
                    (image_id, tag_id),
                ).fetchone()
                if existing:
                    continue
                cursor = conn.execute(
                    "INSERT OR IGNORE INTO image_tags(image_id,tag_id,source_type,score,review_status,"
                    "review_reason,created_at,updated_at) VALUES(?,?,'chat_auto_collection',1,'approved',?,?,?)",
                    (image_id, tag_id, reason or '主聊天自动收图', now, now),
                )
                if cursor.rowcount:
                    changed.append(tag_id)
            if accepted and not conn.execute(
                'SELECT 1 FROM sources WHERE image_id=? AND image_url=?', (image_id, image_url)
            ).fetchone():
                names = [conn.execute('SELECT name FROM tags WHERE id=?', (x,)).fetchone()['name'] for x in accepted]
                metadata = {**extra_json, 'collection_reason': reason, 'selected_tag_ids': accepted}
                conn.execute(
                    'INSERT INTO sources(image_id,platform,post_url,image_url,author,raw_tags,extra_json,created_at) '
                    "VALUES(?,'chat',?,?,?,?,?,?)",
                    (image_id, 'chat://' + str(extra_json.get('source_message_id', '')),
                     image_url, author, json.dumps(names, ensure_ascii=False),
                     json.dumps(metadata, ensure_ascii=False), now),
                )
                source_changed = True
        return {'changed': bool(accepted and (source_changed or changed)),
                'source_changed': source_changed, 'tag_ids_changed': changed,
                'tag_ids_accepted': accepted, 'tag_ids_skipped': [x for x in tag_ids if x not in accepted]}

    @staticmethod
    def normalize_source_post_url(platform: str, post_url: str) -> str:
        raw_post_url = str(post_url or '').strip()
        if not raw_post_url:
            return ''
        platform_text = str(platform or '').strip().lower()
        if platform_text == 'pixiv':
            match = re.search(r'(?:artworks/|illust_id=)(\d+)', raw_post_url)
            if match:
                return f'https://www.pixiv.net/artworks/{match.group(1)}'
        if platform_text == 'xiaohongshu':
            match = re.search(r'/(?:explore|discovery/item|note)/([0-9A-Za-z]+)', raw_post_url)
            if match:
                return f'https://www.xiaohongshu.com/explore/{match.group(1)}'
        return raw_post_url.rstrip('/')

    @staticmethod
    def _source_uid_from_post_url(platform: str, post_url: str) -> str:
        platform_text = str(platform or '').strip().lower()
        if platform_text == 'pixiv':
            match = re.search(r'(?:artworks/|illust_id=)(\d+)', str(post_url or ''))
            if match:
                return match.group(1)
        if platform_text == 'xiaohongshu':
            match = re.search(r'/(?:explore|discovery/item|note)/([0-9A-Za-z]+)', str(post_url or ''))
            if match:
                return match.group(1)
        return ''

    def is_rejected_source_post_url(self, post_url: str, *, platform: str = '') -> bool:
        platform_text = str(platform or '').strip().lower()
        normalized_post_url = self.normalize_source_post_url(platform_text, post_url)
        if not normalized_post_url:
            return False
        sql = 'SELECT 1 FROM rejected_sources WHERE normalized_post_url = ?'
        params: list[Any] = [normalized_post_url]
        if platform_text:
            sql += ' AND platform = ?'
            params.append(platform_text)
        sql += ' LIMIT 1'
        with self._lock, self._connect() as conn:
            return conn.execute(sql, params).fetchone() is not None

    def _upsert_rejected_source_conn(
        self,
        conn: sqlite3.Connection,
        *,
        platform: str,
        post_url: str,
        image_id: int = 0,
        reason: str = '',
        now: str | None = None,
    ) -> sqlite3.Row:
        platform_text = str(platform or '').strip().lower() or 'pixiv'
        post_url_text = str(post_url or '').strip()
        normalized_post_url = self.normalize_source_post_url(platform_text, post_url_text)
        if not normalized_post_url:
            raise ValueError('来源链接不能为空')
        current_time = now or utcnow_str()
        source_uid = self._source_uid_from_post_url(platform_text, normalized_post_url)
        conn.execute(
            """
            INSERT INTO rejected_sources(platform, post_url, normalized_post_url, source_uid, image_id, reason, created_at, updated_at)
            VALUES(?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(platform, normalized_post_url) DO UPDATE SET
                post_url = excluded.post_url,
                source_uid = excluded.source_uid,
                image_id = CASE WHEN excluded.image_id > 0 THEN excluded.image_id ELSE rejected_sources.image_id END,
                reason = excluded.reason,
                updated_at = excluded.updated_at
            """,
            (
                platform_text,
                post_url_text or normalized_post_url,
                normalized_post_url,
                source_uid,
                int(image_id or 0),
                str(reason or '').strip(),
                current_time,
                current_time,
            ),
        )
        return conn.execute(
            'SELECT * FROM rejected_sources WHERE platform = ? AND normalized_post_url = ? LIMIT 1',
            (platform_text, normalized_post_url),
        ).fetchone()

    def has_source_post_url(self, post_url: str, *, platform: str = '') -> bool:
        raw_post_url = str(post_url or '').strip()
        if not raw_post_url:
            return False
        platform_text = str(platform or '').strip().lower()
        normalized_post_url = self.normalize_source_post_url(platform_text, raw_post_url)
        sql = 'SELECT 1 FROM sources WHERE (post_url = ? OR post_url = ?)'
        params: list[Any] = [raw_post_url, normalized_post_url]
        if platform_text:
            sql += ' AND platform = ?'
            params.append(platform_text)
        sql += ' LIMIT 1'
        with self._lock, self._connect() as conn:
            row = conn.execute(sql, params).fetchone()
            return row is not None

    def reject_image_source(
        self,
        image_id: int,
        *,
        platform: str = 'pixiv',
        reason: str = '',
        require_open_review: bool = False,
    ) -> tuple[bool, dict[str, Any]]:
        platform_text = str(platform or 'pixiv').strip().lower() or 'pixiv'
        now = utcnow_str()
        rejected_reason = str(reason or '').strip() or f'{platform_text} 人工拒绝图片'
        with self._lock, self._connect() as conn:
            if require_open_review:
                conn.execute('BEGIN IMMEDIATE')
            image = conn.execute('SELECT id FROM images WHERE id = ? AND is_active = 1 LIMIT 1', (int(image_id),)).fetchone()
            if not image:
                return False, {'message': f'图片不存在：{image_id}'}
            if require_open_review:
                open_task = conn.execute(
                    "SELECT 1 FROM review_tasks WHERE image_id = ? AND status IN ('pending', 'uncertain') LIMIT 1",
                    (int(image_id),),
                ).fetchone()
                if not open_task:
                    return False, {
                        'message': f'图片 #{image_id} 已不在待审队列中。',
                        'code': 'stale_review',
                        'image_id': int(image_id),
                    }
            source = conn.execute(
                """
                SELECT post_url
                FROM sources
                WHERE image_id = ? AND platform = ?
                ORDER BY id DESC
                LIMIT 1
                """,
                (int(image_id), platform_text),
            ).fetchone()
            if not source:
                return False, {'message': f'图片 #{image_id} 没有 {platform_text} 来源记录。'}

            rejected_source = self._upsert_rejected_source_conn(
                conn,
                platform=platform_text,
                post_url=str(source['post_url'] or ''),
                image_id=int(image_id),
                reason=rejected_reason,
                now=now,
            )
            tasks = conn.execute(
                """
                SELECT rt.id, t.name AS tag_name
                FROM review_tasks rt
                JOIN tags t ON t.id = rt.tag_id
                WHERE rt.image_id = ?
                """,
                (int(image_id),),
            ).fetchall()
            conn.execute(
                """
                UPDATE review_tasks
                SET status = 'manual_rejected', manual_result = 'rejected', reason = ?, updated_at = ?
                WHERE image_id = ?
                """,
                (rejected_reason, now, int(image_id)),
            )
            cursor = conn.execute(
                """
                UPDATE image_tags
                SET review_status = 'manual_rejected', review_reason = ?, updated_at = ?
                WHERE image_id = ?
                """,
                (rejected_reason, now, int(image_id)),
            )

        return True, {
            'message': f'已拒绝图片 #{image_id}，后续 {platform_text} 来源搜图会跳过该作品',
            'image_id': int(image_id),
            'post_url': str(rejected_source['normalized_post_url'] or source['post_url']),
            'rejected_tasks': [str(row['tag_name']) for row in tasks],
            'rejected_tag_links': int(cursor.rowcount if cursor.rowcount is not None else 0),
        }

    def create_crawl_job(
        self,
        platform: str,
        source_url: str,
        tags: list[str],
        *,
        source_context: dict[str, Any] | None = None,
        include_tags: list[str] | None = None,
        exclude_tags: list[str] | None = None,
        match_mode: str = 'exact',
        origin: str = 'manual',
        priority: int = 0,
    ) -> int:
        now = utcnow_str()
        with self._lock, self._connect() as conn:
            cursor = conn.execute(
                """
                INSERT INTO crawl_jobs(
                    platform, source_url, source_context_json, tags_text, include_tags_text, exclude_tags_text, tag_match_mode,
                    origin, priority, status, progress, error_log, result_summary, attempt_count, created_at, updated_at
                )
                VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, 'pending', 0, '', '', 0, ?, ?)
                """,
                (
                    platform,
                    source_url,
                    self._serialize_source_context(source_context),
                    ','.join(tags),
                    ','.join(include_tags or []),
                    ','.join(exclude_tags or []),
                    str(match_mode or 'exact'),
                    str(origin or 'manual').strip().lower() or 'manual',
                    int(priority),
                    now,
                    now,
                ),
            )
            return int(cursor.lastrowid)

    def get_crawl_job(self, job_id: int) -> sqlite3.Row | None:
        with self._lock, self._connect() as conn:
            return conn.execute('SELECT * FROM crawl_jobs WHERE id = ?', (job_id,)).fetchone()

    def update_crawl_job(
        self,
        job_id: int,
        *,
        status: str | None = None,
        progress: int | None = None,
        error_log: str | None = None,
        result_summary: str | None = None,
        attempt_count: int | None = None,
        source_context: dict[str, Any] | None = None,
        clear_source_context: bool = False,
    ) -> None:
        fields: list[str] = ['updated_at = ?']
        params: list[Any] = [utcnow_str()]
        if status is not None:
            fields.append('status = ?')
            params.append(status)
        if progress is not None:
            fields.append('progress = ?')
            params.append(progress)
        if error_log is not None:
            fields.append('error_log = ?')
            params.append(error_log)
        if result_summary is not None:
            fields.append('result_summary = ?')
            params.append(result_summary)
        if attempt_count is not None:
            fields.append('attempt_count = ?')
            params.append(attempt_count)
        if source_context is not None:
            fields.append('source_context_json = ?')
            params.append(self._serialize_source_context(source_context))
        if clear_source_context:
            fields.append("source_context_json = '{}'")
        params.append(job_id)
        with self._lock, self._connect() as conn:
            conn.execute(f"UPDATE crawl_jobs SET {', '.join(fields)} WHERE id = ?", params)

    def increment_crawl_job_attempt(self, job_id: int) -> int:
        with self._lock, self._connect() as conn:
            row = conn.execute('SELECT attempt_count FROM crawl_jobs WHERE id = ?', (job_id,)).fetchone()
            current = int(row['attempt_count'] or 0) if row else 0
            current += 1
            conn.execute('UPDATE crawl_jobs SET attempt_count = ?, updated_at = ? WHERE id = ?', (current, utcnow_str(), job_id))
            return current

    def list_crawl_jobs(self, *, limit: int = 20, statuses: Iterable[str] | None = None) -> list[sqlite3.Row]:
        sql = 'SELECT * FROM crawl_jobs'
        params: list[Any] = []
        if statuses:
            placeholders = ','.join('?' for _ in statuses)
            sql += f' WHERE status IN ({placeholders})'
            params.extend(list(statuses))
        sql += ' ORDER BY id DESC LIMIT ?'
        params.append(limit)
        with self._lock, self._connect() as conn:
            return conn.execute(sql, params).fetchall()

    def count_crawl_jobs_by_status(self) -> dict[str, int]:
        with self._lock, self._connect() as conn:
            rows = conn.execute(
                "SELECT status, COUNT(*) AS total FROM crawl_jobs GROUP BY status"
            ).fetchall()
        return {str(row["status"] or ""): int(row["total"] or 0) for row in rows}

    def count_pixiv_backfill_tasks_by_status(self) -> dict[str, int]:
        with self._lock, self._connect() as conn:
            rows = conn.execute(
                "SELECT status, COUNT(*) AS total FROM pixiv_backfill_tasks GROUP BY status"
            ).fetchall()
        return {str(row["status"] or ""): int(row["total"] or 0) for row in rows}

    def count_xhs_backfill_tasks_by_status(self) -> dict[str, int]:
        with self._lock, self._connect() as conn:
            rows = conn.execute(
                "SELECT status, COUNT(*) AS total FROM xhs_backfill_tasks GROUP BY status"
            ).fetchall()
        return {str(row["status"] or ""): int(row["total"] or 0) for row in rows}

    def get_latest_crawl_job(self, *, statuses: Iterable[str] | None = None) -> sqlite3.Row | None:
        sql = "SELECT * FROM crawl_jobs"
        params: list[Any] = []
        status_values = [str(item).strip() for item in (statuses or []) if str(item).strip()]
        if status_values:
            placeholders = ",".join("?" for _ in status_values)
            sql += f" WHERE status IN ({placeholders})"
            params.extend(status_values)
        sql += " ORDER BY updated_at DESC, id DESC LIMIT 1"
        with self._lock, self._connect() as conn:
            return conn.execute(sql, params).fetchone()

    def list_failed_crawl_jobs(self, *, platform: str = "", limit: int = 10) -> list[sqlite3.Row]:
        sql = "SELECT * FROM crawl_jobs WHERE status = 'failed'"
        params: list[Any] = []
        platform_text = str(platform or "").strip().lower()
        if platform_text:
            sql += " AND platform = ?"
            params.append(platform_text)
        sql += " ORDER BY updated_at DESC, id DESC LIMIT ?"
        params.append(max(1, int(limit or 10)))
        with self._lock, self._connect() as conn:
            return conn.execute(sql, params).fetchall()

    def get_latest_crawl_subscription_error(self, *, platform: str = "") -> sqlite3.Row | None:
        sql = "SELECT * FROM crawl_subscriptions WHERE TRIM(COALESCE(last_error, '')) <> ''"
        params: list[Any] = []
        platform_text = str(platform or "").strip().lower()
        if platform_text:
            sql += " AND platform = ?"
            params.append(platform_text)
        sql += " ORDER BY updated_at DESC, id DESC LIMIT 1"
        with self._lock, self._connect() as conn:
            return conn.execute(sql, params).fetchone()

    def get_crawl_provider_state(self, platform: str) -> sqlite3.Row | None:
        platform_text = str(platform or '').strip().lower()
        if not platform_text:
            return None
        with self._lock, self._connect() as conn:
            return conn.execute(
                'SELECT * FROM crawl_provider_states WHERE platform = ? LIMIT 1',
                (platform_text,),
            ).fetchone()

    def set_crawl_provider_state(
        self,
        platform: str,
        *,
        status: str,
        category: str = '',
        reason: str = '',
    ) -> bool:
        platform_text = str(platform or '').strip().lower()
        status_text = str(status or '').strip().lower()
        if not platform_text or status_text not in {'active', 'paused'}:
            raise ValueError('提供者状态必须是 active 或 paused')
        category_text = str(category or '').strip()[:100] if status_text == 'paused' else ''
        reason_text = str(reason or '').strip()[:1000] if status_text == 'paused' else ''
        now = utcnow_str()
        with self._lock, self._connect() as conn:
            existing = conn.execute(
                'SELECT * FROM crawl_provider_states WHERE platform = ? LIMIT 1',
                (platform_text,),
            ).fetchone()
            changed = (
                (existing is None and status_text == 'paused')
                or (
                    existing is not None
                    and (
                        str(existing['status'] or '') != status_text
                        or str(existing['paused_category'] or '') != category_text
                        or str(existing['paused_reason'] or '') != reason_text
                    )
                )
            )
            paused_at = now if status_text == 'paused' else ''
            if existing and status_text == 'paused' and str(existing['status'] or '') == 'paused':
                paused_at = str(existing['paused_at'] or '') or now
            conn.execute(
                """
                INSERT INTO crawl_provider_states(
                    platform, status, paused_category, paused_reason, paused_at,
                    last_checked_at, last_success_at, last_error, updated_at
                )
                VALUES(?, ?, ?, ?, ?, '', '', ?, ?)
                ON CONFLICT(platform) DO UPDATE SET
                    status = excluded.status,
                    paused_category = excluded.paused_category,
                    paused_reason = excluded.paused_reason,
                    paused_at = excluded.paused_at,
                    last_error = CASE
                        WHEN excluded.status = 'active' THEN ''
                        ELSE excluded.last_error
                    END,
                    updated_at = excluded.updated_at
                """,
                (
                    platform_text,
                    status_text,
                    category_text,
                    reason_text,
                    paused_at,
                    reason_text,
                    now,
                ),
            )
        return changed

    def record_crawl_provider_check(
        self,
        platform: str,
        *,
        success: bool,
        error: str = '',
    ) -> None:
        platform_text = str(platform or '').strip().lower()
        if not platform_text:
            return
        now = utcnow_str()
        error_text = str(error or '').strip()[:1000]
        with self._lock, self._connect() as conn:
            conn.execute(
                """
                INSERT INTO crawl_provider_states(
                    platform, status, paused_category, paused_reason, paused_at,
                    last_checked_at, last_success_at, last_error, updated_at
                )
                VALUES(?, 'active', '', '', '', ?, ?, ?, ?)
                ON CONFLICT(platform) DO UPDATE SET
                    last_checked_at = excluded.last_checked_at,
                    last_success_at = CASE
                        WHEN ? = 1 THEN excluded.last_success_at
                        ELSE crawl_provider_states.last_success_at
                    END,
                    last_error = excluded.last_error,
                    updated_at = excluded.updated_at
                """,
                (
                    platform_text,
                    now,
                    now if success else '',
                    '' if success else error_text,
                    now,
                    1 if success else 0,
                ),
            )

    def get_pending_jobs(self) -> list[sqlite3.Row]:
        with self._lock, self._connect() as conn:
            return conn.execute(
                """
                SELECT id, priority
                FROM crawl_jobs
                WHERE status IN ('pending', 'retry')
                ORDER BY priority ASC, id ASC
                """
            ).fetchall()

    def get_pending_job_ids(self) -> list[int]:
        return [int(row['id']) for row in self.get_pending_jobs()]

    def reset_running_jobs(self) -> None:
        with self._lock, self._connect() as conn:
            conn.execute("UPDATE crawl_jobs SET status = 'retry', updated_at = ? WHERE status = 'running'", (utcnow_str(),))

    def has_crawl_job_source_url(self, source_url: str, *, platform: str = '') -> bool:
        url = str(source_url or '').strip()
        if not url:
            return False
        sql = 'SELECT id FROM crawl_jobs WHERE source_url = ?'
        params: list[Any] = [url]
        if platform:
            sql += ' AND platform = ?'
            params.append(str(platform or '').strip().lower())
        sql += ' LIMIT 1'
        with self._lock, self._connect() as conn:
            return conn.execute(sql, params).fetchone() is not None

    def create_pixiv_backfill_task(
        self,
        *,
        tag_id: int,
        tag_name: str,
        tag_text: str,
        query_terms: list[str],
        include_tags: list[str] | None = None,
        exclude_tags: list[str] | None = None,
        max_pages: int = 20,
        max_results: int = 200,
        max_new_jobs: int = 100,
    ) -> int:
        now = utcnow_str()
        canonical_tag = str(tag_name or '').strip()
        with self._lock, self._connect() as conn:
            cursor = conn.execute(
                """
                INSERT INTO pixiv_backfill_tasks(
                    tag_id, tag_name, normalized_tag, tag_text, query_terms_json,
                    include_tags_text, exclude_tags_text, max_pages, max_results, max_new_jobs,
                    status, current_query_text, current_page, current_offset,
                    scanned, matched, queued, skipped_existing, skipped_rejected,
                    skipped_filtered, skipped_duplicate, error_log, created_at, updated_at, completed_at
                )
                VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'pending', '', 0, '', 0, 0, 0, 0, 0, 0, 0, '', ?, ?, '')
                """,
                (
                    int(tag_id or 0),
                    canonical_tag,
                    normalize_tag_name(canonical_tag),
                    str(tag_text or '').strip(),
                    json.dumps(query_terms or [], ensure_ascii=False),
                    ','.join(include_tags or []),
                    ','.join(exclude_tags or []),
                    int(max_pages or 20),
                    int(max_results or 200),
                    int(max_new_jobs or 100),
                    now,
                    now,
                ),
            )
            return int(cursor.lastrowid)

    def get_pixiv_backfill_task(self, task_id: int) -> sqlite3.Row | None:
        with self._lock, self._connect() as conn:
            return conn.execute('SELECT * FROM pixiv_backfill_tasks WHERE id = ?', (int(task_id),)).fetchone()

    def update_pixiv_backfill_task(self, task_id: int, **fields: Any) -> None:
        allowed = {
            'status',
            'current_query_text',
            'current_page',
            'current_offset',
            'scanned',
            'matched',
            'queued',
            'skipped_existing',
            'skipped_rejected',
            'skipped_filtered',
            'skipped_duplicate',
            'error_log',
            'completed_at',
        }
        assignments: list[str] = ['updated_at = ?']
        params: list[Any] = [utcnow_str()]
        for key, value in fields.items():
            if key not in allowed:
                continue
            assignments.append(f'{key} = ?')
            params.append(value)
        if len(assignments) == 1:
            return
        params.append(int(task_id))
        with self._lock, self._connect() as conn:
            conn.execute(f"UPDATE pixiv_backfill_tasks SET {', '.join(assignments)} WHERE id = ?", params)

    def list_pixiv_backfill_tasks(
        self,
        *,
        limit: int = 20,
        statuses: Iterable[str] | None = None,
    ) -> list[sqlite3.Row]:
        sql = 'SELECT * FROM pixiv_backfill_tasks'
        params: list[Any] = []
        if statuses:
            status_values = [str(item).strip() for item in statuses if str(item).strip()]
            if status_values:
                placeholders = ','.join('?' for _ in status_values)
                sql += f' WHERE status IN ({placeholders})'
                params.extend(status_values)
        sql += ' ORDER BY id DESC LIMIT ?'
        params.append(max(1, int(limit or 20)))
        with self._lock, self._connect() as conn:
            return conn.execute(sql, params).fetchall()

    def get_pending_pixiv_backfill_task_ids(self) -> list[int]:
        with self._lock, self._connect() as conn:
            rows = conn.execute(
                "SELECT id FROM pixiv_backfill_tasks WHERE status IN ('pending', 'retry') ORDER BY id ASC"
            ).fetchall()
            return [int(row['id']) for row in rows]

    def reset_running_pixiv_backfill_tasks(self) -> None:
        with self._lock, self._connect() as conn:
            conn.execute(
                "UPDATE pixiv_backfill_tasks SET status = 'retry', updated_at = ? WHERE status = 'running'",
                (utcnow_str(),),
            )

    def create_xhs_backfill_task(
        self,
        *,
        tag_id: int,
        tag_name: str,
        tag_text: str,
        query_terms: list[str],
        match_terms: list[str],
        max_pages: int = 10,
        max_results: int = 200,
        max_new_jobs: int = 50,
    ) -> int:
        now = utcnow_str()
        canonical_tag = str(tag_name or '').strip()
        with self._lock, self._connect() as conn:
            cursor = conn.execute(
                """
                INSERT INTO xhs_backfill_tasks(
                    tag_id, tag_name, normalized_tag, tag_text,
                    query_terms_json, match_terms_json,
                    max_pages, max_results, max_new_jobs,
                    status, current_query_index, current_query_text, next_page,
                    scanned, detailed, matched, queued,
                    skipped_existing, skipped_rejected, skipped_filtered,
                    skipped_duplicate, failed_details, error_log,
                    created_at, updated_at, completed_at
                )
                VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, 'pending', 0, '', 1,
                       0, 0, 0, 0, 0, 0, 0, 0, 0, '', ?, ?, '')
                """,
                (
                    int(tag_id or 0),
                    canonical_tag,
                    normalize_tag_name(canonical_tag),
                    str(tag_text or '').strip(),
                    json.dumps(query_terms or [], ensure_ascii=False),
                    json.dumps(match_terms or [], ensure_ascii=False),
                    int(max_pages or 10),
                    int(max_results or 200),
                    int(max_new_jobs or 50),
                    now,
                    now,
                ),
            )
            return int(cursor.lastrowid)

    def get_xhs_backfill_task(self, task_id: int) -> sqlite3.Row | None:
        with self._lock, self._connect() as conn:
            return conn.execute(
                'SELECT * FROM xhs_backfill_tasks WHERE id = ?',
                (int(task_id),),
            ).fetchone()

    def update_xhs_backfill_task(self, task_id: int, **fields: Any) -> None:
        allowed = {
            'page_snapshot_json', 'page_item_index', 'page_size',
            'status',
            'current_query_index',
            'current_query_text',
            'next_page',
            'scanned',
            'detailed',
            'matched',
            'queued',
            'skipped_existing',
            'skipped_rejected',
            'skipped_filtered',
            'skipped_duplicate',
            'failed_details',
            'error_log',
            'completed_at',
        }
        assignments: list[str] = ['updated_at = ?']
        params: list[Any] = [utcnow_str()]
        for key, value in fields.items():
            if key not in allowed:
                continue
            assignments.append(f'{key} = ?')
            params.append(value)
        if len(assignments) == 1:
            return
        params.append(int(task_id))
        with self._lock, self._connect() as conn:
            conn.execute(
                f"UPDATE xhs_backfill_tasks SET {', '.join(assignments)} WHERE id = ?",
                params,
            )

    def list_xhs_backfill_tasks(
        self,
        *,
        limit: int = 20,
        statuses: Iterable[str] | None = None,
    ) -> list[sqlite3.Row]:
        sql = 'SELECT * FROM xhs_backfill_tasks'
        params: list[Any] = []
        if statuses:
            status_values = [str(item).strip() for item in statuses if str(item).strip()]
            if status_values:
                placeholders = ','.join('?' for _ in status_values)
                sql += f' WHERE status IN ({placeholders})'
                params.extend(status_values)
        sql += ' ORDER BY id DESC LIMIT ?'
        params.append(max(1, int(limit or 20)))
        with self._lock, self._connect() as conn:
            return conn.execute(sql, params).fetchall()

    def get_pending_xhs_backfill_task_ids(self) -> list[int]:
        with self._lock, self._connect() as conn:
            rows = conn.execute(
                "SELECT id FROM xhs_backfill_tasks "
                "WHERE status IN ('pending', 'retry') ORDER BY id ASC"
            ).fetchall()
            return [int(row['id']) for row in rows]

    def has_xhs_backfill_item(self, task_id: int, note_id: str) -> bool:
        with self._lock, self._connect() as conn:
            return conn.execute(
                'SELECT 1 FROM xhs_backfill_items WHERE task_id=? AND note_id=?',
                (task_id, note_id),
            ).fetchone() is not None

    def commit_xhs_backfill_item(
        self, task_id: int, *, note_id: str, post_url: str, next_index: int,
        outcome: str, detailed: bool = False, source_context: dict | None = None,
    ) -> int:
        """Commit one disposition, its crawl job, counters and page position together."""
        now = utcnow_str()
        job_id = 0
        with self._lock, self._connect() as conn:
            task = conn.execute('SELECT * FROM xhs_backfill_tasks WHERE id=?', (task_id,)).fetchone()
            if outcome == 'matched':
                existing = conn.execute(
                    'SELECT * FROM crawl_jobs WHERE platform=? AND source_url=? ORDER BY id DESC LIMIT 1',
                    ('xiaohongshu', post_url),
                ).fetchone()
                if existing:
                    job_id = int(existing['id'])
                    self._merge_crawl_job_values_conn(
                        conn, job_id, tags=[task['tag_name']], source_context=source_context,
                        origin='backfill', priority=50, now=now,
                    )
                    outcome = 'skipped_existing'
                else:
                    cursor = conn.execute(
                        """INSERT INTO crawl_jobs(
                            platform, source_url, source_context_json, tags_text,
                            origin, priority, status, progress, attempt_count, created_at, updated_at
                        ) VALUES('xiaohongshu', ?, ?, ?, 'backfill', 50, 'pending', 0, 0, ?, ?)""",
                        (post_url, self._serialize_source_context(source_context), task['tag_name'], now, now),
                    )
                    job_id = int(cursor.lastrowid)
                    outcome = 'queued'
            counters = {key: 0 for key in (
                'queued', 'skipped_existing', 'skipped_rejected', 'skipped_filtered',
                'skipped_duplicate', 'failed_details',
            )}
            counters[outcome] = 1
            assignments = ', '.join(f'{key}={key}+?' for key in counters)
            conn.execute(
                f'UPDATE xhs_backfill_tasks SET {assignments}, scanned=scanned+1, '
                'detailed=detailed+?, matched=matched+?, page_item_index=?, updated_at=? WHERE id=?',
                (*counters.values(), int(detailed), int(source_context is not None), next_index, now, task_id),
            )
            conn.execute(
                'INSERT OR IGNORE INTO xhs_backfill_items(task_id,note_id,outcome,crawl_job_id) VALUES(?,?,?,?)',
                (task_id, note_id, outcome, job_id),
            )
        return job_id

    def clear_xhs_backfill_saturation(self, task_id: int, query_term: str) -> None:
        with self._lock, self._connect() as conn:
            task = conn.execute('SELECT tag_id,created_at FROM xhs_backfill_tasks WHERE id=?', (task_id,)).fetchone()
            conn.execute(
                """UPDATE crawl_subscription_terms
                SET saturated=0, saturated_at='', saturated_reason='', updated_at=?
                WHERE normalized_term=? AND saturated_at<=? AND subscription_id IN (
                    SELECT id FROM crawl_subscriptions WHERE platform='xiaohongshu' AND tag_id=?
                )""",
                (utcnow_str(), normalize_tag_name(query_term), task['created_at'], task['tag_id']),
            )

    def reset_running_xhs_backfill_tasks(self) -> None:
        with self._lock, self._connect() as conn:
            conn.execute(
                "UPDATE xhs_backfill_tasks SET status = 'retry', updated_at = ? "
                "WHERE status = 'running'",
                (utcnow_str(),),
            )

    def create_review_task(self, image_id: int, tag_id: int, status: str, model_result: str = '', reason: str = '') -> int:
        now = utcnow_str()
        with self._lock, self._connect() as conn:
            existing = conn.execute(
                "SELECT id FROM review_tasks WHERE image_id = ? AND tag_id = ? ORDER BY id DESC LIMIT 1",
                (image_id, tag_id),
            ).fetchone()
            if existing:
                conn.execute(
                    "UPDATE review_tasks SET status = ?, model_result = ?, reason = ?, updated_at = ? WHERE id = ?",
                    (status, model_result, reason, now, existing['id']),
                )
                return int(existing['id'])
            cursor = conn.execute(
                """
                INSERT INTO review_tasks(image_id, tag_id, status, model_result, manual_result, reason, created_at, updated_at)
                VALUES(?, ?, ?, ?, '', ?, ?, ?)
                """,
                (image_id, tag_id, status, model_result, reason, now, now),
            )
            return int(cursor.lastrowid)

    def list_review_tasks(
        self,
        *,
        status: str | None = None,
        statuses: Iterable[str] | None = None,
        limit: int = 20,
    ) -> list[sqlite3.Row]:
        sql = """
            SELECT rt.id, rt.status, rt.reason, rt.model_result, rt.manual_result,
                   rt.created_at, rt.updated_at,
                   i.id AS image_id, i.file_path,
                   t.id AS tag_id, t.name AS tag_name,
                   COALESCE((
                       SELECT it.source_type
                       FROM image_tags it
                       WHERE it.image_id = rt.image_id AND it.tag_id = rt.tag_id
                       ORDER BY CASE
                           WHEN it.source_type LIKE 'crawl:%' THEN 0
                           WHEN it.source_type LIKE 'manual:%' THEN 1
                           ELSE 2
                       END, it.id DESC
                       LIMIT 1
                   ), '') AS source_type
            FROM review_tasks rt
            JOIN images i ON i.id = rt.image_id
            JOIN tags t ON t.id = rt.tag_id
        """
        params: list[Any] = []
        normalized_statuses = [str(item).strip() for item in (statuses or []) if str(item).strip()]
        if normalized_statuses:
            placeholders = ",".join("?" for _ in normalized_statuses)
            sql += f" WHERE rt.status IN ({placeholders})"
            params.extend(normalized_statuses)
        elif status:
            sql += ' WHERE rt.status = ?'
            params.append(status)
        sql += ' ORDER BY rt.id DESC LIMIT ?'
        params.append(limit)
        with self._lock, self._connect() as conn:
            return conn.execute(sql, params).fetchall()

    def get_review_task(self, review_id: int) -> sqlite3.Row | None:
        with self._lock, self._connect() as conn:
            return conn.execute(
                """
                SELECT rt.id, rt.status, rt.reason, rt.model_result, rt.manual_result,
                       i.id AS image_id, i.file_path,
                       t.id AS tag_id, t.name AS tag_name,
                       COALESCE((
                           SELECT it.source_type
                           FROM image_tags it
                           WHERE it.image_id = rt.image_id AND it.tag_id = rt.tag_id
                           ORDER BY CASE
                               WHEN it.source_type LIKE 'crawl:%' THEN 0
                               WHEN it.source_type LIKE 'manual:%' THEN 1
                               ELSE 2
                           END, it.id DESC
                           LIMIT 1
                       ), '') AS source_type
                FROM review_tasks rt
                JOIN images i ON i.id = rt.image_id
                JOIN tags t ON t.id = rt.tag_id
                WHERE rt.id = ?
                """,
                (review_id,),
            ).fetchone()

    def apply_manual_review(self, review_id: int, *, approved: bool, reason: str = '') -> tuple[bool, str]:
        new_status = 'manual_approved' if approved else 'manual_rejected'
        now = utcnow_str()
        review_reason = reason or ('人工通过' if approved else '人工拒绝')
        with self._lock, self._connect() as conn:
            task = conn.execute(
                """
                SELECT rt.id, rt.image_id, rt.tag_id,
                       COALESCE((
                           SELECT it.source_type
                           FROM image_tags it
                           WHERE it.image_id = rt.image_id AND it.tag_id = rt.tag_id
                           ORDER BY CASE
                               WHEN it.source_type LIKE 'crawl:%' THEN 0
                               WHEN it.source_type LIKE 'manual:%' THEN 1
                               ELSE 2
                           END, it.id DESC
                           LIMIT 1
                       ), '') AS source_type
                FROM review_tasks rt
                WHERE rt.id = ?
                LIMIT 1
                """,
                (review_id,),
            ).fetchone()
            if not task:
                return False, f'审核任务不存在：{review_id}'
            conn.execute(
                "UPDATE review_tasks SET status = ?, manual_result = ?, reason = ?, updated_at = ? WHERE id = ?",
                (new_status, 'approved' if approved else 'rejected', reason, now, review_id),
            )
            source_type = str(task['source_type'] or '').strip()
            if source_type:
                conn.execute(
                    """
                    UPDATE image_tags
                    SET review_status = ?, review_reason = ?, updated_at = ?
                    WHERE image_id = ? AND tag_id = ? AND source_type = ?
                    """,
                    (new_status, review_reason, now, int(task['image_id']), int(task['tag_id']), source_type),
                )
            else:
                conn.execute(
                    """
                    UPDATE image_tags
                    SET review_status = ?, review_reason = ?, updated_at = ?
                    WHERE image_id = ? AND tag_id = ? AND source_type = ''
                    """,
                    (new_status, review_reason, now, int(task['image_id']), int(task['tag_id'])),
                )
        return True, f"已{'通过' if approved else '拒绝'}审核任务 #{review_id}"


    def get_review_tasks_for_image(
        self,
        image_id: int,
        *,
        statuses: Iterable[str] | None = None,
    ) -> list[sqlite3.Row]:
        sql = """
            SELECT rt.id, rt.status, rt.reason, rt.model_result, rt.manual_result,
                   rt.created_at, rt.updated_at,
                   t.id AS tag_id, t.name AS tag_name, t.is_character,
                   COALESCE((
                       SELECT it.source_type
                       FROM image_tags it
                       WHERE it.image_id = rt.image_id AND it.tag_id = rt.tag_id
                       ORDER BY CASE
                           WHEN it.source_type LIKE 'crawl:%' THEN 0
                           WHEN it.source_type LIKE 'manual:%' THEN 1
                           ELSE 2
                       END, it.id DESC
                       LIMIT 1
                   ), '') AS source_type
            FROM review_tasks rt
            JOIN tags t ON t.id = rt.tag_id
            WHERE rt.image_id = ?
        """
        params: list[Any] = [image_id]
        normalized_statuses = [str(item).strip() for item in (statuses or []) if str(item).strip()]
        if normalized_statuses:
            placeholders = ','.join('?' for _ in normalized_statuses)
            sql += f' AND rt.status IN ({placeholders})'
            params.extend(normalized_statuses)
        sql += ' ORDER BY rt.id DESC'
        with self._lock, self._connect() as conn:
            return conn.execute(sql, params).fetchall()

    def get_llm_review_image(self, image_id: int) -> sqlite3.Row | None:
        with self._lock, self._connect() as conn:
            return conn.execute(
                """
                SELECT i.id, i.file_path, i.file_name, i.sha256, i.width, i.height, i.format,
                       COALESCE((
                           SELECT s.platform
                           FROM sources s
                           WHERE s.image_id = i.id
                           ORDER BY CASE s.platform
                               WHEN 'xiaohongshu' THEN 0
                               WHEN 'pixiv' THEN 1
                               WHEN 'submission' THEN 2
                               ELSE 3
                           END, s.id DESC
                           LIMIT 1
                       ), '') AS platform
                FROM images i
                WHERE i.id = ? AND i.is_active = 1
                LIMIT 1
                """,
                (int(image_id),),
            ).fetchone()

    def get_llm_review_candidates(
        self,
        image_id: int,
        *,
        statuses: Iterable[str] | None = None,
        max_candidates: int = 8,
        alias_limit: int = 6,
    ) -> list[dict[str, Any]]:
        normalized_statuses = split_status_filter(statuses or ('pending', 'uncertain'))
        if not normalized_statuses:
            return []
        placeholders = ','.join('?' for _ in normalized_statuses)
        candidate_limit = min(max(1, int(max_candidates or 8)), 20)
        aliases_per_tag = min(max(0, int(alias_limit or 0)), 20)
        with self._lock, self._connect() as conn:
            rows = conn.execute(
                f"""
                SELECT t.id AS tag_id, t.name AS tag_name, MAX(rt.id) AS latest_review_id
                FROM review_tasks rt
                JOIN tags t ON t.id = rt.tag_id
                WHERE rt.image_id = ?
                  AND rt.status IN ({placeholders})
                  AND t.status = 'active'
                  AND (t.tag_type = 'character' OR t.is_character = 1)
                GROUP BY t.id, t.name
                ORDER BY latest_review_id DESC, t.id ASC
                LIMIT ?
                """,
                (int(image_id), *normalized_statuses, candidate_limit),
            ).fetchall()
            result: list[dict[str, Any]] = []
            for row in rows:
                aliases: list[str] = []
                if aliases_per_tag > 0:
                    alias_rows = conn.execute(
                        """
                        SELECT alias
                        FROM tag_aliases
                        WHERE tag_id = ?
                        ORDER BY id ASC
                        LIMIT ?
                        """,
                        (int(row['tag_id']), aliases_per_tag),
                    ).fetchall()
                    aliases = [str(alias_row['alias']) for alias_row in alias_rows]
                result.append(
                    {
                        'tag_id': int(row['tag_id']),
                        'tag_name': str(row['tag_name']),
                        'aliases': aliases,
                    }
                )
        return result

    def list_llm_review_image_ids(
        self,
        *,
        statuses: Iterable[str] | None = None,
        platform: str = '',
        limit: int = 20,
        newest_first: bool = True,
    ) -> list[int]:
        normalized_statuses = split_status_filter(statuses or ('pending', 'uncertain'))
        if not normalized_statuses:
            return []
        placeholders = ','.join('?' for _ in normalized_statuses)
        sql = f"""
            SELECT rt.image_id, MAX(rt.id) AS latest_review_id
            FROM review_tasks rt
            JOIN images i ON i.id = rt.image_id
            JOIN tags t ON t.id = rt.tag_id
            WHERE i.is_active = 1
              AND rt.status IN ({placeholders})
              AND t.status = 'active'
              AND (t.tag_type = 'character' OR t.is_character = 1)
        """
        params: list[Any] = list(normalized_statuses)
        platform_text = str(platform or '').strip().lower()
        if platform_text:
            sql += " AND EXISTS (SELECT 1 FROM sources s WHERE s.image_id = rt.image_id AND s.platform = ?)"
            params.append(platform_text)
        sql += ' GROUP BY rt.image_id ORDER BY latest_review_id ' + ('DESC' if newest_first else 'ASC') + ' LIMIT ?'
        params.append(min(max(1, int(limit or 20)), 500))
        with self._lock, self._connect() as conn:
            rows = conn.execute(sql, params).fetchall()
        return [int(row['image_id']) for row in rows]

    def create_llm_image_review_run(
        self,
        *,
        image_id: int,
        platform: str,
        mode: str,
        provider_id: str,
        prompt_version: str,
        input_fingerprint: str,
        image_sha256: str,
        candidates: list[dict[str, Any]],
    ) -> tuple[int, bool]:
        now = utcnow_str()
        fingerprint = str(input_fingerprint or '').strip()
        if not fingerprint:
            raise ValueError('LLM 审图输入指纹不能为空')
        with self._lock, self._connect() as conn:
            existing = conn.execute(
                'SELECT id FROM llm_image_review_runs WHERE input_fingerprint = ? LIMIT 1',
                (fingerprint,),
            ).fetchone()
            if existing:
                return int(existing['id']), False
            cursor = conn.execute(
                """
                INSERT INTO llm_image_review_runs(
                    image_id, platform, mode, provider_id, prompt_version,
                    input_fingerprint, image_sha256, candidates_json, status,
                    attempt_count, created_at, updated_at
                )
                VALUES(?, ?, ?, ?, ?, ?, ?, ?, 'pending', 0, ?, ?)
                """,
                (
                    int(image_id),
                    str(platform or '').strip().lower(),
                    str(mode or 'shadow').strip().lower(),
                    str(provider_id or '').strip(),
                    str(prompt_version or 'v1').strip(),
                    fingerprint,
                    str(image_sha256 or '').strip(),
                    json.dumps(candidates, ensure_ascii=False, separators=(',', ':')),
                    now,
                    now,
                ),
            )
            return int(cursor.lastrowid), True

    def reset_running_llm_image_review_runs(self) -> int:
        now = utcnow_str()
        with self._lock, self._connect() as conn:
            cursor = conn.execute(
                """
                UPDATE llm_image_review_runs
                SET status = 'pending', error_log = 'worker interrupted before completion', updated_at = ?
                WHERE status = 'running'
                """,
                (now,),
            )
            return max(0, int(cursor.rowcount or 0))

    def claim_next_llm_image_review_run(self) -> sqlite3.Row | None:
        now = utcnow_str()
        with self._lock, self._connect() as conn:
            conn.execute('BEGIN IMMEDIATE')
            row = conn.execute(
                """
                SELECT *
                FROM llm_image_review_runs
                WHERE status = 'pending'
                ORDER BY id ASC
                LIMIT 1
                """
            ).fetchone()
            if not row:
                return None
            conn.execute(
                """
                UPDATE llm_image_review_runs
                SET status = 'running', attempt_count = attempt_count + 1,
                    error_log = '', updated_at = ?
                WHERE id = ? AND status = 'pending'
                """,
                (now, int(row['id'])),
            )
            return conn.execute(
                'SELECT * FROM llm_image_review_runs WHERE id = ? LIMIT 1',
                (int(row['id']),),
            ).fetchone()

    def complete_llm_image_review_run(
        self,
        run_id: int,
        *,
        decision: str,
        quality: dict[str, Any],
        selected_tags: list[dict[str, Any]],
        result: dict[str, Any],
        raw_result: str,
        reason: str,
    ) -> None:
        now = utcnow_str()
        with self._lock, self._connect() as conn:
            conn.execute(
                """
                UPDATE llm_image_review_runs
                SET status = 'completed', decision = ?, quality_json = ?,
                    selected_tags_json = ?, result_json = ?, raw_result = ?,
                    reason = ?, error_log = '', completed_at = ?, updated_at = ?
                WHERE id = ?
                """,
                (
                    str(decision or '').strip(),
                    json.dumps(quality, ensure_ascii=False, separators=(',', ':')),
                    json.dumps(selected_tags, ensure_ascii=False, separators=(',', ':')),
                    json.dumps(result, ensure_ascii=False, separators=(',', ':')),
                    str(raw_result or '')[:8000],
                    str(reason or '')[:1000],
                    now,
                    now,
                    int(run_id),
                ),
            )

    def fail_llm_image_review_run(
        self,
        run_id: int,
        *,
        error: str,
        retry: bool,
    ) -> None:
        now = utcnow_str()
        error_text = str(error or '')[:1000]
        with self._lock, self._connect() as conn:
            row = conn.execute(
                'SELECT attempt_count, error_history_json FROM llm_image_review_runs WHERE id = ? LIMIT 1',
                (int(run_id),),
            ).fetchone()
            try:
                history = json.loads(str(row['error_history_json'] or '[]')) if row else []
            except (TypeError, json.JSONDecodeError):
                history = []
            if not isinstance(history, list):
                history = []
            history.append(
                {
                    'attempt': int(row['attempt_count'] or 0) if row else 0,
                    'at': now,
                    'error': error_text,
                }
            )
            history = history[-20:]
            conn.execute(
                """
                UPDATE llm_image_review_runs
                SET status = ?, error_log = ?, error_history_json = ?, updated_at = ?,
                    completed_at = CASE WHEN ? = 'failed' THEN ? ELSE '' END
                WHERE id = ?
                """,
                (
                    'pending' if retry else 'failed',
                    error_text,
                    json.dumps(history, ensure_ascii=False, separators=(',', ':')),
                    now,
                    'pending' if retry else 'failed',
                    now,
                    int(run_id),
                ),
            )

    def retry_failed_llm_image_review_runs(self, *, limit: int = 20) -> int:
        now = utcnow_str()
        with self._lock, self._connect() as conn:
            rows = conn.execute(
                """
                SELECT id FROM llm_image_review_runs
                WHERE status = 'failed'
                ORDER BY id ASC
                LIMIT ?
                """,
                (min(max(1, int(limit or 20)), 200),),
            ).fetchall()
            ids = [int(row['id']) for row in rows]
            if not ids:
                return 0
            placeholders = ','.join('?' for _ in ids)
            conn.execute(
                f"""
                UPDATE llm_image_review_runs
                SET status = 'pending', error_log = '', completed_at = '', updated_at = ?
                WHERE id IN ({placeholders})
                """,
                (now, *ids),
            )
            return len(ids)

    def get_latest_llm_image_review_run(
        self,
        image_id: int,
        *,
        completed_only: bool = True,
    ) -> sqlite3.Row | None:
        sql = 'SELECT * FROM llm_image_review_runs WHERE image_id = ?'
        params: list[Any] = [int(image_id)]
        if completed_only:
            sql += " AND status = 'completed'"
        sql += ' ORDER BY id DESC LIMIT 1'
        with self._lock, self._connect() as conn:
            return conn.execute(sql, params).fetchone()

    def count_llm_image_review_runs_since(self, since: str) -> int:
        with self._lock, self._connect() as conn:
            row = conn.execute(
                """
                SELECT COUNT(*) AS c
                FROM llm_image_review_runs
                WHERE attempt_count > 0 AND updated_at >= ?
                """,
                (str(since or ''),),
            ).fetchone()
        return int(row['c'] or 0) if row else 0

    def get_llm_image_review_stats(self) -> dict[str, int]:
        stats = {
            'pending': 0,
            'running': 0,
            'completed': 0,
            'failed': 0,
        }
        with self._lock, self._connect() as conn:
            rows = conn.execute(
                'SELECT status, COUNT(*) AS c FROM llm_image_review_runs GROUP BY status'
            ).fetchall()
        for row in rows:
            stats[str(row['status'])] = int(row['c'] or 0)
        return stats

    def apply_llm_image_review_approval(
        self,
        run_id: int,
        *,
        selected_tags: list[dict[str, Any]],
        model_result: str,
        reason: str,
    ) -> tuple[bool, dict[str, Any]]:
        selected_by_id: dict[int, float] = {}
        for item in selected_tags:
            try:
                tag_id = int(item.get('tag_id', 0) or 0)
                confidence = max(0.0, min(1.0, float(item.get('confidence', 0) or 0)))
            except (AttributeError, TypeError, ValueError):
                continue
            if tag_id > 0:
                selected_by_id[tag_id] = max(selected_by_id.get(tag_id, 0.0), confidence)
        if not selected_by_id:
            return False, {'code': 'no_selected_tags', 'message': '模型没有选择任何候选 tag'}

        now = utcnow_str()
        review_reason = str(reason or '').strip()[:1000] or 'LLM 高置信度自动通过'
        serialized_result = str(model_result or '')[:8000]
        with self._lock, self._connect() as conn:
            conn.execute('BEGIN IMMEDIATE')
            run = conn.execute(
                'SELECT * FROM llm_image_review_runs WHERE id = ? LIMIT 1',
                (int(run_id),),
            ).fetchone()
            if not run or str(run['status']) != 'running':
                return False, {'code': 'stale_run', 'message': 'LLM 审图运行状态已变化'}
            try:
                candidates = json.loads(str(run['candidates_json'] or '[]'))
            except (TypeError, json.JSONDecodeError):
                candidates = []
            candidate_ids: set[int] = set()
            for item in candidates:
                if not isinstance(item, dict):
                    continue
                try:
                    candidate_id = int(item.get('tag_id', 0) or 0)
                except (TypeError, ValueError):
                    continue
                if candidate_id > 0:
                    candidate_ids.add(candidate_id)
            if not candidate_ids or not set(selected_by_id).issubset(candidate_ids):
                return False, {'code': 'candidate_mismatch', 'message': '模型选择超出候选范围'}

            placeholders = ','.join('?' for _ in candidate_ids)
            task_rows = conn.execute(
                f"""
                SELECT rt.id, rt.tag_id, rt.status, rt.manual_result,
                       t.name AS tag_name, t.status AS tag_status,
                       t.tag_type, t.is_character
                FROM review_tasks rt
                JOIN tags t ON t.id = rt.tag_id
                WHERE rt.image_id = ? AND rt.tag_id IN ({placeholders})
                ORDER BY rt.id DESC
                """,
                (int(run['image_id']), *sorted(candidate_ids)),
            ).fetchall()
            latest_by_tag: dict[int, sqlite3.Row] = {}
            for task in task_rows:
                latest_by_tag.setdefault(int(task['tag_id']), task)
            if set(latest_by_tag) != candidate_ids:
                return False, {'code': 'stale_candidates', 'message': '候选审核任务已变化'}
            if any(
                str(task['tag_status'] or '') != 'active'
                or (
                    str(task['tag_type'] or '') != 'character'
                    and int(task['is_character'] or 0) != 1
                )
                for task in latest_by_tag.values()
            ):
                return False, {'code': 'inactive_candidate', 'message': '候选 tag 已停用或不再是角色'}
            if any(
                str(task['status'] or '') not in {'pending', 'uncertain'}
                or str(task['manual_result'] or '').strip()
                for task in latest_by_tag.values()
            ):
                return False, {'code': 'manual_review_won', 'message': '人工审核已先完成，模型不覆盖'}

            approved_names: list[str] = []
            rejected_names: list[str] = []
            for tag_id, task in latest_by_tag.items():
                selected = tag_id in selected_by_id
                status = 'approved' if selected else 'rejected'
                confidence = selected_by_id.get(tag_id, 0.0)
                conn.execute(
                    """
                    UPDATE review_tasks
                    SET status = ?, model_result = ?, reason = ?, updated_at = ?
                    WHERE id = ? AND status IN ('pending', 'uncertain')
                      AND TRIM(COALESCE(manual_result, '')) = ''
                    """,
                    (status, serialized_result, review_reason, now, int(task['id'])),
                )
                cursor = conn.execute(
                    """
                    UPDATE image_tags
                    SET review_status = ?, score = ?, review_reason = ?, updated_at = ?
                    WHERE image_id = ? AND tag_id = ?
                      AND review_status IN ('pending', 'uncertain', 'rejected')
                    """,
                    (
                        status,
                        confidence,
                        review_reason,
                        now,
                        int(run['image_id']),
                        tag_id,
                    ),
                )
                if int(cursor.rowcount or 0) == 0 and selected:
                    conn.execute(
                        """
                        INSERT INTO image_tags(
                            image_id, tag_id, source_type, score, review_status,
                            review_reason, created_at, updated_at
                        )
                        VALUES(?, ?, 'llm:auto_review', ?, 'approved', ?, ?, ?)
                        """,
                        (int(run['image_id']), tag_id, confidence, review_reason, now, now),
                    )
                if selected:
                    approved_names.append(str(task['tag_name']))
                else:
                    rejected_names.append(str(task['tag_name']))

        return True, {
            'code': 'approved',
            'image_id': int(run['image_id']),
            'approved_tags': approved_names,
            'rejected_tags': rejected_names,
        }

    def _build_pixiv_review_images_query(
        self,
        *,
        statuses: Iterable[str] | None = None,
        keyword: str = '',
        search_context: dict[str, Any] | None = None,
        count: bool = False,
    ) -> tuple[str, list[Any]]:
        normalized_statuses = split_status_filter(statuses or ['pending', 'uncertain'])
        keyword_text = str(keyword or '').strip()
        context = search_context if search_context is not None else self.build_pixiv_review_search_context(keyword_text)
        select_sql = "SELECT i.id AS image_id" if count else """
            SELECT i.id AS image_id,
                   MAX(rt.id) AS latest_review_id,
                   MAX(rt.updated_at) AS latest_updated_at,
                   COUNT(DISTINCT rt.id) AS review_task_count,
                   COUNT(DISTINCT CASE WHEN rt.status IN ('pending', 'uncertain') THEN rt.id END) AS pending_task_count
        """
        sql = f"""
            {select_sql}
            FROM review_tasks rt
            JOIN images i ON i.id = rt.image_id
            WHERE i.is_active = 1
              AND EXISTS (
                  SELECT 1 FROM sources s
                  WHERE s.image_id = rt.image_id AND s.platform = 'pixiv'
              )
        """
        params: list[Any] = []
        if normalized_statuses:
            placeholders = ','.join('?' for _ in normalized_statuses)
            sql += f' AND rt.status IN ({placeholders})'
            params.extend(normalized_statuses)
        if keyword_text and context.get('has_query'):
            search_clauses: list[str] = []
            search_params: list[Any] = []
            target_ids = [
                int(item)
                for item in (context.get('tag_ids') or [])
                if int(item or 0) > 0
            ]
            if target_ids:
                id_placeholders = ','.join('?' for _ in target_ids)
                review_status_sql = ''
                review_status_params: list[Any] = []
                if normalized_statuses:
                    review_status_placeholders = ','.join('?' for _ in normalized_statuses)
                    review_status_sql = f' AND rt2.status IN ({review_status_placeholders})'
                    review_status_params.extend(normalized_statuses)
                search_clauses.append(
                    f"""
                    EXISTS (
                        SELECT 1
                        FROM review_tasks rt2
                        WHERE rt2.image_id = i.id
                          AND rt2.tag_id IN ({id_placeholders})
                          {review_status_sql}
                    )
                    """
                )
                search_params.extend(target_ids)
                search_params.extend(review_status_params)
                search_clauses.append(
                    f"""
                    EXISTS (
                        SELECT 1
                        FROM image_tags it2
                        WHERE it2.image_id = i.id
                          AND it2.tag_id IN ({id_placeholders})
                    )
                    """
                )
                search_params.extend(target_ids)

            normalized_terms = [
                normalize_tag_name(str(item or ''))
                for item in (context.get('terms') or [])
                if normalize_tag_name(str(item or ''))
            ]
            normalized_terms = list(dict.fromkeys(normalized_terms))[:80]
            if normalized_terms:
                tag_like_clauses = []
                tag_like_params: list[Any] = []
                for term in normalized_terms:
                    tag_like_clauses.append('t2.normalized_name LIKE ?')
                    tag_like_params.append(f'%{term}%')
                search_clauses.append(
                    f"""
                    EXISTS (
                        SELECT 1
                        FROM review_tasks rt2
                        JOIN tags t2 ON t2.id = rt2.tag_id
                        WHERE rt2.image_id = i.id
                          {'AND rt2.status IN (' + ','.join('?' for _ in normalized_statuses) + ')' if normalized_statuses else ''}
                          AND ({' OR '.join(tag_like_clauses)})
                    )
                    """
                )
                if normalized_statuses:
                    search_params.extend(normalized_statuses)
                search_params.extend(tag_like_params)
                search_clauses.append(
                    f"""
                    EXISTS (
                        SELECT 1
                        FROM image_tags it2
                        JOIN tags t2 ON t2.id = it2.tag_id
                        WHERE it2.image_id = i.id
                          AND ({' OR '.join(tag_like_clauses)})
                    )
                    """
                )
                search_params.extend(tag_like_params)

            text_terms = self._dedupe_terms([str(item) for item in (context.get('terms') or [])])[:80]
            if text_terms:
                source_clauses = []
                source_params: list[Any] = []
                for term in text_terms:
                    source_clauses.append('(s2.raw_tags LIKE ? OR s2.extra_json LIKE ?)')
                    source_params.extend([f'%{term}%', f'%{term}%'])
                search_clauses.append(
                    f"""
                    EXISTS (
                        SELECT 1
                        FROM sources s2
                        WHERE s2.image_id = i.id
                          AND s2.platform = 'pixiv'
                          AND ({' OR '.join(source_clauses)})
                    )
                    """
                )
                search_params.extend(source_params)

            if search_clauses:
                sql += ' AND (' + ' OR '.join(search_clauses) + ')'
                params.extend(search_params)
        return sql, params

    def list_pixiv_review_images(
        self,
        *,
        statuses: Iterable[str] | None = None,
        limit: int = 20,
        offset: int = 0,
        keyword: str = '',
        search_context: dict[str, Any] | None = None,
    ) -> list[sqlite3.Row]:
        sql, params = self._build_pixiv_review_images_query(
            statuses=statuses,
            keyword=keyword,
            search_context=search_context,
        )
        sql += ' GROUP BY i.id ORDER BY latest_review_id DESC LIMIT ? OFFSET ?'
        params.extend([limit, offset])
        with self._lock, self._connect() as conn:
            return conn.execute(sql, params).fetchall()

    def count_pixiv_review_images(
        self,
        *,
        statuses: Iterable[str] | None = None,
        keyword: str = '',
        search_context: dict[str, Any] | None = None,
    ) -> int:
        sql, params = self._build_pixiv_review_images_query(
            statuses=statuses,
            keyword=keyword,
            search_context=search_context,
            count=True,
        )
        count_sql = f"SELECT COUNT(*) AS total FROM ({sql} GROUP BY i.id) AS pixiv_review_page"
        with self._lock, self._connect() as conn:
            row = conn.execute(count_sql, params).fetchone()
        return int(row["total"] or 0) if row else 0

    @staticmethod
    def _normalize_review_statuses(statuses: Iterable[str] | None = None) -> list[str]:
        return split_status_filter(statuses or ["pending", "uncertain"])

    def get_random_review_image(
        self,
        *,
        platform: str,
        statuses: Iterable[str] | None = None,
        candidate_tag_id: int | None = None,
        exclude_image_ids: Iterable[int] | None = None,
    ) -> sqlite3.Row | None:
        normalized_statuses = self._normalize_review_statuses(statuses)
        platform_text = str(platform or '').strip().lower()
        if not normalized_statuses or not platform_text:
            return None
        status_placeholders = ",".join("?" for _ in normalized_statuses)
        sql = f"""
            SELECT i.id AS image_id,
                   i.file_path,
                   MAX(rt.id) AS latest_review_id,
                   COUNT(DISTINCT rt.id) AS review_task_count
            FROM images i
            JOIN review_tasks rt ON rt.image_id = i.id
            WHERE i.is_active = 1
              AND rt.status IN ({status_placeholders})
              AND EXISTS (
                  SELECT 1 FROM sources s
                  WHERE s.image_id = i.id AND s.platform = ?
              )
        """
        params: list[Any] = [*normalized_statuses, platform_text]
        wanted_tag_id = int(candidate_tag_id or 0)
        if wanted_tag_id > 0:
            sql += f"""
              AND EXISTS (
                  SELECT 1
                  FROM review_tasks rt_filter
                  WHERE rt_filter.image_id = i.id
                    AND rt_filter.tag_id = ?
                    AND rt_filter.status IN ({status_placeholders})
              )
            """
            params.append(wanted_tag_id)
            params.extend(normalized_statuses)
        excluded = sorted(
            {int(item) for item in (exclude_image_ids or []) if int(item or 0) > 0}
        )[:500]
        if excluded:
            placeholders = ",".join("?" for _ in excluded)
            sql += f" AND i.id NOT IN ({placeholders})"
            params.extend(excluded)
        sql += " GROUP BY i.id ORDER BY RANDOM() LIMIT 1"
        with self._lock, self._connect() as conn:
            return conn.execute(sql, params).fetchone()

    def count_open_review_images(
        self,
        *,
        platform: str,
        statuses: Iterable[str] | None = None,
        candidate_tag_id: int | None = None,
    ) -> int:
        normalized_statuses = self._normalize_review_statuses(statuses)
        platform_text = str(platform or '').strip().lower()
        if not normalized_statuses or not platform_text:
            return 0
        status_placeholders = ",".join("?" for _ in normalized_statuses)
        sql = f"""
            SELECT COUNT(DISTINCT i.id) AS total
            FROM images i
            JOIN review_tasks rt ON rt.image_id = i.id
            WHERE i.is_active = 1
              AND rt.status IN ({status_placeholders})
              AND EXISTS (
                  SELECT 1 FROM sources s
                  WHERE s.image_id = i.id AND s.platform = ?
              )
        """
        params: list[Any] = [*normalized_statuses, platform_text]
        wanted_tag_id = int(candidate_tag_id or 0)
        if wanted_tag_id > 0:
            sql += f"""
              AND EXISTS (
                  SELECT 1
                  FROM review_tasks rt_filter
                  WHERE rt_filter.image_id = i.id
                    AND rt_filter.tag_id = ?
                    AND rt_filter.status IN ({status_placeholders})
              )
            """
            params.append(wanted_tag_id)
            params.extend(normalized_statuses)
        with self._lock, self._connect() as conn:
            row = conn.execute(sql, params).fetchone()
        return int(row["total"] or 0) if row else 0

    def is_open_review_image(
        self,
        image_id: int,
        *,
        platform: str,
        statuses: Iterable[str] | None = None,
    ) -> bool:
        normalized_statuses = self._normalize_review_statuses(statuses)
        platform_text = str(platform or '').strip().lower()
        if not normalized_statuses or not platform_text:
            return False
        placeholders = ",".join("?" for _ in normalized_statuses)
        sql = f"""
            SELECT 1
            FROM images i
            JOIN review_tasks rt ON rt.image_id = i.id
            WHERE i.id = ?
              AND i.is_active = 1
              AND rt.status IN ({placeholders})
              AND EXISTS (
                  SELECT 1 FROM sources s
                  WHERE s.image_id = i.id AND s.platform = ?
              )
            LIMIT 1
        """
        with self._lock, self._connect() as conn:
            return conn.execute(
                sql,
                (int(image_id), *normalized_statuses, platform_text),
            ).fetchone() is not None

    def get_random_pixiv_review_image(
        self,
        *,
        statuses: Iterable[str] | None = None,
        candidate_tag_id: int | None = None,
        exclude_image_ids: Iterable[int] | None = None,
    ) -> sqlite3.Row | None:
        normalized_statuses = self._normalize_review_statuses(statuses)
        if not normalized_statuses:
            return None
        status_placeholders = ",".join("?" for _ in normalized_statuses)
        sql = f"""
            SELECT i.id AS image_id,
                   i.file_path,
                   MAX(rt.id) AS latest_review_id,
                   COUNT(DISTINCT rt.id) AS review_task_count
            FROM images i
            JOIN review_tasks rt ON rt.image_id = i.id
            WHERE i.is_active = 1
              AND rt.status IN ({status_placeholders})
              AND EXISTS (
                  SELECT 1 FROM sources s
                  WHERE s.image_id = i.id AND s.platform = 'pixiv'
              )
        """
        params: list[Any] = list(normalized_statuses)
        wanted_tag_id = int(candidate_tag_id or 0)
        if wanted_tag_id > 0:
            sql += f"""
              AND EXISTS (
                  SELECT 1
                  FROM review_tasks rt_filter
                  WHERE rt_filter.image_id = i.id
                    AND rt_filter.tag_id = ?
                    AND rt_filter.status IN ({status_placeholders})
              )
            """
            params.append(wanted_tag_id)
            params.extend(normalized_statuses)

        excluded = sorted(
            {
                int(item)
                for item in (exclude_image_ids or [])
                if int(item or 0) > 0
            }
        )[:500]
        if excluded:
            placeholders = ",".join("?" for _ in excluded)
            sql += f" AND i.id NOT IN ({placeholders})"
            params.extend(excluded)
        sql += " GROUP BY i.id ORDER BY RANDOM() LIMIT 1"
        with self._lock, self._connect() as conn:
            return conn.execute(sql, params).fetchone()

    def count_open_pixiv_review_images(
        self,
        *,
        statuses: Iterable[str] | None = None,
        candidate_tag_id: int | None = None,
    ) -> int:
        normalized_statuses = self._normalize_review_statuses(statuses)
        if not normalized_statuses:
            return 0
        status_placeholders = ",".join("?" for _ in normalized_statuses)
        sql = f"""
            SELECT COUNT(DISTINCT i.id) AS total
            FROM images i
            JOIN review_tasks rt ON rt.image_id = i.id
            WHERE i.is_active = 1
              AND rt.status IN ({status_placeholders})
              AND EXISTS (
                  SELECT 1 FROM sources s
                  WHERE s.image_id = i.id AND s.platform = 'pixiv'
              )
        """
        params: list[Any] = list(normalized_statuses)
        wanted_tag_id = int(candidate_tag_id or 0)
        if wanted_tag_id > 0:
            sql += f"""
              AND EXISTS (
                  SELECT 1
                  FROM review_tasks rt_filter
                  WHERE rt_filter.image_id = i.id
                    AND rt_filter.tag_id = ?
                    AND rt_filter.status IN ({status_placeholders})
              )
            """
            params.append(wanted_tag_id)
            params.extend(normalized_statuses)
        with self._lock, self._connect() as conn:
            row = conn.execute(sql, params).fetchone()
        return int(row["total"] or 0) if row else 0

    def is_open_pixiv_review_image(
        self,
        image_id: int,
        *,
        statuses: Iterable[str] | None = None,
    ) -> bool:
        normalized_statuses = self._normalize_review_statuses(statuses)
        if not normalized_statuses:
            return False
        placeholders = ",".join("?" for _ in normalized_statuses)
        sql = f"""
            SELECT 1
            FROM images i
            JOIN review_tasks rt ON rt.image_id = i.id
            WHERE i.id = ?
              AND i.is_active = 1
              AND rt.status IN ({placeholders})
              AND EXISTS (
                  SELECT 1 FROM sources s
                  WHERE s.image_id = i.id AND s.platform = 'pixiv'
              )
            LIMIT 1
        """
        with self._lock, self._connect() as conn:
            return conn.execute(sql, (int(image_id), *normalized_statuses)).fetchone() is not None

    def build_pixiv_review_search_context(self, keyword: str, *, platform: str = 'pixiv') -> dict[str, Any]:
        keyword_text = str(keyword or '').strip()
        normalized_keyword = normalize_tag_name(keyword_text)
        platform_text = str(platform or 'pixiv').strip().lower() or 'pixiv'
        context: dict[str, Any] = {
            'keyword': keyword_text,
            'normalized_keyword': normalized_keyword,
            'has_query': bool(keyword_text and normalized_keyword),
            'matched_tags': [],
            'expanded_terms': [],
            'terms': [],
            'tag_ids': [],
            'message': '',
        }
        if not context['has_query']:
            return context

        seen_tag_ids: set[int] = set()
        seen_terms: set[str] = set()

        def push_term(value: str) -> None:
            text = str(value or '').strip()
            normalized = normalize_tag_name(text)
            if not text or not normalized or normalized in seen_terms:
                return
            seen_terms.add(normalized)
            context['terms'].append(text)
            context['expanded_terms'].append(text)

        def push_tag(
            conn: sqlite3.Connection,
            row: sqlite3.Row | None,
            match_type: str,
            *,
            include_suggestions: bool = True,
        ) -> None:
            if not row:
                return
            tag_id = int(row['id'])
            tag_name = str(row['name'])
            if tag_id not in seen_tag_ids:
                seen_tag_ids.add(tag_id)
                context['tag_ids'].append(tag_id)
                context['matched_tags'].append(
                    {
                        'id': tag_id,
                        'name': tag_name,
                        'match_type': match_type,
                    }
                )
            push_term(tag_name)
            alias_rows = conn.execute(
                'SELECT alias FROM tag_aliases WHERE tag_id = ? ORDER BY alias ASC',
                (tag_id,),
            ).fetchall()
            for alias_row in alias_rows:
                push_term(str(alias_row['alias']))
            platform_rows = conn.execute(
                """
                SELECT term
                FROM platform_tag_terms
                WHERE tag_id = ? AND platform = ?
                ORDER BY confidence DESC, term ASC
                LIMIT 100
                """,
                (tag_id, platform_text),
            ).fetchall()
            for term_row in platform_rows:
                push_term(str(term_row['term']))
            if include_suggestions:
                source_rows = conn.execute(
                    """
                    SELECT s.raw_tags, s.extra_json
                    FROM sources s
                    JOIN image_tags it ON it.image_id = s.image_id
                    WHERE it.tag_id = ?
                      AND s.platform = ?
                      AND it.review_status IN ('approved', 'manual_approved')
                    ORDER BY s.id DESC
                    LIMIT 200
                    """,
                    (tag_id, platform_text),
                ).fetchall()
                for source_row in source_rows:
                    try:
                        raw_terms = json.loads(source_row['raw_tags'] or '[]') if source_row['raw_tags'] else []
                    except Exception:
                        raw_terms = []
                    try:
                        extra = json.loads(source_row['extra_json'] or '{}') if source_row['extra_json'] else {}
                    except Exception:
                        extra = {}
                    translated_terms = extra.get('translated_tags') if isinstance(extra, dict) else []
                    if not isinstance(translated_terms, list):
                        translated_terms = []
                    for candidate_term in [*raw_terms, *translated_terms]:
                        candidate_row, candidate_match_type = self._resolve_tag_exact_conn(conn, str(candidate_term or ''))
                        if (
                            candidate_row
                            and int(candidate_row['id']) != tag_id
                            and self._looks_like_platform_term(str(candidate_term or ''))
                        ):
                            push_tag(
                                conn,
                                candidate_row,
                                f'suggested_{candidate_match_type or "tag"}',
                                include_suggestions=False,
                            )

        push_term(keyword_text)
        with self._lock, self._connect() as conn:
            exact_row, exact_type = self._resolve_tag_exact_conn(conn, keyword_text)
            push_tag(conn, exact_row, exact_type)
            platform_row = self._resolve_platform_term_exact_conn(conn, platform_text, keyword_text)
            if platform_row:
                tag_row = conn.execute('SELECT * FROM tags WHERE id = ? LIMIT 1', (int(platform_row['tag_id']),)).fetchone()
                push_tag(conn, tag_row, f'platform:{platform_text}')

            like_normalized = f'%{normalized_keyword}%'
            like_text = f'%{keyword_text}%'
            fuzzy_rows = conn.execute(
                """
                SELECT DISTINCT t.id, t.name,
                       CASE
                           WHEN t.normalized_name LIKE ? OR t.name LIKE ? THEN 'fuzzy_tag'
                           ELSE 'fuzzy_alias'
                       END AS match_type
                FROM tags t
                LEFT JOIN tag_aliases a ON a.tag_id = t.id
                WHERE t.normalized_name LIKE ?
                   OR t.name LIKE ?
                   OR a.normalized_alias LIKE ?
                   OR a.alias LIKE ?
                ORDER BY t.name ASC
                LIMIT 20
                """,
                (like_normalized, like_text, like_normalized, like_text, like_normalized, like_text),
            ).fetchall()
            for row in fuzzy_rows:
                push_tag(conn, row, str(row['match_type'] or 'fuzzy_tag'))

            platform_rows = conn.execute(
                """
                SELECT DISTINCT t.id, t.name, 'fuzzy_platform' AS match_type
                FROM platform_tag_terms p
                JOIN tags t ON t.id = p.tag_id
                WHERE p.platform = ?
                  AND (p.normalized_term LIKE ? OR p.term LIKE ?)
                ORDER BY t.name ASC
                LIMIT 20
                """,
                (platform_text, like_normalized, like_text),
            ).fetchall()
            for row in platform_rows:
                push_tag(conn, row, str(row['match_type'] or 'fuzzy_platform'))

        matched_names = [str(item['name']) for item in context['matched_tags']]
        term_preview = [str(item) for item in context['expanded_terms'][:12]]
        if matched_names:
            context['message'] = (
                f"{keyword_text} 命中：" + '、'.join(matched_names[:6])
                + ("；展开词：" + '、'.join(term_preview) if term_preview else '')
            )
        else:
            context['message'] = f"{keyword_text} 未命中主 tag；已按待审 tag / Pixiv 来源词直接模糊搜索"
        return context

    def apply_image_review(
        self,
        image_id: int,
        *,
        selected_tag_names: Iterable[str],
        source_terms: Iterable[str] | None = None,
        platform: str = 'pixiv',
        reason: str = '',
        reject_unselected: bool = True,
        require_open_review: bool = False,
    ) -> tuple[bool, dict[str, Any]]:
        requested_tags: list[str] = []
        seen_tags: set[str] = set()
        for raw in selected_tag_names:
            text = str(raw or '').strip()
            normalized = normalize_tag_name(text)
            if not text or not normalized or normalized in seen_tags:
                continue
            seen_tags.add(normalized)
            requested_tags.append(text)
        if not requested_tags:
            return False, {'message': '请至少选择一个归入主 tag。'}

        requested_terms: list[str] = []
        seen_terms: set[str] = set()
        for raw in (source_terms or []):
            text = str(raw or '').strip()
            normalized = normalize_tag_name(text)
            if not text or not normalized or normalized in seen_terms:
                continue
            seen_terms.add(normalized)
            requested_terms.append(text)
        canonical_tag_name = requested_tags[0]
        alias_terms = self._dedupe_terms([*requested_tags[1:], *requested_terms])

        platform_text = str(platform or 'pixiv').strip().lower() or 'pixiv'
        now = utcnow_str()
        approved_reason = str(reason or '').strip() or f'{platform_text} 人工审批通过'
        rejected_reason = str(reason or '').strip() or f'{platform_text} 人工审批拒绝'

        with self._lock, self._connect() as conn:
            if require_open_review:
                conn.execute('BEGIN IMMEDIATE')
            image = conn.execute('SELECT id FROM images WHERE id = ? AND is_active = 1 LIMIT 1', (image_id,)).fetchone()
            if not image:
                return False, {'message': f'图片不存在：{image_id}'}
            if require_open_review:
                open_task = conn.execute(
                    "SELECT 1 FROM review_tasks WHERE image_id = ? AND status IN ('pending', 'uncertain') LIMIT 1",
                    (image_id,),
                ).fetchone()
                if not open_task:
                    return False, {
                        'message': f'图片 #{image_id} 已不在待审队列中。',
                        'code': 'stale_review',
                        'image_id': image_id,
                    }
            source_exists = conn.execute(
                'SELECT 1 FROM sources WHERE image_id = ? AND platform = ? LIMIT 1',
                (image_id, platform_text),
            ).fetchone()
            if not source_exists:
                return False, {'message': f'图片 #{image_id} 没有 {platform_text} 来源记录。'}

            target_row, _ = self._resolve_tag_exact_conn(conn, canonical_tag_name)
            if not target_row:
                return False, {'message': f'归入主 tag 不存在：{canonical_tag_name}'}

            target_id = int(target_row['id'])
            target_name = str(target_row['name'])
            selected_ids = {target_id}
            selected_names = [target_name]
            def fetch_review_tasks() -> list[sqlite3.Row]:
                return conn.execute(
                    """
                    SELECT rt.id, rt.tag_id, rt.status, t.name AS tag_name,
                           COALESCE((
                               SELECT it.source_type
                               FROM image_tags it
                               WHERE it.image_id = rt.image_id AND it.tag_id = rt.tag_id
                               ORDER BY CASE
                                   WHEN it.source_type = ? THEN 0
                                   WHEN it.source_type LIKE 'crawl:%' THEN 1
                                   WHEN it.source_type LIKE 'manual:%' THEN 2
                                   ELSE 3
                               END, it.id DESC
                               LIMIT 1
                           ), '') AS source_type
                    FROM review_tasks rt
                    JOIN tags t ON t.id = rt.tag_id
                    WHERE rt.image_id = ?
                    ORDER BY rt.id DESC
                    """,
                    (f'crawl:{platform_text}', image_id),
                ).fetchall()

            tasks = fetch_review_tasks()

            def upsert_image_tag_review(tag_id: int, status: str, reason_text: str, default_source_type: str) -> str:
                image_tag_rows = conn.execute(
                    """
                    SELECT id, source_type
                    FROM image_tags
                    WHERE image_id = ? AND tag_id = ?
                    ORDER BY CASE
                        WHEN source_type = ? THEN 0
                        WHEN source_type LIKE 'crawl:%' THEN 1
                        WHEN source_type LIKE 'manual:%' THEN 2
                        ELSE 3
                    END, id DESC
                    """,
                    (image_id, tag_id, f'crawl:{platform_text}'),
                ).fetchall()
                if image_tag_rows:
                    chosen = image_tag_rows[0]
                    chosen_source_type = str(chosen['source_type'] or '').strip() or default_source_type
                    conn.execute(
                        'UPDATE image_tags SET review_status = ?, review_reason = ?, updated_at = ? WHERE image_id = ? AND tag_id = ? AND source_type = ?',
                        (status, reason_text, now, image_id, tag_id, chosen_source_type),
                    )
                    return chosen_source_type
                conn.execute(
                    """
                    INSERT INTO image_tags(image_id, tag_id, source_type, score, review_status, review_reason, created_at, updated_at)
                    VALUES(?, ?, ?, 1.0, ?, ?, ?, ?)
                    """,
                    (image_id, tag_id, default_source_type, status, reason_text, now, now),
                )
                return default_source_type

            mapped_terms: list[dict[str, Any]] = []
            skipped_terms: list[str] = []
            aliases_added: list[str] = []
            merged_tags: list[str] = []
            merged_source_ids: set[int] = set()
            for term in alias_terms:
                plan = self._apply_review_alias_term_conn(
                    conn,
                    platform=platform_text,
                    target_row=target_row,
                    term=term,
                    now=now,
                )
                if plan.get('status') != 'mapped':
                    skipped_terms.append(f'{term}（{str(plan.get("message") or "无法沉淀")}）')
                    continue
                mapped_terms.append(
                    {
                        'term': str(plan['term']),
                        'tag_name': str(plan['tag_name']),
                        'action': str(plan.get('action') or 'add'),
                    }
                )
                if plan.get('alias_added'):
                    aliases_added.append(str(plan['term']))
                if plan.get('merged_tag'):
                    merged_tags.append(str(plan['merged_tag']))
                source_tag_id = int(plan.get('source_tag_id') or 0)
                if source_tag_id > 0 and str(plan.get('action') or '') == 'merge_tag':
                    merged_source_ids.add(source_tag_id)

            tasks = fetch_review_tasks()
            task_by_tag_id = {int(row['tag_id']): row for row in tasks}
            source_type = upsert_image_tag_review(target_id, 'manual_approved', approved_reason, f'manual:{platform_text}_review')
            task = task_by_tag_id.get(target_id)
            if task:
                conn.execute(
                    """
                    UPDATE review_tasks
                    SET status = 'manual_approved', manual_result = 'approved', reason = ?, updated_at = ?
                    WHERE id = ?
                    """,
                    (approved_reason, now, int(task['id'])),
                )
            else:
                cursor = conn.execute(
                    """
                    INSERT INTO review_tasks(image_id, tag_id, status, model_result, manual_result, reason, created_at, updated_at)
                    VALUES(?, ?, 'manual_approved', '', 'approved', ?, ?, ?)
                    """,
                    (image_id, target_id, approved_reason, now, now),
                )
                task_by_tag_id[target_id] = {
                    'id': int(cursor.lastrowid),
                    'tag_id': target_id,
                    'tag_name': target_name,
                    'source_type': source_type,
                }

            rejected_names: list[str] = []
            if reject_unselected:
                for task in tasks:
                    tag_id = int(task['tag_id'])
                    if tag_id in selected_ids or tag_id in merged_source_ids:
                        continue
                    conn.execute(
                        """
                        UPDATE review_tasks
                        SET status = 'manual_rejected', manual_result = 'rejected', reason = ?, updated_at = ?
                        WHERE id = ?
                        """,
                        (rejected_reason, now, int(task['id'])),
                    )
                    source_type = str(task['source_type'] or '').strip()
                    if source_type:
                        conn.execute(
                            'UPDATE image_tags SET review_status = ?, review_reason = ?, updated_at = ? WHERE image_id = ? AND tag_id = ? AND source_type = ?',
                            ('manual_rejected', rejected_reason, now, image_id, tag_id, source_type),
                        )
                    else:
                        conn.execute(
                            'UPDATE image_tags SET review_status = ?, review_reason = ?, updated_at = ? WHERE image_id = ? AND tag_id = ? AND source_type = ?',
                            ('manual_rejected', rejected_reason, now, image_id, tag_id, ''),
                        )
                    rejected_names.append(str(task['tag_name']))

        return True, {
            'message': f'已完成图片 #{image_id} 的人工审核',
            'image_id': image_id,
            'canonical_tag': target_name,
            'approved_tags': selected_names,
            'rejected_tags': rejected_names,
            'alias_terms': alias_terms,
            'aliases_added': aliases_added,
            'merged_tags': merged_tags,
            'mapped_terms': mapped_terms,
            'skipped_terms': skipped_terms,
        }

    def _build_search_images_query(
        self,
        *,
        keyword: str = '',
        review_status: str = '',
        tag_name: str = '',
        platform: str = '',
        count: bool = False,
    ) -> tuple[str, list[Any]]:
        select_sql = "SELECT COUNT(DISTINCT i.id) AS total" if count else """
            SELECT DISTINCT i.id, i.file_path, i.file_name, i.width, i.height, i.format, i.phash, i.updated_at
        """
        sql = f"""
            {select_sql}
            FROM images i
            LEFT JOIN image_tags it ON it.image_id = i.id
            LEFT JOIN tags t ON t.id = it.tag_id
            LEFT JOIN tag_aliases a ON a.tag_id = t.id
            LEFT JOIN sources s ON s.image_id = i.id
            WHERE i.is_active = 1
        """
        params: list[Any] = []
        if keyword:
            normalized = normalize_tag_name(keyword)
            sql += " AND (i.file_name LIKE ? OR t.normalized_name LIKE ? OR a.normalized_alias LIKE ? OR s.post_url LIKE ? OR s.author LIKE ?)"
            params.extend([f'%{keyword}%', f'%{normalized}%', f'%{normalized}%', f'%{keyword}%', f'%{keyword}%'])
        statuses = split_status_filter(review_status)
        if statuses:
            placeholders = ','.join('?' for _ in statuses)
            sql += f' AND it.review_status IN ({placeholders})'
            params.extend(statuses)
        if tag_name:
            sql += ' AND t.normalized_name = ?'
            params.append(normalize_tag_name(tag_name))
        if platform:
            sql += ' AND s.platform = ?'
            params.append(platform)
        return sql, params

    def search_images(self, *, keyword: str = '', review_status: str = '', tag_name: str = '', platform: str = '', limit: int = 100, offset: int = 0) -> list[sqlite3.Row]:
        sql, params = self._build_search_images_query(
            keyword=keyword,
            review_status=review_status,
            tag_name=tag_name,
            platform=platform,
        )
        sql += ' ORDER BY i.id DESC LIMIT ? OFFSET ?'
        params.extend([limit, offset])
        with self._lock, self._connect() as conn:
            return conn.execute(sql, params).fetchall()

    def count_search_images(self, *, keyword: str = '', review_status: str = '', tag_name: str = '', platform: str = '') -> int:
        sql, params = self._build_search_images_query(
            keyword=keyword,
            review_status=review_status,
            tag_name=tag_name,
            platform=platform,
            count=True,
        )
        with self._lock, self._connect() as conn:
            row = conn.execute(sql, params).fetchone()
        return int(row["total"] or 0) if row else 0

    def get_image_detail(self, image_id: int, *, sync_files: bool = True) -> dict[str, Any] | None:
        with self._lock, self._connect() as conn:
            if sync_files:
                self._sync_image_file_state(conn, image_id)
            image = conn.execute('SELECT * FROM images WHERE id = ?', (image_id,)).fetchone()
            if not image:
                return None
            file_locations = conn.execute(
                """
                SELECT file_path, file_name, storage_type, is_active, created_at, updated_at
                FROM image_files
                WHERE image_id = ?
                ORDER BY updated_at DESC, id DESC
                """,
                (image_id,),
            ).fetchall()
            tags = conn.execute(
                """
                SELECT t.name, t.is_character, it.source_type, it.review_status, it.review_reason, it.score
                FROM image_tags it
                JOIN tags t ON t.id = it.tag_id
                WHERE it.image_id = ?
                ORDER BY t.name ASC
                """,
                (image_id,),
            ).fetchall()
            sources = conn.execute('SELECT platform, post_url, image_url, author, raw_tags, extra_json FROM sources WHERE image_id = ?', (image_id,)).fetchall()
            return {
                'image': dict(image),
                'file_locations': [
                    {
                        'file_path': str(row['file_path']),
                        'file_name': str(row['file_name']),
                        'storage_type': str(row['storage_type']),
                        'is_active': bool(row['is_active']),
                        'created_at': str(row['created_at']),
                        'updated_at': str(row['updated_at']),
                    }
                    for row in file_locations
                ],
                'tags': [
                    {
                        'name': str(row['name']),
                        'is_character': bool(row['is_character']),
                        'source_type': str(row['source_type']),
                        'review_status': str(row['review_status']),
                        'review_reason': str(row['review_reason']),
                        'score': float(row['score']),
                    }
                    for row in tags
                ],
                'sources': [
                    {
                        'platform': str(row['platform']),
                        'post_url': str(row['post_url']),
                        'image_url': str(row['image_url']),
                        'author': str(row['author']),
                        'raw_tags': json.loads(row['raw_tags'] or '[]'),
                        'extra': json.loads(row['extra_json'] or '{}'),
                    }
                    for row in sources
                ],
            }

    def trash_image(self, image_id: int, *, trash_path: str | None = None) -> tuple[bool, str]:
        now = utcnow_str()
        with self._lock, self._connect() as conn:
            image = conn.execute('SELECT id FROM images WHERE id = ?', (image_id,)).fetchone()
            if not image:
                return False, f'图片不存在：{image_id}'

            conn.execute(
                'UPDATE image_files SET is_active = 0, updated_at = ? WHERE image_id = ? AND is_active = 1',
                (now, image_id),
            )

            if trash_path:
                trash_file_name = Path(trash_path).name
                self._upsert_file_location(
                    conn,
                    image_id=image_id,
                    file_path=trash_path,
                    file_name=trash_file_name,
                    storage_type='trash',
                    now=now,
                )
                conn.execute(
                    'UPDATE image_files SET is_active = 0, updated_at = ? WHERE image_id = ? AND file_path = ?',
                    (now, image_id, trash_path),
                )
                conn.execute(
                    'UPDATE images SET file_path = ?, file_name = ?, is_active = 0, updated_at = ? WHERE id = ?',
                    (trash_path, trash_file_name, now, image_id),
                )
            else:
                conn.execute(
                    'UPDATE images SET is_active = 0, updated_at = ? WHERE id = ?',
                    (now, image_id),
                )
        return True, f'已将图片 #{image_id} 移出可发送列表。'

    def restore_image(self, image_id: int, *, restored_path: str, trash_path: str | None = None) -> tuple[bool, str]:
        now = utcnow_str()
        with self._lock, self._connect() as conn:
            image = conn.execute('SELECT id FROM images WHERE id = ?', (image_id,)).fetchone()
            if not image:
                return False, f'图片不存在：{image_id}'

            self._upsert_file_location(
                conn,
                image_id=image_id,
                file_path=restored_path,
                file_name=Path(restored_path).name,
                storage_type=self._infer_storage_type(restored_path),
                now=now,
            )
            conn.execute(
                "UPDATE image_files SET is_active = 0, updated_at = ? WHERE image_id = ? AND storage_type = 'trash'",
                (now, image_id),
            )
            if trash_path:
                conn.execute(
                    'UPDATE image_files SET is_active = 0, updated_at = ? WHERE image_id = ? AND file_path = ?',
                    (now, image_id, trash_path),
                )
            self._sync_image_file_state(conn, image_id, preferred_path=restored_path, now=now)
        return True, f'已恢复图片 #{image_id}。'

    def list_tags(self, *, keyword: str = '', limit: int = 100, character_only: bool | None = None) -> list[sqlite3.Row]:
        sql = """
            SELECT t.id, t.name, t.is_character, t.tag_type, t.status,
                   COUNT(DISTINCT CASE WHEN it.review_status IN ('approved', 'manual_approved') AND i.is_active = 1 THEN i.id END) AS image_count,
                   COUNT(DISTINCT a.id) AS alias_count
            FROM tags t
            LEFT JOIN tag_aliases a ON a.tag_id = t.id
            LEFT JOIN image_tags it ON it.tag_id = t.id
            LEFT JOIN images i ON i.id = it.image_id
        """
        params: list[Any] = []
        clauses: list[str] = []
        if character_only is not None:
            clauses.append("t.tag_type = 'character'" if character_only else "t.tag_type <> 'character'")
        if keyword:
            clauses.append('t.normalized_name LIKE ?')
            params.append(f"%{normalize_tag_name(keyword)}%")
        if clauses:
            sql += ' WHERE ' + ' AND '.join(clauses)
        sql += ' GROUP BY t.id, t.name, t.is_character, t.tag_type, t.status ORDER BY image_count DESC, t.name ASC LIMIT ?'
        params.append(limit)
        with self._lock, self._connect() as conn:
            return conn.execute(sql, params).fetchall()

    def preview_non_character_tag_cleanup(self, *, limit: int = 200) -> list[dict[str, Any]]:
        rows = [row for row in self.get_tag_governance_snapshot()['tags'] if bool(row.get('safe_cleanup'))]
        rows.sort(key=lambda row: (-int(row.get('image_link_count') or 0), str(row.get('name') or '')))
        return rows[: max(1, int(limit or 1))]

    def cleanup_non_character_tags(self) -> dict[str, Any]:
        with self._lock:
            snapshot = self.get_tag_governance_snapshot()
            safe_rows = [row for row in snapshot['tags'] if bool(row.get('safe_cleanup'))]
            protected_rows = [row for row in snapshot['tags'] if bool(row.get('protected_other'))]
            tag_ids = [int(row['id']) for row in safe_rows]
            normalized_names = [str(row.get('normalized_name') or '') for row in safe_rows if str(row.get('normalized_name') or '').strip()]
            with self._connect() as conn:
                if not tag_ids:
                    return {
                        'tags_removed': 0,
                        'image_links_removed': 0,
                        'review_tasks_removed': 0,
                        'aliases_removed': 0,
                        'platform_terms_removed': 0,
                        'subscriptions_removed': 0,
                        'protected_tags': len(protected_rows),
                    }
                id_placeholders = ','.join('?' for _ in tag_ids)

                image_cursor = conn.execute(f'DELETE FROM image_tags WHERE tag_id IN ({id_placeholders})', tag_ids)
                review_cursor = conn.execute(f'DELETE FROM review_tasks WHERE tag_id IN ({id_placeholders})', tag_ids)
                alias_cursor = conn.execute(f'DELETE FROM tag_aliases WHERE tag_id IN ({id_placeholders})', tag_ids)
                platform_term_cursor = conn.execute(f'DELETE FROM platform_tag_terms WHERE tag_id IN ({id_placeholders})', tag_ids)

                subscription_sql = f'DELETE FROM crawl_subscriptions WHERE tag_id IN ({id_placeholders})'
                subscription_select_sql = f'SELECT id FROM crawl_subscriptions WHERE tag_id IN ({id_placeholders})'
                subscription_params: list[Any] = list(tag_ids)
                if normalized_names:
                    normalized_placeholders = ','.join('?' for _ in normalized_names)
                    subscription_sql += f' OR normalized_tag IN ({normalized_placeholders})'
                    subscription_select_sql += f' OR normalized_tag IN ({normalized_placeholders})'
                    subscription_params.extend(normalized_names)
                subscription_ids = [
                    int(row['id'])
                    for row in conn.execute(subscription_select_sql, subscription_params).fetchall()
                ]
                if subscription_ids:
                    subscription_placeholders = ','.join('?' for _ in subscription_ids)
                    conn.execute(
                        f'DELETE FROM crawl_subscription_terms WHERE subscription_id IN ({subscription_placeholders})',
                        subscription_ids,
                    )
                subscription_cursor = conn.execute(subscription_sql, subscription_params)
                tag_cursor = conn.execute(f'DELETE FROM tags WHERE id IN ({id_placeholders})', tag_ids)

                return {
                    'tags_removed': max(0, int(tag_cursor.rowcount or 0)),
                    'image_links_removed': max(0, int(image_cursor.rowcount or 0)),
                    'review_tasks_removed': max(0, int(review_cursor.rowcount or 0)),
                    'aliases_removed': max(0, int(alias_cursor.rowcount or 0)),
                    'platform_terms_removed': max(0, int(platform_term_cursor.rowcount or 0)),
                    'subscriptions_removed': max(0, int(subscription_cursor.rowcount or 0)),
                    'protected_tags': len(protected_rows),
                }

    def list_tags_for_auto_crawl(self, *, character_only: bool = True) -> list[sqlite3.Row]:
        sql = 'SELECT id, name, is_character, tag_type, status FROM tags WHERE status = \'active\''
        if character_only:
            sql += " AND tag_type = 'character'"
        sql += ' ORDER BY name ASC'
        with self._lock, self._connect() as conn:
            return conn.execute(sql).fetchall()

    def upsert_crawl_subscription(
        self,
        *,
        platform: str,
        tag_id: int,
        tag_name: str,
        query_text: str = '',
        enabled: bool = True,
    ) -> int:
        normalized_tag = normalize_tag_name(tag_name)
        now = utcnow_str()
        with self._lock, self._connect() as conn:
            existing = conn.execute(
                'SELECT id FROM crawl_subscriptions WHERE platform = ? AND normalized_tag = ? LIMIT 1',
                (platform, normalized_tag),
            ).fetchone()
            if existing:
                conn.execute(
                    """
                    UPDATE crawl_subscriptions
                    SET tag_id = ?, tag_name = ?, query_text = ?, enabled = ?, updated_at = ?
                    WHERE id = ?
                    """,
                    (tag_id, tag_name.strip(), query_text, 1 if enabled else 0, now, int(existing['id'])),
                )
                return int(existing['id'])
            cursor = conn.execute(
                """
                INSERT INTO crawl_subscriptions(
                    platform, tag_id, tag_name, normalized_tag, query_text, enabled,
                    last_seen_source_uid, last_checked_at, last_success_at, last_error, created_at, updated_at
                )
                VALUES(?, ?, ?, ?, ?, ?, '', '', '', '', ?, ?)
                """,
                (platform, tag_id, tag_name.strip(), normalized_tag, query_text, 1 if enabled else 0, now, now),
            )
            return int(cursor.lastrowid)

    def disable_missing_crawl_subscriptions(self, *, platform: str, keep_normalized_tags: set[str]) -> None:
        now = utcnow_str()
        with self._lock, self._connect() as conn:
            if keep_normalized_tags:
                placeholders = ','.join('?' for _ in keep_normalized_tags)
                conn.execute(
                    f"UPDATE crawl_subscriptions SET enabled = 0, updated_at = ? WHERE platform = ? AND normalized_tag NOT IN ({placeholders})",
                    (now, platform, *sorted(keep_normalized_tags)),
                )
            else:
                conn.execute(
                    'UPDATE crawl_subscriptions SET enabled = 0, updated_at = ? WHERE platform = ?',
                    (now, platform),
                )
            conn.execute(
                """
                UPDATE crawl_subscription_terms
                SET enabled = 0, updated_at = ?
                WHERE subscription_id IN (
                    SELECT id FROM crawl_subscriptions WHERE platform = ? AND enabled = 0
                )
                """,
                (now, platform),
            )

    def list_crawl_subscriptions(self, *, platform: str = '', enabled_only: bool = False, limit: int = 200) -> list[sqlite3.Row]:
        sql = 'SELECT * FROM crawl_subscriptions'
        clauses: list[str] = []
        params: list[Any] = []
        if platform:
            clauses.append('platform = ?')
            params.append(platform)
        if enabled_only:
            clauses.append('enabled = 1')
        if clauses:
            sql += ' WHERE ' + ' AND '.join(clauses)
        sql += ' ORDER BY platform ASC, tag_name ASC LIMIT ?'
        params.append(limit)
        with self._lock, self._connect() as conn:
            return conn.execute(sql, params).fetchall()

    def update_crawl_subscription_state(
        self,
        subscription_id: int,
        *,
        query_text: str | None = None,
        enabled: bool | None = None,
        last_seen_source_uid: str | None = None,
        last_checked_at: str | None = None,
        last_success_at: str | None = None,
        last_error: str | None = None,
    ) -> None:
        fields: list[str] = ['updated_at = ?']
        params: list[Any] = [utcnow_str()]
        if query_text is not None:
            fields.append('query_text = ?')
            params.append(query_text)
        if enabled is not None:
            fields.append('enabled = ?')
            params.append(1 if enabled else 0)
        if last_seen_source_uid is not None:
            fields.append('last_seen_source_uid = ?')
            params.append(last_seen_source_uid)
        if last_checked_at is not None:
            fields.append('last_checked_at = ?')
            params.append(last_checked_at)
        if last_success_at is not None:
            fields.append('last_success_at = ?')
            params.append(last_success_at)
        if last_error is not None:
            fields.append('last_error = ?')
            params.append(last_error)
        params.append(subscription_id)
        with self._lock, self._connect() as conn:
            conn.execute(f"UPDATE crawl_subscriptions SET {', '.join(fields)} WHERE id = ?", params)

    @staticmethod
    def _merge_csv_values(*values: str | Iterable[str]) -> list[str]:
        result: list[str] = []
        seen: set[str] = set()
        for value in values:
            if isinstance(value, str):
                items = value.replace('，', ',').split(',')
            else:
                items = [str(item) for item in value]
            for item in items:
                text = str(item or '').strip()
                key = normalize_tag_name(text)
                if not text or not key or key in seen:
                    continue
                seen.add(key)
                result.append(text)
        return result

    @staticmethod
    def _normalize_source_context(value: dict[str, Any] | str | None) -> dict[str, Any]:
        if isinstance(value, str):
            try:
                parsed = json.loads(value or '{}')
            except (TypeError, json.JSONDecodeError):
                parsed = {}
        else:
            parsed = value
        if not isinstance(parsed, dict):
            return {}
        try:
            serialized = json.dumps(parsed, ensure_ascii=False)
            normalized = json.loads(serialized)
        except (TypeError, ValueError, json.JSONDecodeError):
            return {}
        return normalized if isinstance(normalized, dict) else {}

    @classmethod
    def _serialize_source_context(cls, value: dict[str, Any] | str | None) -> str:
        return json.dumps(cls._normalize_source_context(value), ensure_ascii=False, separators=(',', ':'))

    @classmethod
    def _merge_source_context_values(
        cls,
        existing: dict[str, Any] | str | None,
        incoming: dict[str, Any] | str | None,
    ) -> dict[str, Any]:
        merged = cls._normalize_source_context(existing)
        for key, value in cls._normalize_source_context(incoming).items():
            if value is None or value == '':
                continue
            merged[str(key)] = value
        return merged

    @classmethod
    def _merge_crawl_job_values_conn(
        cls,
        conn: sqlite3.Connection,
        job_id: int,
        *,
        tags: Iterable[str] = (),
        include_tags: Iterable[str] = (),
        exclude_tags: Iterable[str] = (),
        source_context: dict[str, Any] | None = None,
        origin: str = '',
        priority: int | None = None,
        now: str,
    ) -> None:
        row = conn.execute('SELECT * FROM crawl_jobs WHERE id = ? LIMIT 1', (int(job_id),)).fetchone()
        if not row:
            return
        merged_tags = cls._merge_csv_values(str(row['tags_text'] or ''), tags)
        merged_include = cls._merge_csv_values(str(row['include_tags_text'] or ''), include_tags)
        merged_exclude = cls._merge_csv_values(str(row['exclude_tags_text'] or ''), exclude_tags)
        merged_context = cls._merge_source_context_values(
            str(row['source_context_json'] or '{}'),
            source_context,
        )
        existing_priority = int(row['priority']) if row['priority'] is not None else 20
        incoming_priority = existing_priority if priority is None else int(priority)
        merged_priority = min(existing_priority, incoming_priority)
        existing_origin = str(row['origin'] or 'manual')
        incoming_origin = str(origin or '').strip().lower()
        merged_origin = (
            incoming_origin
            if incoming_origin and incoming_priority < existing_priority
            else existing_origin
        )
        conn.execute(
            """
            UPDATE crawl_jobs
            SET tags_text = ?, include_tags_text = ?, exclude_tags_text = ?, source_context_json = ?,
                origin = ?, priority = ?, updated_at = ?
            WHERE id = ?
            """,
            (
                ','.join(merged_tags),
                ','.join(merged_include),
                ','.join(merged_exclude),
                cls._serialize_source_context(merged_context),
                merged_origin,
                merged_priority,
                now,
                int(job_id),
            ),
        )

    def get_or_create_crawl_job(
        self,
        platform: str,
        source_url: str,
        tags: list[str],
        *,
        source_context: dict[str, Any] | None = None,
        include_tags: list[str] | None = None,
        exclude_tags: list[str] | None = None,
        match_mode: str = 'exact',
        origin: str = 'auto_incremental',
        priority: int = 20,
    ) -> tuple[int, bool]:
        platform_text = str(platform or '').strip().lower()
        raw_url = str(source_url or '').strip()
        normalized_url = self.normalize_source_post_url(platform_text, raw_url) or raw_url
        if not platform_text or not normalized_url:
            raise ValueError('采集平台和来源 URL 不能为空')
        now = utcnow_str()
        with self._lock, self._connect() as conn:
            row = conn.execute(
                """
                SELECT *
                FROM crawl_jobs
                WHERE platform = ? AND source_url IN (?, ?)
                ORDER BY
                    CASE status
                        WHEN 'pending' THEN 0
                        WHEN 'retry' THEN 0
                        WHEN 'running' THEN 0
                        WHEN 'failed' THEN 1
                        ELSE 2
                    END,
                    id DESC
                LIMIT 1
                """,
                (platform_text, raw_url, normalized_url),
            ).fetchone()
            if row:
                self._merge_crawl_job_values_conn(
                    conn,
                    int(row['id']),
                    tags=tags,
                    include_tags=include_tags or [],
                    exclude_tags=exclude_tags or [],
                    source_context=source_context,
                    origin=origin,
                    priority=priority,
                    now=now,
                )
                return int(row['id']), False

            cursor = conn.execute(
                """
                INSERT INTO crawl_jobs(
                    platform, source_url, source_context_json, tags_text, include_tags_text, exclude_tags_text, tag_match_mode,
                    origin, priority, status, progress, error_log, result_summary, attempt_count, created_at, updated_at
                )
                VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, 'pending', 0, '', '', 0, ?, ?)
                """,
                (
                    platform_text,
                    normalized_url,
                    self._serialize_source_context(source_context),
                    ','.join(self._merge_csv_values(tags)),
                    ','.join(self._merge_csv_values(include_tags or [])),
                    ','.join(self._merge_csv_values(exclude_tags or [])),
                    str(match_mode or 'exact').strip().lower() or 'exact',
                    str(origin or 'auto_incremental').strip().lower() or 'auto_incremental',
                    int(priority),
                    now,
                    now,
                ),
            )
            return int(cursor.lastrowid), True

    def sync_crawl_subscription_terms(
        self,
        subscription_id: int,
        terms: list[tuple[str, str]],
    ) -> list[sqlite3.Row]:
        now = utcnow_str()
        normalized_terms: list[tuple[str, str, str]] = []
        seen: set[str] = set()
        for query_term, query_text in terms:
            term_text = str(query_term or '').strip()
            normalized = normalize_tag_name(term_text)
            if not term_text or not normalized or normalized in seen:
                continue
            seen.add(normalized)
            normalized_terms.append((term_text, normalized, str(query_text or '').strip()))

        with self._lock, self._connect() as conn:
            parent = conn.execute(
                'SELECT * FROM crawl_subscriptions WHERE id = ? LIMIT 1',
                (int(subscription_id),),
            ).fetchone()
            if not parent:
                return []
            existing_rows = conn.execute(
                'SELECT * FROM crawl_subscription_terms WHERE subscription_id = ? ORDER BY position ASC, id ASC',
                (int(subscription_id),),
            ).fetchall()
            existing_by_term = {str(row['normalized_term']): row for row in existing_rows}
            conn.execute(
                'UPDATE crawl_subscription_terms SET enabled = 0, updated_at = ? WHERE subscription_id = ?',
                (now, int(subscription_id)),
            )
            legacy_last_seen = str(parent['last_seen_source_uid'] or '').strip()
            for position, (term_text, normalized, query_text) in enumerate(normalized_terms):
                existing = existing_by_term.get(normalized)
                if existing:
                    conn.execute(
                        """
                        UPDATE crawl_subscription_terms
                        SET query_term = ?, query_text = ?, position = ?, enabled = 1, updated_at = ?
                        WHERE id = ?
                        """,
                        (term_text, query_text, position, now, int(existing['id'])),
                    )
                    continue
                seed_last_seen = legacy_last_seen if not existing_rows and position == 0 else ''
                conn.execute(
                    """
                    INSERT INTO crawl_subscription_terms(
                        subscription_id, query_term, normalized_term, query_text, position, enabled,
                        last_seen_source_uid, last_checked_at, last_success_at, last_error,
                        created_at, updated_at
                    )
                    VALUES(?, ?, ?, ?, ?, 1, ?, '', '', '', ?, ?)
                    """,
                    (
                        int(subscription_id),
                        term_text,
                        normalized,
                        query_text,
                        position,
                        seed_last_seen,
                        now,
                        now,
                    ),
                )
            return conn.execute(
                """
                SELECT * FROM crawl_subscription_terms
                WHERE subscription_id = ? AND enabled = 1
                ORDER BY position ASC, id ASC
                """,
                (int(subscription_id),),
            ).fetchall()

    def list_crawl_subscription_terms(
        self,
        subscription_id: int,
        *,
        enabled_only: bool = True,
    ) -> list[sqlite3.Row]:
        sql = 'SELECT * FROM crawl_subscription_terms WHERE subscription_id = ?'
        params: list[Any] = [int(subscription_id)]
        if enabled_only:
            sql += ' AND enabled = 1'
        sql += ' ORDER BY position ASC, id ASC'
        with self._lock, self._connect() as conn:
            return conn.execute(sql, params).fetchall()

    def has_pending_crawl_subscription_scan(self, subscription_id: int) -> bool:
        with self._lock, self._connect() as conn:
            return conn.execute(
                """
                SELECT 1
                FROM crawl_subscription_terms
                WHERE subscription_id = ? AND enabled = 1
                  AND TRIM(COALESCE(scan_offset, '')) <> ''
                LIMIT 1
                """,
                (int(subscription_id),),
            ).fetchone() is not None

    def count_pending_crawl_term_scans(self, *, platform: str) -> int:
        with self._lock, self._connect() as conn:
            row = conn.execute(
                """
                SELECT COUNT(*) AS c
                FROM crawl_subscription_terms cst
                JOIN crawl_subscriptions cs ON cs.id = cst.subscription_id
                WHERE cs.platform = ? AND cs.enabled = 1 AND cst.enabled = 1
                  AND TRIM(COALESCE(cst.scan_offset, '')) <> ''
                """,
                (str(platform or '').strip().lower(),),
            ).fetchone()
        return int(row['c'] or 0) if row else 0

    def update_crawl_subscription_term_state(
        self,
        term_id: int,
        *,
        last_seen_source_uid: str | None = None,
        last_checked_at: str | None = None,
        last_success_at: str | None = None,
        last_error: str | None = None,
        scan_offset: str | None = None,
        scan_high_watermark: str | None = None,
        scan_target_source_uid: str | None = None,
        saturated: bool | None = None,
        saturated_at: str | None = None,
        saturated_reason: str | None = None,
    ) -> None:
        fields = ['updated_at = ?']
        params: list[Any] = [utcnow_str()]
        if last_seen_source_uid is not None:
            fields.append('last_seen_source_uid = ?')
            params.append(str(last_seen_source_uid))
        if last_checked_at is not None:
            fields.append('last_checked_at = ?')
            params.append(str(last_checked_at))
        if last_success_at is not None:
            fields.append('last_success_at = ?')
            params.append(str(last_success_at))
        if last_error is not None:
            fields.append('last_error = ?')
            params.append(str(last_error))
        if scan_offset is not None:
            fields.append('scan_offset = ?')
            params.append(str(scan_offset))
        if scan_high_watermark is not None:
            fields.append('scan_high_watermark = ?')
            params.append(str(scan_high_watermark))
        if scan_target_source_uid is not None:
            fields.append('scan_target_source_uid = ?')
            params.append(str(scan_target_source_uid))
        if saturated is not None:
            fields.append('saturated = ?')
            params.append(1 if saturated else 0)
        if saturated_at is not None:
            fields.append('saturated_at = ?')
            params.append(str(saturated_at))
        if saturated_reason is not None:
            fields.append('saturated_reason = ?')
            params.append(str(saturated_reason))
        params.append(int(term_id))
        with self._lock, self._connect() as conn:
            conn.execute(f"UPDATE crawl_subscription_terms SET {', '.join(fields)} WHERE id = ?", params)

    def list_saturated_crawl_subscription_terms(
        self,
        *,
        platform: str,
        limit: int = 50,
    ) -> list[sqlite3.Row]:
        with self._lock, self._connect() as conn:
            return conn.execute(
                """
                SELECT cst.*, cs.tag_id, cs.tag_name
                FROM crawl_subscription_terms cst
                JOIN crawl_subscriptions cs ON cs.id = cst.subscription_id
                WHERE cs.platform = ? AND cs.enabled = 1 AND cst.enabled = 1
                  AND cst.saturated = 1
                ORDER BY cst.saturated_at DESC, cst.id ASC
                LIMIT ?
                """,
                (str(platform or '').strip().lower(), max(1, int(limit or 50))),
            ).fetchall()

    def refresh_crawl_subscription_state(self, subscription_id: int) -> None:
        with self._lock, self._connect() as conn:
            rows = conn.execute(
                """
                SELECT * FROM crawl_subscription_terms
                WHERE subscription_id = ? AND enabled = 1
                ORDER BY position ASC, id ASC
                """,
                (int(subscription_id),),
            ).fetchall()
            if not rows:
                return
            primary = rows[0]
            last_checked_at = max((str(row['last_checked_at'] or '') for row in rows), default='')
            last_success_at = max((str(row['last_success_at'] or '') for row in rows), default='')
            errors = [
                f"{str(row['query_term'] or '').strip()}: {str(row['last_error'] or '').strip()}"
                for row in rows
                if str(row['last_error'] or '').strip()
            ]
            conn.execute(
                """
                UPDATE crawl_subscriptions
                SET last_seen_source_uid = ?, last_checked_at = ?, last_success_at = ?,
                    last_error = ?, updated_at = ?
                WHERE id = ?
                """,
                (
                    str(primary['last_seen_source_uid'] or ''),
                    last_checked_at,
                    last_success_at,
                    '；'.join(errors[:3]),
                    utcnow_str(),
                    int(subscription_id),
                ),
            )

    def upsert_crawl_discovery(
        self,
        *,
        platform: str,
        source_uid: str,
        post_url: str,
        tags: Iterable[str],
        source_context: dict[str, Any] | None = None,
    ) -> tuple[sqlite3.Row, bool]:
        platform_text = str(platform or '').strip().lower()
        post_url_text = self.normalize_source_post_url(platform_text, post_url) or str(post_url or '').strip()
        source_uid_text = str(source_uid or '').strip() or self._source_uid_from_post_url(platform_text, post_url_text)
        if not platform_text or not source_uid_text or not post_url_text:
            raise ValueError('发现记录缺少平台、来源 ID 或 URL')
        now = utcnow_str()
        with self._lock, self._connect() as conn:
            existing = conn.execute(
                'SELECT * FROM crawl_discoveries WHERE platform = ? AND source_uid = ? LIMIT 1',
                (platform_text, source_uid_text),
            ).fetchone()
            if existing:
                merged_tags = self._merge_csv_values(str(existing['tags_text'] or ''), tags)
                merged_context = self._merge_source_context_values(
                    str(existing['source_context_json'] or '{}'),
                    source_context,
                )
                conn.execute(
                    """
                    UPDATE crawl_discoveries
                    SET post_url = ?, source_context_json = ?, tags_text = ?, updated_at = ?
                    WHERE id = ?
                    """,
                    (
                        post_url_text,
                        self._serialize_source_context(merged_context),
                        ','.join(merged_tags),
                        now,
                        int(existing['id']),
                    ),
                )
                crawl_job_id = int(existing['crawl_job_id'] or 0)
                if crawl_job_id > 0:
                    self._merge_crawl_job_values_conn(
                        conn,
                        crawl_job_id,
                        tags=merged_tags,
                        source_context=merged_context,
                        now=now,
                    )
                row = conn.execute('SELECT * FROM crawl_discoveries WHERE id = ?', (int(existing['id']),)).fetchone()
                if not row:
                    raise RuntimeError('更新发现记录失败')
                return row, False

            cursor = conn.execute(
                """
                INSERT INTO crawl_discoveries(
                    platform, source_uid, post_url, source_context_json, tags_text, status, crawl_job_id,
                    last_error, discovered_at, updated_at
                )
                VALUES(?, ?, ?, ?, ?, 'pending', 0, '', ?, ?)
                """,
                (
                    platform_text,
                    source_uid_text,
                    post_url_text,
                    self._serialize_source_context(source_context),
                    ','.join(self._merge_csv_values(tags)),
                    now,
                    now,
                ),
            )
            row = conn.execute('SELECT * FROM crawl_discoveries WHERE id = ?', (int(cursor.lastrowid),)).fetchone()
            if not row:
                raise RuntimeError('创建发现记录失败')
            return row, True

    def list_pending_crawl_discoveries(self, *, platform: str, limit: int = 30) -> list[sqlite3.Row]:
        with self._lock, self._connect() as conn:
            return conn.execute(
                """
                SELECT * FROM crawl_discoveries
                WHERE platform = ? AND status = 'pending'
                ORDER BY id ASC
                LIMIT ?
                """,
                (str(platform or '').strip().lower(), max(1, int(limit or 1))),
            ).fetchall()

    def mark_crawl_discovery_submitted(self, discovery_id: int, crawl_job_id: int) -> None:
        with self._lock, self._connect() as conn:
            conn.execute(
                """
                UPDATE crawl_discoveries
                SET status = 'submitted', crawl_job_id = ?, source_context_json = '{}',
                    last_error = '', updated_at = ?
                WHERE id = ?
                """,
                (int(crawl_job_id), utcnow_str(), int(discovery_id)),
            )

    def mark_crawl_discovery_resolved(self, discovery_id: int, *, status: str) -> None:
        resolved_status = str(status or '').strip().lower()
        if resolved_status not in {'imported', 'rejected'}:
            raise ValueError(f'不支持的发现记录完成状态：{status}')
        with self._lock, self._connect() as conn:
            conn.execute(
                """
                UPDATE crawl_discoveries
                SET status = ?, source_context_json = '{}', last_error = '', updated_at = ?
                WHERE id = ?
                """,
                (resolved_status, utcnow_str(), int(discovery_id)),
            )

    def mark_crawl_discovery_error(self, discovery_id: int, error: str) -> None:
        with self._lock, self._connect() as conn:
            conn.execute(
                """
                UPDATE crawl_discoveries
                SET status = 'pending', last_error = ?, updated_at = ?
                WHERE id = ?
                """,
                (str(error or '')[:1000], utcnow_str(), int(discovery_id)),
            )

    def count_crawl_discoveries_by_status(self, *, platform: str = '') -> dict[str, int]:
        sql = 'SELECT status, COUNT(*) AS total FROM crawl_discoveries'
        params: list[Any] = []
        if platform:
            sql += ' WHERE platform = ?'
            params.append(str(platform or '').strip().lower())
        sql += ' GROUP BY status'
        with self._lock, self._connect() as conn:
            rows = conn.execute(sql, params).fetchall()
        return {str(row['status'] or ''): int(row['total'] or 0) for row in rows}

    @classmethod
    def _merge_crawl_subscription_terms_conn(
        cls,
        conn: sqlite3.Connection,
        *,
        source_subscription_id: int,
        target_subscription_id: int,
        now: str,
    ) -> None:
        rows = conn.execute(
            'SELECT * FROM crawl_subscription_terms WHERE subscription_id = ? ORDER BY position ASC, id ASC',
            (int(source_subscription_id),),
        ).fetchall()
        for row in rows:
            existing = conn.execute(
                """
                SELECT * FROM crawl_subscription_terms
                WHERE subscription_id = ? AND normalized_term = ?
                LIMIT 1
                """,
                (int(target_subscription_id), str(row['normalized_term'])),
            ).fetchone()
            if not existing:
                conn.execute(
                    'UPDATE crawl_subscription_terms SET subscription_id = ?, updated_at = ? WHERE id = ?',
                    (int(target_subscription_id), now, int(row['id'])),
                )
                continue

            source_success = str(row['last_success_at'] or '')
            target_success = str(existing['last_success_at'] or '')
            preferred = row if source_success > target_success else existing
            conn.execute(
                """
                UPDATE crawl_subscription_terms
                SET query_term = ?, query_text = ?, position = ?, enabled = ?,
                    last_seen_source_uid = ?, last_checked_at = ?, last_success_at = ?,
                    last_error = ?, updated_at = ?
                WHERE id = ?
                """,
                (
                    str(preferred['query_term'] or ''),
                    str(preferred['query_text'] or ''),
                    min(int(existing['position'] or 0), int(row['position'] or 0)),
                    1 if int(existing['enabled'] or 0) or int(row['enabled'] or 0) else 0,
                    str(preferred['last_seen_source_uid'] or ''),
                    max(str(existing['last_checked_at'] or ''), str(row['last_checked_at'] or '')),
                    max(target_success, source_success),
                    str(preferred['last_error'] or ''),
                    now,
                    int(existing['id']),
                ),
            )
            conn.execute('DELETE FROM crawl_subscription_terms WHERE id = ?', (int(row['id']),))
