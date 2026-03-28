from __future__ import annotations

import json
import sqlite3
import threading
from datetime import datetime
from pathlib import Path
from typing import Any, Iterable

from .matcher import normalize_tag_name
from .models import APPROVED_STATUSES, MatchResult
from .phash import hamming_distance

IMAGE_TAG_STATUS_PRIORITY = {
    "manual_approved": 5,
    "approved": 4,
    "pending": 3,
    "uncertain": 2,
    "manual_rejected": 1,
    "rejected": 0,
}


def utcnow_str() -> str:
    return datetime.utcnow().isoformat(timespec="seconds")


class ImageIndexDB:
    def __init__(self, db_path: Path) -> None:
        self.db_path = Path(db_path).expanduser().resolve()
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.RLock()
        self._init_db()

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path, timeout=30)
        conn.row_factory = sqlite3.Row
        try:
            conn.execute("PRAGMA busy_timeout=30000;")
            conn.execute("PRAGMA temp_store=MEMORY;")
        except sqlite3.OperationalError:
            pass
        return conn

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
                    tags_text TEXT DEFAULT '',
                    include_tags_text TEXT DEFAULT '',
                    exclude_tags_text TEXT DEFAULT '',
                    tag_match_mode TEXT DEFAULT 'exact',
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

                CREATE INDEX IF NOT EXISTS idx_images_active ON images(is_active);
                CREATE INDEX IF NOT EXISTS idx_images_sha256 ON images(sha256);
                CREATE INDEX IF NOT EXISTS idx_image_files_image_id ON image_files(image_id);
                CREATE INDEX IF NOT EXISTS idx_image_files_active ON image_files(is_active);
                CREATE INDEX IF NOT EXISTS idx_image_tags_tag_id ON image_tags(tag_id);
                CREATE INDEX IF NOT EXISTS idx_image_tags_review_status ON image_tags(review_status);
                CREATE INDEX IF NOT EXISTS idx_sources_platform ON sources(platform);
                CREATE INDEX IF NOT EXISTS idx_crawl_jobs_status ON crawl_jobs(status);
                CREATE INDEX IF NOT EXISTS idx_crawl_subscriptions_platform ON crawl_subscriptions(platform, enabled);
                CREATE INDEX IF NOT EXISTS idx_review_tasks_status ON review_tasks(status);
                CREATE INDEX IF NOT EXISTS idx_send_logs_session_id ON send_logs(session_id);
                """
            )
            try:
                conn.execute("PRAGMA journal_mode=DELETE;")
            except sqlite3.OperationalError:
                pass

            self._ensure_column(conn, 'images', 'phash', "TEXT DEFAULT ''")
            self._ensure_column(conn, 'tags', 'is_character', 'INTEGER DEFAULT 0')
            self._ensure_column(conn, 'image_tags', 'score', 'REAL DEFAULT 1.0')
            self._ensure_column(conn, 'image_tags', 'review_status', "TEXT DEFAULT 'approved'")
            self._ensure_column(conn, 'image_tags', 'review_reason', "TEXT DEFAULT ''")
            self._ensure_column(conn, 'image_tags', 'updated_at', "TEXT DEFAULT ''")
            self._ensure_column(conn, 'crawl_jobs', 'attempt_count', 'INTEGER DEFAULT 0')
            self._ensure_column(conn, 'crawl_jobs', 'include_tags_text', "TEXT DEFAULT ''")
            self._ensure_column(conn, 'crawl_jobs', 'exclude_tags_text', "TEXT DEFAULT ''")
            self._ensure_column(conn, 'crawl_jobs', 'tag_match_mode', "TEXT DEFAULT 'exact'")
            self._ensure_column(conn, 'crawl_subscriptions', 'tag_id', 'INTEGER DEFAULT 0')
            self._ensure_column(conn, 'crawl_subscriptions', 'query_text', "TEXT DEFAULT ''")
            self._ensure_column(conn, 'crawl_subscriptions', 'enabled', 'INTEGER DEFAULT 1')
            self._ensure_column(conn, 'crawl_subscriptions', 'last_seen_source_uid', "TEXT DEFAULT ''")
            self._ensure_column(conn, 'crawl_subscriptions', 'last_checked_at', "TEXT DEFAULT ''")
            self._ensure_column(conn, 'crawl_subscriptions', 'last_success_at', "TEXT DEFAULT ''")
            self._ensure_column(conn, 'crawl_subscriptions', 'last_error', "TEXT DEFAULT ''")
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
                LIMIT 500
                """
            ).fetchall()
        matches: list[tuple[int, sqlite3.Row]] = []
        for row in rows:
            distance = hamming_distance(phash, str(row["phash"] or ""))
            if distance <= max_distance:
                matches.append((distance, row))
        matches.sort(key=lambda item: (item[0], -int(item[1]["id"])))
        return [row for _, row in matches[:limit]]

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

    def get_or_create_tag(self, tag_name: str, is_character: bool | None = None) -> int:
        normalized = normalize_tag_name(tag_name)
        now = utcnow_str()
        with self._lock, self._connect() as conn:
            row = conn.execute('SELECT id, is_character FROM tags WHERE normalized_name = ?', (normalized,)).fetchone()
            if row:
                if is_character is True and int(row['is_character']) != 1:
                    conn.execute('UPDATE tags SET is_character = 1 WHERE id = ?', (row['id'],))
                return int(row['id'])
            cursor = conn.execute(
                'INSERT INTO tags(name, normalized_name, is_character, created_at) VALUES(?, ?, ?, ?)',
                (tag_name.strip(), normalized, 1 if is_character else 0, now),
            )
            return int(cursor.lastrowid)

    def set_tag_character(self, tag_name: str, is_character: bool) -> tuple[bool, str]:
        tag_id = self.get_tag_id(tag_name)
        if tag_id is None:
            return False, f'tag 不存在：{tag_name}'
        with self._lock, self._connect() as conn:
            conn.execute('UPDATE tags SET is_character = ? WHERE id = ?', (1 if is_character else 0, tag_id))
        state = '角色 tag' if is_character else '普通 tag'
        return True, f'已将 {tag_name} 标记为：{state}'

    def get_tag_row(self, tag_name: str) -> sqlite3.Row | None:
        normalized = normalize_tag_name(tag_name)
        with self._lock, self._connect() as conn:
            return conn.execute('SELECT * FROM tags WHERE normalized_name = ?', (normalized,)).fetchone()

    def get_tag_row_by_id(self, tag_id: int) -> sqlite3.Row | None:
        with self._lock, self._connect() as conn:
            return conn.execute('SELECT * FROM tags WHERE id = ?', (tag_id,)).fetchone()

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

        source_name_alias_added = False
        delete_cursor = conn.execute('DELETE FROM crawl_subscriptions WHERE tag_id = ?', (source_id,))
        subscriptions_removed = max(0, int(delete_cursor.rowcount or 0))
        conn.execute('DELETE FROM tags WHERE id = ?', (source_id,))

        ok, _ = self._insert_alias_conn(conn, tag_id=target_id, alias=str(source['name']), now=now)
        source_name_alias_added = ok

        if int(source['is_character'] or 0) == 1 and int(target['is_character'] or 0) != 1:
            conn.execute('UPDATE tags SET is_character = 1 WHERE id = ?', (target_id,))

        return {
            'source_name': str(source['name']),
            'image_links_migrated': image_links_migrated,
            'review_tasks_migrated': review_tasks_migrated,
            'review_tasks_merged': review_tasks_merged,
            'aliases_migrated': aliases_migrated,
            'aliases_skipped': aliases_skipped,
            'source_name_alias_added': source_name_alias_added,
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
                source_name = str(source_row['name'])
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

    def has_source_post_url(self, post_url: str, *, platform: str = '') -> bool:
        raw_post_url = str(post_url or '').strip()
        if not raw_post_url:
            return False
        sql = 'SELECT 1 FROM sources WHERE post_url = ?'
        params: list[Any] = [raw_post_url]
        if platform:
            sql += ' AND platform = ?'
            params.append(platform)
        sql += ' LIMIT 1'
        with self._lock, self._connect() as conn:
            row = conn.execute(sql, params).fetchone()
            return row is not None

    def create_crawl_job(
        self,
        platform: str,
        source_url: str,
        tags: list[str],
        *,
        include_tags: list[str] | None = None,
        exclude_tags: list[str] | None = None,
        match_mode: str = 'exact',
    ) -> int:
        now = utcnow_str()
        with self._lock, self._connect() as conn:
            cursor = conn.execute(
                """
                INSERT INTO crawl_jobs(
                    platform, source_url, tags_text, include_tags_text, exclude_tags_text, tag_match_mode,
                    status, progress, error_log, result_summary, attempt_count, created_at, updated_at
                )
                VALUES(?, ?, ?, ?, ?, ?, 'pending', 0, '', '', 0, ?, ?)
                """,
                (
                    platform,
                    source_url,
                    ','.join(tags),
                    ','.join(include_tags or []),
                    ','.join(exclude_tags or []),
                    str(match_mode or 'exact'),
                    now,
                    now,
                ),
            )
            return int(cursor.lastrowid)

    def get_crawl_job(self, job_id: int) -> sqlite3.Row | None:
        with self._lock, self._connect() as conn:
            return conn.execute('SELECT * FROM crawl_jobs WHERE id = ?', (job_id,)).fetchone()

    def update_crawl_job(self, job_id: int, *, status: str | None = None, progress: int | None = None, error_log: str | None = None, result_summary: str | None = None, attempt_count: int | None = None) -> None:
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

    def get_pending_job_ids(self) -> list[int]:
        with self._lock, self._connect() as conn:
            rows = conn.execute("SELECT id FROM crawl_jobs WHERE status IN ('pending', 'retry') ORDER BY id ASC").fetchall()
            return [int(row['id']) for row in rows]

    def reset_running_jobs(self) -> None:
        with self._lock, self._connect() as conn:
            conn.execute("UPDATE crawl_jobs SET status = 'retry', updated_at = ? WHERE status = 'running'", (utcnow_str(),))

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
                   it.source_type AS source_type
            FROM review_tasks rt
            JOIN images i ON i.id = rt.image_id
            JOIN tags t ON t.id = rt.tag_id
            LEFT JOIN image_tags it ON it.image_id = rt.image_id AND it.tag_id = rt.tag_id
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
                       it.source_type AS source_type
                FROM review_tasks rt
                JOIN images i ON i.id = rt.image_id
                JOIN tags t ON t.id = rt.tag_id
                LEFT JOIN image_tags it ON it.image_id = rt.image_id AND it.tag_id = rt.tag_id
                WHERE rt.id = ?
                """,
                (review_id,),
            ).fetchone()

    def apply_manual_review(self, review_id: int, *, approved: bool, reason: str = '') -> tuple[bool, str]:
        task = self.get_review_task(review_id)
        if not task:
            return False, f'审核任务不存在：{review_id}'
        new_status = 'manual_approved' if approved else 'manual_rejected'
        with self._lock, self._connect() as conn:
            conn.execute(
                "UPDATE review_tasks SET status = ?, manual_result = ?, reason = ?, updated_at = ? WHERE id = ?",
                (new_status, 'approved' if approved else 'rejected', reason, utcnow_str(), review_id),
            )
        source_type = str(task['source_type'] or '')
        source_prefix = source_type.split(':', 1)[0] if source_type else None
        self.update_image_tag_review(
            int(task['image_id']),
            int(task['tag_id']),
            new_status,
            reason=reason or ('人工通过' if approved else '人工拒绝'),
            source_type_prefix=source_prefix,
        )
        return True, f"已{'通过' if approved else '拒绝'}审核任务 #{review_id}"

    def search_images(self, *, keyword: str = '', review_status: str = '', tag_name: str = '', platform: str = '', limit: int = 100, offset: int = 0) -> list[sqlite3.Row]:
        sql = """
            SELECT DISTINCT i.id, i.file_path, i.file_name, i.width, i.height, i.format, i.phash, i.updated_at
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
        if review_status:
            sql += ' AND it.review_status = ?'
            params.append(review_status)
        if tag_name:
            sql += ' AND t.normalized_name = ?'
            params.append(normalize_tag_name(tag_name))
        if platform:
            sql += ' AND s.platform = ?'
            params.append(platform)
        sql += ' ORDER BY i.id DESC LIMIT ? OFFSET ?'
        params.extend([limit, offset])
        with self._lock, self._connect() as conn:
            return conn.execute(sql, params).fetchall()

    def get_image_detail(self, image_id: int) -> dict[str, Any] | None:
        with self._lock, self._connect() as conn:
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
            SELECT t.id, t.name, t.is_character,
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
            clauses.append('t.is_character = ?')
            params.append(1 if character_only else 0)
        if keyword:
            clauses.append('t.normalized_name LIKE ?')
            params.append(f"%{normalize_tag_name(keyword)}%")
        if clauses:
            sql += ' WHERE ' + ' AND '.join(clauses)
        sql += ' GROUP BY t.id, t.name, t.is_character ORDER BY image_count DESC, t.name ASC LIMIT ?'
        params.append(limit)
        with self._lock, self._connect() as conn:
            return conn.execute(sql, params).fetchall()

    def preview_non_character_tag_cleanup(self, *, limit: int = 200) -> list[sqlite3.Row]:
        return self.list_tags(limit=limit, character_only=False)

    def cleanup_non_character_tags(self) -> dict[str, int]:
        with self._lock, self._connect() as conn:
            rows = conn.execute('SELECT id, normalized_name FROM tags WHERE is_character = 0').fetchall()
            if not rows:
                return {
                    'tags_removed': 0,
                    'image_links_removed': 0,
                    'review_tasks_removed': 0,
                    'aliases_removed': 0,
                    'subscriptions_removed': 0,
                }
            tag_ids = [int(row['id']) for row in rows]
            normalized_names = [str(row['normalized_name'] or '') for row in rows if str(row['normalized_name'] or '').strip()]
            id_placeholders = ','.join('?' for _ in tag_ids)

            image_cursor = conn.execute(f'DELETE FROM image_tags WHERE tag_id IN ({id_placeholders})', tag_ids)
            review_cursor = conn.execute(f'DELETE FROM review_tasks WHERE tag_id IN ({id_placeholders})', tag_ids)
            alias_cursor = conn.execute(f'DELETE FROM tag_aliases WHERE tag_id IN ({id_placeholders})', tag_ids)

            subscription_sql = f'DELETE FROM crawl_subscriptions WHERE tag_id IN ({id_placeholders})'
            subscription_params: list[Any] = list(tag_ids)
            if normalized_names:
                normalized_placeholders = ','.join('?' for _ in normalized_names)
                subscription_sql += f' OR normalized_tag IN ({normalized_placeholders})'
                subscription_params.extend(normalized_names)
            subscription_cursor = conn.execute(subscription_sql, subscription_params)
            tag_cursor = conn.execute(f'DELETE FROM tags WHERE id IN ({id_placeholders})', tag_ids)

            return {
                'tags_removed': max(0, int(tag_cursor.rowcount or 0)),
                'image_links_removed': max(0, int(image_cursor.rowcount or 0)),
                'review_tasks_removed': max(0, int(review_cursor.rowcount or 0)),
                'aliases_removed': max(0, int(alias_cursor.rowcount or 0)),
                'subscriptions_removed': max(0, int(subscription_cursor.rowcount or 0)),
            }

    def list_tags_for_auto_crawl(self, *, character_only: bool = True) -> list[sqlite3.Row]:
        sql = 'SELECT id, name, is_character FROM tags'
        if character_only:
            sql += ' WHERE is_character = 1'
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
