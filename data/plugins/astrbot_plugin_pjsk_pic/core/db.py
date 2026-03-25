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


def utcnow_str() -> str:
    return datetime.utcnow().isoformat(timespec="seconds")


class ImageIndexDB:
    def __init__(self, db_path: Path) -> None:
        self.db_path = db_path
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.RLock()
        self._init_db()

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
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
                PRAGMA journal_mode=WAL;

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
                    status TEXT NOT NULL DEFAULT 'pending',
                    progress INTEGER DEFAULT 0,
                    error_log TEXT DEFAULT '',
                    result_summary TEXT DEFAULT '',
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
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
                CREATE INDEX IF NOT EXISTS idx_review_tasks_status ON review_tasks(status);
                CREATE INDEX IF NOT EXISTS idx_send_logs_session_id ON send_logs(session_id);
                """
            )

            self._ensure_column(conn, 'images', 'phash', "TEXT DEFAULT ''")
            self._ensure_column(conn, 'tags', 'is_character', 'INTEGER DEFAULT 0')
            self._ensure_column(conn, 'image_tags', 'score', 'REAL DEFAULT 1.0')
            self._ensure_column(conn, 'image_tags', 'review_status', "TEXT DEFAULT 'approved'")
            self._ensure_column(conn, 'image_tags', 'review_reason', "TEXT DEFAULT ''")
            self._ensure_column(conn, 'image_tags', 'updated_at', "TEXT DEFAULT ''")
            self._ensure_column(conn, 'crawl_jobs', 'attempt_count', 'INTEGER DEFAULT 0')
            self._ensure_file_locations_initialized(conn)

    @staticmethod
    def _infer_storage_type(file_path: str) -> str:
        normalized = str(file_path or "").replace("\\", "/").lower()
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
                SELECT id, file_path, phash
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
        normalized_tag = normalize_tag_name(tag_name)
        normalized = normalize_tag_name(alias)
        if normalized == normalized_tag:
            return False, f'alias \u4E0D\u80FD\u4E0E\u4E3B tag \u76F8\u540C\uFF1A{alias}'
        with self._lock, self._connect() as conn:
            conflict_tag = conn.execute(
                'SELECT id, name FROM tags WHERE normalized_name = ? LIMIT 1',
                (normalized,),
            ).fetchone()
            if conflict_tag:
                if int(conflict_tag['id']) == tag_id:
                    return False, f'alias \u4E0D\u80FD\u4E0E\u4E3B tag \u76F8\u540C\uFF1A{alias}'
                return False, f'alias \u4E0E\u73B0\u6709 tag \u51B2\u7A81\uFF1A{conflict_tag["name"]}'
            exists = conn.execute(
                "SELECT a.tag_id, t.name FROM tag_aliases a JOIN tags t ON t.id = a.tag_id WHERE a.normalized_alias = ? LIMIT 1",
                (normalized,),
            ).fetchone()
            if exists:
                if int(exists['tag_id']) == tag_id:
                    return False, f'alias \u5DF2\u5B58\u5728\uFF1A{alias}'
                return False, f'alias \u5DF2\u88AB {exists["name"]} \u4F7F\u7528\uFF1A{alias}'
            conn.execute(
                'INSERT INTO tag_aliases(tag_id, alias, normalized_alias, created_at) VALUES(?, ?, ?, ?)',
                (tag_id, alias, normalized, utcnow_str()),
            )
        return True, f'\u5DF2\u6DFB\u52A0\u522B\u540D\uFF1A{tag_name} -> {alias}'

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
            review_count = conn.execute("SELECT COUNT(*) AS c FROM review_tasks WHERE status IN ('pending', 'uncertain')").fetchone()['c']
        return {
            'images': int(images_count),
            'tags': int(tags_count),
            'aliases': int(alias_count),
            'crawl_jobs': int(job_count),
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

    def create_crawl_job(self, platform: str, source_url: str, tags: list[str]) -> int:
        now = utcnow_str()
        with self._lock, self._connect() as conn:
            cursor = conn.execute(
                """
                INSERT INTO crawl_jobs(platform, source_url, tags_text, status, progress, error_log, result_summary, attempt_count, created_at, updated_at)
                VALUES(?, ?, ?, 'pending', 0, '', '', 0, ?, ?)
                """,
                (platform, source_url, ','.join(tags), now, now),
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

    def list_tags(self, *, keyword: str = '', limit: int = 100) -> list[sqlite3.Row]:
        sql = """
            SELECT t.id, t.name, t.is_character,
                   COUNT(DISTINCT CASE WHEN it.review_status IN ('approved', 'manual_approved') AND i.is_active = 1 THEN i.id END) AS image_count
            FROM tags t
            LEFT JOIN image_tags it ON it.tag_id = t.id
            LEFT JOIN images i ON i.id = it.image_id
        """
        params: list[Any] = []
        if keyword:
            sql += ' WHERE t.normalized_name LIKE ?'
            params.append(f"%{normalize_tag_name(keyword)}%")
        sql += ' GROUP BY t.id, t.name, t.is_character ORDER BY image_count DESC, t.name ASC LIMIT ?'
        params.append(limit)
        with self._lock, self._connect() as conn:
            return conn.execute(sql, params).fetchall()
