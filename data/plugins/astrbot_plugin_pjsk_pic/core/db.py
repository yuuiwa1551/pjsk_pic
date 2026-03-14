from __future__ import annotations

import sqlite3
import threading
from datetime import datetime
from pathlib import Path

from .matcher import normalize_tag_name
from .models import MatchResult


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
                    created_at TEXT NOT NULL,
                    UNIQUE(image_id, tag_id, source_type),
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
                CREATE INDEX IF NOT EXISTS idx_image_tags_tag_id ON image_tags(tag_id);
                CREATE INDEX IF NOT EXISTS idx_send_logs_session_id ON send_logs(session_id);
                """
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
    ) -> int:
        now = utcnow_str()
        with self._lock, self._connect() as conn:
            row = conn.execute(
                "SELECT id FROM images WHERE file_path = ?",
                (file_path,),
            ).fetchone()
            if row:
                conn.execute(
                    """
                    UPDATE images
                    SET file_name = ?, sha256 = ?, width = ?, height = ?, format = ?, is_active = 1, updated_at = ?
                    WHERE id = ?
                    """,
                    (file_name, sha256, width, height, format_, now, row["id"]),
                )
                return int(row["id"])

            cursor = conn.execute(
                """
                INSERT INTO images(file_path, file_name, sha256, width, height, format, is_active, created_at, updated_at)
                VALUES(?, ?, ?, ?, ?, ?, 1, ?, ?)
                """,
                (file_path, file_name, sha256, width, height, format_, now, now),
            )
            return int(cursor.lastrowid)

    def mark_missing_files_inactive(self, library_root: str, seen_paths: set[str]) -> int:
        root = str(Path(library_root).resolve())
        count = 0
        with self._lock, self._connect() as conn:
            rows = conn.execute(
                "SELECT id, file_path FROM images WHERE file_path LIKE ?",
                (f"{root}%",),
            ).fetchall()
            for row in rows:
                if row["file_path"] not in seen_paths:
                    conn.execute(
                        "UPDATE images SET is_active = 0, updated_at = ? WHERE id = ?",
                        (utcnow_str(), row["id"]),
                    )
                    count += 1
        return count

    def get_or_create_tag(self, tag_name: str) -> int:
        normalized = normalize_tag_name(tag_name)
        now = utcnow_str()
        with self._lock, self._connect() as conn:
            row = conn.execute(
                "SELECT id FROM tags WHERE normalized_name = ?",
                (normalized,),
            ).fetchone()
            if row:
                return int(row["id"])
            cursor = conn.execute(
                "INSERT INTO tags(name, normalized_name, created_at) VALUES(?, ?, ?)",
                (tag_name.strip(), normalized, now),
            )
            return int(cursor.lastrowid)

    def link_image_tag(self, image_id: int, tag_id: int, source_type: str = "directory") -> None:
        with self._lock, self._connect() as conn:
            conn.execute(
                """
                INSERT OR IGNORE INTO image_tags(image_id, tag_id, source_type, created_at)
                VALUES(?, ?, ?, ?)
                """,
                (image_id, tag_id, source_type, utcnow_str()),
            )

    def add_alias(self, tag_name: str, alias: str) -> tuple[bool, str]:
        tag_name = tag_name.strip()
        alias = alias.strip()
        if not tag_name or not alias:
            return False, "tag 和 alias 都不能为空。"
        tag_id = self.get_tag_id(tag_name)
        if tag_id is None:
            return False, f"tag 不存在：{tag_name}"
        normalized = normalize_tag_name(alias)
        with self._lock, self._connect() as conn:
            exists = conn.execute(
                "SELECT id FROM tag_aliases WHERE normalized_alias = ?",
                (normalized,),
            ).fetchone()
            if exists:
                return False, f"alias 已存在：{alias}"
            conn.execute(
                "INSERT INTO tag_aliases(tag_id, alias, normalized_alias, created_at) VALUES(?, ?, ?, ?)",
                (tag_id, alias, normalized, utcnow_str()),
            )
        return True, f"已添加别名：{tag_name} -> {alias}"

    def remove_alias(self, tag_name: str, alias: str) -> tuple[bool, str]:
        tag_id = self.get_tag_id(tag_name.strip())
        if tag_id is None:
            return False, f"tag 不存在：{tag_name}"
        normalized = normalize_tag_name(alias)
        with self._lock, self._connect() as conn:
            cursor = conn.execute(
                "DELETE FROM tag_aliases WHERE tag_id = ? AND normalized_alias = ?",
                (tag_id, normalized),
            )
            if cursor.rowcount <= 0:
                return False, f"别名不存在：{alias}"
        return True, f"已删除别名：{tag_name} -> {alias}"

    def list_aliases(self, tag_name: str) -> list[str]:
        tag_id = self.get_tag_id(tag_name.strip())
        if tag_id is None:
            return []
        with self._lock, self._connect() as conn:
            rows = conn.execute(
                "SELECT alias FROM tag_aliases WHERE tag_id = ? ORDER BY alias ASC",
                (tag_id,),
            ).fetchall()
            return [str(row["alias"]) for row in rows]

    def get_tag_id(self, tag_name: str) -> int | None:
        normalized = normalize_tag_name(tag_name)
        with self._lock, self._connect() as conn:
            row = conn.execute(
                "SELECT id FROM tags WHERE normalized_name = ?",
                (normalized,),
            ).fetchone()
            return int(row["id"]) if row else None

    def get_stats(self) -> dict[str, int]:
        with self._lock, self._connect() as conn:
            images_count = conn.execute(
                "SELECT COUNT(*) AS c FROM images WHERE is_active = 1",
            ).fetchone()["c"]
            tags_count = conn.execute(
                """
                SELECT COUNT(*) AS c
                FROM tags t
                WHERE EXISTS (
                    SELECT 1
                    FROM image_tags it
                    JOIN images i ON i.id = it.image_id
                    WHERE it.tag_id = t.id AND i.is_active = 1
                )
                """,
            ).fetchone()["c"]
            alias_count = conn.execute(
                "SELECT COUNT(*) AS c FROM tag_aliases",
            ).fetchone()["c"]
        return {
            "images": int(images_count),
            "tags": int(tags_count),
            "aliases": int(alias_count),
        }

    def count_images_for_tag(self, tag_name: str) -> int:
        tag_id = self.get_tag_id(tag_name)
        if tag_id is None:
            return 0
        with self._lock, self._connect() as conn:
            row = conn.execute(
                """
                SELECT COUNT(DISTINCT i.id) AS c
                FROM image_tags it
                JOIN images i ON i.id = it.image_id
                WHERE it.tag_id = ? AND i.is_active = 1
                """,
                (tag_id,),
            ).fetchone()
            return int(row["c"])

    def resolve_tag(self, query: str, allow_fuzzy: bool = True, candidate_limit: int = 5) -> MatchResult:
        normalized = normalize_tag_name(query)
        if not normalized:
            return MatchResult(matched=False)

        with self._lock, self._connect() as conn:
            exact_tag = conn.execute(
                """
                SELECT t.id, t.name
                FROM tags t
                WHERE t.normalized_name = ?
                LIMIT 1
                """,
                (normalized,),
            ).fetchone()
            if exact_tag:
                return MatchResult(
                    matched=True,
                    tag_id=int(exact_tag["id"]),
                    tag_name=str(exact_tag["name"]),
                    match_type="exact_tag",
                )

            exact_alias = conn.execute(
                """
                SELECT t.id, t.name
                FROM tag_aliases a
                JOIN tags t ON t.id = a.tag_id
                WHERE a.normalized_alias = ?
                LIMIT 1
                """,
                (normalized,),
            ).fetchone()
            if exact_alias:
                return MatchResult(
                    matched=True,
                    tag_id=int(exact_alias["id"]),
                    tag_name=str(exact_alias["name"]),
                    match_type="exact_alias",
                )

            if not allow_fuzzy:
                return MatchResult(matched=False)

            candidates = conn.execute(
                """
                SELECT DISTINCT t.id, t.name, COUNT(DISTINCT i.id) AS image_count
                FROM tags t
                LEFT JOIN tag_aliases a ON a.tag_id = t.id
                LEFT JOIN image_tags it ON it.tag_id = t.id
                LEFT JOIN images i ON i.id = it.image_id AND i.is_active = 1
                WHERE t.normalized_name LIKE ? OR a.normalized_alias LIKE ?
                GROUP BY t.id, t.name
                ORDER BY image_count DESC, t.name ASC
                LIMIT ?
                """,
                (f"%{normalized}%", f"%{normalized}%", candidate_limit + 1),
            ).fetchall()

        if not candidates:
            return MatchResult(matched=False)
        if len(candidates) == 1:
            row = candidates[0]
            return MatchResult(
                matched=True,
                tag_id=int(row["id"]),
                tag_name=str(row["name"]),
                match_type="fuzzy",
            )
        return MatchResult(
            matched=False,
            candidates=[str(row["name"]) for row in candidates[:candidate_limit]],
        )

    def get_random_image_for_tag(self, tag_id: int, excluded_image_ids: list[int] | None = None) -> sqlite3.Row | None:
        excluded_image_ids = excluded_image_ids or []
        with self._lock, self._connect() as conn:
            if excluded_image_ids:
                placeholders = ",".join("?" for _ in excluded_image_ids)
                row = conn.execute(
                    f"""
                    SELECT DISTINCT i.id, i.file_path, i.file_name
                    FROM image_tags it
                    JOIN images i ON i.id = it.image_id
                    WHERE it.tag_id = ?
                      AND i.is_active = 1
                      AND i.id NOT IN ({placeholders})
                    ORDER BY RANDOM()
                    LIMIT 1
                    """,
                    (tag_id, *excluded_image_ids),
                ).fetchone()
                if row:
                    return row

            return conn.execute(
                """
                SELECT DISTINCT i.id, i.file_path, i.file_name
                FROM image_tags it
                JOIN images i ON i.id = it.image_id
                WHERE it.tag_id = ? AND i.is_active = 1
                ORDER BY RANDOM()
                LIMIT 1
                """,
                (tag_id,),
            ).fetchone()

    def record_send_log(self, session_id: str, image_id: int, matched_tag: str) -> None:
        with self._lock, self._connect() as conn:
            conn.execute(
                "INSERT INTO send_logs(session_id, image_id, matched_tag, sent_at) VALUES(?, ?, ?, ?)",
                (session_id, image_id, matched_tag, utcnow_str()),
            )
