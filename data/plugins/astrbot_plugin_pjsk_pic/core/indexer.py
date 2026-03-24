from __future__ import annotations

import hashlib
from pathlib import Path

from astrbot.api import logger
from PIL import Image

from .db import ImageIndexDB

SUPPORTED_SUFFIXES = {".jpg", ".jpeg", ".png", ".webp", ".gif", ".bmp"}


class LibraryIndexer:
    def __init__(self, db: ImageIndexDB) -> None:
        self.db = db

    def scan(self, library_root: Path) -> dict[str, int]:
        library_root = library_root.resolve()
        library_root.mkdir(parents=True, exist_ok=True)

        scanned = 0
        inserted_or_updated = 0
        linked = 0
        skipped = 0
        seen_paths: set[str] = set()

        for file_path in library_root.rglob("*"):
            if not file_path.is_file():
                continue
            if file_path.suffix.lower() not in SUPPORTED_SUFFIXES:
                continue

            scanned += 1
            try:
                relative = file_path.relative_to(library_root)
            except ValueError:
                skipped += 1
                continue

            if len(relative.parts) < 2:
                skipped += 1
                continue

            tag_name = relative.parts[0].strip()
            if not tag_name:
                skipped += 1
                continue

            file_path_resolved = str(file_path.resolve())
            seen_paths.add(file_path_resolved)

            try:
                sha256 = self._sha256_of(file_path)
                width, height, format_ = self._read_image_meta(file_path)
                image_id = self.db.upsert_image(
                    file_path=file_path_resolved,
                    file_name=file_path.name,
                    sha256=sha256,
                    width=width,
                    height=height,
                    format_=format_,
                    storage_type="library",
                )
                tag_id = self.db.get_or_create_tag(tag_name)
                self.db.link_image_tag(image_id, tag_id, source_type="directory")
                inserted_or_updated += 1
                linked += 1
            except Exception as exc:
                logger.warning(f"[PJSKPic] 扫描图片失败，已跳过: {file_path_resolved} ({exc})")
                skipped += 1

        missing = self.db.mark_missing_files_inactive(str(library_root), seen_paths)
        return {
            "scanned": scanned,
            "indexed": inserted_or_updated,
            "linked": linked,
            "skipped": skipped,
            "missing_marked_inactive": missing,
        }

    @staticmethod
    def _sha256_of(file_path: Path) -> str:
        hasher = hashlib.sha256()
        with open(file_path, "rb") as f:
            for chunk in iter(lambda: f.read(1024 * 1024), b""):
                hasher.update(chunk)
        return hasher.hexdigest()

    @staticmethod
    def _read_image_meta(file_path: Path) -> tuple[int, int, str]:
        with Image.open(file_path) as img:
            width, height = img.size
            return int(width), int(height), str(img.format or file_path.suffix.lower().lstrip("."))
