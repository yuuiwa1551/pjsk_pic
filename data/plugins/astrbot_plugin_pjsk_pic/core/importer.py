from __future__ import annotations

import asyncio
import hashlib
import mimetypes
import time
import urllib.parse
import urllib.error
import urllib.request
from io import BytesIO
from pathlib import Path

from PIL import Image

from .db import ImageIndexDB
from .models import CrawlCandidate, ImportedImage
from .phash import compute_image_phash, hamming_distance

DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "image/avif,image/webp,image/apng,image/svg+xml,image/*,*/*;q=0.8",
}

FORMAT_EXTENSIONS = {
    "jpeg": ".jpg",
    "jpg": ".jpg",
    "png": ".png",
    "webp": ".webp",
    "gif": ".gif",
    "bmp": ".bmp",
}

FORMAT_QUALITY_RANK = {
    "png": 4,
    "webp": 3,
    "jpeg": 2,
    "jpg": 2,
    "bmp": 1,
    "gif": 0,
}


class ImportedImageService:
    def __init__(
        self,
        db: ImageIndexDB,
        data_dir: Path,
        *,
        timeout_seconds: int = 20,
        enable_phash_dedupe: bool = True,
        phash_max_distance: int = 8,
    ) -> None:
        self.db = db
        self.data_dir = data_dir
        self.import_root = (data_dir / "images" / "imported").resolve()
        self.import_root.mkdir(parents=True, exist_ok=True)
        self.timeout_seconds = timeout_seconds
        self.enable_phash_dedupe = enable_phash_dedupe
        self.phash_max_distance = phash_max_distance
        self.download_retry_times = 3

    async def import_candidate(self, candidate: CrawlCandidate) -> ImportedImage:
        return await asyncio.to_thread(self._import_candidate_sync, candidate)

    async def import_local_file(self, source_path: str | Path, *, platform: str = "submission") -> ImportedImage:
        return await asyncio.to_thread(self._import_local_file_sync, Path(source_path), platform)

    def _import_candidate_sync(self, candidate: CrawlCandidate) -> ImportedImage:
        headers = dict(DEFAULT_HEADERS)
        extra_headers = candidate.extra.get("request_headers", {}) if isinstance(candidate.extra, dict) else {}
        headers.update({str(k): str(v) for k, v in dict(extra_headers).items()})
        body, content_type, final_url = self._download_remote_bytes(candidate.image_url, headers=headers)

        return self._store_imported_bytes(
            body,
            source_name=final_url,
            content_type=content_type,
            platform=candidate.platform,
        )

    def _download_remote_bytes(self, image_url: str, *, headers: dict[str, str]) -> tuple[bytes, str, str]:
        last_error: Exception | None = None
        max_attempts = max(1, int(self.download_retry_times or 1))
        for attempt in range(1, max_attempts + 1):
            request = urllib.request.Request(image_url, headers=headers)
            try:
                with urllib.request.urlopen(request, timeout=self.timeout_seconds) as response:
                    return (
                        response.read(),
                        response.headers.get("Content-Type", ""),
                        response.geturl(),
                    )
            except (urllib.error.URLError, TimeoutError, OSError) as exc:
                last_error = exc
                if attempt >= max_attempts:
                    raise
                time.sleep(min(1.5 * attempt, 3.0))
            except Exception as exc:
                last_error = exc
                if attempt >= max_attempts:
                    raise
                time.sleep(min(1.5 * attempt, 3.0))
        if last_error is not None:
            raise last_error
        raise RuntimeError("download failed without a captured error")

    def _import_local_file_sync(self, source_path: Path, platform: str) -> ImportedImage:
        body = source_path.read_bytes()
        guessed_type, _ = mimetypes.guess_type(source_path.name)
        return self._store_imported_bytes(
            body,
            source_name=str(source_path),
            content_type=guessed_type or "",
            platform=platform,
        )

    def _store_imported_bytes(
        self,
        body: bytes,
        *,
        source_name: str,
        content_type: str,
        platform: str,
    ) -> ImportedImage:
        sha256 = hashlib.sha256(body).hexdigest()
        width, height, format_name = self._read_image_meta(body)
        phash = compute_image_phash(body) if self.enable_phash_dedupe else ""
        similar_rows = self.db.find_similar_images_by_phash(phash, max_distance=self.phash_max_distance) if phash else []
        duplicate_target = self._pick_duplicate_target(
            similar_rows,
            phash=phash,
            width=width,
            height=height,
        )
        extension = self._guess_extension(source_name, content_type, format_name)
        file_dir = self.import_root / platform / sha256[:2]
        file_dir.mkdir(parents=True, exist_ok=True)
        file_path = file_dir / f"{sha256}{extension}"
        candidate_quality = self._quality_key(width=width, height=height, body_size=len(body), format_name=format_name)

        if duplicate_target is not None:
            target_id = int(duplicate_target["id"])
            existing_quality = self._quality_key(
                width=int(duplicate_target["width"] or 0),
                height=int(duplicate_target["height"] or 0),
                body_size=0,
                format_name=str(duplicate_target["format"] or ""),
            )
            if candidate_quality > existing_quality:
                if not file_path.exists():
                    file_path.write_bytes(body)
                image_id = self.db.attach_image_variant(
                    target_id,
                    file_path=str(file_path),
                    file_name=file_path.name,
                    sha256=sha256,
                    phash=phash,
                    width=width,
                    height=height,
                    format_=format_name,
                    storage_type="imported",
                    make_primary=True,
                )
                return ImportedImage(
                    image_id=image_id,
                    file_path=file_path,
                    sha256=sha256,
                    phash=phash,
                    width=width,
                    height=height,
                    format=format_name,
                    similar_image_ids=[int(row["id"]) for row in similar_rows if int(row["id"]) != image_id],
                )

            existing = self.db.get_image_row(target_id)
            resolved_path = Path(self.db.get_image_file_path(target_id) or str(existing["file_path"] if existing else ""))
            if not resolved_path.exists():
                resolved_path = file_path
            return ImportedImage(
                image_id=target_id,
                file_path=resolved_path,
                sha256=str(existing["sha256"] or sha256) if existing else sha256,
                phash=str(existing["phash"] or phash) if existing else phash,
                width=int(existing["width"] or width) if existing else width,
                height=int(existing["height"] or height) if existing else height,
                format=str(existing["format"] or format_name) if existing else format_name,
                similar_image_ids=[int(row["id"]) for row in similar_rows if int(row["id"]) != target_id],
            )

        if not file_path.exists():
            file_path.write_bytes(body)

        image_id = self.db.upsert_image(
            file_path=str(file_path),
            file_name=file_path.name,
            sha256=sha256,
            phash=phash,
            width=width,
            height=height,
            format_=format_name,
            storage_type="imported",
        )
        return ImportedImage(
            image_id=image_id,
            file_path=file_path,
            sha256=sha256,
            phash=phash,
            width=width,
            height=height,
            format=format_name,
            similar_image_ids=[int(row["id"]) for row in similar_rows if int(row["id"]) != image_id],
        )

    @staticmethod
    def _read_image_meta(body: bytes) -> tuple[int, int, str]:
        with Image.open(BytesIO(body)) as image:
            width, height = image.size
            format_name = str(image.format or "").lower() or "bin"
            return int(width), int(height), format_name

    @staticmethod
    def _guess_extension(final_url: str, content_type: str, format_name: str) -> str:
        guessed = FORMAT_EXTENSIONS.get((format_name or "").lower())
        if guessed:
            return guessed
        by_type = mimetypes.guess_extension((content_type or "").split(";")[0].strip())
        if by_type:
            return by_type
        by_url = Path(urllib.parse.urlparse(final_url).path).suffix.lower()
        if by_url:
            return by_url
        return ".bin"

    @staticmethod
    def _quality_key(*, width: int, height: int, body_size: int, format_name: str) -> tuple[int, int, int, int]:
        area = max(0, int(width or 0)) * max(0, int(height or 0))
        longest_edge = max(int(width or 0), int(height or 0))
        format_rank = FORMAT_QUALITY_RANK.get(str(format_name or "").lower(), 0)
        return (area, longest_edge, max(0, int(body_size or 0)), format_rank)

    @staticmethod
    def _roughly_same_aspect_ratio(width: int, height: int, other_width: int, other_height: int) -> bool:
        if min(width, height, other_width, other_height) <= 0:
            return False
        ratio = width / height
        other_ratio = other_width / other_height
        return abs(ratio - other_ratio) <= 0.03

    def _pick_duplicate_target(
        self,
        similar_rows,
        *,
        phash: str,
        width: int,
        height: int,
    ):
        if not phash:
            return None
        strict_distance = max(0, min(int(self.phash_max_distance or 0), 3))
        best_row = None
        best_key = None
        for row in similar_rows:
            other_phash = str(row["phash"] or "")
            if not other_phash:
                continue
            distance = hamming_distance(phash, other_phash)
            if distance > strict_distance:
                continue
            other_width = int(row["width"] or 0)
            other_height = int(row["height"] or 0)
            if not self._roughly_same_aspect_ratio(width, height, other_width, other_height):
                continue
            quality_key = self._quality_key(
                width=other_width,
                height=other_height,
                body_size=0,
                format_name=str(row["format"] or ""),
            )
            candidate_key = (distance, tuple(-value for value in quality_key), -int(row["id"]))
            if best_key is None or candidate_key < best_key:
                best_key = candidate_key
                best_row = row
        return best_row
