from __future__ import annotations

import asyncio
import hashlib
import imghdr
import mimetypes
import urllib.parse
import urllib.request
from io import BytesIO
from pathlib import Path

from PIL import Image

from .db import ImageIndexDB
from .models import CrawlCandidate, ImportedImage
from .phash import compute_image_phash

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

    async def import_candidate(self, candidate: CrawlCandidate) -> ImportedImage:
        return await asyncio.to_thread(self._import_candidate_sync, candidate)

    def _import_candidate_sync(self, candidate: CrawlCandidate) -> ImportedImage:
        headers = dict(DEFAULT_HEADERS)
        extra_headers = candidate.extra.get("request_headers", {}) if isinstance(candidate.extra, dict) else {}
        headers.update({str(k): str(v) for k, v in dict(extra_headers).items()})
        request = urllib.request.Request(candidate.image_url, headers=headers)
        with urllib.request.urlopen(request, timeout=self.timeout_seconds) as response:
            body = response.read()
            content_type = response.headers.get("Content-Type", "")
            final_url = response.geturl()

        sha256 = hashlib.sha256(body).hexdigest()
        width, height, format_name = self._read_image_meta(body)
        phash = compute_image_phash(body) if self.enable_phash_dedupe else ""
        similar_rows = self.db.find_similar_images_by_phash(phash, max_distance=self.phash_max_distance) if phash else []
        extension = self._guess_extension(final_url, content_type, format_name)
        file_dir = self.import_root / candidate.platform / sha256[:2]
        file_dir.mkdir(parents=True, exist_ok=True)
        file_path = file_dir / f"{sha256}{extension}"
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
            format_name = str(image.format or "").lower() or (imghdr.what(None, h=body) or "bin")
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
