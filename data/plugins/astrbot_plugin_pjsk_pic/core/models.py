from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any


APPROVED_STATUSES = ("approved", "manual_approved")
REVIEWABLE_STATUSES = ("pending", "uncertain")


@dataclass
class MatchResult:
    matched: bool
    tag_id: int | None = None
    tag_name: str | None = None
    match_type: str | None = None
    candidates: list[str] = field(default_factory=list)


@dataclass
class CrawlCandidate:
    platform: str
    post_url: str
    image_url: str
    normalized_post_url: str = ""
    source_uid: str = ""
    raw_tags: list[str] = field(default_factory=list)
    author: str = ""
    title: str = ""
    extra: dict[str, Any] = field(default_factory=dict)


@dataclass
class ImportedImage:
    image_id: int
    file_path: Path
    sha256: str
    phash: str
    width: int
    height: int
    format: str
    similar_image_ids: list[int] = field(default_factory=list)


@dataclass
class ReviewDecision:
    status: str
    confidence: float
    reason: str
    raw_result: str = ""
