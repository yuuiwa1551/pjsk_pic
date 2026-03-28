from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Iterable


_DIRECTIVE_KEYWORDS = {
    "tags": {"tag", "tags", "标签"},
    "include": {"include", "包含", "白名单"},
    "exclude": {"exclude", "排除", "黑名单"},
}

_DIRECTIVE_PATTERN = re.compile(
    r"(?P<key>tag|tags|include|exclude|标签|包含|排除|白名单|黑名单)\s*[:：]\s*",
    flags=re.IGNORECASE,
)


def _dedupe(items: Iterable[str]) -> list[str]:
    result: list[str] = []
    seen: set[str] = set()
    for item in items:
        text = str(item or "").strip()
        key = text.casefold()
        if not text or key in seen:
            continue
        seen.add(key)
        result.append(text)
    return result


def parse_tag_csv(value: str | Iterable[str]) -> list[str]:
    if isinstance(value, str):
        raw_items = (
            str(value)
            .replace("；", ";")
            .replace("，", ",")
            .replace("、", ",")
            .replace(";", ",")
            .split(",")
        )
    else:
        raw_items = list(value)
    return _dedupe(raw_items)


def _normalize_directive_key(value: str) -> str:
    text = str(value or "").strip().casefold()
    for normalized, aliases in _DIRECTIVE_KEYWORDS.items():
        if text in {alias.casefold() for alias in aliases}:
            return normalized
    return ""


@dataclass
class CrawlTagRules:
    manual_tags: list[str] = field(default_factory=list)
    include_tags: list[str] = field(default_factory=list)
    exclude_tags: list[str] = field(default_factory=list)

    def deduped(self) -> "CrawlTagRules":
        return CrawlTagRules(
            manual_tags=_dedupe(self.manual_tags),
            include_tags=_dedupe(self.include_tags),
            exclude_tags=_dedupe(self.exclude_tags),
        )

    def merged_with(self, other: "CrawlTagRules | None") -> "CrawlTagRules":
        other = other or CrawlTagRules()
        return CrawlTagRules(
            manual_tags=_dedupe([*self.manual_tags, *other.manual_tags]),
            include_tags=_dedupe([*self.include_tags, *other.include_tags]),
            exclude_tags=_dedupe([*self.exclude_tags, *other.exclude_tags]),
        )

    @classmethod
    def from_config(cls, config) -> "CrawlTagRules":
        return cls(
            include_tags=parse_tag_csv(config.get("crawl_include_tags", "")),
            exclude_tags=parse_tag_csv(config.get("crawl_exclude_tags", "")),
        ).deduped()

    @classmethod
    def from_db_row(cls, row) -> "CrawlTagRules":
        return cls(
            manual_tags=parse_tag_csv(str(row["tags_text"] or "")),
            include_tags=parse_tag_csv(str(row["include_tags_text"] or "")),
            exclude_tags=parse_tag_csv(str(row["exclude_tags_text"] or "")),
        ).deduped()

    def to_db_payload(self) -> tuple[str, str, str]:
        deduped = self.deduped()
        return (
            ",".join(deduped.manual_tags),
            ",".join(deduped.include_tags),
            ",".join(deduped.exclude_tags),
        )

    def has_filters(self) -> bool:
        return bool(self.include_tags or self.exclude_tags)


def parse_crawl_rule_text(raw_text: str) -> CrawlTagRules:
    text = str(raw_text or "").strip()
    if not text:
        return CrawlTagRules()

    matches = list(_DIRECTIVE_PATTERN.finditer(text))
    if not matches:
        return CrawlTagRules(manual_tags=parse_tag_csv(text))

    manual_segments: list[str] = []
    rules = CrawlTagRules()
    cursor = 0

    for index, match in enumerate(matches):
        if match.start() > cursor:
            manual_segments.append(text[cursor:match.start()].strip())

        value_end = matches[index + 1].start() if index + 1 < len(matches) else len(text)
        value_text = text[match.end():value_end].strip()
        normalized_key = _normalize_directive_key(match.group("key"))
        values = parse_tag_csv(value_text)

        if normalized_key == "tags":
            rules.manual_tags.extend(values)
        elif normalized_key == "include":
            rules.include_tags.extend(values)
        elif normalized_key == "exclude":
            rules.exclude_tags.extend(values)

        cursor = value_end

    if cursor < len(text):
        manual_segments.append(text[cursor:].strip())

    for segment in manual_segments:
        rules.manual_tags.extend(parse_tag_csv(segment))

    return rules.deduped()
