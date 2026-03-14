import re
import unicodedata

TRIGGER_PATTERNS = [
    re.compile(r"^(?:看看|看下|看一看)(.+?)(?:的?(?:图片|图))?$"),
    re.compile(r"^(?:来张|来一张|发一张)(.+?)(?:的?(?:图片|图))?$"),
    re.compile(r"^(?:来点)(.+?)(?:图片|图)$"),
]

NOISE_SUFFIXES = [
    "图片",
    "图图",
    "图",
    "老婆",
    "老公",
    "来一张",
    "来张",
    "看看",
    "看下",
    "看一看",
]


def normalize_tag_name(value: str) -> str:
    text = unicodedata.normalize("NFKC", value or "").strip().lower()
    text = re.sub(r"\s+", "", text)
    return text


def cleanup_query(value: str) -> str:
    text = unicodedata.normalize("NFKC", value or "").strip()
    text = re.sub(r"[。！？!?,，\s]+$", "", text)
    changed = True
    while changed and text:
        changed = False
        for suffix in NOISE_SUFFIXES:
            if text.endswith(suffix) and len(text) > len(suffix):
                text = text[: -len(suffix)].strip()
                changed = True
    return text


def extract_query_from_text(message: str) -> str | None:
    text = unicodedata.normalize("NFKC", message or "").strip()
    if not text:
        return None

    for pattern in TRIGGER_PATTERNS:
        match = pattern.match(text)
        if match:
            query = cleanup_query(match.group(1))
            return query or None
    return None
