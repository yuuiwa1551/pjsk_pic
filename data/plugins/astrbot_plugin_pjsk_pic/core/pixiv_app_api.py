from __future__ import annotations

import hashlib
import json
import urllib.parse

import requests
from dataclasses import dataclass
from datetime import datetime
from typing import Any

# Adapted from the refresh-token App API flow used by upbit/pixivpy (Unlicense):
# https://github.com/upbit/pixivpy
PIXIV_APP_CLIENT_ID = "MOBrBDS8blbauoSck0ZfDbtuzpyT"
PIXIV_APP_CLIENT_SECRET = "lsACyCD94FhDUtGTXi3QzcFE2uU1hqtDaKeqrdwj"
PIXIV_APP_HASH_SECRET = "28c1fdd170a5204386cb1313c7077b34f83e4aaf4aa829ce78c231e05b0bae2c"
PIXIV_APP_USER_AGENT = "PixivIOSApp/7.13.3 (iOS 14.6; iPhone13,2)"
PIXIV_APP_OS = "ios"
PIXIV_APP_OS_VERSION = "14.6"
PIXIV_AUTH_URL = "https://oauth.secure.pixiv.net/auth/token"
PIXIV_API_HOST = "https://app-api.pixiv.net"


class PixivAppAPIError(RuntimeError):
    pass


@dataclass
class PixivToken:
    access_token: str
    refresh_token: str
    user_id: str = ""


def _request_json(
    url: str,
    *,
    method: str = "GET",
    headers: dict[str, str] | None = None,
    data: dict[str, Any] | None = None,
    timeout_seconds: int = 20,
) -> dict[str, Any]:
    request_headers = {
        "User-Agent": PIXIV_APP_USER_AGENT,
        "app-os": PIXIV_APP_OS,
        "app-os-version": PIXIV_APP_OS_VERSION,
    }
    if headers:
        request_headers.update({str(key): str(value) for key, value in headers.items() if value is not None})
    body = None
    if data is not None:
        body = {str(key): value for key, value in data.items()}
        request_headers.setdefault("Content-Type", "application/x-www-form-urlencoded")
    try:
        response = requests.request(
            method.upper(),
            url,
            headers=request_headers,
            data=body,
            timeout=max(5, int(timeout_seconds or 20)),
        )
        response.raise_for_status()
        payload = response.text
    except requests.HTTPError as exc:
        payload = getattr(exc.response, 'text', '') if getattr(exc, 'response', None) is not None else ''
        status_code = getattr(exc.response, 'status_code', 'unknown') if getattr(exc, 'response', None) is not None else 'unknown'
        raise PixivAppAPIError(f"HTTP {status_code}: {payload or exc}") from exc
    except requests.RequestException as exc:
        raise PixivAppAPIError(f"?????{exc}") from exc
    except Exception as exc:
        raise PixivAppAPIError(f"?????{exc}") from exc

    try:
        parsed = json.loads(payload)
    except json.JSONDecodeError as exc:
        raise PixivAppAPIError(f"?????? JSON?{payload[:200]}") from exc
    if not isinstance(parsed, dict):
        raise PixivAppAPIError("Pixiv API ?????? JSON ??")
    return parsed


def authenticate_with_refresh_token(refresh_token: str, *, timeout_seconds: int = 20) -> PixivToken:
    token = str(refresh_token or "").strip()
    if not token:
        raise PixivAppAPIError("??? Pixiv refresh token")

    local_time = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S+00:00")
    headers = {
        "x-client-time": local_time,
        "x-client-hash": hashlib.md5((local_time + PIXIV_APP_HASH_SECRET).encode("utf-8")).hexdigest(),
    }
    payload = _request_json(
        PIXIV_AUTH_URL,
        method="POST",
        headers=headers,
        data={
            "get_secure_url": 1,
            "client_id": PIXIV_APP_CLIENT_ID,
            "client_secret": PIXIV_APP_CLIENT_SECRET,
            "grant_type": "refresh_token",
            "refresh_token": token,
        },
        timeout_seconds=timeout_seconds,
    )
    response = payload.get("response")
    if not isinstance(response, dict):
        raise PixivAppAPIError(f"refresh token ?????{payload}")

    access_token = str(response.get("access_token", "") or "").strip()
    next_refresh_token = str(response.get("refresh_token", "") or "").strip()
    if not access_token:
        raise PixivAppAPIError(f"Pixiv ??? access_token?{payload}")

    user = response.get("user")
    user_id = ""
    if isinstance(user, dict):
        user_id = str(user.get("id", "") or "").strip()
    return PixivToken(
        access_token=access_token,
        refresh_token=next_refresh_token or token,
        user_id=user_id,
    )


def fetch_illust_detail(
    illust_id: str | int,
    *,
    refresh_token: str,
    timeout_seconds: int = 20,
    accept_language: str = "zh-cn",
) -> dict[str, Any]:
    token = authenticate_with_refresh_token(refresh_token, timeout_seconds=timeout_seconds)
    query = urllib.parse.urlencode({"illust_id": str(illust_id)})
    headers = {"Authorization": f"Bearer {token.access_token}"}
    if accept_language:
        headers["Accept-Language"] = accept_language
    payload = _request_json(
        f"{PIXIV_API_HOST}/v1/illust/detail?{query}",
        headers=headers,
        timeout_seconds=timeout_seconds,
    )
    illust = payload.get("illust")
    if not isinstance(illust, dict):
        raise PixivAppAPIError(f"?????????{payload}")
    return illust


def search_illusts(
    word: str,
    *,
    refresh_token: str,
    search_target: str = "partial_match_for_tags",
    sort: str = "date_desc",
    search_ai_type: int | None = 0,
    offset: int | str | None = None,
    timeout_seconds: int = 20,
    accept_language: str = "zh-cn",
) -> dict[str, Any]:
    query: dict[str, Any] = {
        "word": str(word or "").strip(),
        "search_target": search_target or "partial_match_for_tags",
        "sort": sort or "date_desc",
        "filter": "for_ios",
    }
    if not query["word"]:
        raise PixivAppAPIError("Pixiv ???????")
    if search_ai_type is not None:
        query["search_ai_type"] = int(search_ai_type)
    if offset not in (None, ""):
        query["offset"] = offset

    token = authenticate_with_refresh_token(refresh_token, timeout_seconds=timeout_seconds)
    headers = {"Authorization": f"Bearer {token.access_token}"}
    if accept_language:
        headers["Accept-Language"] = accept_language
    payload = _request_json(
        f"{PIXIV_API_HOST}/v1/search/illust?{urllib.parse.urlencode(query)}",
        headers=headers,
        timeout_seconds=timeout_seconds,
    )
    illusts = payload.get("illusts")
    if not isinstance(illusts, list):
        raise PixivAppAPIError(f"Pixiv ???????{payload}")
    return payload


def extract_offset_from_next_url(next_url: str | None) -> int | None:
    raw_url = str(next_url or "").strip()
    if not raw_url:
        return None
    parsed = urllib.parse.urlparse(raw_url)
    values = urllib.parse.parse_qs(parsed.query).get("offset") or []
    if not values:
        return None
    try:
        return int(values[0])
    except (TypeError, ValueError):
        return None
