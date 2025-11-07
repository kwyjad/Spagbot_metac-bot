"""Resilient Metaculus API client with Cloudflare fallback."""
from __future__ import annotations

import asyncio
import os
from typing import Any, Dict, Iterable, Mapping, MutableMapping, Optional

import httpx

from spagbot.util.diagnostics import write_jsonl

# Environment knobs ------------------------------------------------------------
_DEFAULT_API_BASE = "https://www.metaculus.com/api"
_API_BASE = os.getenv("METACULUS_API_BASE", _DEFAULT_API_BASE).rstrip("/")
_TOKEN = os.getenv("METACULUS_TOKEN", "")
_TIMEOUT = float(os.getenv("METACULUS_REQUEST_TIMEOUT", os.getenv("METACULUS_HTTP_TIMEOUT", "30")))
_MAX_RETRIES = int(os.getenv("METACULUS_MAX_RETRIES", "3"))
_DIAG_DIR = os.getenv("LOGS_BASE_DIR", "forecast_logs")
_DIAG_PATH = os.path.join(_DIAG_DIR, "diagnostics", "metaculus_http.jsonl")

_CF_MARKERS: Iterable[str] = ("Just a moment", "__cf_chl", "challenge-platform")

_TYPE_ALIASES: Dict[str, str] = {
    "binary": "binary",
    "multiple_choice": "multiple_choice",
    "mcq": "multiple_choice",
    "numeric": "numeric",
    "discrete": "discrete",
}


def _headers() -> Dict[str, str]:
    headers = {
        "User-Agent": os.getenv(
            "SPAGBOT_UA",
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Spagbot/1.0",
        ),
        "Accept": "application/json, text/plain, */*",
        "Accept-Language": "en-US,en;q=0.9",
        "Referer": "https://www.metaculus.com/",
        "Cache-Control": "no-cache",
        "Pragma": "no-cache",
    }
    if _TOKEN:
        headers["Authorization"] = f"Token {_TOKEN}"
    return headers


def _looks_like_cloudflare(content_type: str, body: str) -> bool:
    if "text/html" in (content_type or "").lower():
        return True
    return any(marker in body for marker in _CF_MARKERS)


def _coerce_type(value: Optional[str]) -> Optional[str]:
    if not value:
        return None
    key = str(value).strip().lower()
    if not key:
        return None
    return _TYPE_ALIASES.get(key, key)


def _normalise_question_block(
    question: MutableMapping[str, Any],
    *,
    fallback_title: Optional[str] = None,
    fallback_type: Optional[str] = None,
) -> MutableMapping[str, Any]:
    qtype = _coerce_type(
        question.get("type")
        or question.get("question_type")
        or fallback_type
    )
    if qtype:
        question["type"] = qtype
        question["question_type"] = qtype

    title = (
        question.get("title")
        or question.get("question_text")
        or question.get("name")
        or fallback_title
    )
    if title:
        question.setdefault("title", title)
        question.setdefault("question_text", title)

    return question


def _normalise_post_dict(post: Mapping[str, Any]) -> Dict[str, Any]:
    post_map: MutableMapping[str, Any] = dict(post)

    question_data = post_map.get("question")
    if isinstance(question_data, Mapping):
        question_map: MutableMapping[str, Any] = dict(question_data)
    else:
        question_map = {}

    fallback_title = (
        post_map.get("title")
        or question_map.get("title")
        or question_map.get("question_text")
        or post_map.get("name")
        or post_map.get("question")
    )
    fallback_type = (
        post_map.get("type")
        or post_map.get("question_type")
        or question_map.get("type")
        or question_map.get("question_type")
    )

    question_map = _normalise_question_block(
        question_map,
        fallback_title=fallback_title,
        fallback_type=fallback_type,
    )

    if question_map.get("id") is None:
        question_map["id"] = post_map.get("id") or post_map.get("post_id")
    if question_map.get("url") is None and post_map.get("url"):
        question_map["url"] = post_map.get("url")

    post_type = _coerce_type(
        post_map.get("type")
        or post_map.get("question_type")
        or question_map.get("type")
    )
    if post_type:
        post_map["type"] = post_type
        post_map["question_type"] = post_type
        question_map["type"] = post_type
        question_map["question_type"] = post_type

    if fallback_title:
        post_map.setdefault("title", fallback_title)

    post_map["question"] = question_map
    return dict(post_map)


def _normalise_wrapper_question(question: Any) -> Dict[str, Any]:
    if isinstance(question, Mapping):
        q_map: MutableMapping[str, Any] = dict(question)
    else:
        q_map = {}
        for attr in ("to_dict", "model_dump", "dict"):
            getter = getattr(question, attr, None)
            if callable(getter):
                try:
                    maybe = getter()
                except Exception:  # pragma: no cover - defensive
                    continue
                if isinstance(maybe, Mapping):
                    q_map = dict(maybe)
                    break
        if not q_map:
            maybe_dict = getattr(question, "__dict__", None)
            if isinstance(maybe_dict, Mapping):
                q_map = dict(maybe_dict)

    post_id = q_map.get("post_id") or q_map.get("id") or getattr(question, "post_id", None)
    if post_id is None:
        post_id = getattr(question, "id", None)

    title = (
        q_map.get("question_text")
        or q_map.get("title")
        or getattr(question, "question_text", None)
        or getattr(question, "title", None)
    )
    url = q_map.get("page_url") or q_map.get("url") or getattr(question, "page_url", None) or getattr(question, "url", None)
    raw_type = (
        q_map.get("question_type")
        or q_map.get("type")
        or getattr(question, "question_type", None)
        or getattr(question, "type", None)
    )
    close_time = q_map.get("close_time") or getattr(question, "close_time", None)
    if hasattr(close_time, "isoformat"):
        try:
            close_time = close_time.isoformat()
        except Exception:  # pragma: no cover - defensive
            close_time = None

    base_post: Dict[str, Any] = {
        "id": post_id,
        "post_id": post_id,
        "title": title,
        "url": url,
        "type": raw_type,
        "close_time": close_time,
        "question": {
            "id": post_id,
            "title": title,
            "question_text": title,
            "url": url,
            "question_type": raw_type,
            "type": raw_type,
            "close_time": close_time,
        },
    }

    # Preserve any extra fields from the wrapper mapping.
    base_post.update({k: v for k, v in q_map.items() if k not in base_post})

    return _normalise_post_dict(base_post)


async def _http_list_posts(tournament_id: str | int, *, limit: int, offset: int) -> Mapping[str, Any]:
    params = {
        "limit": limit,
        "offset": offset,
        "order_by": "-hotness",
        "forecast_type": "binary,multiple_choice,numeric,discrete",
        "tournaments": tournament_id,
        "statuses": "open",
        "include_description": "true",
    }
    url = f"{_API_BASE}/posts/"

    async with httpx.AsyncClient(timeout=_TIMEOUT, follow_redirects=False, headers=_headers()) as client:
        last_exc: Exception | None = None
        for attempt in range(1, _MAX_RETRIES + 1):
            try:
                response = await client.get(url, params=params)
                content_type = response.headers.get("content-type", "")
                if response.status_code == 200 and content_type.lower().startswith("application/json"):
                    data = response.json()
                    if isinstance(data, Mapping):
                        payload = dict(data)
                        for key in ("results", "posts"):
                            if isinstance(payload.get(key), list):
                                payload[key] = [_normalise_post_dict(p) for p in payload[key]]
                        return payload
                    if isinstance(data, list):
                        return {"results": [_normalise_post_dict(p) for p in data]}
                    return {"results": []}

                body_snippet = (response.text or "")[:300]
                is_cf = _looks_like_cloudflare(content_type, body_snippet)
                write_jsonl(
                    _DIAG_PATH,
                    {
                        "phase": "metaculus_http",
                        "status": response.status_code,
                        "ctype": content_type,
                        "is_cf": bool(is_cf),
                        "body_snippet": body_snippet,
                    },
                )
                if not is_cf and response.status_code < 500:
                    response.raise_for_status()
            except Exception as exc:  # noqa: BLE001 - we intentionally capture all for retry
                last_exc = exc
            await asyncio.sleep(min(2 ** attempt, 8))

    if last_exc is not None:
        raise last_exc
    raise RuntimeError("Metaculus posts fetch failed without explicit exception.")


async def list_posts_from_tournament_resilient(
    tournament_id: str | int,
    *,
    limit: int,
    offset: int = 0,
) -> Mapping[str, Any]:
    """Fetch tournament posts via HTTP, falling back to forecasting-tools on errors."""
    try:
        return await _http_list_posts(tournament_id, limit=limit, offset=offset)
    except Exception as primary_error:
        write_jsonl(
            _DIAG_PATH,
            {
                "phase": "metaculus_http_error",
                "error": type(primary_error).__name__,
                "message": str(primary_error)[:200],
            },
        )
        try:
            from forecasting_tools import MetaculusApi  # type: ignore

            questions = MetaculusApi.get_all_open_questions_from_tournament(
                tournament_id=tournament_id
            )
            normalised = [_normalise_wrapper_question(q) for q in questions]
            write_jsonl(
                _DIAG_PATH,
                {"phase": "metaculus_fallback", "count": len(normalised)},
            )
            return {"results": normalised, "posts": normalised}
        except Exception as fallback_error:  # pragma: no cover - rare path
            write_jsonl(
                _DIAG_PATH,
                {
                    "phase": "metaculus_fallback_error",
                    "error": type(fallback_error).__name__,
                    "message": str(fallback_error)[:200],
                },
            )
            raise primary_error
