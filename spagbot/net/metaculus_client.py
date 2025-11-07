"""Resilient Metaculus API client with Cloudflare fallback."""
from __future__ import annotations

import asyncio
import os
from typing import Any, Dict, Iterable, Mapping, MutableMapping

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
                        return data
                    return {"results": data}

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


def _normalise_question(question: Any) -> Dict[str, Any]:
    if isinstance(question, Mapping):
        q_map: MutableMapping[str, Any] = dict(question)
    else:
        q_map = {}
        for attr in ("to_dict", "model_dump", "dict"):
            getter = getattr(question, attr, None)
            if callable(getter):
                try:
                    maybe = getter()
                    if isinstance(maybe, Mapping):
                        q_map = dict(maybe)
                        break
                except Exception:
                    continue
        if not q_map:
            for attr in ("__dict__",):
                data = getattr(question, attr, None)
                if isinstance(data, Mapping):
                    q_map = dict(data)
                    break

    post_id = q_map.get("post_id") or q_map.get("id")
    question_text = q_map.get("question_text") or q_map.get("title")
    url = q_map.get("page_url") or q_map.get("url")
    qtype = q_map.get("question_type") or q_map.get("type")
    close_time = q_map.get("close_time")

    return {
        "id": post_id,
        "post_id": post_id,
        "question": {
            "id": post_id,
            "question_text": question_text,
            "title": question_text,
            "url": url,
            "question_type": qtype,
            "close_time": close_time,
        },
        "url": url,
        "type": qtype,
        "close_time": close_time,
    }


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
            normalised = [_normalise_question(q) for q in questions]
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
