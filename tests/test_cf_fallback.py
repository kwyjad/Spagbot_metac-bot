from __future__ import annotations
import asyncio
import importlib
import sys
import types
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import spagbot.net.metaculus_client as mc_module


class FakeResponse:
    def __init__(self, *, status_code: int, text: str, headers: dict[str, str]):
        self.status_code = status_code
        self.text = text
        self.headers = headers

    def json(self) -> dict:
        return {}


@pytest.mark.asyncio
def test_cf_html_triggers_fallback(monkeypatch, tmp_path):
    monkeypatch.setenv("METACULUS_TOKEN", "token123")
    monkeypatch.setenv("LOGS_BASE_DIR", str(tmp_path))
    monkeypatch.setenv("METACULUS_MAX_RETRIES", "1")
    monkeypatch.setenv("METACULUS_REQUEST_TIMEOUT", "1")

    mc = importlib.reload(mc_module)

    async def fake_get(self, url, params=None):  # noqa: ANN001
        return FakeResponse(
            status_code=403,
            text="<!doctype html><title>Just a moment...</title> __cf_chl",
            headers={"content-type": "text/html"},
        )

    class DummyClient:
        def __init__(self, *_, **__):  # noqa: D401, ANN002, ANN003
            """HTTP client stub."""

        async def __aenter__(self):
            return self

        async def __aexit__(self, *exc_info):  # noqa: ANN002
            return False

        async def get(self, url, params=None):  # noqa: ANN001
            return await fake_get(self, url, params=params)

    monkeypatch.setattr(mc.httpx, "AsyncClient", DummyClient)

    class StubQuestion:
        def __init__(self):
            self.post_id = 123
            self.question_text = "Q?"
            self.page_url = "https://example.com/q"
            self.question_type = "binary"
            self.close_time = None

    class StubApi:
        @staticmethod
        def get_all_open_questions_from_tournament(tournament_id: str):  # noqa: ANN001
            assert tournament_id == "demo"
            return [StubQuestion()]

    monkeypatch.setitem(
        sys.modules,
        "forecasting_tools",
        types.SimpleNamespace(MetaculusApi=StubApi),
    )

    data = asyncio.run(mc.list_posts_from_tournament_resilient("demo", limit=10))
    results = data.get("results")
    assert isinstance(results, list)
    assert results and results[0]["id"] == 123
    assert results[0]["type"] == "binary"
    assert results[0]["title"] == "Q?"
    assert results[0]["question"]["type"] == "binary"
    assert results[0]["question"]["title"] == "Q?"
