from __future__ import annotations

import httpx
import pytest

from app.config import Settings
from app.supabase_store import SupabaseStore


class SequenceClient:
    def __init__(self, outcomes):
        self.outcomes = list(outcomes)
        self.calls = 0

    def request(self, *args, **kwargs):
        self.calls += 1
        outcome = self.outcomes.pop(0)
        if isinstance(outcome, Exception):
            raise outcome
        return outcome

    def close(self):
        return None


def response(status: int, payload=None) -> httpx.Response:
    request = httpx.Request("GET", "https://example.supabase.co/rest/v1/table")
    if payload is None:
        return httpx.Response(status, request=request)
    return httpx.Response(status, json=payload, request=request)


def make_store(max_retries: int = 3) -> SupabaseStore:
    return SupabaseStore(Settings(
        supabase_url="https://example.supabase.co",
        supabase_service_role_key="sb_secret_test",
        request_timeout_seconds=1,
        max_retries=max_retries,
    ))


def test_select_retries_remote_protocol_disconnect(monkeypatch):
    store = make_store()
    client = SequenceClient([
        httpx.RemoteProtocolError("Server disconnected without sending a response."),
        response(200, [{"id": "ok"}]),
    ])
    store.client = client
    monkeypatch.setattr(store, "_renew_client", lambda: None)
    monkeypatch.setattr("app.supabase_store.time.sleep", lambda _: None)

    assert store.select("stock25_research_jobs") == [{"id": "ok"}]
    assert client.calls == 2


def test_patch_retries_transient_503(monkeypatch):
    store = make_store()
    client = SequenceClient([response(503, {"message": "temporary"}), response(204)])
    store.client = client
    monkeypatch.setattr("app.supabase_store.time.sleep", lambda _: None)

    store.update("stock25_research_jobs", {"id": "eq.1"}, {"status": "running"})
    assert client.calls == 2


def test_plain_insert_is_not_replayed_after_disconnect(monkeypatch):
    store = make_store(max_retries=7)
    client = SequenceClient([
        httpx.RemoteProtocolError("Server disconnected without sending a response."),
        response(201, [{"id": "duplicate-risk"}]),
    ])
    store.client = client
    monkeypatch.setattr("app.supabase_store.time.sleep", lambda _: None)

    with pytest.raises(RuntimeError, match="after 1 attempt"):
        store.insert("stock25_research_jobs", {"status": "queued"})
    assert client.calls == 1
