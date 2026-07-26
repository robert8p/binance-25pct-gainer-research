from __future__ import annotations

import hashlib
import logging
import mimetypes
import random
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from urllib.parse import quote

import httpx

from app.config import Settings

logger = logging.getLogger(__name__)


class SupabaseStore:
    def __init__(self, settings: Settings):
        self.settings = settings
        self.base = f"{settings.supabase_url.rstrip('/')}/rest/v1"
        self.storage_base = f"{settings.supabase_url.rstrip('/')}/storage/v1"
        key = settings.supabase_service_role_key
        self.auth_headers = {"apikey": key}
        # New sb_secret_* keys are opaque API keys, not JWTs. Sending them as a
        # Bearer token can cause a 401. Legacy service_role JWTs still need both.
        if not key.startswith(("sb_secret_", "sb_publishable_")):
            self.auth_headers["Authorization"] = f"Bearer {key}"
        self.headers = {**self.auth_headers, "Content-Type": "application/json"}
        self.client = httpx.Client(timeout=settings.request_timeout_seconds)

    def close(self) -> None:
        self.client.close()

    def _renew_client(self) -> None:
        """Replace a connection pool after a dropped/poisoned HTTP connection."""
        try:
            self.client.close()
        except Exception:
            pass
        self.client = httpx.Client(timeout=self.settings.request_timeout_seconds)

    def _request(
        self,
        method: str,
        table: str,
        *,
        params: dict[str, Any] | None = None,
        json: Any = None,
        prefer: str | None = None,
        extra_headers: dict[str, str] | None = None,
    ) -> Any:
        headers = dict(self.headers)
        if prefer:
            headers["Prefer"] = prefer
        if extra_headers:
            headers.update(extra_headers)

        # GET/PATCH/DELETE are safe to replay here, as are PostgREST upserts with
        # an explicit on_conflict target. Plain INSERTs are deliberately not
        # replayed because the server may have committed before the response was lost.
        replay_safe = method.upper() in {"GET", "PATCH", "DELETE"} or (
            method.upper() == "POST" and bool((params or {}).get("on_conflict"))
        )
        attempts = max(3, self.settings.max_retries) if replay_safe else 1
        last_error: Exception | None = None

        for attempt in range(attempts):
            try:
                response = self.client.request(
                    method, f"{self.base}/{table}", headers=headers, params=params, json=json
                )
                retryable_status = response.status_code in {408, 425, 429, 520} or response.status_code >= 500
                if response.status_code >= 400:
                    if replay_safe and retryable_status and attempt + 1 < attempts:
                        wait = min(30.0, (2 ** attempt) + random.random())
                        logger.warning(
                            "Supabase REST %s for %s; retrying in %.1fs",
                            response.status_code, table, wait,
                        )
                        time.sleep(wait)
                        continue
                    raise RuntimeError(
                        f"Supabase HTTP {response.status_code}: {response.text[:1500]}"
                    )
                if not response.content:
                    return None
                return response.json()
            except httpx.HTTPError as exc:
                last_error = exc
                if not replay_safe or attempt + 1 >= attempts:
                    break
                wait = min(30.0, (2 ** attempt) + random.random())
                logger.warning(
                    "Supabase REST network failure for %s (%s); retrying in %.1fs",
                    table, exc, wait,
                )
                self._renew_client()
                time.sleep(wait)

        if last_error is not None:
            raise RuntimeError(
                f"Supabase REST network failure after {attempts} attempt(s): {last_error}"
            ) from last_error
        raise RuntimeError(f"Supabase REST request failed after {attempts} attempt(s)")

    def insert(self, table: str, rows: dict[str, Any] | list[dict[str, Any]]) -> list[dict[str, Any]]:
        payload = [rows] if isinstance(rows, dict) else rows
        return self._request("POST", table, json=payload, prefer="return=representation") or []

    def upsert(
        self,
        table: str,
        rows: dict[str, Any] | list[dict[str, Any]],
        *,
        on_conflict: str,
        chunk_size: int = 500,
        return_representation: bool = False,
    ) -> list[dict[str, Any]]:
        payload = [rows] if isinstance(rows, dict) else rows
        returned: list[dict[str, Any]] = []
        for i in range(0, len(payload), chunk_size):
            chunk = payload[i : i + chunk_size]
            prefer = "resolution=merge-duplicates,return=representation" if return_representation else "resolution=merge-duplicates,return=minimal"
            result = self._request(
                "POST",
                table,
                params={"on_conflict": on_conflict},
                json=chunk,
                prefer=prefer,
            )
            if result:
                returned.extend(result)
        return returned

    def update(self, table: str, filters: dict[str, str], values: dict[str, Any]) -> None:
        self._request("PATCH", table, params=filters, json=values, prefer="return=minimal")

    def delete(self, table: str, filters: dict[str, str]) -> None:
        self._request("DELETE", table, params=filters, prefer="return=minimal")

    def select(
        self,
        table: str,
        *,
        select: str = "*",
        filters: dict[str, str] | None = None,
        order: str | None = None,
        limit: int | None = None,
        offset: int | None = None,
    ) -> list[dict[str, Any]]:
        params: dict[str, str] = {"select": select}
        if filters:
            params.update(filters)
        if order:
            params["order"] = order
        if limit is not None:
            params["limit"] = str(limit)
        if offset is not None:
            params["offset"] = str(offset)
        return self._request("GET", table, params=params) or []

    def select_all(
        self,
        table: str,
        *,
        select: str = "*",
        filters: dict[str, str] | None = None,
        order: str | None = None,
        page_size: int = 1000,
        max_rows: int | None = None,
    ) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        offset = 0
        while True:
            remaining = page_size if max_rows is None else min(page_size, max_rows - len(rows))
            if remaining <= 0:
                break
            page = self.select(
                table,
                select=select,
                filters=filters,
                order=order,
                limit=remaining,
                offset=offset,
            )
            rows.extend(page)
            if len(page) < remaining:
                break
            offset += len(page)
        return rows

    def enqueue_scan(self, parameters: dict[str, Any], source: str = "manual") -> dict[str, Any]:
        return self.insert(
            "stock25_scans",
            {
                "status": "queued",
                "source": source,
                "parameters": parameters,
                "progress_stage": "queued",
                "progress_current": 0,
                "progress_total": 0,
            },
        )[0]

    def claim_next_scan(self) -> dict[str, Any] | None:
        rows = self.select("stock25_scans", filters={"status": "eq.queued"}, order="created_at.asc", limit=1)
        if not rows:
            return None
        scan = rows[0]
        now = datetime.now(timezone.utc).isoformat()
        self.update(
            "stock25_scans",
            {"id": f"eq.{scan['id']}", "status": "eq.queued"},
            {"status": "running", "started_at": now, "heartbeat_at": now, "progress_stage": "starting", "error_message": None},
        )
        check = self.select("stock25_scans", filters={"id": f"eq.{scan['id']}"}, limit=1)
        return check[0] if check and check[0].get("status") == "running" else None

    def update_scan(self, scan_id: str, **values: Any) -> None:
        values.setdefault("heartbeat_at", datetime.now(timezone.utc).isoformat())
        self.update("stock25_scans", {"id": f"eq.{scan_id}"}, values)

    def enqueue_research_job(self, source_scan_id: str, parameters: dict[str, Any]) -> dict[str, Any]:
        return self.insert(
            "stock25_research_jobs",
            {
                "source_scan_id": source_scan_id,
                "status": "queued",
                "parameters": parameters,
                "progress_stage": "queued",
            },
        )[0]

    def claim_next_research_job(self) -> dict[str, Any] | None:
        rows = self.select("stock25_research_jobs", filters={"status": "eq.queued"}, order="created_at.asc", limit=1)
        if not rows:
            return None
        job = rows[0]
        now = datetime.now(timezone.utc).isoformat()
        self.update(
            "stock25_research_jobs",
            {"id": f"eq.{job['id']}", "status": "eq.queued"},
            {"status": "running", "started_at": now, "heartbeat_at": now, "progress_stage": "starting", "error_message": None},
        )
        check = self.select("stock25_research_jobs", filters={"id": f"eq.{job['id']}"}, limit=1)
        return check[0] if check and check[0].get("status") == "running" else None

    def update_research_job(self, job_id: str, **values: Any) -> None:
        values.setdefault("heartbeat_at", datetime.now(timezone.utc).isoformat())
        self.update("stock25_research_jobs", {"id": f"eq.{job_id}"}, values)

    def recover_stale_jobs(self, stale_minutes: int = 20) -> dict[str, int]:
        """Requeue jobs whose worker heartbeat stopped, allowing safe resume after a deploy/crash."""
        cutoff = (datetime.now(timezone.utc) - timedelta(minutes=stale_minutes)).isoformat()
        recovered = {"stock25_scans": 0, "stock25_research_jobs": 0, "stock25_control_jobs": 0, "stock25_entry_jobs": 0, "stock25_backtest_jobs": 0}
        for table in ("stock25_scans", "stock25_research_jobs", "stock25_control_jobs", "stock25_entry_jobs", "stock25_backtest_jobs"):
            stale = self.select_all(
                table,
                select="id",
                filters={"status": "eq.running", "heartbeat_at": f"lt.{cutoff}"},
                page_size=500,
            )
            # Handles running jobs created before heartbeat support was added.
            no_heartbeat = self.select_all(
                table,
                select="id",
                filters={
                    "status": "eq.running",
                    "heartbeat_at": "is.null",
                    "started_at": f"lt.{cutoff}",
                },
                page_size=500,
            )
            ids = {row["id"] for row in stale + no_heartbeat}
            for job_id in ids:
                self.update(
                    table,
                    {"id": f"eq.{job_id}", "status": "eq.running"},
                    {
                        "status": "queued",
                        "progress_stage": "resuming_after_interruption",
                        "error_message": None,
                    },
                )
            recovered[table] = len(ids)
        return recovered

    def save_asset_snapshot(self, snapshot_date: str, assets: list[dict[str, Any]]) -> None:
        rows = [
            {
                "snapshot_date": snapshot_date,
                "asset_id": a.get("id"),
                "symbol": a.get("symbol"),
                "name": a.get("name"),
                "exchange": a.get("exchange"),
                "status": a.get("status"),
                "tradable": bool(a.get("tradable")),
                "fractionable": bool(a.get("fractionable")),
                "shortable": bool(a.get("shortable")),
                "easy_to_borrow": bool(a.get("easy_to_borrow")),
                "marginable": bool(a.get("marginable")),
                "attributes": a.get("attributes") or [],
            }
            for a in assets
        ]
        self.upsert("stock25_asset_snapshots", rows, on_conflict="snapshot_date,asset_id")

    def upload_file(self, local_path: str | Path, storage_path: str, *, content_type: str | None = None) -> dict[str, Any]:
        path = Path(local_path)
        mime = content_type or mimetypes.guess_type(path.name)[0] or "application/octet-stream"
        url = f"{self.storage_base}/object/{quote(self.settings.supabase_storage_bucket, safe='')}/{quote(storage_path, safe='/')}"
        headers = {
            **self.auth_headers,
            "Content-Type": mime,
            "x-upsert": "true",
        }
        # Objects are deliberately capped below 45 MB, so a bounded in-memory upload
        # gives Supabase a deterministic Content-Length and avoids unreliable chunked POSTs.
        payload = path.read_bytes()
        headers["Content-Length"] = str(len(payload))
        last_response = None
        for attempt in range(max(3, self.settings.max_retries)):
            try:
                response = self.client.post(url, headers=headers, content=payload)
                last_response = response
                if response.status_code < 400:
                    return response.json() if response.content else {"Key": storage_path}
                body = response.text[:1500]
                permanent_size_error = response.status_code == 400 and any(
                    token in body.lower() for token in ("maximum", "too large", "file size", "payload size")
                )
                retryable = response.status_code in {400, 408, 409, 425, 429, 520} or response.status_code >= 500
                if permanent_size_error or not retryable:
                    raise RuntimeError(f"Supabase Storage HTTP {response.status_code}: {body}")
                wait = min(30.0, (2 ** attempt) + random.random())
                time.sleep(wait)
            except httpx.HTTPError as exc:
                if attempt + 1 >= max(3, self.settings.max_retries):
                    raise RuntimeError(f"Supabase Storage network failure: {exc}") from exc
                wait = min(30.0, (2 ** attempt) + random.random())
                time.sleep(wait)
        status_code = last_response.status_code if last_response is not None else "network"
        body = last_response.text[:1500] if last_response is not None else "no response"
        raise RuntimeError(f"Supabase Storage HTTP {status_code} after retries: {body}")

    def create_signed_url(self, storage_path: str, expires_in: int | None = None) -> str:
        url = f"{self.storage_base}/object/sign/{quote(self.settings.supabase_storage_bucket, safe='')}/{quote(storage_path, safe='/')}"
        headers = {**self.auth_headers, "Content-Type": "application/json"}
        response = self.client.post(url, headers=headers, json={"expiresIn": expires_in or self.settings.signed_url_expiry_seconds})
        if response.status_code >= 400:
            raise RuntimeError(f"Supabase signed URL HTTP {response.status_code}: {response.text[:1000]}")
        signed = response.json().get("signedURL") or response.json().get("signedUrl")
        if not signed:
            raise RuntimeError("Supabase did not return a signed URL")
        return signed if signed.startswith("http") else f"{self.settings.supabase_url.rstrip('/')}/storage/v1{signed}"

    @staticmethod
    def sha256(path: str | Path) -> str:
        h = hashlib.sha256()
        with Path(path).open("rb") as fh:
            for chunk in iter(lambda: fh.read(1024 * 1024), b""):
                h.update(chunk)
        return h.hexdigest()

# v3 matched-control methods are attached explicitly so v2 installations retain the
# same class behaviour while gaining a resumable third job type.
def _enqueue_control_job(self: SupabaseStore, source_research_job_id: str, parameters: dict[str, Any]) -> dict[str, Any]:
    return self.insert(
        "stock25_control_jobs",
        {
            "source_research_job_id": source_research_job_id,
            "status": "queued",
            "parameters": parameters,
            "progress_stage": "queued",
        },
    )[0]


def _claim_next_control_job(self: SupabaseStore) -> dict[str, Any] | None:
    rows = self.select("stock25_control_jobs", filters={"status": "eq.queued"}, order="created_at.asc", limit=1)
    if not rows:
        return None
    job = rows[0]
    now = datetime.now(timezone.utc).isoformat()
    self.update(
        "stock25_control_jobs",
        {"id": f"eq.{job['id']}", "status": "eq.queued"},
        {
            "status": "running",
            "started_at": now,
            "heartbeat_at": now,
            "progress_stage": "starting",
            "error_message": None,
        },
    )
    check = self.select("stock25_control_jobs", filters={"id": f"eq.{job['id']}"}, limit=1)
    return check[0] if check and check[0].get("status") == "running" else None


def _update_control_job(self: SupabaseStore, job_id: str, **values: Any) -> None:
    values.setdefault("heartbeat_at", datetime.now(timezone.utc).isoformat())
    self.update("stock25_control_jobs", {"id": f"eq.{job_id}"}, values)


def _download_file(self: SupabaseStore, storage_path: str, local_path: str | Path) -> Path:
    path = Path(local_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    url = f"{self.storage_base}/object/authenticated/{quote(self.settings.supabase_storage_bucket, safe='')}/{quote(storage_path, safe='/')}"
    with self.client.stream("GET", url, headers=self.auth_headers) as response:
        if response.status_code >= 400:
            body = response.read().decode("utf-8", errors="replace")
            raise RuntimeError(f"Supabase Storage download HTTP {response.status_code}: {body[:1500]}")
        with path.open("wb") as fh:
            for chunk in response.iter_bytes(1024 * 1024):
                fh.write(chunk)
    return path


SupabaseStore.enqueue_control_job = _enqueue_control_job  # type: ignore[attr-defined]
SupabaseStore.claim_next_control_job = _claim_next_control_job  # type: ignore[attr-defined]
SupabaseStore.update_control_job = _update_control_job  # type: ignore[attr-defined]
SupabaseStore.download_file = _download_file  # type: ignore[attr-defined]


def _iter_select_pages(
    self: SupabaseStore,
    table: str,
    *,
    select: str = "*",
    filters: dict[str, str] | None = None,
    order: str | None = None,
    page_size: int = 1000,
):
    offset = 0
    while True:
        page = self.select(
            table,
            select=select,
            filters=filters,
            order=order,
            limit=page_size,
            offset=offset,
        )
        if not page:
            break
        yield page
        if len(page) < page_size:
            break
        offset += len(page)


SupabaseStore.iter_select_pages = _iter_select_pages  # type: ignore[attr-defined]


# v3.0.3 entry-feasibility/export job methods.
def _enqueue_entry_job(self: SupabaseStore, source_control_job_id: str, parameters: dict[str, Any]) -> dict[str, Any]:
    return self.insert(
        "stock25_entry_jobs",
        {
            "source_control_job_id": source_control_job_id,
            "status": "queued",
            "parameters": parameters,
            "progress_stage": "queued",
        },
    )[0]


def _claim_next_entry_job(self: SupabaseStore) -> dict[str, Any] | None:
    rows = self.select("stock25_entry_jobs", filters={"status": "eq.queued"}, order="created_at.asc", limit=1)
    if not rows:
        return None
    job = rows[0]
    now = datetime.now(timezone.utc).isoformat()
    self.update(
        "stock25_entry_jobs",
        {"id": f"eq.{job['id']}", "status": "eq.queued"},
        {
            "status": "running",
            "started_at": now,
            "heartbeat_at": now,
            "progress_stage": "starting",
            "error_message": None,
        },
    )
    check = self.select("stock25_entry_jobs", filters={"id": f"eq.{job['id']}"}, limit=1)
    return check[0] if check and check[0].get("status") == "running" else None


def _update_entry_job(self: SupabaseStore, job_id: str, **values: Any) -> None:
    values.setdefault("heartbeat_at", datetime.now(timezone.utc).isoformat())
    self.update("stock25_entry_jobs", {"id": f"eq.{job_id}"}, values)


SupabaseStore.enqueue_entry_job = _enqueue_entry_job  # type: ignore[attr-defined]
SupabaseStore.claim_next_entry_job = _claim_next_entry_job  # type: ignore[attr-defined]
SupabaseStore.update_entry_job = _update_entry_job  # type: ignore[attr-defined]


# v4 execution-backtest job methods.
def _enqueue_backtest_job(self: SupabaseStore, source_entry_job_id: str, parameters: dict[str, Any]) -> dict[str, Any]:
    return self.insert("stock25_backtest_jobs", {
        "source_entry_job_id": source_entry_job_id, "status": "queued",
        "parameters": parameters, "progress_stage": "queued",
    })[0]

def _claim_next_backtest_job(self: SupabaseStore) -> dict[str, Any] | None:
    rows = self.select("stock25_backtest_jobs", filters={"status": "eq.queued"}, order="created_at.asc", limit=1)
    if not rows:
        return None
    job = rows[0]
    now = datetime.now(timezone.utc).isoformat()
    self.update("stock25_backtest_jobs", {"id": f"eq.{job['id']}", "status": "eq.queued"}, {
        "status": "running", "started_at": now, "heartbeat_at": now,
        "progress_stage": "starting", "error_message": None,
    })
    check = self.select("stock25_backtest_jobs", filters={"id": f"eq.{job['id']}"}, limit=1)
    return check[0] if check and check[0].get("status") == "running" else None

def _update_backtest_job(self: SupabaseStore, job_id: str, **values: Any) -> None:
    values.setdefault("heartbeat_at", datetime.now(timezone.utc).isoformat())
    self.update("stock25_backtest_jobs", {"id": f"eq.{job_id}"}, values)

SupabaseStore.enqueue_backtest_job = _enqueue_backtest_job  # type: ignore[attr-defined]
SupabaseStore.claim_next_backtest_job = _claim_next_backtest_job  # type: ignore[attr-defined]
SupabaseStore.update_backtest_job = _update_backtest_job  # type: ignore[attr-defined]
