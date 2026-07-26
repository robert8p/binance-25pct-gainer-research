from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Any

import requests


class SupabaseError(RuntimeError):
    pass


class SupabaseClient:
    def __init__(self, url: str, key: str, bucket: str):
        self.url = url.rstrip("/")
        self.key = key
        self.bucket = bucket
        self.session = requests.Session()
        self.headers = {
            "apikey": key,
            "Content-Type": "application/json",
        }
        # Supabase's modern sb_secret_* keys are opaque API keys, not JWTs.
        # Sending them as Bearer tokens causes an "Invalid JWT" rejection.
        # Legacy service_role JWT keys still require the Authorization header.
        if not key.startswith(("sb_secret_", "sb_publishable_")):
            self.headers["Authorization"] = f"Bearer {key}"

    def _request(self, method: str, url: str, *, retries: int = 5, **kwargs: Any) -> requests.Response:
        last: Exception | None = None
        for attempt in range(retries):
            try:
                response = self.session.request(method, url, timeout=120, **kwargs)
                if response.status_code in {429, 500, 502, 503, 504}:
                    last = SupabaseError(
                        f"HTTP {response.status_code}: {response.text[:500]}"
                    )
                    wait = int(response.headers.get("Retry-After", "0") or 0) or min(30, 2 ** attempt)
                    time.sleep(wait)
                    continue
                return response
            except requests.RequestException as exc:
                last = exc
                time.sleep(min(30, 2 ** attempt))
        raise SupabaseError(f"Request failed after retries: {last}")

    def select(
        self,
        table: str,
        *,
        columns: str = "*",
        filters: dict[str, str] | None = None,
        order: str | None = None,
        limit: int = 1000,
        offset: int = 0,
    ) -> list[dict[str, Any]]:
        params: dict[str, Any] = {"select": columns, "limit": limit, "offset": offset}
        if filters:
            params.update(filters)
        if order:
            params["order"] = order
        response = self._request(
            "GET",
            f"{self.url}/rest/v1/{table}",
            headers=self.headers,
            params=params,
        )
        if response.status_code != 200:
            raise SupabaseError(f"Select {table} failed ({response.status_code}): {response.text[:1000]}")
        return response.json()

    def select_all(
        self,
        table: str,
        *,
        columns: str = "*",
        filters: dict[str, str] | None = None,
        order: str | None = None,
        page_size: int = 1000,
    ) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        offset = 0
        while True:
            page = self.select(
                table,
                columns=columns,
                filters=filters,
                order=order,
                limit=page_size,
                offset=offset,
            )
            rows.extend(page)
            if len(page) < page_size:
                break
            offset += page_size
        return rows

    def insert(self, table: str, payload: dict[str, Any] | list[dict[str, Any]]) -> list[dict[str, Any]]:
        headers = dict(self.headers)
        headers["Prefer"] = "return=representation"
        response = self._request(
            "POST", f"{self.url}/rest/v1/{table}", headers=headers, data=json.dumps(payload, default=str)
        )
        if response.status_code not in {200, 201}:
            raise SupabaseError(f"Insert {table} failed ({response.status_code}): {response.text[:1000]}")
        return response.json()

    def upsert(
        self,
        table: str,
        payload: list[dict[str, Any]],
        *,
        on_conflict: str,
        chunk_size: int = 500,
    ) -> None:
        if not payload:
            return
        headers = dict(self.headers)
        headers["Prefer"] = "resolution=merge-duplicates,return=minimal"
        for start in range(0, len(payload), chunk_size):
            chunk = payload[start : start + chunk_size]
            response = self._request(
                "POST",
                f"{self.url}/rest/v1/{table}",
                headers=headers,
                params={"on_conflict": on_conflict},
                data=json.dumps(chunk, default=str),
            )
            if response.status_code not in {200, 201, 204}:
                raise SupabaseError(
                    f"Upsert {table} failed ({response.status_code}): {response.text[:1000]}"
                )

    def update(self, table: str, filters: dict[str, str], payload: dict[str, Any]) -> None:
        headers = dict(self.headers)
        headers["Prefer"] = "return=minimal"
        response = self._request(
            "PATCH",
            f"{self.url}/rest/v1/{table}",
            headers=headers,
            params=filters,
            data=json.dumps(payload, default=str),
        )
        if response.status_code not in {200, 204}:
            raise SupabaseError(f"Update {table} failed ({response.status_code}): {response.text[:1000]}")

    def delete(self, table: str, filters: dict[str, str]) -> None:
        response = self._request(
            "DELETE", f"{self.url}/rest/v1/{table}", headers=self.headers, params=filters
        )
        if response.status_code not in {200, 204}:
            raise SupabaseError(f"Delete {table} failed ({response.status_code}): {response.text[:1000]}")

    def upload_file(self, path: str, local_path: Path, content_type: str = "application/octet-stream") -> None:
        headers = {
            "apikey": self.key,
            "Content-Type": content_type,
            "x-upsert": "true",
        }
        if not self.key.startswith(("sb_secret_", "sb_publishable_")):
            headers["Authorization"] = f"Bearer {self.key}"
        url = f"{self.url}/storage/v1/object/{self.bucket}/{path}"
        last_error: Exception | None = None
        for attempt in range(4):
            try:
                # Reopen the file for every attempt. Reusing one stream would retry from EOF.
                with local_path.open("rb") as handle:
                    response = self.session.post(url, headers=headers, data=handle, timeout=300)
                if response.status_code in {200, 201}:
                    return
                if response.status_code in {429, 500, 502, 503, 504}:
                    last_error = SupabaseError(
                        f"HTTP {response.status_code}: {response.text[:500]}"
                    )
                    wait = int(response.headers.get("Retry-After", "0") or 0) or min(30, 2 ** attempt)
                    time.sleep(wait)
                    continue
                if response.status_code == 413:
                    raise SupabaseError(
                        f"Storage upload failed (413): {local_path.name} is "
                        f"{local_path.stat().st_size:,} bytes and exceeds the configured "
                        "Supabase object-size limit. The ChatGPT-led package builder should "
                        "split files below MAX_STORAGE_OBJECT_BYTES."
                    )
                raise SupabaseError(
                    f"Storage upload failed ({response.status_code}): {response.text[:1000]}"
                )
            except requests.RequestException as exc:
                last_error = exc
                time.sleep(min(30, 2 ** attempt))
        raise SupabaseError(f"Storage upload failed after retries: {last_error}")


    def download_file(self, path: str, destination: Path) -> None:
        """Download a private Storage object using the server-side key."""
        headers = {"apikey": self.key}
        if not self.key.startswith(("sb_secret_", "sb_publishable_")):
            headers["Authorization"] = f"Bearer {self.key}"
        url = f"{self.url}/storage/v1/object/{self.bucket}/{path}"
        response = self._request("GET", url, headers=headers, retries=5, stream=True)
        if response.status_code != 200:
            raise SupabaseError(
                f"Storage download failed ({response.status_code}): {response.text[:1000]}"
            )
        destination.parent.mkdir(parents=True, exist_ok=True)
        with destination.open("wb") as handle:
            for chunk in response.iter_content(chunk_size=1024 * 1024):
                if chunk:
                    handle.write(chunk)

    def signed_url(self, path: str, expires_in: int = 3600) -> str:
        response = self._request(
            "POST",
            f"{self.url}/storage/v1/object/sign/{self.bucket}/{path}",
            headers=self.headers,
            data=json.dumps({"expiresIn": expires_in}),
        )
        if response.status_code != 200:
            raise SupabaseError(f"Signing URL failed ({response.status_code}): {response.text[:1000]}")
        signed = response.json().get("signedURL") or response.json().get("signedUrl")
        if not signed:
            raise SupabaseError("Supabase did not return a signed URL")
        return signed if signed.startswith("http") else f"{self.url}/storage/v1{signed}"
