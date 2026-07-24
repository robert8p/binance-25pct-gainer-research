from __future__ import annotations

import hashlib
import time
import zipfile
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

import requests


INTERVAL_MS = {
    "1s": 1_000,
    "1m": 60_000,
    "3m": 180_000,
    "5m": 300_000,
    "15m": 900_000,
    "30m": 1_800_000,
    "1h": 3_600_000,
    "4h": 14_400_000,
    "1d": 86_400_000,
}


class BinanceError(RuntimeError):
    pass


class BinanceClient:
    def __init__(self, base_urls: tuple[str, ...]):
        self.base_urls = base_urls
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": "binance-8h-25pct-research/1.0"})

    def _get(self, path: str, params: dict[str, Any] | None = None) -> requests.Response:
        errors: list[str] = []
        for base in self.base_urls:
            for attempt in range(6):
                try:
                    response = self.session.get(f"{base}{path}", params=params, timeout=60)
                    if response.status_code == 200:
                        return response
                    if response.status_code in {418, 429}:
                        errors.append(f"{base}:{response.status_code}:rate limited")
                        wait = int(response.headers.get("Retry-After", "0") or 0) or min(60, 2 ** attempt)
                        time.sleep(wait)
                        continue
                    if response.status_code in {451, 500, 502, 503, 504}:
                        errors.append(f"{base}:{response.status_code}:{response.text[:200]}")
                        time.sleep(min(20, 2 ** attempt))
                        break
                    raise BinanceError(f"Binance {path} failed ({response.status_code}): {response.text[:500]}")
                except requests.RequestException as exc:
                    errors.append(f"{base}:{exc}")
                    time.sleep(min(20, 2 ** attempt))
                    break
        raise BinanceError("All Binance API endpoints failed: " + " | ".join(errors[-6:]))

    def exchange_info(self) -> dict[str, Any]:
        return self._get("/api/v3/exchangeInfo").json()

    def klines(
        self,
        symbol: str,
        interval: str,
        start_ms: int,
        end_ms_exclusive: int,
    ) -> list[list[Any]]:
        step = INTERVAL_MS[interval]
        cursor = start_ms
        rows: list[list[Any]] = []
        while cursor < end_ms_exclusive:
            response = self._get(
                "/api/v3/klines",
                {
                    "symbol": symbol,
                    "interval": interval,
                    "startTime": cursor,
                    "endTime": end_ms_exclusive - 1,
                    "limit": 1000,
                },
            )
            batch = response.json()
            if not batch:
                break
            rows.extend(batch)
            last_open = int(batch[-1][0])
            next_cursor = last_open + step
            if next_cursor <= cursor:
                raise BinanceError(f"Kline pagination did not advance for {symbol} {interval}")
            cursor = next_cursor
            if len(batch) < 1000:
                break
            time.sleep(0.04)
        return [row for row in rows if start_ms <= int(row[0]) < end_ms_exclusive]

    def aggregate_trades(
        self,
        symbol: str,
        start_ms: int,
        end_ms_inclusive: int,
        *,
        max_pages: int = 500,
    ) -> tuple[list[dict[str, Any]], bool]:
        params: dict[str, Any] = {
            "symbol": symbol,
            "startTime": start_ms,
            "endTime": end_ms_inclusive,
            "limit": 1000,
        }
        rows: list[dict[str, Any]] = []
        truncated = False
        for page in range(max_pages):
            batch = self._get("/api/v3/aggTrades", params).json()
            if not batch:
                break
            for trade in batch:
                if start_ms <= int(trade["T"]) <= end_ms_inclusive:
                    rows.append(trade)
            last_id = int(batch[-1]["a"])
            last_time = int(batch[-1]["T"])
            if len(batch) < 1000 or last_time > end_ms_inclusive:
                break
            params = {"symbol": symbol, "fromId": last_id + 1, "limit": 1000}
            time.sleep(0.05)
        else:
            truncated = True
        rows = [row for row in rows if int(row["T"]) <= end_ms_inclusive]
        return rows, truncated


def archive_url(data_type: str, symbol: str, day: date, interval: str | None = None) -> str:
    stamp = day.isoformat()
    if data_type == "klines":
        if not interval:
            raise ValueError("interval is required for kline archive")
        return (
            f"https://data.binance.vision/data/spot/daily/klines/{symbol}/{interval}/"
            f"{symbol}-{interval}-{stamp}.zip"
        )
    if data_type in {"aggTrades", "trades"}:
        return (
            f"https://data.binance.vision/data/spot/daily/{data_type}/{symbol}/"
            f"{symbol}-{data_type}-{stamp}.zip"
        )
    raise ValueError(data_type)


def download_archive(url: str, destination: Path) -> bool:
    response = requests.get(url, timeout=180, stream=True, headers={"User-Agent": "binance-8h-25pct-research/1.0"})
    if response.status_code == 404:
        return False
    response.raise_for_status()
    with destination.open("wb") as handle:
        for chunk in response.iter_content(chunk_size=1024 * 1024):
            if chunk:
                handle.write(chunk)
    if not zipfile.is_zipfile(destination):
        destination.unlink(missing_ok=True)
        raise BinanceError(f"Downloaded archive is not a ZIP: {url}")

    checksum = requests.get(
        f"{url}.CHECKSUM",
        timeout=60,
        headers={"User-Agent": "binance-8h-25pct-research/1.0"},
    )
    if checksum.status_code == 200:
        expected = checksum.text.strip().split()[0].lower()
        actual = sha256_file(destination)
        if expected and expected != actual:
            destination.unlink(missing_ok=True)
            raise BinanceError(
                f"Archive checksum mismatch for {url}: expected {expected}, got {actual}"
            )
    return True


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def normalize_archive_timestamp(value: int) -> int:
    # Binance spot archive timestamps are milliseconds before 2025 and microseconds from 2025 onward.
    return value // 1000 if value > 10_000_000_000_000 else value
