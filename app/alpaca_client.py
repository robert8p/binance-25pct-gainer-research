from __future__ import annotations

import logging
import random
import re
import time
from datetime import date, datetime
from typing import Any, Iterable, Iterator
from urllib.parse import quote, unquote

import httpx

from app.config import Settings

logger = logging.getLogger(__name__)

_INVALID_SYMBOL_PATTERN = re.compile(
    r"invalid\s+symbol(?:\(s\)|s)?\s*:\s*([^\"}\]\n]+)",
    re.IGNORECASE,
)
_LIKELY_STOCK_SYMBOL_PATTERN = re.compile(r"^[A-Z][A-Z0-9.\-]{0,14}$")


def extract_invalid_symbols(message: str) -> set[str]:
    """Extract one or more rejected symbols from an Alpaca HTTP error."""
    invalid: set[str] = set()
    for match in _INVALID_SYMBOL_PATTERN.finditer(str(message)):
        for raw in match.group(1).split(","):
            symbol = unquote(raw).strip().strip("'\"").upper()
            if symbol:
                invalid.add(symbol)
    return invalid


def is_likely_stock_symbol(value: Any) -> bool:
    """Reject CUSIP-like identifiers and malformed asset-master entries.

    Alpaca's asset master can contain inactive identifiers that are accepted by
    the trading asset lookup but rejected by the stock market-data endpoints.
    US exchange-listed symbols begin with a letter; suffixes such as BRK.B are
    preserved. API-side pruning remains the final safeguard.
    """
    symbol = str(value or "").strip().upper()
    return bool(_LIKELY_STOCK_SYMBOL_PATTERN.fullmatch(symbol))



class AlpacaError(RuntimeError):
    pass


class AlpacaClient:
    def __init__(self, settings: Settings):
        self.settings = settings
        self.headers = {
            "APCA-API-KEY-ID": settings.alpaca_api_key,
            "APCA-API-SECRET-KEY": settings.alpaca_secret_key,
            "Accept": "application/json",
        }
        self.client = httpx.Client(timeout=settings.request_timeout_seconds)

    def close(self) -> None:
        self.client.close()

    def _request(self, method: str, url: str, *, params: dict[str, Any] | None = None) -> Any:
        last_error: Exception | None = None
        for attempt in range(self.settings.max_retries):
            try:
                response = self.client.request(method, url, headers=self.headers, params=params)
                if response.status_code == 429:
                    retry_after = float(response.headers.get("Retry-After", "0") or 0)
                    wait = max(retry_after, min(30.0, (2**attempt) + random.random()))
                    logger.warning("Alpaca rate limit hit; retrying in %.1fs", wait)
                    time.sleep(wait)
                    continue
                if 500 <= response.status_code < 600:
                    wait = min(30.0, (2**attempt) + random.random())
                    logger.warning(
                        "Alpaca server error %s; retrying in %.1fs",
                        response.status_code,
                        wait,
                    )
                    time.sleep(wait)
                    continue
                if response.status_code >= 400:
                    raise AlpacaError(
                        f"Alpaca HTTP {response.status_code}: {response.text[:1500]}"
                    )
                if self.settings.request_pause_seconds:
                    time.sleep(self.settings.request_pause_seconds)
                return response.json()
            except (httpx.HTTPError, AlpacaError) as exc:
                last_error = exc
                if isinstance(exc, AlpacaError) and "HTTP 4" in str(exc) and "429" not in str(exc):
                    raise
                wait = min(30.0, (2**attempt) + random.random())
                logger.warning("Alpaca request failed (%s); retrying in %.1fs", exc, wait)
                time.sleep(wait)
        raise AlpacaError(f"Alpaca request failed after retries: {last_error}")

    def get_assets(self, *, all_statuses: bool = True) -> list[dict[str, Any]]:
        params: dict[str, Any] = {"asset_class": "us_equity"}
        if not all_statuses:
            params["status"] = "active"
        payload = self._request(
            "GET", f"{self.settings.alpaca_trading_base_url}/v2/assets", params=params
        )
        return list(payload)

    def get_calendar(self, start: date, end: date) -> list[dict[str, Any]]:
        payload = self._request(
            "GET",
            f"{self.settings.alpaca_trading_base_url}/v2/calendar",
            params={"start": start.isoformat(), "end": end.isoformat()},
        )
        return list(payload)

    def get_bars(
        self,
        symbols: Iterable[str],
        *,
        timeframe: str,
        start: datetime,
        end: datetime,
        feed: str,
        adjustment: str = "split",
        asof: date | None = None,
    ) -> dict[str, list[dict[str, Any]]]:
        requested = list(dict.fromkeys(str(s).strip().upper() for s in symbols if str(s).strip()))
        if not requested:
            return {}

        combined: dict[str, list[dict[str, Any]]] = {symbol: [] for symbol in requested}
        active = list(requested)
        while active:
            params: dict[str, Any] = {
                "symbols": ",".join(active),
                "timeframe": timeframe,
                "start": start.isoformat(),
                "end": end.isoformat(),
                "limit": 10000,
                "adjustment": adjustment,
                "feed": feed,
                "sort": "asc",
            }
            if asof:
                params["asof"] = asof.isoformat()

            attempt_rows: dict[str, list[dict[str, Any]]] = {symbol: [] for symbol in active}
            page_token: str | None = None
            try:
                while True:
                    if page_token:
                        params["page_token"] = page_token
                    else:
                        params.pop("page_token", None)
                    payload = self._request(
                        "GET", f"{self.settings.alpaca_data_base_url}/v2/stocks/bars", params=params
                    )
                    for symbol, bars in (payload.get("bars") or {}).items():
                        attempt_rows.setdefault(symbol, []).extend(bars)
                    page_token = payload.get("next_page_token")
                    if not page_token:
                        break
            except AlpacaError as exc:
                rejected = extract_invalid_symbols(str(exc)).intersection(active)
                if not rejected:
                    raise
                logger.warning(
                    "Skipping Alpaca asset-master symbol(s) rejected by market data: %s",
                    ", ".join(sorted(rejected)),
                )
                active = [symbol for symbol in active if symbol not in rejected]
                continue

            for symbol, bars in attempt_rows.items():
                combined[symbol].extend(bars)
            break
        return combined

    def get_single_bars(
        self,
        symbol: str,
        *,
        timeframe: str,
        start: datetime,
        end: datetime,
        feed: str,
        adjustment: str = "raw",
        asof: date | None = None,
    ) -> list[dict[str, Any]]:
        params: dict[str, Any] = {
            "timeframe": timeframe,
            "start": start.isoformat(),
            "end": end.isoformat(),
            "limit": 10000,
            "adjustment": adjustment,
            "feed": feed,
            "sort": "asc",
        }
        if asof:
            params["asof"] = asof.isoformat()
        rows: list[dict[str, Any]] = []
        page_token: str | None = None
        while True:
            if page_token:
                params["page_token"] = page_token
            else:
                params.pop("page_token", None)
            payload = self._request(
                "GET",
                f"{self.settings.alpaca_data_base_url}/v2/stocks/{quote(symbol, safe='')}/bars",
                params=params,
            )
            rows.extend(payload.get("bars") or [])
            page_token = payload.get("next_page_token")
            if not page_token:
                break
        return rows

    def _iter_single_pages(
        self,
        symbol: str,
        *,
        endpoint: str,
        response_key: str,
        start: datetime,
        end: datetime,
        feed: str | None = None,
        asof: date | None = None,
        limit: int = 10000,
    ) -> Iterator[list[dict[str, Any]]]:
        params: dict[str, Any] = {
            "start": start.isoformat(),
            "end": end.isoformat(),
            "limit": limit,
            "sort": "asc",
        }
        if feed:
            params["feed"] = feed
        if asof:
            params["asof"] = asof.isoformat()
        page_token: str | None = None
        while True:
            if page_token:
                params["page_token"] = page_token
            else:
                params.pop("page_token", None)
            payload = self._request(
                "GET",
                f"{self.settings.alpaca_data_base_url}/v2/stocks/{quote(symbol, safe='')}/{endpoint}",
                params=params,
            )
            rows = payload.get(response_key) or []
            if rows:
                yield rows
            page_token = payload.get("next_page_token")
            if not page_token:
                break

    def iter_trades(
        self,
        symbol: str,
        *,
        start: datetime,
        end: datetime,
        feed: str,
        asof: date | None = None,
    ) -> Iterator[list[dict[str, Any]]]:
        return self._iter_single_pages(
            symbol,
            endpoint="trades",
            response_key="trades",
            start=start,
            end=end,
            feed=feed,
            asof=asof,
        )

    def iter_quotes(
        self,
        symbol: str,
        *,
        start: datetime,
        end: datetime,
        feed: str,
        asof: date | None = None,
    ) -> Iterator[list[dict[str, Any]]]:
        return self._iter_single_pages(
            symbol,
            endpoint="quotes",
            response_key="quotes",
            start=start,
            end=end,
            feed=feed,
            asof=asof,
        )

    def iter_auctions(
        self,
        symbol: str,
        *,
        start: datetime,
        end: datetime,
        asof: date | None = None,
    ) -> Iterator[list[dict[str, Any]]]:
        # Alpaca's current official endpoint is the multi-symbol auctions route.
        params: dict[str, Any] = {
            "symbols": symbol,
            "start": start.isoformat(),
            "end": end.isoformat(),
            "limit": 10000,
            "feed": "sip",
            "sort": "asc",
        }
        if asof:
            params["asof"] = asof.isoformat()
        page_token: str | None = None
        while True:
            if page_token:
                params["page_token"] = page_token
            else:
                params.pop("page_token", None)
            payload = self._request(
                "GET",
                f"{self.settings.alpaca_data_base_url}/v2/stocks/auctions",
                params=params,
            )
            data = payload.get("auctions") or {}
            if isinstance(data, dict):
                rows = data.get(symbol) or []
            elif isinstance(data, list):
                rows = data
            else:
                rows = []
            if rows:
                yield rows
            page_token = payload.get("next_page_token")
            if not page_token:
                break

    def get_news(
        self,
        symbol: str,
        *,
        start: datetime,
        end: datetime,
        include_content: bool = True,
    ) -> list[dict[str, Any]]:
        params: dict[str, Any] = {
            "symbols": symbol,
            "start": start.isoformat(),
            "end": end.isoformat(),
            "limit": 50,
            "sort": "asc",
            "include_content": str(include_content).lower(),
        }
        rows: list[dict[str, Any]] = []
        page_token: str | None = None
        while True:
            if page_token:
                params["page_token"] = page_token
            else:
                params.pop("page_token", None)
            payload = self._request(
                "GET", f"{self.settings.alpaca_data_base_url}/v1beta1/news", params=params
            )
            rows.extend(payload.get("news") or [])
            page_token = payload.get("next_page_token")
            if not page_token:
                break
        return rows

    def get_corporate_actions(
        self,
        symbol: str,
        *,
        start: date,
        end: date,
    ) -> list[dict[str, Any]]:
        params: dict[str, Any] = {
            "symbols": symbol,
            "start": start.isoformat(),
            "end": end.isoformat(),
            "region": "us",
            "limit": 1000,
            "sort": "asc",
        }
        rows: list[dict[str, Any]] = []
        page_token: str | None = None
        while True:
            if page_token:
                params["page_token"] = page_token
            else:
                params.pop("page_token", None)
            payload = self._request(
                "GET",
                f"{self.settings.alpaca_data_base_url}/v1/corporate-actions",
                params=params,
            )
            # Alpaca has used both a flat list and grouped payloads over time.
            data = payload.get("corporate_actions") or payload.get("data") or []
            if isinstance(data, dict):
                for values in data.values():
                    if isinstance(values, list):
                        rows.extend(values)
            elif isinstance(data, list):
                rows.extend(data)
            page_token = payload.get("next_page_token")
            if not page_token:
                break
        return rows

# v3 helpers are defined outside the class in source history; attach a method explicitly
# to preserve backwards compatibility with the v2 client surface.
def _get_corporate_actions_multi(
    self: AlpacaClient,
    symbols: Iterable[str],
    *,
    start: date,
    end: date,
) -> list[dict[str, Any]]:
    active = sorted({str(s).strip().upper() for s in symbols if str(s).strip()})
    while active:
        params: dict[str, Any] = {
            "symbols": ",".join(active),
            "start": start.isoformat(),
            "end": end.isoformat(),
            "region": "us",
            "limit": 1000,
            "sort": "asc",
        }
        rows: list[dict[str, Any]] = []
        page_token: str | None = None
        try:
            while True:
                if page_token:
                    params["page_token"] = page_token
                else:
                    params.pop("page_token", None)
                payload = self._request(
                    "GET",
                    f"{self.settings.alpaca_data_base_url}/v1/corporate-actions",
                    params=params,
                )
                data = payload.get("corporate_actions") or payload.get("data") or []
                if isinstance(data, dict):
                    for group_name, values in data.items():
                        if not isinstance(values, list):
                            continue
                        for value in values:
                            row = dict(value)
                            row.setdefault("corporate_action_type", group_name)
                            rows.append(row)
                elif isinstance(data, list):
                    rows.extend(dict(value) for value in data)
                page_token = payload.get("next_page_token")
                if not page_token:
                    break
            return rows
        except AlpacaError as exc:
            rejected = extract_invalid_symbols(str(exc)).intersection(active)
            if not rejected:
                raise
            logger.warning(
                "Skipping invalid symbol(s) in corporate-action request: %s",
                ", ".join(sorted(rejected)),
            )
            active = [symbol for symbol in active if symbol not in rejected]
    return []


AlpacaClient.get_corporate_actions_multi = _get_corporate_actions_multi  # type: ignore[attr-defined]
