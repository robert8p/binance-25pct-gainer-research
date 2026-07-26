from __future__ import annotations

import hashlib
import json
import logging
from collections import defaultdict
from dataclasses import asdict
from datetime import date, datetime, time, timedelta, timezone
from typing import Any, Iterable
from zoneinfo import ZoneInfo

from app.alpaca_client import AlpacaClient, AlpacaError
from app.config import RESEARCH_TARGET_PCT, Settings
from app.detector import detect_regular_session_gainer
from app.supabase_store import SupabaseStore

logger = logging.getLogger(__name__)
ET = ZoneInfo("America/New_York")
UTC = timezone.utc


def chunks(items: list[Any], size: int) -> Iterable[list[Any]]:
    for i in range(0, len(items), size):
        yield items[i : i + size]


def parse_bar(raw: dict[str, Any]) -> dict[str, Any]:
    ts = datetime.fromisoformat(raw["t"].replace("Z", "+00:00"))
    return {
        "timestamp": ts,
        "open": float(raw["o"]),
        "high": float(raw["h"]),
        "low": float(raw["l"]),
        "close": float(raw["c"]),
        "volume": int(raw.get("v") or 0),
        "trade_count": int(raw.get("n") or 0),
        "vwap": float(raw.get("vw") or 0),
    }


def market_dt(day: date, hhmm: str) -> datetime:
    hour, minute = [int(part) for part in hhmm.split(":")[:2]]
    return datetime.combine(day, time(hour, minute), ET).astimezone(UTC)


def stable_hash(payload: Any) -> str:
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str).encode()
    return hashlib.sha256(encoded).hexdigest()


class ScanRunner:
    def __init__(self, settings: Settings, store: SupabaseStore, alpaca: AlpacaClient):
        self.settings = settings
        self.store = store
        self.alpaca = alpaca

    def _update(self, scan_id: str, stage: str, current: int, total: int, **extra: Any) -> None:
        self.store.update_scan(
            scan_id,
            progress_stage=stage,
            progress_current=current,
            progress_total=total,
            **extra,
        )

    def _resolve_sessions(self, lookback_days: int, include_partial: bool) -> list[dict[str, Any]]:
        today_et = datetime.now(ET).date()
        start = today_et - timedelta(days=lookback_days + 14)
        calendar = self.alpaca.get_calendar(start, today_et)
        sessions = []
        now_utc = datetime.now(UTC)
        safe_cutoff = now_utc - timedelta(minutes=20)
        for row in calendar:
            day = date.fromisoformat(row["date"])
            open_dt = market_dt(day, row.get("open", "09:30"))
            close_dt = market_dt(day, row.get("close", "16:00"))
            completed = close_dt <= safe_cutoff
            if completed or include_partial:
                effective_close = min(close_dt, safe_cutoff) if include_partial else close_dt
                if effective_close > open_dt:
                    sessions.append(
                        {
                            "date": day,
                            "open": open_dt,
                            "close": effective_close,
                            "official_close": close_dt,
                            "completed": completed,
                        }
                    )
        cutoff = today_et - timedelta(days=lookback_days)
        return [session for session in sessions if session["date"] >= cutoff]

    def run(self, scan: dict[str, Any]) -> None:
        scan_id = scan["id"]
        p = scan.get("parameters") or {}
        lookback_days = int(p.get("lookback_days", self.settings.default_lookback_days))
        threshold_pct = float(p.get("threshold_pct", self.settings.default_threshold_pct))
        if abs(threshold_pct - RESEARCH_TARGET_PCT) > 1e-9:
            raise ValueError("This research fork is locked to a 25% gain threshold")
        universe_mode = str(p.get("universe_mode", self.settings.default_universe_mode))
        feed = str(p.get("feed", self.settings.default_feed))
        include_partial = bool(
            p.get("include_partial_current_day", self.settings.include_partial_current_day)
        )
        save_event_bars = bool(p.get("save_event_bars", self.settings.save_event_bars))

        try:
            self._update(scan_id, "calendar", 0, 1)
            sessions = self._resolve_sessions(lookback_days, include_partial)
            if not sessions:
                raise RuntimeError("No eligible market sessions found")
            session_by_date = {s["date"]: s for s in sessions}
            lookback_start = sessions[0]["date"]
            lookback_end = sessions[-1]["date"]

            assets = self.alpaca.get_assets(all_statuses=True)
            assets = [a for a in assets if a.get("class") == "us_equity"]
            self.store.save_asset_snapshot(datetime.now(ET).date().isoformat(), assets)

            if universe_mode == "all_recent_alpaca_assets":
                selected = assets
            else:
                selected = [
                    a
                    for a in assets
                    if a.get("status") == "active" and bool(a.get("tradable"))
                ]

            if not self.settings.include_otc:
                selected = [a for a in selected if a.get("exchange") != "OTC"]

            asset_by_symbol = {a["symbol"]: a for a in selected if a.get("symbol")}
            symbols = sorted(asset_by_symbol)
            asset_feed_by_symbol = {
                symbol: ("otc" if asset.get("exchange") == "OTC" else feed)
                for symbol, asset in asset_by_symbol.items()
            }
            coverage_warnings: list[str] = []
            self._update(
                scan_id,
                "daily_bars",
                0,
                len(symbols),
                universe_count=len(symbols),
                lookback_start=lookback_start.isoformat(),
                lookback_end=lookback_end.isoformat(),
            )

            # Extra calendar buffer supplies the prior regular-session close for the first target day.
            daily_start = sessions[0]["open"] - timedelta(days=10)
            daily_end = sessions[-1]["official_close"]
            daily_by_symbol: dict[str, list[dict[str, Any]]] = defaultdict(list)
            processed = 0
            symbols_by_feed: dict[str, list[str]] = defaultdict(list)
            for symbol in symbols:
                symbols_by_feed[asset_feed_by_symbol[symbol]].append(symbol)

            for data_feed, feed_symbols in symbols_by_feed.items():
                for batch in chunks(feed_symbols, self.settings.daily_batch_size):
                    try:
                        raw = self.alpaca.get_bars(
                            batch,
                            timeframe="1Day",
                            start=daily_start,
                            end=daily_end,
                            feed=data_feed,
                            adjustment="split",
                            asof=lookback_end,
                        )
                    except AlpacaError as exc:
                        if data_feed == "otc" and ("403" in str(exc) or "permission" in str(exc).lower()):
                            coverage_warnings.append(
                                "OTC assets were present but skipped because the Alpaca account lacks the special OTC market-data permission."
                            )
                            processed += len(batch)
                            self._update(scan_id, "daily_bars", processed, len(symbols))
                            continue
                        raise
                    for symbol, bars in raw.items():
                        daily_by_symbol[symbol].extend(parse_bar(b) for b in bars)
                    processed += len(batch)
                    self._update(scan_id, "daily_bars", processed, len(symbols))

            candidates_by_date: dict[date, list[dict[str, Any]]] = defaultdict(list)
            for symbol, bars in daily_by_symbol.items():
                ordered = sorted(bars, key=lambda b: b["timestamp"])
                previous_close: float | None = None
                for bar in ordered:
                    event_date = bar["timestamp"].astimezone(ET).date()
                    if previous_close and event_date in session_by_date:
                        if bar["high"] >= previous_close * (1.0 + threshold_pct / 100.0):
                            candidates_by_date[event_date].append(
                                {
                                    "symbol": symbol,
                                    "prior_close": previous_close,
                                    "daily_high": bar["high"],
                                    "data_feed": asset_feed_by_symbol.get(symbol, feed),
                                }
                            )
                    previous_close = bar["close"]

            candidate_count = sum(len(v) for v in candidates_by_date.values())
            self._update(
                scan_id,
                "minute_verification",
                0,
                candidate_count,
                candidate_day_count=candidate_count,
            )

            result_count = 0
            verified = 0
            for event_date in sorted(candidates_by_date):
                session = session_by_date[event_date]
                candidates = candidates_by_date[event_date]
                candidates_by_feed: dict[str, list[dict[str, Any]]] = defaultdict(list)
                for candidate in candidates:
                    candidates_by_feed[candidate["data_feed"]].append(candidate)

                for data_feed, feed_candidates in candidates_by_feed.items():
                    prior_close_map = {c["symbol"]: c["prior_close"] for c in feed_candidates}
                    for batch in chunks(
                        [c["symbol"] for c in feed_candidates], self.settings.minute_batch_size
                    ):
                        raw = self.alpaca.get_bars(
                            batch,
                            timeframe="1Min",
                            start=session["open"],
                            end=session["close"],
                            feed=data_feed,
                            adjustment="split",
                            asof=lookback_end,
                        )
                        for symbol in batch:
                            bars = [parse_bar(b) for b in raw.get(symbol, [])]
                            detection = detect_regular_session_gainer(
                                bars,
                                prior_close=prior_close_map[symbol],
                                threshold_pct=threshold_pct,
                                market_open=session["open"],
                                market_close=session["close"],
                            )
                            verified += 1
                            if not detection or not detection.qualifies:
                                continue
    
                            asset = asset_by_symbol.get(symbol, {})
                            d = asdict(detection)
                            quality_flags = list(d.pop("quality_flags"))
                            if universe_mode == "all_recent_alpaca_assets" and not (
                                asset.get("status") == "active" and asset.get("tradable")
                            ):
                                quality_flags.append("historical_tradability_unverified")
    
                            result_row = {
                                "scan_id": scan_id,
                                "symbol": symbol,
                                "company_name": asset.get("name"),
                                "exchange": asset.get("exchange"),
                                "event_date": event_date.isoformat(),
                                "current_status": asset.get("status"),
                                "currently_tradable": bool(asset.get("tradable")),
                                "fractionable": bool(asset.get("fractionable")),
                                "shortable": bool(asset.get("shortable")),
                                "easy_to_borrow": bool(asset.get("easy_to_borrow")),
                                "feed": data_feed,
                                "threshold_pct": threshold_pct,
                                "prior_close": d["prior_close"],
                                "threshold_price": d["threshold_price"],
                                "session_open": d["session_open"],
                                "session_high": d["session_high"],
                                "session_low": d["session_low"],
                                "session_close": d["session_close"],
                                "session_volume": d["session_volume"],
                                "session_trade_count": d["session_trade_count"],
                                "opening_gap_pct": d["opening_gap_pct"],
                                "high_vs_prior_close_pct": d["high_vs_prior_close_pct"],
                                "open_to_peak_pct": d["open_to_peak_pct"],
                                "first_minute_close": d["first_minute_close"],
                                "first_minute_entry_to_peak_pct": d[
                                    "first_minute_entry_to_peak_pct"
                                ],
                                "threshold_cross_bar_start": d[
                                    "threshold_cross_bar_start"
                                ].isoformat()
                                if d["threshold_cross_bar_start"]
                                else None,
                                "peak_bar_start": d["peak_bar_start"].isoformat()
                                if d["peak_bar_start"]
                                else None,
                                "peak_price": d["peak_price"],
                                "minutes_from_open_to_cross": d["minutes_from_open_to_cross"],
                                "minutes_from_open_to_peak": d["minutes_from_open_to_peak"],
                                "first_bar_volume": d["first_bar_volume"],
                                "first_bar_trade_count": d["first_bar_trade_count"],
                                "peak_bar_volume": d["peak_bar_volume"],
                                "peak_bar_trade_count": d["peak_bar_trade_count"],
                                "max_missing_bar_gap_minutes": d[
                                    "max_missing_bar_gap_minutes"
                                ],
                                "quality_flags": quality_flags,
                                "result_hash": stable_hash(
                                    {
                                        "symbol": symbol,
                                        "event_date": event_date.isoformat(),
                                        "prior_close": d["prior_close"],
                                        "peak": d["peak_price"],
                                        "feed": data_feed,
                                    }
                                ),
                            }
                            saved = self.store.insert("stock25_scan_results", result_row)[0]
                            result_count += 1
    
                            if save_event_bars:
                                event_rows = [
                                    {
                                        "scan_id": scan_id,
                                        "result_id": saved["id"],
                                        "symbol": symbol,
                                        "event_date": event_date.isoformat(),
                                        "bar_timestamp": b["timestamp"].isoformat(),
                                        "open": b["open"],
                                        "high": b["high"],
                                        "low": b["low"],
                                        "close": b["close"],
                                        "volume": b["volume"],
                                        "trade_count": b["trade_count"],
                                        "vwap": b["vwap"],
                                    }
                                    for b in bars
                                ]
                                self.store.upsert(
                                    "stock25_event_bars",
                                    event_rows,
                                    on_conflict="scan_id,symbol,bar_timestamp",
                                    chunk_size=500,
                                )
                    self._update(
                        scan_id,
                        "minute_verification",
                        verified,
                        candidate_count,
                        result_count=result_count,
                    )

            if not self.settings.include_otc and any(a.get("exchange") == "OTC" for a in assets):
                coverage_warnings.append(
                    "OTC assets were excluded because Alpaca OTC market data requires a special broker-partner subscription."
                )
            if universe_mode == "current_tradable":
                coverage_warnings.append(
                    "Initial history uses the current active/tradable Alpaca universe and can omit stocks delisted or made inactive during the lookback. Daily asset snapshots improve point-in-time coverage going forward."
                )

            self.store.update_scan(
                scan_id,
                status="completed",
                completed_at=datetime.now(UTC).isoformat(),
                progress_stage="completed",
                progress_current=candidate_count,
                progress_total=candidate_count,
                result_count=result_count,
                coverage_notes=coverage_warnings,
                parameters_hash=stable_hash(p),
            )
        except Exception as exc:
            logger.exception("Scan %s failed", scan_id)
            self.store.update_scan(
                scan_id,
                status="failed",
                completed_at=datetime.now(UTC).isoformat(),
                progress_stage="failed",
                error_message=str(exc)[:4000],
            )
            raise
