from __future__ import annotations

import hashlib
import json
import logging
import os
import tempfile
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from .binance import BinanceClient
from .classifier import (
    classify_symbol,
    decision_observations,
    first_rolling_surge,
    parse_kline,
    pct_change,
)
from .runtime import collect_memory, ensure_disk_headroom, log_resources
from .supabase import SupabaseClient

logger = logging.getLogger(__name__)


def utc_ms(dt: datetime) -> int:
    return int(dt.timestamp() * 1000)


def floor_utc_day(dt: datetime) -> datetime:
    return dt.astimezone(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)


def _iso_from_ms(value: int) -> str:
    return datetime.fromtimestamp(value / 1000, tz=timezone.utc).isoformat()


class Scanner:
    def __init__(
        self,
        db: SupabaseClient,
        binance: BinanceClient,
        temp_root: Path,
        *,
        persist_event_agg_trades: bool = False,
        minimum_disk_free_bytes: int = 750_000_000,
    ):
        self.db = db
        self.binance = binance
        self.temp_root = temp_root
        self.persist_event_agg_trades = persist_event_agg_trades
        self.minimum_disk_free_bytes = minimum_disk_free_bytes

    def _symbol_universe(self, scan_id: str, quote_assets: list[str]) -> list[dict[str, Any]]:
        existing = self.db.select_all(
            "binance_symbol_snapshots",
            filters={"scan_id": f"eq.{scan_id}", "selected_canonical": "eq.true"},
            order="symbol.asc",
        )
        if existing:
            return [
                {
                    "symbol": row["symbol"],
                    "base_asset": row["base_asset"],
                    "quote_asset": row["quote_asset"],
                    "quote_priority": int(row.get("quote_priority") or 0),
                    "status": row.get("status") or "TRADING",
                    "spot_permission": bool(row.get("spot_permission", True)),
                    "is_spot_trading_allowed": bool(row.get("is_spot_trading_allowed", True)),
                    "stablecoin_like": bool(row.get("stablecoin_like", False)),
                    "leveraged_token_like": bool(row.get("leveraged_token_like", False)),
                    "raw_json": row.get("raw_json") or {},
                }
                for row in existing
            ]

        exchange = self.binance.exchange_info()
        snapshot_at = datetime.now(timezone.utc).isoformat()
        candidates: list[dict[str, Any]] = []
        quote_rank = {quote: rank for rank, quote in enumerate(quote_assets)}
        for raw in exchange.get("symbols", []):
            item = classify_symbol(raw)
            if item["quote_asset"] not in quote_rank:
                continue
            if item["status"] != "TRADING" or not item["spot_permission"] or not item["is_spot_trading_allowed"]:
                continue
            if "LIMIT" not in item["order_types"]:
                continue
            item["snapshot_at"] = snapshot_at
            item["scan_id"] = scan_id
            item["quote_priority"] = quote_rank[item["quote_asset"]]
            candidates.append(item)

        chosen_by_base: dict[str, dict[str, Any]] = {}
        for item in candidates:
            current = chosen_by_base.get(item["base_asset"])
            if current is None or (item["quote_priority"], item["symbol"]) < (
                current["quote_priority"], current["symbol"]
            ):
                chosen_by_base[item["base_asset"]] = item
        symbols = sorted(chosen_by_base.values(), key=lambda row: row["symbol"])
        selected_symbols = {row["symbol"] for row in symbols}
        self.db.upsert(
            "binance_symbol_snapshots",
            [
                {
                    "scan_id": scan_id,
                    "snapshot_at": snapshot_at,
                    "symbol": item["symbol"],
                    "base_asset": item["base_asset"],
                    "quote_asset": item["quote_asset"],
                    "quote_priority": item["quote_priority"],
                    "selected_canonical": item["symbol"] in selected_symbols,
                    "status": item["status"],
                    "spot_permission": item["spot_permission"],
                    "is_spot_trading_allowed": item["is_spot_trading_allowed"],
                    "stablecoin_like": item["stablecoin_like"],
                    "leveraged_token_like": item["leveraged_token_like"],
                    "raw_json": item["raw_json"],
                }
                for item in candidates
            ],
            on_conflict="scan_id,symbol",
        )
        return symbols

    @staticmethod
    def _universe_hash(symbols: list[dict[str, Any]]) -> str:
        payload = [
            (row["symbol"], row["base_asset"], row["quote_asset"], row.get("quote_priority", 0))
            for row in symbols
        ]
        return hashlib.sha256(json.dumps(payload, separators=(",", ":")).encode("utf-8")).hexdigest()

    def run(self, job: dict[str, Any]) -> dict[str, Any]:
        job_id = str(job["id"])
        event_definition_version = str(job.get("event_definition_version") or "v1_25pct_rolling_8h")
        lookback = int(job.get("lookback_days") or 60)
        threshold_pct = float(job.get("threshold_pct") or 25)
        window_minutes = int(job.get("window_minutes") or 480)
        if event_definition_version != "v1_25pct_rolling_8h":
            raise ValueError("This deployment accepts only v1_25pct_rolling_8h scan jobs")
        if abs(threshold_pct - 25.0) > 1e-12:
            raise ValueError("This deployment accepts only the fixed 25% event threshold")
        if window_minutes != 480:
            raise ValueError("This deployment accepts only the fixed 480-minute event window")
        min_exit = float(job["min_exit_notional"])
        confirmation_seconds = int(job["confirmation_window_seconds"])
        quote_assets = [x.strip().upper() for x in (job.get("quote_assets") or ["USDT"]) if x.strip()]

        now = datetime.now(timezone.utc)
        latest_completed_end = floor_utc_day(now)
        requested_start = job.get("window_start_date")
        requested_end = job.get("window_end_date_exclusive")
        if requested_start or requested_end:
            if not requested_start or not requested_end:
                raise ValueError("Both window_start_date and window_end_date_exclusive are required")
            candidate_start = datetime.fromisoformat(str(requested_start)).replace(tzinfo=timezone.utc)
            end = datetime.fromisoformat(str(requested_end)).replace(tzinfo=timezone.utc)
            if candidate_start >= end:
                raise ValueError("Historical scan start must be before end")
            if end > latest_completed_end:
                raise ValueError("Historical scan end must not include the current incomplete UTC day")
            span_days = (end - candidate_start).days
            if span_days < 1 or span_days > 180:
                raise ValueError("Historical scan window must be between 1 and 180 completed UTC days")
            lookback = span_days
        else:
            end = latest_completed_end
            candidate_start = end - timedelta(days=lookback)
        # Load one extra daily bar so the first candidate date can reference the prior day.
        start = candidate_start - timedelta(days=1)

        symbols = self._symbol_universe(job_id, quote_assets)
        universe_hash = self._universe_hash(symbols)
        checkpoint = dict(job.get("checkpoint_json") or {})
        resume_index = int(checkpoint.get("next_symbol_index", job.get("symbols_processed") or 0))
        resume_index = min(max(resume_index, 0), len(symbols))
        stored_hash = checkpoint.get("symbol_universe_sha256")
        if stored_hash and stored_hash != universe_hash:
            raise RuntimeError("The frozen scan symbol universe changed; refusing an unsafe resume")

        existing_events = self.db.select_all(
            "binance_gainer_events",
            columns="id,sellability_pass",
            filters={"scan_id": f"eq.{job_id}"},
        )
        existing_event_ids = {str(row["id"]) for row in existing_events}
        saleable_events = sum(bool(row.get("sellability_pass")) for row in existing_events)
        surge_candidates = len(existing_events)
        failures = int(checkpoint.get("failures", job.get("failures") or 0))
        daily_rows_written = int(checkpoint.get("daily_rows", job.get("daily_rows") or 0))

        checkpoint.update(
            {
                "schema_version": 1,
                "phase": "scan_symbols",
                "next_symbol_index": resume_index,
                "symbol_universe_sha256": universe_hash,
                "symbols_total": len(symbols),
                "candidates_found": surge_candidates,
                "events_found": saleable_events,
                "failures": failures,
                "daily_rows": daily_rows_written,
            }
        )
        self.db.update(
            "binance_scan_jobs",
            {"id": f"eq.{job_id}"},
            {
                "symbols_total": len(symbols),
                "symbols_processed": resume_index,
                "candidates_found": surge_candidates,
                "events_found": saleable_events,
                "failures": failures,
                "daily_rows": daily_rows_written,
                "checkpoint_json": checkpoint,
                "last_stage": "scan_symbols",
                "last_unit": symbols[resume_index - 1]["symbol"] if resume_index else None,
                "last_checkpoint_at": datetime.now(timezone.utc).isoformat(),
            },
        )
        log_resources("scan_resume", path=self.temp_root, extra={"resume_index": resume_index, "symbols_total": len(symbols)})

        factor = 1.0 + threshold_pct / 100.0
        for index, symbol_info in enumerate(symbols[resume_index:], start=resume_index + 1):
            symbol = symbol_info["symbol"]
            try:
                raw_daily = self.binance.klines(symbol, "1d", utc_ms(start), utc_ms(end))
                daily = [parse_kline(row, symbol, "1d") for row in raw_daily]
                for row in daily:
                    row["scan_id"] = job_id
                self.db.upsert(
                    "binance_daily_bars",
                    daily,
                    on_conflict="scan_id,symbol,open_time",
                )
                daily_rows_written += len(daily)

                for pos, current in enumerate(daily):
                    event_dt = datetime.fromisoformat(current["open_time"])
                    if event_dt < candidate_start:
                        continue
                    prior_bar_available = False
                    previous = {
                        "open_time": (event_dt - timedelta(days=1)).isoformat(),
                        "close": current["open"],
                        "low": current["low"],
                    }
                    if pos > 0:
                        possible_previous = daily[pos - 1]
                        previous_dt = datetime.fromisoformat(possible_previous["open_time"])
                        if event_dt - previous_dt == timedelta(days=1):
                            previous = possible_previous
                            prior_bar_available = True

                    # Cheap necessary-condition filter only. It deliberately
                    # over-includes days; minute-level ordering decides whether a
                    # valid <=8-hour move actually occurred. First listing days
                    # are included even when no prior daily bar exists.
                    possible_baseline = min(float(previous["low"]), float(current["low"]))
                    if float(current["high"]) + 1e-15 < possible_baseline * factor:
                        continue

                    outcome = self._process_candidate(
                        job_id,
                        symbol_info,
                        previous,
                        current,
                        prior_bar_available,
                        event_definition_version,
                        threshold_pct,
                        window_minutes,
                        min_exit,
                        confirmation_seconds,
                    )
                    if outcome is not None and outcome["event_id"] not in existing_event_ids:
                        existing_event_ids.add(outcome["event_id"])
                        surge_candidates += 1
                        if outcome["sellability_pass"]:
                            saleable_events += 1
            except Exception as exc:
                failures += 1
                self.db.insert(
                    "binance_scan_issues",
                    {
                        "scan_id": job_id,
                        "symbol": symbol,
                        "stage": "symbol_scan",
                        "message": str(exc)[:4000],
                    },
                )
            checkpoint.update(
                {
                    "phase": "scan_symbols",
                    "next_symbol_index": index,
                    "last_symbol": symbol,
                    "candidates_found": surge_candidates,
                    "events_found": saleable_events,
                    "failures": failures,
                    "daily_rows": daily_rows_written,
                }
            )
            now_iso = datetime.now(timezone.utc).isoformat()
            self.db.update(
                "binance_scan_jobs",
                {"id": f"eq.{job_id}"},
                {
                    "symbols_processed": index,
                    "candidates_found": surge_candidates,
                    "events_found": saleable_events,
                    "failures": failures,
                    "daily_rows": daily_rows_written,
                    "heartbeat_at": now_iso,
                    "checkpoint_json": checkpoint,
                    "last_stage": "scan_symbols",
                    "last_unit": symbol,
                    "last_checkpoint_at": now_iso,
                },
            )
            collect_memory()
            if index % 10 == 0 or index == len(symbols):
                log_resources("scan_symbol_checkpoint", path=self.temp_root, extra={"symbol": symbol, "processed": index})

        checkpoint.update({"phase": "complete", "next_symbol_index": len(symbols)})
        self.db.update(
            "binance_scan_jobs",
            {"id": f"eq.{job_id}"},
            {
                "checkpoint_json": checkpoint,
                "last_stage": "complete",
                "last_unit": symbols[-1]["symbol"] if symbols else None,
                "last_checkpoint_at": datetime.now(timezone.utc).isoformat(),
            },
        )
        return {
            "event_definition_version": event_definition_version,
            "symbols_total": len(symbols),
            "symbols_processed": len(symbols),
            "candidates_found": surge_candidates,
            "events_found": saleable_events,
            "failures": failures,
            "daily_rows": daily_rows_written,
            "window_start": candidate_start.isoformat(),
            "window_end_exclusive": end.isoformat(),
            "measurement": (
                f"earliest later-minute high >= {threshold_pct:.8g}% above the lowest prior-minute "
                f"low within a conservative {window_minutes}-minute rolling window"
            ),
            "sellability": (
                f"at least {min_exit:.8g} quote units of seller-initiated executed notional at any "
                f"price within {confirmation_seconds} seconds after the exact crossing trade"
            ),
        }

    def _process_candidate(
        self,
        scan_id: str,
        symbol_info: dict[str, Any],
        previous: dict[str, Any],
        current: dict[str, Any],
        previous_day_bar_available: bool,
        event_definition_version: str,
        threshold_pct: float,
        window_minutes: int,
        min_exit: float,
        confirmation_seconds: int,
    ) -> dict[str, Any] | None:
        symbol = symbol_info["symbol"]
        day_start = datetime.fromisoformat(current["open_time"])
        day_end = day_start + timedelta(days=1)
        extended_start = day_start - timedelta(minutes=window_minutes)
        raw_minutes = self.binance.klines(symbol, "1m", utc_ms(extended_start), utc_ms(day_end))
        minutes = [parse_kline(row, symbol, "1m") for row in raw_minutes]
        surge = first_rolling_surge(
            minutes,
            event_day_start=day_start,
            event_day_end=day_end,
            threshold_pct=threshold_pct,
            window_minutes=window_minutes,
        )
        if surge is None:
            return None

        baseline = surge["baseline_row"]
        crossing = surge["crossing_row"]
        baseline_dt = surge["baseline_time"]
        crossing_dt = surge["crossing_time"]
        baseline_price = float(surge["baseline_price"])
        threshold = float(surge["threshold_price"])
        day_minutes = [
            row for row in minutes
            if day_start <= datetime.fromisoformat(row["open_time"]) < day_end
        ]
        if not day_minutes:
            return None
        peak = max(day_minutes, key=lambda row: row["high"])
        peak_dt = datetime.fromisoformat(peak["open_time"])
        first_minute = day_minutes[0]
        pre_cross = [row for row in minutes if datetime.fromisoformat(row["open_time"]) < crossing_dt]

        expected = int((day_end - extended_start).total_seconds() // 60)
        missing_minutes = max(0, expected - len(minutes))
        exact_window_rows = [
            row for row in minutes
            if baseline_dt <= datetime.fromisoformat(row["open_time"]) <= crossing_dt
        ]
        expected_window_rows = int((crossing_dt - baseline_dt).total_seconds() // 60) + 1
        missing_window_minutes = max(0, expected_window_rows - len(exact_window_rows))

        event_id = str(
            uuid.uuid5(
                uuid.NAMESPACE_URL,
                f"{scan_id}:{symbol}:{day_start.date().isoformat()}:rolling-{window_minutes}m",
            )
        )

        # Resolve aggregate trades page-by-page. This avoids retaining hundreds
        # of thousands of hot-market trades in RAM.
        baseline_start_ms = utc_ms(baseline_dt)
        price_tolerance = max(abs(baseline_price) * 1e-12, 1e-15)
        baseline_trade = None
        baseline_state: dict[str, Any] = {}
        for page in self.binance.iter_aggregate_trade_pages(
            symbol, baseline_start_ms, baseline_start_ms + 59_999, state=baseline_state
        ):
            for trade in page:
                if abs(float(trade["p"]) - baseline_price) <= price_tolerance:
                    if baseline_trade is None or int(trade["T"]) > int(baseline_trade["T"]):
                        baseline_trade = trade
        exact_baseline_ms = int(baseline_trade["T"]) if baseline_trade else None

        minute_start_ms = utc_ms(crossing_dt)
        first_cross_trade = None
        crossing_state: dict[str, Any] = {}
        for page in self.binance.iter_aggregate_trade_pages(
            symbol, minute_start_ms, minute_start_ms + 59_999, state=crossing_state
        ):
            first_cross_trade = next(
                (trade for trade in page if float(trade["p"]) + price_tolerance >= threshold),
                None,
            )
            if first_cross_trade is not None:
                break

        sell_state: dict[str, Any] = {"truncated": False}
        exact_cross_ms = int(first_cross_trade["T"]) if first_cross_trade else None
        exact_elapsed_seconds = (
            (exact_cross_ms - exact_baseline_ms) / 1000
            if exact_cross_ms is not None and exact_baseline_ms is not None
            else None
        )
        exact_window_pass = (
            exact_elapsed_seconds is not None
            and 0 < exact_elapsed_seconds <= window_minutes * 60
        )

        trades_truncated = bool(baseline_state.get("truncated")) or bool(crossing_state.get("truncated"))
        seller_notional_any_price = 0.0
        seller_base_quantity_any_price = 0.0
        seller_notional_at_or_above = 0.0
        all_trade_notional_at_or_above = 0.0
        first_seller_exit_at: str | None = None
        cumulative_hit_at: str | None = None
        cumulative_hit_price: float | None = None
        exit_vwap: float | None = None
        exit_base_for_min_notional = 0.0
        lowest_seller_exit_price: float | None = None
        highest_seller_exit_price: float | None = None

        trade_spool: Path | None = None
        spool_handle = None
        if self.persist_event_agg_trades and exact_cross_ms is not None:
            ensure_disk_headroom(self.temp_root, self.minimum_disk_free_bytes)
            spool_dir = self.temp_root / "scan-trade-spool"
            spool_dir.mkdir(parents=True, exist_ok=True)
            fd, name = tempfile.mkstemp(prefix=f"{event_id}-", suffix=".jsonl", dir=spool_dir)
            os.close(fd)
            trade_spool = Path(name)
            spool_handle = trade_spool.open("w", encoding="utf-8")

        if exact_cross_ms is not None:
            sell_end = exact_cross_ms + confirmation_seconds * 1000
            for page in self.binance.iter_aggregate_trade_pages(
                symbol, exact_cross_ms, sell_end, state=sell_state
            ):
                for trade in page:
                    price = float(trade["p"])
                    qty = float(trade["q"])
                    notional = price * qty
                    at_threshold = price + price_tolerance >= threshold
                    seller_taker = bool(trade["m"])
                    if at_threshold:
                        all_trade_notional_at_or_above += notional
                    if seller_taker:
                        previous_seller_notional = seller_notional_any_price
                        seller_notional_any_price += notional
                        seller_base_quantity_any_price += qty
                        if at_threshold:
                            seller_notional_at_or_above += notional
                        ts = _iso_from_ms(int(trade["T"]))
                        if first_seller_exit_at is None:
                            first_seller_exit_at = ts
                        lowest_seller_exit_price = price if lowest_seller_exit_price is None else min(lowest_seller_exit_price, price)
                        highest_seller_exit_price = price if highest_seller_exit_price is None else max(highest_seller_exit_price, price)
                        if cumulative_hit_at is None and min_exit > 0:
                            remaining = max(0.0, min_exit - previous_seller_notional)
                            used_notional = min(remaining, notional)
                            if used_notional > 0 and price > 0:
                                exit_base_for_min_notional += used_notional / price
                            if seller_notional_any_price + 1e-12 >= min_exit:
                                cumulative_hit_at = ts
                                cumulative_hit_price = price
                                if exit_base_for_min_notional > 0:
                                    exit_vwap = min_exit / exit_base_for_min_notional

                    if spool_handle is not None:
                        spool_handle.write(
                            json.dumps(
                                {
                                    "event_id": event_id,
                                    "scan_id": scan_id,
                                    "symbol": symbol,
                                    "event_date": day_start.date().isoformat(),
                                    "agg_trade_id": int(trade["a"]),
                                    "trade_time": _iso_from_ms(int(trade["T"])),
                                    "price": price,
                                    "quantity": qty,
                                    "quote_notional": notional,
                                    "buyer_was_maker": seller_taker,
                                    "at_or_above_threshold": at_threshold,
                                },
                                separators=(",", ":"),
                            ) + "\n"
                        )
        if spool_handle is not None:
            spool_handle.close()
        trades_truncated = trades_truncated or bool(sell_state.get("truncated"))

        sellability_pass = (
            baseline_trade is not None
            and first_cross_trade is not None
            and exact_window_pass
            and seller_notional_any_price + 1e-12 >= min_exit
        )
        event = {
            "id": event_id,
            "scan_id": scan_id,
            "symbol": symbol,
            "base_asset": symbol_info["base_asset"],
            "quote_asset": symbol_info["quote_asset"],
            "event_date": day_start.date().isoformat(),
            "event_definition_version": event_definition_version,
            "previous_day_close": previous["close"],
            "previous_day_bar_available": previous_day_bar_available,
            "threshold_pct": threshold_pct,
            "threshold_price": threshold,
            "window_minutes": window_minutes,
            "measurement_method": (
                "lowest prior one-minute low to a later one-minute high; baseline-minute-open "
                "gap capped at window_minutes-1 to guarantee exact trades can remain within the window"
            ),
            "baseline_time": baseline["open_time"],
            "baseline_price": baseline_price,
            "baseline_trade_time": _iso_from_ms(exact_baseline_ms) if exact_baseline_ms is not None else None,
            "baseline_agg_trade_id": int(baseline_trade["a"]) if baseline_trade else None,
            "baseline_trade_unresolved": baseline_trade is None,
            "minutes_baseline_open_to_cross_open": surge["minutes_baseline_open_to_cross_open"],
            "exact_baseline_to_cross_seconds": exact_elapsed_seconds,
            "exact_window_pass": exact_window_pass,
            "rolling_gain_pct_at_cross_trade": (
                pct_change(float(first_cross_trade["p"]), baseline_price) if first_cross_trade else None
            ),
            "day_open": current["open"],
            "first_minute_close": first_minute["close"],
            "first_cross_time": crossing["open_time"],
            "first_cross_trade_time": _iso_from_ms(exact_cross_ms) if exact_cross_ms is not None else None,
            "crossing_agg_trade_id": int(first_cross_trade["a"]) if first_cross_trade else None,
            "crossing_trade_price": float(first_cross_trade["p"]) if first_cross_trade else None,
            "crossing_trade_unresolved": first_cross_trade is None,
            "crossing_minute_open": crossing["open"],
            "crossing_minute_high": crossing["high"],
            "day_high": peak["high"],
            "day_high_time": peak["open_time"],
            "day_close": current["close"],
            "day_quote_volume": current["quote_volume"],
            "day_trade_count": current["trade_count"],
            "previous_close_to_high_pct": pct_change(peak["high"], previous["close"]),
            "day_open_to_high_pct": pct_change(peak["high"], current["open"]),
            "first_minute_close_to_high_pct": pct_change(peak["high"], first_minute["close"]),
            "minutes_from_day_start_to_cross": int((crossing_dt - day_start).total_seconds() // 60),
            "minutes_from_day_start_to_peak": int((peak_dt - day_start).total_seconds() // 60),
            "pre_cross_minutes": len(pre_cross),
            "pre_cross_quote_volume": sum(row["quote_volume"] for row in pre_cross),
            "crossed_in_first_minute": crossing_dt == day_start,
            "missing_minute_bars": missing_minutes,
            "missing_window_minute_bars": missing_window_minutes,
            "stablecoin_like": symbol_info["stablecoin_like"],
            "leveraged_token_like": symbol_info["leveraged_token_like"],
            "sellability_method": (
                "executed seller-initiated aggregate trades at any price after the exact threshold "
                "crossing; not historical displayed order-book depth and not a guarantee of fill price"
            ),
            "confirmation_window_seconds": confirmation_seconds,
            "minimum_exit_notional": min_exit,
            "seller_taker_notional_at_or_above": seller_notional_at_or_above,
            "all_trade_notional_at_or_above": all_trade_notional_at_or_above,
            "seller_taker_notional_any_price": seller_notional_any_price,
            "seller_taker_base_quantity_any_price": seller_base_quantity_any_price,
            "minimum_exit_vwap": exit_vwap,
            "minimum_exit_vwap_pct_vs_threshold": pct_change(exit_vwap, threshold) if exit_vwap else None,
            "minimum_exit_reached_price": cumulative_hit_price,
            "lowest_seller_exit_price": lowest_seller_exit_price,
            "highest_seller_exit_price": highest_seller_exit_price,
            "first_seller_exit_time": first_seller_exit_at,
            "minimum_exit_reached_time": cumulative_hit_at,
            "sellability_pass": sellability_pass,
            "sellability_trades_truncated": trades_truncated,
            "current_exchange_tradability_only": True,
            "quality_status": (
                "warning"
                if missing_minutes
                or missing_window_minutes
                or trades_truncated
                or baseline_trade is None
                or first_cross_trade is None
                or not exact_window_pass
                else "pass"
            ),
        }
        self.db.upsert("binance_gainer_events", [event], on_conflict="scan_id,symbol,event_date")
        for row in minutes:
            row.update({"scan_id": scan_id, "event_id": event_id, "event_date": day_start.date().isoformat()})
        self.db.upsert(
            "binance_event_minute_bars",
            minutes,
            on_conflict="event_id,open_time",
        )
        if trade_spool is not None and trade_spool.exists():
            batch: list[dict[str, Any]] = []
            with trade_spool.open("r", encoding="utf-8") as handle:
                for line in handle:
                    batch.append(json.loads(line))
                    if len(batch) >= 500:
                        self.db.upsert(
                            "binance_event_agg_trades", batch, on_conflict="event_id,agg_trade_id"
                        )
                        batch.clear()
                if batch:
                    self.db.upsert(
                        "binance_event_agg_trades", batch, on_conflict="event_id,agg_trade_id"
                    )
            trade_spool.unlink(missing_ok=True)
        observations = decision_observations(day_minutes, day_start.date().isoformat())
        for observation in observations:
            observation.update({"event_id": event_id, "scan_id": scan_id, "symbol": symbol})
        self.db.upsert(
            "binance_decision_observations",
            observations,
            on_conflict="event_id,decision_label",
        )
        return {"event_id": event_id, "candidate_recorded": True, "sellability_pass": sellability_pass}
