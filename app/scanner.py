from __future__ import annotations

import uuid
from datetime import datetime, timedelta, timezone
from typing import Any

from .binance import BinanceClient
from .classifier import (
    classify_symbol,
    decision_observations,
    first_rolling_surge,
    parse_kline,
    pct_change,
)
from .supabase import SupabaseClient


def utc_ms(dt: datetime) -> int:
    return int(dt.timestamp() * 1000)


def floor_utc_day(dt: datetime) -> datetime:
    return dt.astimezone(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)


def _iso_from_ms(value: int) -> str:
    return datetime.fromtimestamp(value / 1000, tz=timezone.utc).isoformat()


class Scanner:
    def __init__(self, db: SupabaseClient, binance: BinanceClient):
        self.db = db
        self.binance = binance

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

        exchange = self.binance.exchange_info()
        snapshot_at = datetime.now(timezone.utc).isoformat()
        raw_symbols = exchange.get("symbols", [])
        candidates: list[dict[str, Any]] = []
        quote_rank = {quote: rank for rank, quote in enumerate(quote_assets)}
        for raw in raw_symbols:
            item = classify_symbol(raw)
            if item["quote_asset"] not in quote_rank:
                continue
            if item["status"] != "TRADING" or not item["spot_permission"] or not item["is_spot_trading_allowed"]:
                continue
            if "LIMIT" not in item["order_types"]:
                continue
            item["snapshot_at"] = snapshot_at
            item["scan_id"] = job_id
            item["quote_priority"] = quote_rank[item["quote_asset"]]
            candidates.append(item)

        # One canonical pair per base asset prevents duplicate coin events across
        # USDT/USDC/FDUSD. Quote order is the preference order.
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
                    "scan_id": job_id,
                    "snapshot_at": snapshot_at,
                    "symbol": s["symbol"],
                    "base_asset": s["base_asset"],
                    "quote_asset": s["quote_asset"],
                    "quote_priority": s["quote_priority"],
                    "selected_canonical": s["symbol"] in selected_symbols,
                    "status": s["status"],
                    "spot_permission": s["spot_permission"],
                    "is_spot_trading_allowed": s["is_spot_trading_allowed"],
                    "stablecoin_like": s["stablecoin_like"],
                    "leveraged_token_like": s["leveraged_token_like"],
                    "raw_json": s["raw_json"],
                }
                for s in candidates
            ],
            on_conflict="scan_id,symbol",
        )
        self.db.update("binance_scan_jobs", {"id": f"eq.{job_id}"}, {"symbols_total": len(symbols)})

        saleable_events = 0
        surge_candidates = 0
        failures = 0
        daily_rows_written = 0
        factor = 1.0 + threshold_pct / 100.0
        for index, symbol_info in enumerate(symbols, start=1):
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
                    if outcome is not None:
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
            self.db.update(
                "binance_scan_jobs",
                {"id": f"eq.{job_id}"},
                {
                    "symbols_processed": index,
                    "candidates_found": surge_candidates,
                    "events_found": saleable_events,
                    "failures": failures,
                    "daily_rows": daily_rows_written,
                    "heartbeat_at": datetime.now(timezone.utc).isoformat(),
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

        # Resolve the minute low to an executed aggregate trade. The latest trade
        # at the low gives the most conservative elapsed-time proof.
        baseline_start_ms = utc_ms(baseline_dt)
        baseline_trades, baseline_page_truncated = self.binance.aggregate_trades(
            symbol, baseline_start_ms, baseline_start_ms + 59_999
        )
        price_tolerance = max(abs(baseline_price) * 1e-12, 1e-15)
        baseline_matches = [
            trade for trade in baseline_trades
            if abs(float(trade["p"]) - baseline_price) <= price_tolerance
        ]
        baseline_trade = max(baseline_matches, key=lambda trade: int(trade["T"]), default=None)
        exact_baseline_ms = int(baseline_trade["T"]) if baseline_trade else None

        minute_start_ms = utc_ms(crossing_dt)
        crossing_minute_trades, crossing_page_truncated = self.binance.aggregate_trades(
            symbol, minute_start_ms, minute_start_ms + 59_999
        )
        first_cross_trade = next(
            (trade for trade in crossing_minute_trades if float(trade["p"]) + price_tolerance >= threshold),
            None,
        )

        # The exact crossing must be resolved inside the kline crossing minute.
        # A later recross is not substituted because the user explicitly does
        # not require the price to remain above the threshold.
        sell_page_truncated = False
        if first_cross_trade is None:
            exact_cross_ms = None
            trades: list[dict[str, Any]] = []
        else:
            exact_cross_ms = int(first_cross_trade["T"])
            sell_end = exact_cross_ms + confirmation_seconds * 1000
            trades, sell_page_truncated = self.binance.aggregate_trades(
                symbol, exact_cross_ms, sell_end
            )

        exact_elapsed_seconds = (
            (exact_cross_ms - exact_baseline_ms) / 1000
            if exact_cross_ms is not None and exact_baseline_ms is not None
            else None
        )
        exact_window_pass = (
            exact_elapsed_seconds is not None
            and 0 < exact_elapsed_seconds <= window_minutes * 60
        )

        trades_truncated = baseline_page_truncated or crossing_page_truncated or sell_page_truncated
        normalized_trades: list[dict[str, Any]] = []
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

        for trade in trades:
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

                # Compute the executable VWAP for the first min_exit quote units,
                # partially using the final aggregate trade when necessary.
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

            normalized_trades.append(
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
                }
            )

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
        self.db.upsert(
            "binance_event_agg_trades",
            normalized_trades,
            on_conflict="event_id,agg_trade_id",
        )
        observations = decision_observations(day_minutes, day_start.date().isoformat())
        for observation in observations:
            observation.update({"event_id": event_id, "scan_id": scan_id, "symbol": symbol})
        self.db.upsert(
            "binance_decision_observations",
            observations,
            on_conflict="event_id,decision_label",
        )
        return {"candidate_recorded": True, "sellability_pass": sellability_pass}
