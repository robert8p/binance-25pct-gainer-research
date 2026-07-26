from __future__ import annotations

import csv
import hashlib
import json
import logging
import math
import shutil
import sqlite3
import statistics
import zipfile
from collections import defaultdict
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable, Iterable

import pyarrow as pa
import pyarrow.parquet as pq

from app.alpaca_client import AlpacaClient, is_likely_stock_symbol
from app.config import Settings
from app.matching import MatchConfig, classify_positive_tier, compute_symbol_date_features, match_controls_for_date
from app.research import (
    BAR_SCHEMA,
    GENERIC_SCHEMA,
    MAX_FREE_SAFE_OBJECT_BYTES,
    QUOTE_SCHEMA,
    SECOND_SCHEMA,
    TRADE_SCHEMA,
    SecondAggregator,
    et_dt,
    json_text,
    normalize_bar,
    normalize_quote,
    normalize_trade,
    parse_ts,
    safe_name,
    sha256_file,
    timestamp_ns,
)
from app.supabase_store import SupabaseStore

logger = logging.getLogger(__name__)
UTC = timezone.utc


def stable_hash(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def chunks(items: list[Any], size: int) -> Iterable[list[Any]]:
    for i in range(0, len(items), max(1, size)):
        yield items[i : i + max(1, size)]


def _json_object(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return value
    if isinstance(value, str) and value.strip():
        try:
            parsed = json.loads(value)
            return parsed if isinstance(parsed, dict) else {}
        except json.JSONDecodeError:
            return {}
    return {}


def _json_list(value: Any) -> list[Any]:
    if isinstance(value, list):
        return value
    if isinstance(value, str) and value.strip():
        try:
            parsed = json.loads(value)
            return parsed if isinstance(parsed, list) else []
        except json.JSONDecodeError:
            return []
    return []


def _action_symbol(row: dict[str, Any]) -> str | None:
    for key in ("symbol", "new_symbol", "old_symbol", "ca_symbol"):
        value = row.get(key)
        if value:
            return str(value).upper()
    symbols = row.get("symbols")
    if isinstance(symbols, list) and symbols:
        return str(symbols[0]).upper()
    return None


def _action_date(row: dict[str, Any]) -> date | None:
    for key in ("process_date", "ex_date", "record_date", "payable_date", "declaration_date"):
        value = row.get(key)
        if not value:
            continue
        try:
            return date.fromisoformat(str(value)[:10])
        except ValueError:
            continue
    return None


def summarize_minute_rows(rows: list[dict[str, Any]]) -> dict[str, Any]:
    if not rows:
        return {"bar_count": 0}
    ordered = sorted(rows, key=lambda row: row["timestamp"])
    opens = [float(r["open"]) for r in ordered if float(r.get("open") or 0) > 0]
    closes = [float(r["close"]) for r in ordered if float(r.get("close") or 0) > 0]
    highs = [float(r["high"]) for r in ordered if float(r.get("high") or 0) > 0]
    lows = [float(r["low"]) for r in ordered if float(r.get("low") or 0) > 0]
    volumes = [int(r.get("volume") or 0) for r in ordered]
    trade_counts = [int(r.get("trade_count") or 0) for r in ordered]
    returns = [math.log(closes[i] / closes[i - 1]) for i in range(1, len(closes)) if closes[i] > 0 and closes[i - 1] > 0]
    max_abs_return = max((abs(v) for v in returns), default=0.0)
    session_counts: dict[str, int] = defaultdict(int)
    session_volume: dict[str, int] = defaultdict(int)
    for row in ordered:
        label = str(row.get("session") or "unknown")
        session_counts[label] += 1
        session_volume[label] += int(row.get("volume") or 0)
    first_open = opens[0] if opens else None
    last_close = closes[-1] if closes else None
    return {
        "bar_count": len(ordered),
        "first_timestamp": ordered[0]["timestamp"],
        "last_timestamp": ordered[-1]["timestamp"],
        "first_open": first_open,
        "last_close": last_close,
        "high": max(highs) if highs else None,
        "low": min(lows) if lows else None,
        "return_pct": ((last_close / first_open - 1.0) * 100.0) if first_open and last_close else None,
        "realized_vol": statistics.stdev(returns) if len(returns) >= 2 else 0.0,
        "max_abs_one_min_log_return": max_abs_return,
        "volume": sum(volumes),
        "trade_count": sum(trade_counts),
        "session_bar_counts": dict(session_counts),
        "session_volume": dict(session_volume),
    }


def summarize_second_rows(rows: list[dict[str, Any]]) -> dict[str, Any]:
    if not rows:
        return {"active_seconds": 0}
    spreads = [float(r["min_spread"]) for r in rows if r.get("min_spread") is not None and float(r["min_spread"]) >= 0]
    bid_depth = [int(r["bid_size_shares"]) for r in rows if r.get("bid_size_shares") is not None]
    ask_depth = [int(r["ask_size_shares"]) for r in rows if r.get("ask_size_shares") is not None]
    trade_counts = [int(r.get("trade_count") or 0) for r in rows]
    trade_volumes = [int(r.get("trade_volume") or 0) for r in rows]
    quote_updates = [int(r.get("quote_updates") or 0) for r in rows]
    return {
        "active_seconds": len(rows),
        "trade_seconds": sum(1 for r in rows if int(r.get("trade_count") or 0) > 0),
        "quote_seconds": sum(1 for r in rows if int(r.get("quote_updates") or 0) > 0),
        "trade_count": sum(trade_counts),
        "trade_volume": sum(trade_volumes),
        "quote_updates": sum(quote_updates),
        "max_trades_per_second": max(trade_counts, default=0),
        "max_volume_per_second": max(trade_volumes, default=0),
        "median_spread": statistics.median(spreads) if spreads else None,
        "p90_spread": _percentile(spreads, 0.90),
        "median_bid_depth_shares": statistics.median(bid_depth) if bid_depth else None,
        "median_ask_depth_shares": statistics.median(ask_depth) if ask_depth else None,
    }


def _percentile(values: list[float], q: float) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    position = (len(ordered) - 1) * q
    lower = int(math.floor(position))
    upper = int(math.ceil(position))
    if lower == upper:
        return ordered[lower]
    return ordered[lower] + (ordered[upper] - ordered[lower]) * (position - lower)


class FeatureCache:
    """Disk-backed candidate feature cache to keep the Render worker memory bounded."""

    COLUMNS = (
        "symbol", "exchange", "event_date", "prior_close", "event_high",
        "event_high_vs_prior_close_pct", "no_threshold_hit", "median_dollar_volume_10",
        "median_volume_10", "realized_vol_10", "atr_pct_10", "prior_day_return",
        "momentum_10", "listing_sessions_observed", "corporate_action_45d", "price_band",
        "log_prior_close", "log_median_dollar_volume_10", "log_listing_sessions",
        "feature_cutoff_date", "asset_id", "asset_name", "current_status", "currently_tradable",
    )

    def __init__(self, path: Path):
        path.parent.mkdir(parents=True, exist_ok=True)
        self.conn = sqlite3.connect(path)
        self.conn.execute("pragma journal_mode=WAL")
        self.conn.execute(
            """
            create table if not exists features (
              symbol text not null, exchange text, event_date text not null,
              prior_close real, event_high real, event_high_vs_prior_close_pct real,
              no_threshold_hit integer, median_dollar_volume_10 real, median_volume_10 real,
              realized_vol_10 real, atr_pct_10 real, prior_day_return real, momentum_10 real,
              listing_sessions_observed integer, corporate_action_45d integer, price_band text,
              log_prior_close real, log_median_dollar_volume_10 real, log_listing_sessions real,
              feature_cutoff_date text, asset_id text, asset_name text, current_status text,
              currently_tradable integer, primary key(symbol,event_date)
            )
            """
        )
        self.conn.execute("create index if not exists features_date_idx on features(event_date,no_threshold_hit)")
        self.conn.commit()

    def close(self) -> None:
        self.conn.close()

    def upsert(self, rows: list[dict[str, Any]]) -> None:
        if not rows:
            return
        placeholders = ",".join("?" for _ in self.COLUMNS)
        updates = ",".join(f"{c}=excluded.{c}" for c in self.COLUMNS if c not in {"symbol", "event_date"})
        sql = f"insert into features ({','.join(self.COLUMNS)}) values ({placeholders}) on conflict(symbol,event_date) do update set {updates}"
        values = []
        for row in rows:
            values.append(tuple(int(bool(row.get(c))) if c in {"no_threshold_hit", "corporate_action_45d", "currently_tradable"} else row.get(c) for c in self.COLUMNS))
        self.conn.executemany(sql, values)
        self.conn.commit()

    def get(self, symbol: str, event_date: str) -> dict[str, Any] | None:
        cursor = self.conn.execute(
            f"select {','.join(self.COLUMNS)} from features where symbol=? and event_date=?",
            (symbol, event_date),
        )
        row = cursor.fetchone()
        return self._row(row) if row else None

    def for_date(self, event_date: str, *, negatives_only: bool = True) -> list[dict[str, Any]]:
        sql = f"select {','.join(self.COLUMNS)} from features where event_date=?"
        params: tuple[Any, ...] = (event_date,)
        if negatives_only:
            sql += " and no_threshold_hit=1"
        return [self._row(row) for row in self.conn.execute(sql, params).fetchall()]

    def _row(self, row: tuple[Any, ...]) -> dict[str, Any]:
        result = dict(zip(self.COLUMNS, row))
        result["no_threshold_hit"] = bool(result["no_threshold_hit"])
        result["corporate_action_45d"] = bool(result["corporate_action_45d"])
        result["currently_tradable"] = bool(result.get("currently_tradable"))
        return result


class ControlDatasetCollector:
    def __init__(self, settings: Settings, store: SupabaseStore, alpaca: AlpacaClient, job_id: str, params: dict[str, Any]):
        self.settings = settings
        self.store = store
        self.alpaca = alpaca
        self.job_id = job_id
        self.params = params

    def _register_file(
        self,
        path: Path,
        storage_path: str,
        kind: str,
        *,
        observation_id: str | None = None,
        dataset_id: str | None = None,
        content_type: str = "application/octet-stream",
    ) -> dict[str, Any]:
        self.store.upload_file(path, storage_path, content_type=content_type)
        return self.store.upsert(
            "stock25_control_files",
            {
                "control_job_id": self.job_id,
                "control_observation_id": observation_id,
                "control_dataset_id": dataset_id,
                "file_kind": kind,
                "storage_path": storage_path,
                "filename": path.name,
                "size_bytes": path.stat().st_size,
                "sha256": sha256_file(path),
            },
            on_conflict="control_job_id,storage_path",
            return_representation=True,
        )[0]

    @staticmethod
    def _write_parquet(path: Path, rows: list[dict[str, Any]], schema: pa.Schema) -> int:
        if not rows:
            return 0
        path.parent.mkdir(parents=True, exist_ok=True)
        pq.write_table(pa.Table.from_pylist(rows, schema=schema), path, compression="zstd", use_dictionary=True)
        return len(rows)

    def _write_upload_rows_safely(
        self,
        *,
        dataset_id: str,
        storage_prefix: str,
        kind: str,
        base_name: str,
        rows: list[dict[str, Any]],
        schema: pa.Schema,
        temp_dir: Path,
        part_no: int,
    ) -> tuple[list[dict[str, Any]], int]:
        if not rows:
            return [], part_no
        path = temp_dir / f"{base_name}_part{part_no:05d}.parquet"
        self._write_parquet(path, rows, schema)
        if path.stat().st_size > MAX_FREE_SAFE_OBJECT_BYTES and len(rows) > 1:
            path.unlink(missing_ok=True)
            midpoint = len(rows) // 2
            first, next_no = self._write_upload_rows_safely(
                dataset_id=dataset_id, storage_prefix=storage_prefix, kind=kind, base_name=base_name,
                rows=rows[:midpoint], schema=schema, temp_dir=temp_dir, part_no=part_no,
            )
            second, next_no = self._write_upload_rows_safely(
                dataset_id=dataset_id, storage_prefix=storage_prefix, kind=kind, base_name=base_name,
                rows=rows[midpoint:], schema=schema, temp_dir=temp_dir, part_no=next_no,
            )
            return first + second, next_no
        storage_path = f"{storage_prefix}/raw/{kind}/{path.name}"
        row = self._register_file(path, storage_path, f"raw_{kind}", dataset_id=dataset_id, content_type="application/vnd.apache.parquet")
        path.unlink(missing_ok=True)
        return [row], part_no + 1

    def _stream_pages(
        self,
        *,
        dataset_id: str,
        storage_prefix: str,
        kind: str,
        window_name: str,
        pages: Iterable[list[dict[str, Any]]],
        normalizer: Callable[[dict[str, Any]], dict[str, Any]],
        schema: pa.Schema,
        aggregator: SecondAggregator,
        temp_dir: Path,
        exclusive_end_ns: int,
    ) -> tuple[int, list[dict[str, Any]], bool]:
        total = 0
        files: list[dict[str, Any]] = []
        truncated = False
        part_no = 1
        row_limit = self.settings.max_raw_rows_per_file
        for page in pages:
            rows = [normalizer(row) for row in page]
            rows = [row for row in rows if timestamp_ns(row["timestamp"]) < exclusive_end_ns]
            if row_limit and total + len(rows) > row_limit:
                rows = rows[: max(0, row_limit - total)]
                truncated = True
            for row in rows:
                aggregator.add_trade(row) if kind == "trades" else aggregator.add_quote(row)
            new_files, part_no = self._write_upload_rows_safely(
                dataset_id=dataset_id, storage_prefix=storage_prefix, kind=kind,
                base_name=safe_name(window_name), rows=rows, schema=schema,
                temp_dir=temp_dir, part_no=part_no,
            )
            files.extend(new_files)
            total += len(rows)
            if row_limit and total >= row_limit:
                break
        return total, files, truncated

    def ensure_dataset(
        self,
        *,
        symbol: str,
        session_date: date,
        window_type: str,
        start: datetime,
        end: datetime,
        end_raw: str,
        feed: str,
        asof: date,
    ) -> dict[str, Any]:
        window_key = stable_hash(f"{window_type}|{end_raw}")[:20]
        filters = {
            "control_job_id": f"eq.{self.job_id}",
            "symbol": f"eq.{symbol}",
            "session_date": f"eq.{session_date.isoformat()}",
            "window_type": f"eq.{window_type}",
            "window_key": f"eq.{window_key}",
            "feed": f"eq.{feed}",
        }
        existing = self.store.select("stock25_control_datasets", filters=filters, limit=1)
        if existing and existing[0].get("status") == "completed":
            return existing[0]
        storage_prefix = f"control_jobs/{self.job_id}/datasets/{safe_name(symbol)}/{session_date.isoformat()}/{window_type}_{window_key}"
        payload = {
            "control_job_id": self.job_id,
            "symbol": symbol,
            "session_date": session_date.isoformat(),
            "window_type": window_type,
            "window_start": start.isoformat(),
            "window_end": end.isoformat(),
            "window_end_raw": end_raw,
            "window_key": window_key,
            "feed": feed,
            "status": "collecting",
            "storage_prefix": storage_prefix,
            "error_message": None,
            "completed_at": None,
        }
        dataset = self.store.upsert(
            "stock25_control_datasets", payload,
            on_conflict="control_job_id,symbol,session_date,window_type,window_key,feed",
            return_representation=True,
        )[0]
        dataset_id = dataset["id"]
        if existing:
            self.store.delete("stock25_control_files", {"control_dataset_id": f"eq.{dataset_id}"})
        temp_dir = Path(self.settings.temp_root) / self.job_id / "datasets" / dataset_id
        shutil.rmtree(temp_dir, ignore_errors=True)
        temp_dir.mkdir(parents=True, exist_ok=True)
        counts = {"minute_bars": 0, "trades": 0, "quotes": 0, "second_rows": 0, "auctions": 0, "news": 0}
        flags: list[str] = []
        minute_rows: list[dict[str, Any]] = []
        files: list[dict[str, Any]] = []
        exclusive_end_ns = timestamp_ns(end_raw)
        try:
            raw_bars = self.alpaca.get_single_bars(
                symbol, timeframe="1Min", start=start, end=end, feed=feed,
                adjustment="raw", asof=asof,
            )
            for bar in raw_bars:
                bar_start = parse_ts(bar["t"])
                if timestamp_ns(bar["t"]) >= exclusive_end_ns:
                    continue
                if window_type == "prefix" and timestamp_ns(bar["t"]) + 60_000_000_000 > exclusive_end_ns:
                    continue
                minute_rows.append(normalize_bar(bar, symbol, "1Min", "raw", session_date.isoformat()))
            minute_path = temp_dir / "minute_bars.parquet"
            counts["minute_bars"] = self._write_parquet(minute_path, minute_rows, BAR_SCHEMA)
            if minute_rows:
                files.append(self._register_file(
                    minute_path, f"{storage_prefix}/derived/{minute_path.name}", "minute_bars",
                    dataset_id=dataset_id, content_type="application/vnd.apache.parquet",
                ))

            aggregator = SecondAggregator(session_date.isoformat() if window_type == "full_session" else f"{session_date.isoformat()}_to_cutoff")
            if self.params.get("include_raw_trades", True):
                n, new_files, truncated = self._stream_pages(
                    dataset_id=dataset_id, storage_prefix=storage_prefix, kind="trades",
                    window_name=session_date.isoformat(),
                    pages=self.alpaca.iter_trades(symbol, start=start, end=end, feed=feed, asof=asof),
                    normalizer=lambda row: normalize_trade(row, symbol, session_date.isoformat()),
                    schema=TRADE_SCHEMA, aggregator=aggregator, temp_dir=temp_dir,
                    exclusive_end_ns=exclusive_end_ns,
                )
                counts["trades"] = n
                files.extend(new_files)
                if truncated:
                    flags.append("trades_truncated")
            if self.params.get("include_raw_quotes", True):
                n, new_files, truncated = self._stream_pages(
                    dataset_id=dataset_id, storage_prefix=storage_prefix, kind="quotes",
                    window_name=session_date.isoformat(),
                    pages=self.alpaca.iter_quotes(symbol, start=start, end=end, feed=feed, asof=asof),
                    normalizer=lambda row: normalize_quote(row, symbol, session_date.isoformat(), session_date),
                    schema=QUOTE_SCHEMA, aggregator=aggregator, temp_dir=temp_dir,
                    exclusive_end_ns=exclusive_end_ns,
                )
                counts["quotes"] = n
                files.extend(new_files)
                if truncated:
                    flags.append("quotes_truncated")

            second_rows = aggregator.rows()
            if self.params.get("derive_one_second", True) and second_rows:
                second_path = temp_dir / "second_summary.parquet"
                counts["second_rows"] = self._write_parquet(second_path, second_rows, SECOND_SCHEMA)
                files.append(self._register_file(
                    second_path, f"{storage_prefix}/derived/{second_path.name}", "second_summary",
                    dataset_id=dataset_id, content_type="application/vnd.apache.parquet",
                ))

            if self.params.get("include_auctions", True):
                try:
                    auction_rows: list[dict[str, Any]] = []
                    for page in self.alpaca.iter_auctions(symbol, start=start, end=end, asof=asof):
                        for raw in page:
                            ts = str(raw.get("t") or raw.get("timestamp") or "")
                            if ts and timestamp_ns(ts) < exclusive_end_ns:
                                auction_rows.append({"symbol": symbol, "timestamp": ts, "kind": "auction", "raw_json": json_text(raw)})
                    auction_path = temp_dir / "auctions.parquet"
                    counts["auctions"] = self._write_parquet(auction_path, auction_rows, GENERIC_SCHEMA)
                    if auction_rows:
                        files.append(self._register_file(
                            auction_path, f"{storage_prefix}/derived/{auction_path.name}", "auctions",
                            dataset_id=dataset_id, content_type="application/vnd.apache.parquet",
                        ))
                    else:
                        flags.append("auctions_no_records_returned")
                except Exception as exc:
                    flags.append(f"auctions_unavailable:{type(exc).__name__}")

            if self.params.get("include_news", True):
                try:
                    news = self.alpaca.get_news(symbol, start=start, end=end, include_content=True)
                    news_rows = []
                    for item in news:
                        ts = str(item.get("created_at") or item.get("updated_at") or "")
                        if ts and timestamp_ns(ts) < exclusive_end_ns:
                            news_rows.append({"symbol": symbol, "timestamp": ts, "kind": "news", "raw_json": json_text(item)})
                    news_path = temp_dir / "news.parquet"
                    counts["news"] = self._write_parquet(news_path, news_rows, GENERIC_SCHEMA)
                    if news_rows:
                        files.append(self._register_file(
                            news_path, f"{storage_prefix}/derived/{news_path.name}", "news",
                            dataset_id=dataset_id, content_type="application/vnd.apache.parquet",
                        ))
                except Exception as exc:
                    flags.append(f"news_unavailable:{type(exc).__name__}")

            derived = {
                "minute": summarize_minute_rows(minute_rows),
                "second": summarize_second_rows(second_rows),
                "auction_coverage": {
                    "requested": bool(self.params.get("include_auctions", True)),
                    "records_returned": int(counts.get("auctions", 0)),
                    "status": (
                        "not_requested" if not self.params.get("include_auctions", True)
                        else "records_returned" if int(counts.get("auctions", 0)) > 0
                        else "no_records_returned" if "auctions_no_records_returned" in flags
                        else "unavailable"
                    ),
                },
                "leakage_boundary_raw": end_raw if window_type == "prefix" else None,
            }
            self.store.update(
                "stock25_control_datasets", {"id": f"eq.{dataset_id}"},
                {
                    "status": "completed", "row_counts": counts,
                    "derived_features": derived, "quality_flags": sorted(set(flags)),
                    "completed_at": datetime.now(UTC).isoformat(), "error_message": None,
                },
            )
            return self.store.select("stock25_control_datasets", filters={"id": f"eq.{dataset_id}"}, limit=1)[0]
        except Exception as exc:
            self.store.update(
                "stock25_control_datasets", {"id": f"eq.{dataset_id}"},
                {"status": "failed", "error_message": f"{type(exc).__name__}: {exc}"[:4000], "quality_flags": sorted(set(flags))},
            )
            raise
        finally:
            shutil.rmtree(temp_dir, ignore_errors=True)


class ControlRunner:
    def __init__(self, settings: Settings, store: SupabaseStore, alpaca: AlpacaClient):
        self.settings = settings
        self.store = store
        self.alpaca = alpaca

    def _update(self, job_id: str, stage: str, current: int, total: int, **extra: Any) -> None:
        self.store.update_control_job(
            job_id, progress_stage=stage, progress_current=current, progress_total=total, **extra
        )

    def _load_source(self, job: dict[str, Any], max_positive_events: int) -> tuple[dict[str, Any], list[dict[str, Any]], dict[str, dict[str, Any]]]:
        research_jobs = self.store.select(
            "stock25_research_jobs", filters={"id": f"eq.{job['source_research_job_id']}"}, limit=1
        )
        if not research_jobs or research_jobs[0].get("status") != "completed":
            raise RuntimeError("The source research job must exist and be completed")
        source_job = research_jobs[0]
        positives = self.store.select_all(
            "stock25_research_events",
            filters={
                "research_job_id": f"eq.{source_job['id']}",
                "eligible": "eq.true",
                "status": "eq.completed",
            },
            order="event_date.asc,symbol.asc",
        )
        if max_positive_events:
            positives = positives[:max_positive_events]
        results = self.store.select_all(
            "stock25_scan_results",
            filters={"scan_id": f"eq.{source_job['source_scan_id']}"},
            order="event_date.asc,symbol.asc",
        )
        return source_job, positives, {row["id"]: row for row in results}

    def _corporate_action_map(self, symbols: list[str], start: date, end: date, job_id: str) -> tuple[dict[str, set[date]], list[str]]:
        mapping: dict[str, set[date]] = defaultdict(set)
        flags: list[str] = []
        try:
            batches = list(chunks(symbols, self.settings.control_asset_batch_size))
            for idx, batch in enumerate(batches, start=1):
                rows = self.alpaca.get_corporate_actions_multi(batch, start=start, end=end)  # type: ignore[attr-defined]
                for row in rows:
                    symbol = _action_symbol(row)
                    action_day = _action_date(row)
                    if symbol and action_day:
                        mapping[symbol].add(action_day)
                self._update(job_id, "corporate_actions", idx, len(batches))
        except Exception as exc:
            logger.warning("Corporate-action matching unavailable: %s", exc)
            flags.append(f"corporate_action_matching_unavailable:{type(exc).__name__}")
        return mapping, flags

    def _build_feature_cache(
        self,
        *,
        job_id: str,
        assets: list[dict[str, Any]],
        event_dates: set[date],
        threshold_pct: float,
        feature_sessions: int,
        history_calendar_days: int,
        feed: str,
        action_map: dict[str, set[date]],
        cache: FeatureCache,
    ) -> None:
        symbols = sorted({str(a.get("symbol")) for a in assets if a.get("symbol")})
        exchange_by_symbol = {str(a.get("symbol")): str(a.get("exchange") or "UNKNOWN") for a in assets if a.get("symbol")}
        asset_by_symbol = {str(a.get("symbol")): a for a in assets if a.get("symbol")}
        min_date, max_date = min(event_dates), max(event_dates)
        start = et_dt(min_date - timedelta(days=history_calendar_days), 0)
        end = et_dt(max_date + timedelta(days=1), 0)
        batches = list(chunks(symbols, self.settings.control_asset_batch_size))
        for idx, batch in enumerate(batches, start=1):
            raw = self.alpaca.get_bars(
                batch, timeframe="1Day", start=start, end=end, feed=feed,
                adjustment="split", asof=max_date,
            )
            output: list[dict[str, Any]] = []
            for symbol in batch:
                symbol_features = compute_symbol_date_features(
                    symbol,
                    exchange_by_symbol.get(symbol),
                    raw.get(symbol) or [],
                    event_dates,
                    threshold_pct=threshold_pct,
                    feature_sessions=feature_sessions,
                    corporate_action_dates=action_map.get(symbol, set()),
                )
                asset = asset_by_symbol.get(symbol, {})
                for feature in symbol_features:
                    feature.update({
                        "asset_id": asset.get("id"),
                        "asset_name": asset.get("name"),
                        "current_status": asset.get("status"),
                        "currently_tradable": bool(asset.get("tradable")),
                    })
                output.extend(symbol_features)
            cache.upsert(output)
            self._update(job_id, "daily_feature_cache", idx, len(batches))

    def _write_pre_download_balance_report(
        self,
        *,
        job_id: str,
        positives: list[dict[str, Any]],
        pairs: list[dict[str, Any]],
        diagnostics: list[dict[str, Any]],
        cfg: MatchConfig,
    ) -> dict[str, Any]:
        """Persist and upload the balance gate before any detailed control data is downloaded."""
        root = Path(self.settings.temp_root) / job_id / "balance_report"
        shutil.rmtree(root, ignore_errors=True)
        root.mkdir(parents=True, exist_ok=True)

        pair_rows: list[dict[str, Any]] = []
        for pair in pairs:
            deltas = _json_object(pair.get("standardized_deltas"))
            positive_features = _json_object(pair.get("positive_features"))
            control_features = _json_object(pair.get("control_features"))
            positive_price = max(float(positive_features.get("prior_close") or 0), 1e-9)
            control_price = max(float(control_features.get("prior_close") or 0), 1e-9)
            positive_dv = max(float(positive_features.get("median_dollar_volume_10") or 0), 1.0)
            control_dv = max(float(control_features.get("median_dollar_volume_10") or 0), 1.0)
            positive_vol = max(float(positive_features.get("realized_vol_10") or 0), 1e-5)
            control_vol = max(float(control_features.get("realized_vol_10") or 0), 1e-5)
            bands = ("lt_0_5", "0_5_1", "1_2", "2_5", "5_10", "10_20", "20_50", "gte_50")
            band_index = {name: idx for idx, name in enumerate(bands)}
            positive_band = str(positive_features.get("price_band") or "")
            control_band = str(control_features.get("price_band") or "")
            pair_rows.append({
                "positive_research_event_id": pair.get("positive_research_event_id"),
                "event_date": pair.get("event_date"),
                "positive_symbol": pair.get("positive_symbol"),
                "control_symbol": pair.get("control_symbol"),
                "positive_exchange": positive_features.get("exchange"),
                "control_exchange": pair.get("control_exchange"),
                "exchange_mismatch": int(bool(deltas.get("exchange_mismatch"))),
                "corporate_action_mismatch": int(bool(deltas.get("corporate_action_mismatch"))),
                "positive_price_band": positive_band,
                "control_price_band": control_band,
                "price_band_distance": abs(band_index.get(positive_band, 999) - band_index.get(control_band, 999)),
                "price_ratio": control_price / positive_price,
                "dollar_volume_ratio": control_dv / positive_dv,
                "volatility_ratio": control_vol / positive_vol,
                "match_score": pair.get("match_score"),
                "match_quality": pair.get("match_quality"),
                **{f"z_{key}": deltas.get(key) for key in (
                    "log_prior_close", "log_median_dollar_volume_10", "realized_vol_10",
                    "atr_pct_10", "prior_day_return", "momentum_10", "log_listing_sessions",
                )},
            })

        shortfall_rows: list[dict[str, Any]] = []
        for item in diagnostics:
            event = _json_object(item.get("event"))
            shortfall_rows.append({
                "positive_research_event_id": event.get("research_event_id") or event.get("id"),
                "positive_symbol": event.get("symbol"),
                "event_date": event.get("event_date"),
                "reason": item.get("reason"),
                "selected_count": item.get("selected_count", 0),
                "requested_count": item.get("requested_count", cfg.controls_per_event),
                "candidate_count": item.get("candidate_count", 0),
                "accepted_candidate_count": item.get("accepted_candidate_count", 0),
                "rejection_counts_json": json_text(item.get("rejection_counts") or {}),
                "nearest_rejected_json": json_text(item.get("nearest_rejected") or []),
            })

        def write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
            fields = sorted({key for row in rows for key in row}) if rows else ["empty"]
            with path.open("w", newline="", encoding="utf-8") as fh:
                writer = csv.DictWriter(fh, fieldnames=fields)
                writer.writeheader()
                writer.writerows(rows)

        qualities = [str(row.get("match_quality")) for row in pairs]
        scores = [float(row.get("match_score") or 0) for row in pairs]
        abs_delta_max: dict[str, float] = {}
        for key in (
            "log_prior_close", "log_median_dollar_volume_10", "realized_vol_10",
            "atr_pct_10", "prior_day_return", "momentum_10", "log_listing_sessions",
        ):
            values = [abs(float(_json_object(row.get("standardized_deltas")).get(key) or 0)) for row in pairs]
            abs_delta_max[key] = max(values, default=0.0)

        gate_violations: list[str] = []
        if not pairs:
            gate_violations.append("no_strong_controls_selected")
        if any(quality not in {"excellent", "good"} for quality in qualities):
            gate_violations.append("weak_control_selected")
        if cfg.require_corporate_action_match and any(
            bool(_json_object(row.get("standardized_deltas")).get("corporate_action_mismatch")) for row in pairs
        ):
            gate_violations.append("corporate_action_mismatch_selected")
        if scores and max(scores) > cfg.max_match_score + 1e-9:
            gate_violations.append("match_score_gate_breached")
        z_limits = {
            "log_prior_close": cfg.max_abs_log_prior_close_z,
            "log_median_dollar_volume_10": cfg.max_abs_log_median_dollar_volume_z,
            "realized_vol_10": cfg.max_abs_realized_vol_z,
            "atr_pct_10": cfg.max_abs_atr_pct_z,
            "prior_day_return": cfg.max_abs_prior_day_return_z,
            "momentum_10": cfg.max_abs_momentum_z,
            "log_listing_sessions": cfg.max_abs_log_listing_sessions_z,
        }
        for key, limit in z_limits.items():
            if abs_delta_max.get(key, 0.0) > limit + 1e-9:
                gate_violations.append(f"{key}_balance_gate_breached")
        if any(int(row.get("price_band_distance") or 0) > cfg.max_price_band_distance for row in pair_rows):
            gate_violations.append("price_band_gate_breached")
        if any(not cfg.price_ratio_min <= float(row.get("price_ratio") or 0) <= cfg.price_ratio_max for row in pair_rows):
            gate_violations.append("price_ratio_gate_breached")
        if any(not cfg.dollar_volume_ratio_min <= float(row.get("dollar_volume_ratio") or 0) <= cfg.dollar_volume_ratio_max for row in pair_rows):
            gate_violations.append("dollar_volume_gate_breached")
        if any(not cfg.volatility_ratio_min <= float(row.get("volatility_ratio") or 0) <= cfg.volatility_ratio_max for row in pair_rows):
            gate_violations.append("volatility_ratio_gate_breached")
        gate_violations = sorted(set(gate_violations))

        rejection_totals: dict[str, int] = defaultdict(int)
        for item in diagnostics:
            for reason, count in _json_object(item.get("rejection_counts")).items():
                rejection_totals[str(reason)] += int(count or 0)

        report = {
            "version": "3.0.2",
            "matching_version": "strict_global_v3.0.2",
            "created_at": datetime.now(UTC).isoformat(),
            "control_job_id": job_id,
            "gate_status": "failed" if gate_violations else "passed",
            "gate_violations": gate_violations,
            "selected_positive_event_count": len(positives),
            "requested_controls_per_event": cfg.controls_per_event,
            "matched_pair_count": len(pairs),
            "excellent_pair_count": qualities.count("excellent"),
            "good_pair_count": qualities.count("good"),
            "weak_pair_count": sum(1 for q in qualities if q not in {"excellent", "good"}),
            "positive_events_with_shortfall": len(diagnostics),
            "maximum_match_score": max(scores, default=None),
            "median_match_score": statistics.median(scores) if scores else None,
            "maximum_absolute_standardized_deltas": abs_delta_max,
            "exchange_mismatch_count": sum(int(bool(_json_object(row.get("standardized_deltas")).get("exchange_mismatch"))) for row in pairs),
            "corporate_action_mismatch_count": sum(int(bool(_json_object(row.get("standardized_deltas")).get("corporate_action_mismatch"))) for row in pairs),
            "rejection_reason_totals": dict(sorted(rejection_totals.items())),
            "hard_balance_settings": {
                "max_match_score": cfg.max_match_score,
                "max_price_band_distance": cfg.max_price_band_distance,
                "require_corporate_action_match": cfg.require_corporate_action_match,
                "price_ratio": [cfg.price_ratio_min, cfg.price_ratio_max],
                "dollar_volume_ratio": [cfg.dollar_volume_ratio_min, cfg.dollar_volume_ratio_max],
                "volatility_ratio": [cfg.volatility_ratio_min, cfg.volatility_ratio_max],
                "max_abs_standardized_deltas": {
                    "log_prior_close": cfg.max_abs_log_prior_close_z,
                    "log_median_dollar_volume_10": cfg.max_abs_log_median_dollar_volume_z,
                    "realized_vol_10": cfg.max_abs_realized_vol_z,
                    "atr_pct_10": cfg.max_abs_atr_pct_z,
                    "prior_day_return": cfg.max_abs_prior_day_return_z,
                    "momentum_10": cfg.max_abs_momentum_z,
                    "log_listing_sessions": cfg.max_abs_log_listing_sessions_z,
                },
            },
            "download_gate_rule": "Detailed control trades and quotes start only after this report passes. The matcher may return 1-5 strong controls and never packages weak controls.",
        }

        json_path = root / "pre_download_balance_report.json"
        pair_path = root / "pre_download_balance_pairs.csv"
        shortfall_path = root / "control_match_shortfalls.csv"
        json_path.write_text(json.dumps(report, indent=2, default=str), encoding="utf-8")
        write_csv(pair_path, pair_rows)
        write_csv(shortfall_path, shortfall_rows)

        collector = ControlDatasetCollector(self.settings, self.store, self.alpaca, job_id, {})
        uploaded = []
        for path, kind, content_type in (
            (json_path, "pre_download_balance_report", "application/json"),
            (pair_path, "pre_download_balance_pairs", "text/csv"),
            (shortfall_path, "control_match_shortfalls", "text/csv"),
        ):
            uploaded.append(collector._register_file(
                path, f"control_jobs/{job_id}/matching/{path.name}", kind, content_type=content_type
            ))
        report["uploaded_files"] = [{"id": row.get("id"), "storage_path": row.get("storage_path")} for row in uploaded]
        self.store.update_control_job(
            job_id,
            matching_version="strict_global_v3.0.2",
            balance_gate_status=report["gate_status"],
            balance_report_storage_path=f"control_jobs/{job_id}/matching/{json_path.name}",
            excellent_pair_count=report["excellent_pair_count"],
            good_pair_count=report["good_pair_count"],
            strong_pair_count=len(pairs),
        )
        return report

    def _create_matches(
        self,
        *,
        job: dict[str, Any],
        source_job: dict[str, Any],
        positives: list[dict[str, Any]],
        results_by_id: dict[str, dict[str, Any]],
        params: dict[str, Any],
    ) -> tuple[int, int]:
        job_id = job["id"]
        event_dates = {date.fromisoformat(row["event_date"]) for row in positives}
        if not event_dates:
            raise RuntimeError("The source research job contains no eligible completed events")
        source_scan = self.store.select("stock25_scans", filters={"id": f"eq.{source_job['source_scan_id']}"}, limit=1)[0]
        scan_params = _json_object(source_scan.get("parameters"))
        threshold_pct = float(scan_params.get("threshold_pct", self.settings.default_threshold_pct))
        feed = str(params.get("feed") or scan_params.get("feed") or self.settings.default_feed)

        raw_assets = [a for a in self.alpaca.get_assets(all_statuses=True) if a.get("class") == "us_equity"]
        assets = [a for a in raw_assets if is_likely_stock_symbol(a.get("symbol"))]
        skipped_identifiers = len(raw_assets) - len(assets)
        if skipped_identifiers:
            logger.warning(
                "Skipped %s CUSIP-like or malformed entries from Alpaca's all-status asset master",
                skipped_identifiers,
            )
        if not self.settings.include_otc:
            assets = [a for a in assets if a.get("exchange") != "OTC"]
        asset_symbols = sorted({str(a.get("symbol")) for a in assets if a.get("symbol")})
        # Ensure every historical positive symbol is considered even if today's asset list omits it.
        known = set(asset_symbols)
        for positive in positives:
            if positive["symbol"] not in known:
                result = results_by_id.get(positive["source_result_id"], {})
                assets.append({"symbol": positive["symbol"], "exchange": result.get("exchange") or "UNKNOWN", "class": "us_equity"})
                known.add(positive["symbol"])
        self.store.save_asset_snapshot(datetime.now().date().isoformat(), [a for a in assets if a.get("id")])

        action_map, action_flags = self._corporate_action_map(
            sorted(known), min(event_dates) - timedelta(days=45), max(event_dates), job_id
        )
        root = Path(self.settings.temp_root) / job_id
        root.mkdir(parents=True, exist_ok=True)
        cache = FeatureCache(root / "matching_features.sqlite")
        try:
            self._build_feature_cache(
                job_id=job_id, assets=assets, event_dates=event_dates,
                threshold_pct=threshold_pct,
                feature_sessions=int(params["feature_sessions"]),
                history_calendar_days=int(params["history_calendar_days"]),
                feed=feed, action_map=action_map, cache=cache,
            )
            events_by_date: dict[str, list[dict[str, Any]]] = defaultdict(list)
            missing_feature_diagnostics: list[dict[str, Any]] = []
            for positive in positives:
                result = results_by_id.get(positive["source_result_id"])
                feature = cache.get(positive["symbol"], positive["event_date"])
                if not result or not feature:
                    missing_feature_diagnostics.append({
                        "event": {
                            **positive,
                            "research_event_id": positive.get("id"),
                        },
                        "reason": "positive_missing_required_matching_history",
                        "selected_count": 0,
                        "requested_count": int(params["controls_per_event"]),
                        "candidate_count": 0,
                        "accepted_candidate_count": 0,
                        "rejection_counts": {"positive_missing_required_matching_history": 1},
                        "nearest_rejected": [],
                        "matching_version": "3.0.2",
                    })
                    continue
                row_counts = _json_object(positive.get("row_counts"))
                feature["corporate_action_45d"] = bool(row_counts.get("corporate_actions", 0))
                event = {
                    **positive,
                    **feature,
                    "research_event_id": positive["id"],
                    "exchange": result.get("exchange") or feature.get("exchange"),
                    "positive_tier": classify_positive_tier(positive),
                }
                events_by_date[positive["event_date"]].append(event)

            cfg = MatchConfig(
                controls_per_event=int(params["controls_per_event"]),
                max_control_symbol_uses=int(params["max_control_symbol_uses"]),
                exact_exchange_first=False,
                allow_exchange_fallback=True,
                require_corporate_action_match=bool(params.get("require_corporate_action_match", True)),
                max_match_score=float(params.get("max_match_score", 4.0)),
                max_abs_log_prior_close_z=float(params.get("max_abs_log_prior_close_z", 1.5)),
                max_abs_log_median_dollar_volume_z=float(params.get("max_abs_log_median_dollar_volume_z", 1.5)),
                max_abs_realized_vol_z=float(params.get("max_abs_realized_vol_z", 2.0)),
                max_abs_atr_pct_z=float(params.get("max_abs_atr_pct_z", 2.0)),
                max_abs_prior_day_return_z=float(params.get("max_abs_prior_day_return_z", 2.0)),
                max_abs_momentum_z=float(params.get("max_abs_momentum_z", 2.0)),
                max_abs_log_listing_sessions_z=float(params.get("max_abs_log_listing_sessions_z", 2.0)),
            )
            symbol_uses: dict[str, int] = {}
            all_pairs: list[dict[str, Any]] = []
            unmatched: list[dict[str, Any]] = list(missing_feature_diagnostics)
            dates = sorted(events_by_date)
            for idx, event_date_text in enumerate(dates, start=1):
                pairs, misses = match_controls_for_date(
                    events_by_date[event_date_text], cache.for_date(event_date_text),
                    cfg=cfg, global_symbol_uses=symbol_uses,
                )
                all_pairs.extend(pairs)
                unmatched.extend(misses)
                self._update(job_id, "matching", idx, len(dates))

            for item in unmatched:
                event = _json_object(item.get("event"))
                positive_id = event.get("research_event_id") or event.get("id")
                if not positive_id:
                    continue
                self.store.upsert(
                    "stock25_control_match_diagnostics",
                    {
                        "control_job_id": job_id,
                        "positive_research_event_id": positive_id,
                        "positive_symbol": event.get("symbol"),
                        "event_date": event.get("event_date"),
                        "requested_control_count": int(item.get("requested_count") or cfg.controls_per_event),
                        "selected_control_count": int(item.get("selected_count") or 0),
                        "reason": item.get("reason"),
                        "candidate_count": int(item.get("candidate_count") or 0),
                        "accepted_candidate_count": int(item.get("accepted_candidate_count") or 0),
                        "rejection_counts": item.get("rejection_counts") or {},
                        "nearest_rejected": item.get("nearest_rejected") or [],
                        "matching_version": "strict_global_v3.0.2",
                    },
                    on_conflict="control_job_id,positive_research_event_id",
                )

            balance_report = self._write_pre_download_balance_report(
                job_id=job_id, positives=positives, pairs=all_pairs, diagnostics=unmatched, cfg=cfg
            )
            if balance_report["gate_status"] != "passed":
                raise RuntimeError(
                    "Pre-download balance gate failed: " + ", ".join(balance_report["gate_violations"])
                )

            for pair in all_pairs:
                pseudo_raw = str(pair["pseudo_event_timestamp_raw"])
                pseudo_key = stable_hash(pseudo_raw)[:20]
                control_features = pair["control_features"]
                control_quality_flags = list(action_flags) + [
                    "point_in_time_tradability_unverified",
                    "matched_by_strict_global_v3.0.2",
                ]
                if not bool(control_features.get("currently_tradable")):
                    control_quality_flags.append("currently_not_tradable_control")
                observation = self.store.upsert(
                    "stock25_control_observations",
                    {
                        "control_job_id": job_id,
                        "symbol": pair["control_symbol"],
                        "exchange": pair.get("control_exchange"),
                        "event_date": pair["event_date"],
                        "pseudo_event_timestamp": pair["pseudo_event_timestamp"],
                        "pseudo_event_timestamp_raw": pseudo_raw,
                        "pseudo_event_key": pseudo_key,
                        "status": "matched",
                        "feature_snapshot": control_features,
                        "quality_flags": sorted(set(control_quality_flags)),
                    },
                    on_conflict="control_job_id,symbol,event_date,pseudo_event_key",
                    return_representation=True,
                )[0]
                pair_row = {
                    "control_job_id": job_id,
                    "positive_research_event_id": pair["positive_research_event_id"],
                    "positive_source_result_id": pair["positive_source_result_id"],
                    "control_observation_id": observation["id"],
                    "positive_symbol": pair["positive_symbol"],
                    "event_date": pair["event_date"],
                    "positive_tier": pair["positive_tier"],
                    "control_symbol": pair["control_symbol"],
                    "control_exchange": pair.get("control_exchange"),
                    "control_rank": pair["control_rank"],
                    "match_score": pair["match_score"],
                    "match_quality": pair["match_quality"],
                    "matching_version": "strict_global_v3.0.2",
                    "pseudo_event_timestamp": pair["pseudo_event_timestamp"],
                    "pseudo_event_timestamp_raw": pseudo_raw,
                    "positive_features": pair["positive_features"],
                    "control_features": control_features,
                    "standardized_deltas": pair["standardized_deltas"],
                    "status": "matched",
                    "quality_flags": sorted(set(control_quality_flags)),
                }
                self.store.upsert(
                    "stock25_control_pairs", pair_row,
                    on_conflict="control_job_id,positive_research_event_id,control_rank",
                )
            unmatched_count = len({
                str(_json_object(item.get("event")).get("research_event_id") or _json_object(item.get("event")).get("id"))
                for item in unmatched
            })
            return len(all_pairs), unmatched_count
        finally:
            cache.close()
            try:
                (root / "matching_features.sqlite").unlink(missing_ok=True)
                (root / "matching_features.sqlite-wal").unlink(missing_ok=True)
                (root / "matching_features.sqlite-shm").unlink(missing_ok=True)
            except OSError:
                pass

    def _prior_sessions(self, event_date: date, count: int) -> list[date]:
        calendar = self.alpaca.get_calendar(event_date - timedelta(days=count * 3 + 15), event_date)
        days = sorted(date.fromisoformat(row["date"]) for row in calendar if date.fromisoformat(row["date"]) < event_date)
        return days[-count:]

    def _collect_observation(self, job_id: str, observation: dict[str, Any], params: dict[str, Any]) -> None:
        observation_id = observation["id"]
        symbol = observation["symbol"]
        event_date = date.fromisoformat(observation["event_date"])
        pseudo_raw = observation["pseudo_event_timestamp_raw"]
        cutoff = parse_ts(pseudo_raw)
        feed = str(params["feed"])
        prior_sessions = self._prior_sessions(event_date, int(params["prior_sessions"]))
        collector = ControlDatasetCollector(self.settings, self.store, self.alpaca, job_id, params)
        dataset_rows: list[dict[str, Any]] = []
        row_counts: dict[str, int] = defaultdict(int)
        flags = list(_json_list(observation.get("quality_flags")))
        self.store.update(
            "stock25_control_observations", {"id": f"eq.{observation_id}"},
            {"status": "collecting", "prior_sessions": [d.isoformat() for d in prior_sessions], "error_message": None},
        )
        self.store.update("stock25_control_pairs", {"control_observation_id": f"eq.{observation_id}"}, {"status": "collecting"})
        try:
            for session_date in prior_sessions:
                dataset = collector.ensure_dataset(
                    symbol=symbol, session_date=session_date, window_type="full_session",
                    start=et_dt(session_date, 4), end=et_dt(session_date, 20),
                    end_raw=et_dt(session_date, 20).isoformat(), feed=feed, asof=event_date,
                )
                dataset_rows.append(dataset)
            prefix = collector.ensure_dataset(
                symbol=symbol, session_date=event_date, window_type="prefix",
                start=et_dt(event_date, 4), end=cutoff + timedelta(microseconds=1),
                end_raw=pseudo_raw, feed=feed, asof=event_date,
            )
            dataset_rows.append(prefix)
            for dataset in dataset_rows:
                for key, value in _json_object(dataset.get("row_counts")).items():
                    row_counts[key] += int(value or 0)
                flags.extend(_json_list(dataset.get("quality_flags")))

            root = Path(self.settings.temp_root) / job_id / "observations" / observation_id
            shutil.rmtree(root, ignore_errors=True)
            root.mkdir(parents=True, exist_ok=True)
            daily_rows: list[dict[str, Any]] = []
            daily_start = et_dt((prior_sessions[0] if prior_sessions else event_date - timedelta(days=30)) - timedelta(days=5), 0)
            daily_end = et_dt(event_date, 0)
            for adjustment in ("raw", "split"):
                for raw in self.alpaca.get_single_bars(
                    symbol, timeframe="1Day", start=daily_start, end=daily_end,
                    feed=feed, adjustment=adjustment, asof=event_date,
                ):
                    if parse_ts(raw["t"]) < daily_end:
                        daily_rows.append(normalize_bar(raw, symbol, "1Day", adjustment, "prior_context"))
            daily_path = root / "daily_bars.parquet"
            ControlDatasetCollector._write_parquet(daily_path, daily_rows, BAR_SCHEMA)

            action_rows: list[dict[str, Any]] = []
            if params.get("include_corporate_actions", True):
                try:
                    actions = self.alpaca.get_corporate_actions(symbol, start=event_date - timedelta(days=45), end=event_date)
                    for action in actions:
                        action_rows.append({
                            "symbol": symbol,
                            "timestamp": str(action.get("process_date") or action.get("ex_date") or ""),
                            "kind": "corporate_action",
                            "raw_json": json_text(action),
                        })
                    if action_rows:
                        flags.append("corporate_actions_point_in_time_publication_unverified")
                except Exception as exc:
                    flags.append(f"corporate_actions_unavailable:{type(exc).__name__}")
            action_path = root / "corporate_actions.parquet"
            ControlDatasetCollector._write_parquet(action_path, action_rows, GENERIC_SCHEMA)

            dataset_manifest = []
            for dataset in dataset_rows:
                files = self.store.select_all(
                    "stock25_control_files", filters={"control_dataset_id": f"eq.{dataset['id']}"},
                    order="file_kind.asc,storage_path.asc",
                )
                dataset_manifest.append({"dataset": dataset, "files": files})
            metadata = {
                "control_job_id": job_id,
                "control_observation_id": observation_id,
                "label": 0,
                "symbol": symbol,
                "event_date": event_date.isoformat(),
                "pseudo_event_timestamp": observation["pseudo_event_timestamp"],
                "pseudo_event_timestamp_raw": pseudo_raw,
                "prior_trading_sessions": [d.isoformat() for d in prior_sessions],
                "feature_snapshot": _json_object(observation.get("feature_snapshot")),
                "dataset_manifest": dataset_manifest,
                "row_counts": dict(row_counts),
                "quality_flags": sorted(set(flags)),
                "leakage_rule": "All event-day control trades, quotes, auctions, news and completed minute bars end strictly before the matched positive event's threshold-crossing timestamp. Event-day daily bars are excluded.",
                "negative_label_rule": "The split-adjusted event-day high remained below 125% of the previous regular-session close; therefore the control is an unambiguous non-hit rather than an unsellable threshold print.",
            }
            (root / "observation_metadata.json").write_text(json.dumps(metadata, indent=2, default=str), encoding="utf-8")
            (root / "data_quality_report.json").write_text(json.dumps({"quality_flags": metadata["quality_flags"], "row_counts": dict(row_counts)}, indent=2), encoding="utf-8")
            (root / "dataset_manifest.json").write_text(json.dumps(dataset_manifest, indent=2, default=str), encoding="utf-8")
            archive = root.parent / f"{event_date.isoformat()}_{safe_name(symbol)}_{observation['pseudo_event_key']}_compact.zip"
            with zipfile.ZipFile(archive, "w", compression=zipfile.ZIP_DEFLATED, allowZip64=True) as zf:
                for path in sorted(root.iterdir()):
                    if path.is_file():
                        zf.write(path, path.name)
            if archive.stat().st_size > MAX_FREE_SAFE_OBJECT_BYTES:
                raise RuntimeError(f"Control compact package exceeded 45 MB safety limit: {archive.stat().st_size}")
            storage_path = f"control_jobs/{job_id}/observations/{archive.name}"
            collector._register_file(
                archive, storage_path, "control_compact_package",
                observation_id=observation_id, content_type="application/zip",
            )
            self.store.update(
                "stock25_control_observations", {"id": f"eq.{observation_id}"},
                {
                    "status": "completed", "row_counts": dict(row_counts),
                    "quality_flags": sorted(set(flags)), "compact_storage_path": storage_path,
                    "compact_size_bytes": archive.stat().st_size,
                    "completed_at": datetime.now(UTC).isoformat(), "error_message": None,
                },
            )
            self.store.update(
                "stock25_control_pairs", {"control_observation_id": f"eq.{observation_id}"},
                {"status": "completed", "completed_at": datetime.now(UTC).isoformat(), "quality_flags": sorted(set(flags)), "error_message": None},
            )
        except Exception as exc:
            message = f"{type(exc).__name__}: {exc}"[:4000]
            self.store.update("stock25_control_observations", {"id": f"eq.{observation_id}"}, {"status": "failed", "error_message": message})
            self.store.update("stock25_control_pairs", {"control_observation_id": f"eq.{observation_id}"}, {"status": "failed", "error_message": message})
            raise
        finally:
            shutil.rmtree(Path(self.settings.temp_root) / job_id / "observations" / observation_id, ignore_errors=True)

    def _build_index(
        self, job_id: str, source_job: dict[str, Any], params: dict[str, Any],
        selected_positives: list[dict[str, Any]],
    ) -> tuple[Path, dict[str, Any]]:
        root = Path(self.settings.temp_root) / job_id / "index"
        shutil.rmtree(root, ignore_errors=True)
        root.mkdir(parents=True, exist_ok=True)
        balance_root = Path(self.settings.temp_root) / job_id / "balance_report"
        copied_balance_names: set[str] = set()
        if balance_root.exists():
            for source in balance_root.iterdir():
                if source.is_file():
                    shutil.copy2(source, root / source.name)
                    copied_balance_names.add(source.name)
        # /tmp is ephemeral across Render redeploys. Recover the small pre-download
        # balance files from Supabase Storage when a resumed job builds its index.
        balance_files = self.store.select_all(
            "stock25_control_files", filters={"control_job_id": f"eq.{job_id}"},
            order="created_at.asc",
        )
        for file_row in balance_files:
            if file_row.get("file_kind") not in {
                "pre_download_balance_report", "pre_download_balance_pairs", "control_match_shortfalls"
            }:
                continue
            filename = str(file_row.get("filename"))
            if filename in copied_balance_names:
                continue
            try:
                self.store.download_file(file_row["storage_path"], root / filename)  # type: ignore[attr-defined]
                copied_balance_names.add(filename)
            except Exception as exc:
                logger.warning("Could not recover balance file %s: %s", filename, exc)
        pairs = self.store.select_all("stock25_control_pairs", filters={"control_job_id": f"eq.{job_id}"}, order="event_date.asc,positive_symbol.asc,control_rank.asc")
        observations = self.store.select_all("stock25_control_observations", filters={"control_job_id": f"eq.{job_id}"}, order="event_date.asc,symbol.asc")
        datasets = self.store.select_all("stock25_control_datasets", filters={"control_job_id": f"eq.{job_id}"}, order="symbol.asc,session_date.asc")
        selected_positive_ids = {str(row["id"]) for row in selected_positives}
        selected_result_ids = {str(row["source_result_id"]) for row in selected_positives}
        positive_events = sorted(
            [row for row in selected_positives if str(row.get("id")) in selected_positive_ids],
            key=lambda row: (str(row.get("event_date")), str(row.get("symbol"))),
        )
        source_results = [
            row for row in self.store.select_all(
                "stock25_scan_results", filters={"scan_id": f"eq.{source_job['source_scan_id']}"}, order="event_date.asc,symbol.asc"
            ) if str(row.get("id")) in selected_result_ids
        ]
        match_diagnostics = self.store.select_all(
            "stock25_control_match_diagnostics", filters={"control_job_id": f"eq.{job_id}"},
            order="event_date.asc,positive_symbol.asc",
        )
        positive_minute_files = self.store.select_all(
            "stock25_research_files",
            select="id,research_event_id,file_kind,storage_path,filename,size_bytes,sha256",
            filters={"research_job_id": f"eq.{source_job['id']}", "file_kind": "eq.minute_bars"},
            order="research_event_id.asc",
        )
        positive_minute_files = [
            row for row in positive_minute_files if str(row.get("research_event_id")) in selected_positive_ids
        ]

        def write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
            fields = sorted({key for row in rows for key in row}) if rows else ["empty"]
            with path.open("w", newline="", encoding="utf-8") as fh:
                writer = csv.DictWriter(fh, fieldnames=fields)
                writer.writeheader()
                for row in rows:
                    writer.writerow({k: json_text(v) if isinstance(v, (dict, list)) else v for k, v in row.items()})

        write_csv(root / "matched_pairs.csv", pairs)
        write_csv(root / "control_match_diagnostics.csv", match_diagnostics)
        write_csv(root / "control_observations.csv", observations)
        write_csv(root / "dataset_manifest.csv", datasets)
        control_file_fields = [
            "id", "control_job_id", "control_observation_id", "control_dataset_id",
            "file_kind", "storage_path", "filename", "size_bytes", "sha256", "created_at",
        ]
        positive_file_fields = [
            "id", "research_job_id", "research_event_id", "file_kind", "storage_path",
            "filename", "size_bytes", "sha256", "created_at",
        ]

        def write_streamed_manifest(path: Path, table: str, filters: dict[str, str], fields: list[str]) -> int:
            count = 0
            with path.open("w", newline="", encoding="utf-8") as fh:
                writer = csv.DictWriter(fh, fieldnames=fields)
                writer.writeheader()
                for page in self.store.iter_select_pages(  # type: ignore[attr-defined]
                    table, select=",".join(fields), filters=filters, order="storage_path.asc", page_size=1000
                ):
                    for row in page:
                        writer.writerow({key: row.get(key) for key in fields})
                    count += len(page)
            return count

        control_file_count = write_streamed_manifest(
            root / "control_file_manifest.csv", "stock25_control_files", {"control_job_id": f"eq.{job_id}"}, control_file_fields
        )
        positive_file_count = 0
        with (root / "positive_file_manifest.csv").open("w", newline="", encoding="utf-8") as fh:
            writer = csv.DictWriter(fh, fieldnames=positive_file_fields)
            writer.writeheader()
            for page in self.store.iter_select_pages(  # type: ignore[attr-defined]
                "stock25_research_files", select=",".join(positive_file_fields),
                filters={"research_job_id": f"eq.{source_job['id']}"},
                order="storage_path.asc", page_size=1000,
            ):
                for row in page:
                    if str(row.get("research_event_id")) not in selected_positive_ids:
                        continue
                    writer.writerow({key: row.get(key) for key in positive_file_fields})
                    positive_file_count += 1
        write_csv(root / "positive_event_manifest.csv", positive_events)
        write_csv(root / "source_scan_results.csv", source_results)

        # Compact Parquet tables are the primary analysis inputs; raw tick paths remain in manifests.
        observation_rows: list[dict[str, Any]] = []
        positive_by_id = {row["id"]: row for row in positive_events}
        pairs_by_positive: dict[str, list[dict[str, Any]]] = defaultdict(list)
        for pair in pairs:
            pairs_by_positive[pair["positive_research_event_id"]].append(pair)
        for positive in positive_events:
            positive_pairs = pairs_by_positive.get(positive["id"], [])
            first_pair = positive_pairs[0] if positive_pairs else {}
            observation_rows.append({
                "observation_id": positive["id"], "label": 1, "observation_type": "sellable_25pct_event",
                "pair_group_id": positive["id"], "symbol": positive["symbol"], "event_date": positive["event_date"],
                "cutoff_timestamp": positive.get("exact_cross_timestamp"),
                "cutoff_timestamp_raw": positive.get("exact_cross_timestamp_raw"),
                "cohort_tier": first_pair.get("positive_tier") or classify_positive_tier(positive),
                "matched_control_count": len(positive_pairs),
                "features_json": json_text(_json_object(first_pair.get("positive_features"))),
                "quality_flags_json": json_text(_json_list(positive.get("quality_flags"))),
            })
        for pair in pairs:
            observation_rows.append({
                "observation_id": pair.get("control_observation_id"), "label": 0, "observation_type": "matched_non_hit_control",
                "pair_group_id": pair["positive_research_event_id"], "symbol": pair["control_symbol"], "event_date": pair["event_date"],
                "cutoff_timestamp": pair.get("pseudo_event_timestamp"),
                "cutoff_timestamp_raw": pair.get("pseudo_event_timestamp_raw"),
                "cohort_tier": pair.get("positive_tier"),
                "matched_control_count": 1,
                "features_json": json_text(_json_object(pair.get("control_features"))),
                "quality_flags_json": json_text(_json_list(pair.get("quality_flags"))),
            })
        observation_schema = pa.schema([
            ("observation_id", pa.string()), ("label", pa.int8()), ("observation_type", pa.string()),
            ("pair_group_id", pa.string()), ("symbol", pa.string()), ("event_date", pa.string()),
            ("cutoff_timestamp", pa.string()), ("cutoff_timestamp_raw", pa.string()),
            ("cohort_tier", pa.string()), ("matched_control_count", pa.int32()),
            ("features_json", pa.string()), ("quality_flags_json", pa.string()),
        ])
        pq.write_table(pa.Table.from_pylist(observation_rows, schema=observation_schema), root / "observation_master.parquet", compression="zstd")
        pair_schema = pa.schema([
            ("pair_id", pa.string()), ("positive_observation_id", pa.string()), ("control_observation_id", pa.string()),
            ("event_date", pa.string()), ("positive_symbol", pa.string()), ("control_symbol", pa.string()),
            ("control_rank", pa.int32()), ("match_score", pa.float64()), ("match_quality", pa.string()),
            ("positive_tier", pa.string()), ("standardized_deltas_json", pa.string()),
        ])
        pair_rows = [{
            "pair_id": row["id"], "positive_observation_id": row["positive_research_event_id"],
            "control_observation_id": row.get("control_observation_id"), "event_date": row["event_date"],
            "positive_symbol": row["positive_symbol"], "control_symbol": row["control_symbol"],
            "control_rank": int(row["control_rank"]), "match_score": float(row["match_score"]),
            "match_quality": row["match_quality"], "positive_tier": row["positive_tier"],
            "standardized_deltas_json": json_text(_json_object(row.get("standardized_deltas"))),
        } for row in pairs]
        pq.write_table(pa.Table.from_pylist(pair_rows, schema=pair_schema), root / "matched_pairs.parquet", compression="zstd")

        session_schema = pa.schema([
            ("dataset_id", pa.string()), ("symbol", pa.string()), ("session_date", pa.string()),
            ("window_type", pa.string()), ("window_start", pa.string()), ("window_end", pa.string()),
            ("window_end_raw", pa.string()), ("feed", pa.string()), ("derived_features_json", pa.string()),
            ("row_counts_json", pa.string()), ("quality_flags_json", pa.string()),
        ])
        session_rows = [{
            "dataset_id": row["id"], "symbol": row["symbol"], "session_date": row["session_date"],
            "window_type": row["window_type"], "window_start": row["window_start"], "window_end": row["window_end"],
            "window_end_raw": row["window_end_raw"], "feed": row["feed"],
            "derived_features_json": json_text(_json_object(row.get("derived_features"))),
            "row_counts_json": json_text(_json_object(row.get("row_counts"))),
            "quality_flags_json": json_text(_json_list(row.get("quality_flags"))),
        } for row in datasets]
        pq.write_table(pa.Table.from_pylist(session_rows, schema=session_schema), root / "control_session_features.parquet", compression="zstd")

        # Build an observation-to-normalised-dataset join. This is essential because a
        # prior symbol-session can be reused by multiple controls without duplicating raw files.
        datasets_by_symbol_date: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
        for dataset in datasets:
            datasets_by_symbol_date[(dataset["symbol"], dataset["session_date"])].append(dataset)
        observation_dataset_rows: list[dict[str, Any]] = []
        dataset_by_id = {row["id"]: row for row in datasets}
        for observation in observations:
            prior_sessions = [str(value) for value in _json_list(observation.get("prior_sessions"))]
            for session_date_text in prior_sessions:
                candidates = [
                    row for row in datasets_by_symbol_date.get((observation["symbol"], session_date_text), [])
                    if row.get("window_type") == "full_session" and row.get("status") == "completed"
                ]
                if candidates:
                    dataset = sorted(candidates, key=lambda row: row.get("created_at") or "")[0]
                    observation_dataset_rows.append({
                        "control_observation_id": observation["id"],
                        "control_dataset_id": dataset["id"],
                        "relation": "prior_full_session",
                        "session_date": session_date_text,
                    })
            prefix_candidates = [
                row for row in datasets_by_symbol_date.get((observation["symbol"], observation["event_date"]), [])
                if row.get("window_type") == "prefix"
                and row.get("status") == "completed"
                and str(row.get("window_end_raw")) == str(observation.get("pseudo_event_timestamp_raw"))
            ]
            if prefix_candidates:
                dataset = sorted(prefix_candidates, key=lambda row: row.get("created_at") or "")[0]
                observation_dataset_rows.append({
                    "control_observation_id": observation["id"],
                    "control_dataset_id": dataset["id"],
                    "relation": "event_day_prefix",
                    "session_date": observation["event_date"],
                })

        observation_dataset_schema = pa.schema([
            ("control_observation_id", pa.string()), ("control_dataset_id", pa.string()),
            ("relation", pa.string()), ("session_date", pa.string()),
        ])
        pq.write_table(
            pa.Table.from_pylist(observation_dataset_rows, schema=observation_dataset_schema),
            root / "observation_dataset_map.parquet", compression="zstd",
        )
        write_csv(root / "observation_dataset_map.csv", observation_dataset_rows)

        # Build a symmetric minute-level session feature table for positives and controls.
        # Positive minute-bar objects are small compared with raw ticks, so they can be
        # downloaded one at a time without materialising the 18.9 GB positive raw dataset.
        symmetric_rows: list[dict[str, Any]] = []
        observation_by_id = {row["id"]: row for row in observations}
        for mapping in observation_dataset_rows:
            dataset = dataset_by_id.get(mapping["control_dataset_id"])
            observation = observation_by_id.get(mapping["control_observation_id"])
            if not dataset or not observation:
                continue
            derived = _json_object(dataset.get("derived_features"))
            symmetric_rows.append({
                "observation_id": observation["id"],
                "label": 0,
                "symbol": observation["symbol"],
                "session_date": dataset["session_date"],
                "window_name": dataset["session_date"] if dataset["window_type"] == "full_session" else f"{dataset['session_date']}_to_cutoff",
                "window_type": dataset["window_type"],
                "cutoff_timestamp_raw": dataset.get("window_end_raw") if dataset["window_type"] == "prefix" else None,
                "minute_features_json": json_text(_json_object(derived.get("minute"))),
                "quality_flags_json": json_text(sorted(set(_json_list(dataset.get("quality_flags")) + _json_list(observation.get("quality_flags"))))),
            })
        positive_by_file_event = {row.get("research_event_id"): row for row in positive_minute_files if row.get("research_event_id")}
        positive_feature_failures = 0
        positive_temp = root / "positive_minute_temp"
        positive_temp.mkdir(exist_ok=True)
        for positive in positive_events:
            file_row = positive_by_file_event.get(positive["id"])
            if not file_row:
                positive_feature_failures += 1
                continue
            local = positive_temp / f"{positive['id']}.parquet"
            try:
                self.store.download_file(file_row["storage_path"], local)  # type: ignore[attr-defined]
                table = pq.read_table(local)
                grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
                for row in table.to_pylist():
                    grouped[str(row.get("source_window") or "unknown")].append(row)
                for window_name, rows in grouped.items():
                    session_date_text = window_name[:10] if len(window_name) >= 10 else positive["event_date"]
                    is_prefix = window_name.endswith("_to_cross")
                    symmetric_rows.append({
                        "observation_id": positive["id"],
                        "label": 1,
                        "symbol": positive["symbol"],
                        "session_date": session_date_text,
                        "window_name": window_name,
                        "window_type": "prefix" if is_prefix else "full_session",
                        "cutoff_timestamp_raw": positive.get("exact_cross_timestamp_raw") if is_prefix else None,
                        "minute_features_json": json_text(summarize_minute_rows(rows)),
                        "quality_flags_json": json_text(_json_list(positive.get("quality_flags"))),
                    })
            except Exception as exc:
                logger.warning("Could not build positive minute features for %s: %s", positive["id"], exc)
                positive_feature_failures += 1
            finally:
                local.unlink(missing_ok=True)
        shutil.rmtree(positive_temp, ignore_errors=True)

        symmetric_schema = pa.schema([
            ("observation_id", pa.string()), ("label", pa.int8()), ("symbol", pa.string()),
            ("session_date", pa.string()), ("window_name", pa.string()), ("window_type", pa.string()),
            ("cutoff_timestamp_raw", pa.string()), ("minute_features_json", pa.string()),
            ("quality_flags_json", pa.string()),
        ])
        pq.write_table(pa.Table.from_pylist(symmetric_rows, schema=symmetric_schema), root / "session_features.parquet", compression="zstd")

        summary = {
            "version": "3.0.2",
            "matching_version": "strict_global_v3.0.2",
            "control_job_id": job_id,
            "source_research_job_id": source_job["id"],
            "source_scan_id": source_job["source_scan_id"],
            "parameters": params,
            "positive_event_count": len(positive_events),
            "matched_pair_count": len(pairs),
            "excellent_pair_count": sum(1 for row in pairs if row.get("match_quality") == "excellent"),
            "good_pair_count": sum(1 for row in pairs if row.get("match_quality") == "good"),
            "weak_pair_count": sum(1 for row in pairs if row.get("match_quality") not in {"excellent", "good"}),
            "positive_events_with_control_shortfall": len(match_diagnostics),
            "control_observation_count": len(observations),
            "completed_control_count": sum(1 for row in observations if row.get("status") == "completed"),
            "failed_control_count": sum(1 for row in observations if row.get("status") == "failed"),
            "normalised_control_dataset_count": len(datasets),
            "control_file_count": control_file_count,
            "positive_file_count": positive_file_count,
            "observation_dataset_map_rows": len(observation_dataset_rows),
            "symmetric_session_feature_rows": len(symmetric_rows),
            "positive_minute_feature_failures": positive_feature_failures,
            "auction_coverage": {
                "datasets_requested": sum(1 for row in datasets if params.get("include_auctions", True)),
                "datasets_with_records": sum(1 for row in datasets if int(_json_object(row.get("row_counts")).get("auctions") or 0) > 0),
                "datasets_no_records": sum(1 for row in datasets if "auctions_no_records_returned" in _json_list(row.get("quality_flags"))),
                "datasets_unavailable": sum(1 for row in datasets if any(str(flag).startswith("auctions_unavailable:") for flag in _json_list(row.get("quality_flags")))),
            },
            "leakage_boundary": "Every positive and control observation is cut off strictly before its exact/matched timestamp. Event-day daily bars and overlapping one-minute bars are excluded.",
            "control_label": "Controls are strict non-hits: split-adjusted event-day high below 125% of prior close, not merely threshold prints that failed sellability.",
            "raw_data_layout": "Raw trades/quotes and derived session files remain normalized in Supabase Storage. The file manifests are the authoritative join map and avoid duplicating overlapping control sessions.",
        }
        (root / "dataset_manifest.json").write_text(json.dumps(summary, indent=2, default=str), encoding="utf-8")
        (root / "README.txt").write_text(
            "V3.0.2 strict matched-control analysis export. Start with pre-download balance files, control_match_diagnostics.csv, observation_master.parquet, matched_pairs.parquet and control_session_features.parquet. Use the CSV manifests to retrieve raw or minute-level objects. Never use rows at or after cutoff_timestamp_raw when constructing predictors. Keep the chronological test period sealed until fixed rules survive validation.\n",
            encoding="utf-8",
        )
        archive = root.parent / "matched_control_analysis_index.zip"
        with zipfile.ZipFile(archive, "w", compression=zipfile.ZIP_DEFLATED, allowZip64=True) as zf:
            for path in sorted(root.iterdir()):
                if path.is_file():
                    zf.write(path, path.name)
        return archive, summary

    def run(self, job: dict[str, Any]) -> None:
        job_id = job["id"]
        params = {
            "controls_per_event": self.settings.default_controls_per_event,
            "feature_sessions": self.settings.default_control_feature_sessions,
            "history_calendar_days": self.settings.default_control_history_calendar_days,
            "max_control_symbol_uses": self.settings.default_max_control_symbol_uses,
            "prior_sessions": self.settings.default_prior_sessions,
            "feed": self.settings.default_feed,
            "exact_exchange_first": False,
            "allow_exchange_fallback": True,
            "require_corporate_action_match": True,
            "max_match_score": 4.0,
            "max_abs_log_prior_close_z": 1.5,
            "max_abs_log_median_dollar_volume_z": 1.5,
            "max_abs_realized_vol_z": 2.0,
            "max_abs_atr_pct_z": 2.0,
            "max_abs_prior_day_return_z": 2.0,
            "max_abs_momentum_z": 2.0,
            "max_abs_log_listing_sessions_z": 2.0,
            "matching_version": "strict_global_v3.0.2",
            "include_raw_trades": self.settings.default_control_include_raw_trades,
            "include_raw_quotes": self.settings.default_control_include_raw_quotes,
            "derive_one_second": self.settings.default_control_derive_one_second,
            "include_news": self.settings.default_control_include_news,
            "include_auctions": self.settings.default_control_include_auctions,
            "include_corporate_actions": self.settings.default_control_include_corporate_actions,
            "build_analysis_export": self.settings.default_build_analysis_export,
            "max_positive_events": 0,
            **_json_object(job.get("parameters")),
        }
        try:
            source_job, positives, results_by_id = self._load_source(job, int(params.get("max_positive_events") or 0))
            self._update(job_id, "starting", 0, len(positives), positive_event_count=len(positives))
            pairs = self.store.select("stock25_control_pairs", filters={"control_job_id": f"eq.{job_id}"}, limit=1)
            matching_was_committed = int(job.get("matched_pair_count") or 0) > 0 and int(job.get("unique_control_count") or 0) > 0
            if not pairs or not matching_was_committed:
                pair_count, unmatched = self._create_matches(
                    job=job, source_job=source_job, positives=positives,
                    results_by_id=results_by_id, params=params,
                )
                observations = self.store.select_all("stock25_control_observations", filters={"control_job_id": f"eq.{job_id}"})
                self._update(
                    job_id, "matched", 0, len(observations), matched_pair_count=pair_count,
                    unique_control_count=len(observations), unmatched_positive_count=unmatched,
                )
            observations = self.store.select_all(
                "stock25_control_observations", filters={"control_job_id": f"eq.{job_id}"}, order="event_date.asc,symbol.asc"
            )
            total = len(observations)
            completed = sum(1 for row in observations if row.get("status") == "completed")
            failed = 0
            for idx, observation in enumerate(observations, start=1):
                if observation.get("status") == "completed":
                    self._update(job_id, "collecting_controls", idx, total, completed_control_count=completed, failed_control_count=failed)
                    continue
                try:
                    self._collect_observation(job_id, observation, params)
                    completed += 1
                except Exception:
                    failed += 1
                    logger.exception("Control observation failed: %s", observation["id"])
                self._update(job_id, "collecting_controls", idx, total, completed_control_count=completed, failed_control_count=failed)

            if failed:
                raise RuntimeError(f"{failed} control observations failed. Retry the same job to resume only incomplete observations.")
            archive = None
            summary: dict[str, Any] = {}
            if params.get("build_analysis_export", True):
                self._update(job_id, "building_analysis_export", 0, 1)
                archive, summary = self._build_index(job_id, source_job, params, positives)
                storage_path = f"control_jobs/{job_id}/{archive.name}"
                collector = ControlDatasetCollector(self.settings, self.store, self.alpaca, job_id, params)
                file_row = collector._register_file(archive, storage_path, "analysis_export_index", content_type="application/zip")
                archive.unlink(missing_ok=True)
                self.store.update_control_job(
                    job_id, analysis_export_file_id=file_row["id"], analysis_export_storage_path=storage_path
                )
            self.store.update_control_job(
                job_id, status="completed", progress_stage="completed", progress_current=total,
                progress_total=total, completed_control_count=completed, failed_control_count=0,
                completed_at=datetime.now(UTC).isoformat(), error_message=None,
            )
            logger.info("Control job %s completed: %s", job_id, summary)
        except Exception as exc:
            self.store.update_control_job(
                job_id, status="failed", progress_stage="failed",
                error_message=f"{type(exc).__name__}: {exc}"[:4000],
            )
            logger.exception("Control job %s failed", job_id)
