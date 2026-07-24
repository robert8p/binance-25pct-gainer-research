from __future__ import annotations

import json
import shutil
import tempfile
import uuid
import zipfile
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from .binance import (
    BinanceClient,
    archive_url,
    download_archive,
    normalize_archive_timestamp,
    sha256_file,
)
from .supabase import SupabaseClient


AGG_COLUMNS = [
    "aggregate_trade_id",
    "price",
    "quantity",
    "first_trade_id",
    "last_trade_id",
    "timestamp",
    "buyer_was_maker",
    "best_price_match",
]
TRADE_COLUMNS = [
    "trade_id",
    "price",
    "quantity",
    "quote_quantity",
    "timestamp",
    "buyer_was_maker",
    "best_price_match",
]
KLINE_COLUMNS = [
    "open_time",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "close_time",
    "quote_volume",
    "trade_count",
    "taker_buy_base_volume",
    "taker_buy_quote_volume",
    "ignore",
]


class ResearchBuilder:
    def __init__(self, db: SupabaseClient, binance: BinanceClient, temp_root: Path):
        self.db = db
        self.binance = binance
        self.temp_root = temp_root

    def run(self, job: dict[str, Any]) -> dict[str, Any]:
        job_id = str(job["id"])
        scan_id = str(job["scan_id"])
        prior_days = int(job["prior_days"])
        maximum_events = int(job.get("maximum_events") or 0)
        include_1s = bool(job.get("include_1s_klines", True))
        include_agg = bool(job.get("include_agg_trades", True))
        include_trades = bool(job.get("include_raw_trades", False))

        events = self.db.select_all(
            "binance_gainer_events",
            filters={"scan_id": f"eq.{scan_id}", "sellability_pass": "eq.true"},
            order="event_date.asc,symbol.asc",
        )
        if maximum_events > 0:
            events = events[:maximum_events]
        completed = 0
        failed = 0
        event_manifest: list[dict[str, Any]] = []
        file_manifest: list[dict[str, Any]] = []
        existing_events = self.db.select_all(
            "binance_research_events",
            filters={"research_job_id": f"eq.{job_id}"},
        )
        completed_event_ids = {
            str(row["event_id"]): row
            for row in existing_events
            if row.get("status") in {"completed", "completed_with_warnings"}
        }
        existing_files = self.db.select_all(
            "binance_research_files",
            filters={"research_job_id": f"eq.{job_id}"},
        )
        files_by_event: dict[str, list[dict[str, Any]]] = {}
        for row in existing_files:
            if row.get("event_id"):
                files_by_event.setdefault(str(row["event_id"]), []).append(row)

        work = Path(tempfile.mkdtemp(prefix=f"binance-research-{job_id}-", dir=self.temp_root))
        try:
            for index, event in enumerate(events, start=1):
                event_id = str(event["id"])
                if event_id in completed_event_ids:
                    prior = completed_event_ids[event_id]
                    event_manifest.append(
                        {
                            "event_id": event_id,
                            "symbol": event["symbol"],
                            "event_date": event["event_date"],
                            "status": prior["status"],
                            "warning_count": prior.get("warning_count", 0),
                            "storage_path": prior.get("storage_path"),
                            "resumed_without_redownload": True,
                        }
                    )
                    file_manifest.extend(files_by_event.get(event_id, []))
                    completed += 1
                    self.db.update(
                        "binance_research_jobs",
                        {"id": f"eq.{job_id}"},
                        {
                            "events_total": len(events),
                            "events_processed": index,
                            "events_completed": completed,
                            "events_failed": failed,
                            "heartbeat_at": datetime.now(timezone.utc).isoformat(),
                        },
                    )
                    continue
                try:
                    record, files = self._build_event(
                        job_id,
                        event,
                        prior_days=prior_days,
                        include_1s=include_1s,
                        include_agg=include_agg,
                        include_trades=include_trades,
                    )
                    event_manifest.append(record)
                    file_manifest.extend(files)
                    completed += 1
                except Exception as exc:
                    failed += 1
                    event_manifest.append(
                        {
                            "event_id": event["id"],
                            "symbol": event["symbol"],
                            "event_date": event["event_date"],
                            "status": "failed",
                            "message": str(exc)[:4000],
                        }
                    )
                    self.db.insert(
                        "binance_research_issues",
                        {
                            "research_job_id": job_id,
                            "event_id": event["id"],
                            "stage": "event_bundle",
                            "message": str(exc)[:4000],
                        },
                    )
                    self.db.upsert(
                        "binance_research_events",
                        [{
                            "research_job_id": job_id,
                            "event_id": event["id"],
                            "symbol": event["symbol"],
                            "event_date": event["event_date"],
                            "status": "failed",
                            "warning_count": 0,
                        }],
                        on_conflict="research_job_id,event_id",
                    )
                self.db.update(
                    "binance_research_jobs",
                    {"id": f"eq.{job_id}"},
                    {
                        "events_total": len(events),
                        "events_processed": index,
                        "events_completed": completed,
                        "events_failed": failed,
                        "heartbeat_at": datetime.now(timezone.utc).isoformat(),
                    },
                )

            index_dir = work / "index"
            index_dir.mkdir(parents=True, exist_ok=True)
            pd.DataFrame(event_manifest).to_csv(index_dir / "event_manifest.csv", index=False)
            pd.DataFrame(file_manifest).to_csv(index_dir / "file_manifest.csv", index=False)
            pd.DataFrame(events).to_csv(index_dir / "sellable_source_events.csv", index=False)
            (index_dir / "research_job.json").write_text(
                json.dumps(job, indent=2, default=str), encoding="utf-8"
            )
            (index_dir / "README.txt").write_text(
                "Binance rolling eight-hour >=25% surge research index. Event-day predictor data ends before the first threshold-crossing minute.\n"
                "Historical saleability is inferred from executed seller-initiated aggregate trades at any post-crossing price, not displayed order-book depth.\n",
                encoding="utf-8",
            )
            index_zip = work / "research_index.zip"
            self._zip_directory(index_dir, index_zip)
            storage_path = f"jobs/{job_id}/research_index.zip"
            self.db.upload_file(storage_path, index_zip, "application/zip")
            self.db.upsert(
                "binance_research_files",
                [
                    {
                        "research_job_id": job_id,
                        "event_id": None,
                        "storage_path": storage_path,
                        "filename": "research_index.zip",
                        "size_bytes": index_zip.stat().st_size,
                        "sha256": sha256_file(index_zip),
                        "content_type": "application/zip",
                        "role": "research_job_index",
                        "source_url": None,
                    }
                ],
                on_conflict="research_job_id,storage_path",
            )
            return {
                "events_total": len(events),
                "events_completed": completed,
                "events_failed": failed,
                "index_storage_path": storage_path,
            }
        finally:
            shutil.rmtree(work, ignore_errors=True)

    def _build_event(
        self,
        job_id: str,
        event: dict[str, Any],
        *,
        prior_days: int,
        include_1s: bool,
        include_agg: bool,
        include_trades: bool,
    ) -> tuple[dict[str, Any], list[dict[str, Any]]]:
        symbol = str(event["symbol"])
        event_day = date.fromisoformat(str(event["event_date"]))
        cross = datetime.fromisoformat(str(event["first_cross_time"]))
        if cross.tzinfo is None:
            cross = cross.replace(tzinfo=timezone.utc)
        cutoff_ms = int(cross.timestamp() * 1000)
        event_root = Path(tempfile.mkdtemp(prefix=f"{symbol}-{event_day}-", dir=self.temp_root))
        manifest: list[dict[str, Any]] = []
        uploaded_files: list[dict[str, Any]] = []
        warnings: list[str] = []
        storage_prefix = f"jobs/{job_id}/events/{event_day.isoformat()}_{symbol}"

        def upload_data(path: Path, *, role: str, source_url: str | None = None) -> None:
            relative = str(path.relative_to(event_root))
            storage_path = f"{storage_prefix}/{relative}"
            local_record = self._file_record(path, event_root, role=role, source_url=source_url)
            local_record["storage_path"] = storage_path
            content_type = "application/zip" if path.suffix == ".zip" else "application/vnd.apache.parquet"
            self.db.upload_file(storage_path, path, content_type)
            db_file = {
                "research_job_id": job_id,
                "event_id": event["id"],
                "storage_path": storage_path,
                "filename": path.name,
                "size_bytes": path.stat().st_size,
                "sha256": local_record["sha256"],
                "content_type": content_type,
                "role": role,
                "source_url": source_url,
            }
            self.db.upsert(
                "binance_research_files", [db_file], on_conflict="research_job_id,storage_path"
            )
            manifest.append(local_record)
            uploaded_files.append(db_file)
            path.unlink(missing_ok=True)

        try:
            (event_root / "source_archives").mkdir()
            (event_root / "event_day_pre_cross").mkdir()
            (event_root / "metadata").mkdir()

            start_day = event_day - timedelta(days=prior_days)
            for offset in range(prior_days):
                day = start_day + timedelta(days=offset)
                for data_type, interval, enabled in [
                    ("klines", "1m", True),
                    ("klines", "1s", include_1s),
                    ("aggTrades", None, include_agg),
                    ("trades", None, include_trades),
                ]:
                    if not enabled:
                        continue
                    url = archive_url(data_type, symbol, day, interval)
                    name = url.rsplit("/", 1)[-1]
                    dest = event_root / "source_archives" / name
                    try:
                        available = download_archive(url, dest)
                        if not available:
                            warnings.append(f"Archive unavailable: {name}")
                            continue
                        upload_data(dest, role="prior_day_raw_source_archive", source_url=url)
                    except Exception as exc:
                        dest.unlink(missing_ok=True)
                        warnings.append(f"{name}: {exc}")

            # Event-day 1m bars are generated through REST and cut off before the crossing minute.
            day_start = datetime.combine(event_day, datetime.min.time(), tzinfo=timezone.utc)
            minute_rows = self.binance.klines(
                symbol,
                "1m",
                int(day_start.timestamp() * 1000),
                cutoff_ms,
            )
            minute_df = pd.DataFrame(minute_rows, columns=KLINE_COLUMNS)
            minute_path = event_root / "event_day_pre_cross" / "klines_1m_pre_cross.parquet"
            minute_df.to_parquet(minute_path, index=False, compression="zstd")
            upload_data(minute_path, role="event_day_predictor_bars")

            filtered_sources = [
                ("klines", "1s", include_1s, KLINE_COLUMNS, "open_time", "klines_1s_pre_cross.parquet"),
                ("aggTrades", None, include_agg, AGG_COLUMNS, "timestamp", "aggTrades_pre_cross.parquet"),
                ("trades", None, include_trades, TRADE_COLUMNS, "timestamp", "trades_pre_cross.parquet"),
            ]
            for data_type, interval, enabled, columns, timestamp_column, filename in filtered_sources:
                if not enabled:
                    continue
                url = archive_url(data_type, symbol, event_day, interval)
                output = event_root / "event_day_pre_cross" / filename
                self._filtered_archive_to_parquet(
                    url,
                    output,
                    columns,
                    timestamp_column=timestamp_column,
                    cutoff_ms=cutoff_ms,
                    warnings=warnings,
                )
                if output.exists():
                    upload_data(output, role="event_day_pre_cross_filtered", source_url=url)

            metadata = dict(event)
            metadata["research_cutoff_utc"] = cross.isoformat()
            metadata["cutoff_rule"] = (
                "All event-day predictor rows have timestamps strictly before the first "
                "threshold-crossing minute."
            )
            metadata["prior_complete_utc_days"] = prior_days
            metadata["warnings"] = warnings
            metadata["storage_design"] = (
                "Large data files are uploaded separately and deleted from the worker immediately. "
                "The compact event ZIP contains metadata and the file manifest only."
            )
            metadata_path = event_root / "metadata" / "event_metadata.json"
            metadata_path.write_text(json.dumps(metadata, indent=2, default=str), encoding="utf-8")
            manifest.append(self._file_record(metadata_path, event_root, role="metadata"))
            manifest_path = event_root / "metadata" / "file_manifest.csv"
            pd.DataFrame(manifest).to_csv(manifest_path, index=False)

            zip_name = f"{symbol}_{event_day.isoformat()}_compact.zip"
            zip_path = self.temp_root / f"{uuid.uuid4()}_{zip_name}"
            self._zip_directory(event_root, zip_path)
            storage_path = f"{storage_prefix}/{zip_name}"
            self.db.upload_file(storage_path, zip_path, "application/zip")
            compact_file = {
                "research_job_id": job_id,
                "event_id": event["id"],
                "storage_path": storage_path,
                "filename": zip_name,
                "size_bytes": zip_path.stat().st_size,
                "sha256": sha256_file(zip_path),
                "content_type": "application/zip",
                "role": "event_compact_metadata_and_manifest",
                "source_url": None,
            }
            self.db.upsert(
                "binance_research_files", [compact_file], on_conflict="research_job_id,storage_path"
            )
            uploaded_files.append(compact_file)
            self.db.upsert(
                "binance_research_events",
                [
                    {
                        "research_job_id": job_id,
                        "event_id": event["id"],
                        "symbol": symbol,
                        "event_date": event_day.isoformat(),
                        "status": "completed_with_warnings" if warnings else "completed",
                        "storage_path": storage_path,
                        "warning_count": len(warnings),
                    }
                ],
                on_conflict="research_job_id,event_id",
            )
            zip_path.unlink(missing_ok=True)
            return (
                {
                    "event_id": event["id"],
                    "symbol": symbol,
                    "event_date": event_day.isoformat(),
                    "status": "completed_with_warnings" if warnings else "completed",
                    "warning_count": len(warnings),
                    "storage_path": storage_path,
                },
                uploaded_files,
            )
        finally:
            shutil.rmtree(event_root, ignore_errors=True)

    def _filtered_archive_to_parquet(
        self,
        url: str,
        output: Path,
        columns: list[str],
        *,
        timestamp_column: str,
        cutoff_ms: int,
        warnings: list[str],
    ) -> None:
        source = output.with_suffix(".source.zip")
        writer: pq.ParquetWriter | None = None
        try:
            if not download_archive(url, source):
                warnings.append(f"Archive unavailable: {url.rsplit('/', 1)[-1]}")
                return
            with zipfile.ZipFile(source) as archive:
                names = [n for n in archive.namelist() if not n.endswith("/")]
                if not names:
                    warnings.append(f"Archive empty: {url}")
                    return
                with archive.open(names[0]) as raw:
                    for chunk in pd.read_csv(raw, header=None, names=columns, chunksize=250_000):
                        ts = pd.to_numeric(chunk[timestamp_column], errors="coerce").fillna(0).astype("int64")
                        normalized = ts.where(ts <= 10_000_000_000_000, ts // 1000)
                        kept = chunk.loc[normalized < cutoff_ms].copy()
                        if kept.empty:
                            continue
                        kept["timestamp_ms_normalized"] = normalized.loc[kept.index].astype("int64")
                        table = pa.Table.from_pandas(kept, preserve_index=False)
                        if writer is None:
                            writer = pq.ParquetWriter(output, table.schema, compression="zstd")
                        writer.write_table(table)
            if writer is None:
                empty = pd.DataFrame({column: pd.Series(dtype="string") for column in columns})
                empty["timestamp_ms_normalized"] = pd.Series(dtype="int64")
                empty.to_parquet(output, index=False, compression="zstd")
        except Exception as exc:
            warnings.append(f"Could not filter {url.rsplit('/', 1)[-1]}: {exc}")
            output.unlink(missing_ok=True)
        finally:
            if writer is not None:
                writer.close()
            source.unlink(missing_ok=True)

    @staticmethod
    def _file_record(path: Path, root: Path, *, role: str, source_url: str | None = None) -> dict[str, Any]:
        return {
            "relative_path": str(path.relative_to(root)),
            "role": role,
            "source_url": source_url,
            "size_bytes": path.stat().st_size,
            "sha256": sha256_file(path),
        }

    @staticmethod
    def _zip_directory(source: Path, destination: Path) -> None:
        with zipfile.ZipFile(destination, "w", compression=zipfile.ZIP_DEFLATED, compresslevel=6) as archive:
            for path in sorted(source.rglob("*")):
                if path.is_file():
                    archive.write(path, path.relative_to(source))
