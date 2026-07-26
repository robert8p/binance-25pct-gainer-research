from __future__ import annotations

import logging
import time

from app.alpaca_client import AlpacaClient
from app.config import settings
from app.control import ControlRunner
from app.entry import EntryExporterRunner
from app.backtest import BacktestRunner
from app.research import ResearchRunner
from app.scanner import ScanRunner
from app.supabase_store import SupabaseStore

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
logger = logging.getLogger(__name__)


def main() -> None:
    settings.validate_worker()
    logger.info("Worker started")
    last_recovery_check = 0.0
    while True:
        store = SupabaseStore(settings)
        alpaca = AlpacaClient(settings)
        try:
            now = time.monotonic()
            if now - last_recovery_check >= 300:
                recovered = store.recover_stale_jobs(settings.stale_job_minutes)
                if any(recovered.values()):
                    logger.warning("Recovered interrupted work: %s", recovered)
                last_recovery_check = now

            scan = store.claim_next_scan()
            if scan:
                logger.info("Claimed scan %s", scan["id"])
                ScanRunner(settings, store, alpaca).run(scan)
                continue

            job = store.claim_next_research_job()
            if job:
                logger.info("Claimed research job %s", job["id"])
                ResearchRunner(settings, store, alpaca).run(job)
                continue

            control_job = store.claim_next_control_job()
            if control_job:
                logger.info("Claimed control job %s", control_job["id"])
                ControlRunner(settings, store, alpaca).run(control_job)
                continue

            entry_job = store.claim_next_entry_job()
            if entry_job:
                logger.info("Claimed entry/export job %s", entry_job["id"])
                EntryExporterRunner(settings, store).run(entry_job)
                continue

            backtest_job = store.claim_next_backtest_job() if settings.enable_backtest_stage else None
            if backtest_job:
                logger.info("Claimed execution backtest job %s", backtest_job["id"])
                BacktestRunner(settings, store, alpaca).run(backtest_job)
                continue
        except Exception:
            logger.exception("Worker loop failed")
        finally:
            alpaca.close()
            store.close()
        time.sleep(settings.worker_poll_seconds)


if __name__ == "__main__":
    main()
