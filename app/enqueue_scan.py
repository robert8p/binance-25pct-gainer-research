from __future__ import annotations

import argparse

from app.config import settings
from app.supabase_store import SupabaseStore


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--lookback-days", type=int, default=settings.default_lookback_days)
    parser.add_argument("--threshold-pct", type=float, default=settings.default_threshold_pct)
    parser.add_argument(
        "--universe-mode",
        choices=["current_tradable", "all_recent_alpaca_assets"],
        default=settings.default_universe_mode,
    )
    parser.add_argument("--feed", default=settings.default_feed)
    parser.add_argument("--auto", action="store_true")
    args = parser.parse_args()
    settings.validate_storage()
    store = SupabaseStore(settings)
    try:
        scan = store.enqueue_scan(
            {
                "lookback_days": args.lookback_days,
                "threshold_pct": args.threshold_pct,
                "universe_mode": args.universe_mode,
                "feed": args.feed,
                "include_partial_current_day": False,
                "save_event_bars": settings.save_event_bars,
            },
            source="scheduled" if args.auto else "command_line",
        )
        print(scan["id"])
    finally:
        store.close()


if __name__ == "__main__":
    main()
