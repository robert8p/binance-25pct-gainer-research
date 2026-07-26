from __future__ import annotations

import os
from dataclasses import dataclass

RESEARCH_TARGET_PCT = 25.0


def _bool(name: str, default: bool = False) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


def _int(name: str, default: int) -> int:
    raw = os.getenv(name)
    return int(raw) if raw not in (None, "") else default


def _float(name: str, default: float) -> float:
    raw = os.getenv(name)
    return float(raw) if raw not in (None, "") else default


@dataclass(frozen=True)
class Settings:
    alpaca_api_key: str = os.getenv("ALPACA_API_KEY", "")
    alpaca_secret_key: str = os.getenv("ALPACA_SECRET_KEY", "")
    alpaca_trading_base_url: str = os.getenv(
        "ALPACA_TRADING_BASE_URL", "https://paper-api.alpaca.markets"
    )
    alpaca_data_base_url: str = os.getenv(
        "ALPACA_DATA_BASE_URL", "https://data.alpaca.markets"
    )
    supabase_url: str = os.getenv("SUPABASE_URL", "")
    supabase_service_role_key: str = os.getenv("SUPABASE_SERVICE_ROLE_KEY", "")
    supabase_storage_bucket: str = os.getenv(
        "SUPABASE_STORAGE_BUCKET", "alpaca-25pct-research"
    )
    app_username: str = os.getenv("APP_USERNAME", "admin")
    app_password: str = os.getenv("APP_PASSWORD", "")
    worker_poll_seconds: int = _int("WORKER_POLL_SECONDS", 10)
    request_timeout_seconds: int = _int("REQUEST_TIMEOUT_SECONDS", 120)
    max_retries: int = _int("MAX_RETRIES", 7)
    daily_batch_size: int = _int("DAILY_BATCH_SIZE", 100)
    minute_batch_size: int = _int("MINUTE_BATCH_SIZE", 20)
    request_pause_seconds: float = _float("REQUEST_PAUSE_SECONDS", 0.20)
    default_lookback_days: int = _int("DEFAULT_LOOKBACK_DAYS", 90)
    default_threshold_pct: float = _float("DEFAULT_THRESHOLD_PCT", 25.0)
    default_universe_mode: str = os.getenv("DEFAULT_UNIVERSE_MODE", "current_tradable")
    default_feed: str = os.getenv("DEFAULT_FEED", "sip")
    include_otc: bool = _bool("INCLUDE_OTC", False)
    include_partial_current_day: bool = _bool("INCLUDE_PARTIAL_CURRENT_DAY", False)
    save_event_bars: bool = _bool("SAVE_EVENT_BARS", True)
    app_environment: str = os.getenv("APP_ENVIRONMENT", "production")
    enable_backtest_stage: bool = _bool("ENABLE_BACKTEST_STAGE", False)

    # Research package defaults.
    default_prior_sessions: int = _int("DEFAULT_PRIOR_SESSIONS", 10)
    default_sellability_notional: float = _float(
        "DEFAULT_SELLABILITY_NOTIONAL", 500.0
    )
    default_sellability_window_seconds: int = _int(
        "DEFAULT_SELLABILITY_WINDOW_SECONDS", 300
    )
    default_include_raw_trades: bool = _bool("DEFAULT_INCLUDE_RAW_TRADES", True)
    default_include_raw_quotes: bool = _bool("DEFAULT_INCLUDE_RAW_QUOTES", True)
    default_include_news: bool = _bool("DEFAULT_INCLUDE_NEWS", True)
    default_include_auctions: bool = _bool("DEFAULT_INCLUDE_AUCTIONS", True)
    default_include_corporate_actions: bool = _bool(
        "DEFAULT_INCLUDE_CORPORATE_ACTIONS", True
    )
    max_raw_rows_per_file: int = _int("MAX_RAW_ROWS_PER_FILE", 0)
    signed_url_expiry_seconds: int = _int("SIGNED_URL_EXPIRY_SECONDS", 3600)
    temp_root: str = os.getenv("TEMP_ROOT", "/tmp/alpaca-25pct-research")
    stale_job_minutes: int = _int("STALE_JOB_MINUTES", 20)

    # Matched-control defaults.
    default_controls_per_event: int = _int("DEFAULT_CONTROLS_PER_EVENT", 5)
    default_control_feature_sessions: int = _int("DEFAULT_CONTROL_FEATURE_SESSIONS", 10)
    default_control_history_calendar_days: int = _int("DEFAULT_CONTROL_HISTORY_CALENDAR_DAYS", 120)
    default_max_control_symbol_uses: int = _int("DEFAULT_MAX_CONTROL_SYMBOL_USES", 20)
    default_control_include_raw_trades: bool = _bool("DEFAULT_CONTROL_INCLUDE_RAW_TRADES", True)
    default_control_include_raw_quotes: bool = _bool("DEFAULT_CONTROL_INCLUDE_RAW_QUOTES", True)
    default_control_derive_one_second: bool = _bool("DEFAULT_CONTROL_DERIVE_ONE_SECOND", True)
    default_control_include_news: bool = _bool("DEFAULT_CONTROL_INCLUDE_NEWS", True)
    default_control_include_auctions: bool = _bool("DEFAULT_CONTROL_INCLUDE_AUCTIONS", True)
    default_control_include_corporate_actions: bool = _bool("DEFAULT_CONTROL_INCLUDE_CORPORATE_ACTIONS", True)
    default_build_analysis_export: bool = _bool("DEFAULT_BUILD_ANALYSIS_EXPORT", True)
    control_asset_batch_size: int = _int("CONTROL_ASSET_BATCH_SIZE", 100)
    control_feature_upsert_chunk_size: int = _int("CONTROL_FEATURE_UPSERT_CHUNK_SIZE", 500)

    # Entry-feasibility and fixed-time export defaults.
    default_entry_notional: float = _float("DEFAULT_ENTRY_NOTIONAL", 500.0)
    default_entry_reaction_delay_seconds: int = _int("DEFAULT_ENTRY_REACTION_DELAY_SECONDS", 5)
    default_entry_minimum_opportunity_seconds: int = _int("DEFAULT_ENTRY_MINIMUM_OPPORTUNITY_SECONDS", 30)
    default_entry_minimum_gross_edge_pct: float = _float("DEFAULT_ENTRY_MINIMUM_GROSS_EDGE_PCT", 0.0)
    default_entry_require_subsequent_trade: bool = _bool("DEFAULT_ENTRY_REQUIRE_SUBSEQUENT_TRADE", True)

    # Execution-aware full-universe backtest defaults.
    default_backtest_position_notional: float = _float("DEFAULT_BACKTEST_POSITION_NOTIONAL", 500.0)
    default_backtest_reaction_delay_seconds: int = _int("DEFAULT_BACKTEST_REACTION_DELAY_SECONDS", 5)
    default_backtest_stop_loss_pct: float = _float("DEFAULT_BACKTEST_STOP_LOSS_PCT", 5.0)
    default_backtest_slippage_bps: float = _float("DEFAULT_BACKTEST_SLIPPAGE_BPS", 5.0)
    default_backtest_max_trades_per_day: int = _int("DEFAULT_BACKTEST_MAX_TRADES_PER_DAY", 5)
    default_backtest_close_exit_minutes_before: int = _int("DEFAULT_BACKTEST_CLOSE_EXIT_MINUTES_BEFORE", 5)
    backtest_symbol_batch_size: int = _int("BACKTEST_SYMBOL_BATCH_SIZE", 100)

    @staticmethod
    def _require(pairs: tuple[tuple[str, str], ...]) -> None:
        missing = [name for name, value in pairs if not value]
        if missing:
            raise RuntimeError(f"Missing required environment variables: {', '.join(missing)}")

    def validate_web(self) -> None:
        self._require((
            ("SUPABASE_URL", self.supabase_url),
            ("SUPABASE_SERVICE_ROLE_KEY", self.supabase_service_role_key),
            ("APP_PASSWORD", self.app_password),
        ))

    def validate_worker(self) -> None:
        self._require((
            ("ALPACA_API_KEY", self.alpaca_api_key),
            ("ALPACA_SECRET_KEY", self.alpaca_secret_key),
            ("SUPABASE_URL", self.supabase_url),
            ("SUPABASE_SERVICE_ROLE_KEY", self.supabase_service_role_key),
        ))

    def validate_storage(self) -> None:
        self._require((
            ("SUPABASE_URL", self.supabase_url),
            ("SUPABASE_SERVICE_ROLE_KEY", self.supabase_service_role_key),
        ))


settings = Settings()
