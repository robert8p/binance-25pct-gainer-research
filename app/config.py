from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class Settings:
    supabase_url: str
    supabase_service_role_key: str
    app_password: str
    storage_bucket: str
    binance_api_base_urls: tuple[str, ...]
    poll_seconds: int
    temp_data_dir: Path

    @classmethod
    def from_env(cls) -> "Settings":
        url = os.getenv("SUPABASE_URL", "").rstrip("/")
        key = os.getenv("SUPABASE_SERVICE_ROLE_KEY", "").strip()
        password = os.getenv("APP_PASSWORD", "").strip()
        if not url or not key:
            raise RuntimeError("SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY must be set")
        if not password:
            raise RuntimeError("APP_PASSWORD must be set")
        bases = tuple(
            x.strip().rstrip("/")
            for x in os.getenv(
                "BINANCE_API_BASE_URLS",
                "https://api.binance.com,https://data-api.binance.vision",
            ).split(",")
            if x.strip()
        )
        temp = Path(os.getenv("TEMP_DATA_DIR", "/tmp/binance-gainer"))
        temp.mkdir(parents=True, exist_ok=True)
        return cls(
            supabase_url=url,
            supabase_service_role_key=key,
            app_password=password,
            storage_bucket=os.getenv("SUPABASE_STORAGE_BUCKET", "binance-25pct-gainer-research"),
            binance_api_base_urls=bases,
            poll_seconds=max(3, int(os.getenv("POLL_SECONDS", "10"))),
            temp_data_dir=temp,
        )
