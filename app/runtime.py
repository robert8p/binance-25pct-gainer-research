from __future__ import annotations

import gc
import logging
import os
import resource
import shutil
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)


def rss_mb() -> float:
    """Return peak resident memory in MiB on Linux (Render)."""
    value = float(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss)
    # Linux reports KiB; macOS reports bytes. Render is Linux, but keep this portable.
    if value > 10_000_000:
        return value / (1024.0 * 1024.0)
    return value / 1024.0


def log_resources(stage: str, *, path: Path | None = None, extra: dict[str, Any] | None = None) -> None:
    payload: dict[str, Any] = {"stage": stage, "rss_mb": round(rss_mb(), 1)}
    if path is not None:
        usage = shutil.disk_usage(path)
        payload.update(
            {
                "disk_free_gb": round(usage.free / (1024 ** 3), 2),
                "disk_used_gb": round(usage.used / (1024 ** 3), 2),
            }
        )
    if extra:
        payload.update(extra)
    logger.info("resource checkpoint %s", payload)


def collect_memory() -> None:
    gc.collect()
    # Ask glibc to release unused arenas where available. Failure is harmless.
    try:
        import ctypes

        libc = ctypes.CDLL("libc.so.6")
        libc.malloc_trim(0)
    except Exception:
        pass


def ensure_disk_headroom(path: Path, minimum_free_bytes: int) -> None:
    path.mkdir(parents=True, exist_ok=True)
    free = shutil.disk_usage(path).free
    if free < minimum_free_bytes:
        raise RuntimeError(
            f"Insufficient worker disk headroom: {free:,} bytes free; "
            f"at least {minimum_free_bytes:,} bytes required"
        )


def env_bool(name: str, default: bool = False) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}
