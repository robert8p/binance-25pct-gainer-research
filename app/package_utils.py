from __future__ import annotations

import os
import shutil
import uuid
import zipfile
from pathlib import Path
from typing import Callable, Iterable, Sequence

# Free Supabase projects permit a maximum object size of 50 MB. Keep a
# deliberate safety margin for multipart/proxy overhead and decimal/binary
# differences. This can be raised on paid projects through Render env vars.
MAX_STORAGE_OBJECT_BYTES = int(os.getenv("MAX_STORAGE_OBJECT_BYTES", "45000000"))


class PackageTooLargeError(RuntimeError):
    pass


def zip_directory(source: Path, destination: Path) -> None:
    with zipfile.ZipFile(destination, "w", compression=zipfile.ZIP_DEFLATED, compresslevel=9) as archive:
        for path in sorted(source.rglob("*")):
            if path.is_file():
                archive.write(path, path.relative_to(source))


def build_grouped_zip_parts(
    *,
    work_dir: Path,
    base_name: str,
    group_ids: Sequence[str],
    writer: Callable[[Path, list[str]], None],
    max_bytes: int | None = None,
) -> list[Path]:
    """Build independently readable ZIP parts, recursively splitting groups.

    Each output is a normal ZIP containing a complete, row-partitioned package;
    parts are not opaque binary fragments. This lets ChatGPT analyse every part
    directly while keeping each Supabase Storage object below the plan limit.
    """
    limit = int(max_bytes or MAX_STORAGE_OBJECT_BYTES)
    if limit < 1_000_000:
        raise ValueError("MAX_STORAGE_OBJECT_BYTES must be at least 1,000,000")

    ids = list(dict.fromkeys(str(value) for value in group_ids))
    if not ids:
        ids = ["__EMPTY_PACKAGE__"]

    scratch = work_dir / f".{base_name}_parts"
    shutil.rmtree(scratch, ignore_errors=True)
    scratch.mkdir(parents=True, exist_ok=True)
    accepted: list[Path] = []

    def build(candidate_ids: list[str]) -> None:
        token = uuid.uuid4().hex
        folder = scratch / token
        archive = scratch / f"{token}.zip"
        folder.mkdir(parents=True, exist_ok=True)
        writer(folder, candidate_ids)
        zip_directory(folder, archive)
        size = archive.stat().st_size
        shutil.rmtree(folder, ignore_errors=True)

        if size <= limit:
            accepted.append(archive)
            return
        archive.unlink(missing_ok=True)
        if len(candidate_ids) <= 1:
            raise PackageTooLargeError(
                f"A single matched group produced {size:,} bytes, above the "
                f"configured {limit:,}-byte object limit."
            )
        midpoint = len(candidate_ids) // 2
        build(candidate_ids[:midpoint])
        build(candidate_ids[midpoint:])

    build(ids)
    output: list[Path] = []
    total = len(accepted)
    for number, source in enumerate(accepted, start=1):
        name = f"{base_name}.zip" if total == 1 else f"{base_name}_part{number:03d}_of_{total:03d}.zip"
        destination = work_dir / name
        destination.unlink(missing_ok=True)
        source.replace(destination)
        output.append(destination)
    shutil.rmtree(scratch, ignore_errors=True)
    return output
