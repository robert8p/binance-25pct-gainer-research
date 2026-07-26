"""Minimal pyarrow import shim for the build container.

Render installs real pyarrow from requirements.txt. The local validation image used to
assemble this package does not provide binary pyarrow wheels, while the unit tests do
not exercise Parquet I/O. This shim permits collection without masking production use.
"""
from __future__ import annotations

import sys
import types

try:
    import pyarrow  # noqa: F401
except ModuleNotFoundError:
    pa = types.ModuleType("pyarrow")
    pq = types.ModuleType("pyarrow.parquet")

    class _Schema:
        def __init__(self, fields=()):
            self.fields = list(fields)
            self.names = [f[0] if isinstance(f, tuple) else str(f) for f in self.fields]

    class _Table:
        @classmethod
        def from_pylist(cls, rows, schema=None):
            obj = cls(); obj.rows = list(rows); obj.schema = schema; return obj

    pa.Schema = _Schema
    pa.Table = _Table
    pa.schema = lambda fields=(): _Schema(fields)
    pa.string = lambda: "string"
    pa.float64 = lambda: "float64"
    pa.int64 = lambda: "int64"
    pa.int32 = lambda: "int32"
    pa.int8 = lambda: "int8"
    pa.timestamp = lambda *args, **kwargs: "timestamp"
    pa.list_ = lambda value: ("list", value)
    pa.array = lambda values, type=None: list(values)
    pa.table = lambda mapping, schema=None: mapping
    pa.__version__ = "0.0-test-shim"
    pq.write_table = lambda *args, **kwargs: None
    pq.read_table = lambda *args, **kwargs: None
    pq.ParquetFile = object
    sys.modules["pyarrow"] = pa
    sys.modules["pyarrow.parquet"] = pq
