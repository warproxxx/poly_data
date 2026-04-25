from __future__ import annotations

import logging

from poly_data.io.parquet_store import ParquetStore

logger = logging.getLogger(__name__)


def compact_all(store: ParquetStore, source: str) -> dict[str, int]:
    """Compact every (year, month) partition for `source`."""
    base = store.root / source
    if not base.is_dir():
        return {}

    out: dict[str, int] = {}
    for year_dir in sorted(base.glob("year=*")):
        try:
            year = int(year_dir.name.split("=", 1)[1])
        except ValueError:
            continue
        for month_dir in sorted(year_dir.glob("month=*")):
            try:
                month = int(month_dir.name.split("=", 1)[1])
            except ValueError:
                continue
            n = store.compact(source, year, month)
            if n == 0:
                files = list(month_dir.glob("*.parquet"))
                if files:
                    import polars as pl
                    n = int(
                        pl.scan_parquet([str(f) for f in files])
                        .select(pl.len()).collect().item()
                    )
            out[f"{year}-{month}"] = n
            logger.info("compacted %s %d-%d → %d rows", source, year, month, n)
    return out
