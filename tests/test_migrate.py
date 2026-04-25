from __future__ import annotations

import csv
from pathlib import Path

import polars as pl

from scripts.migrate_csv_to_parquet import migrate


def _write_csv(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        w.writeheader()
        w.writerows(rows)


def test_migrate_orderfilled_csv_to_parquet(tmp_path: Path) -> None:
    src = tmp_path / "goldsky" / "orderFilled.csv"
    _write_csv(src, [
        {
            "timestamp": 1700000000, "maker": "0xa", "makerAssetId": "0",
            "makerAmountFilled": "1000000", "taker": "0xb", "takerAssetId": "1",
            "takerAmountFilled": "2000000", "transactionHash": "0xt",
            "id": "i1",
        },
        {
            "timestamp": 1700000000, "maker": "0xa", "makerAssetId": "0",
            "makerAmountFilled": "1000000", "taker": "0xb", "takerAssetId": "1",
            "takerAmountFilled": "2000000", "transactionHash": "0xt",
            "id": "i1",
        },
    ])

    out = tmp_path / "data"
    n = migrate(repo_root=tmp_path, store_root=out)
    assert n["orderFilled"] == 1

    df = pl.scan_parquet(str(out / "orderFilled" / "**/*.parquet")).collect()
    assert df["id"].to_list() == ["i1"]
