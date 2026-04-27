"""Smoke-run examples/04-benchmark-polars-vs-duckdb.ipynb via nbclient.

Requires:
- `python update_all.py` has populated `data/trades/`.
- `pip install -e .[analysis,dev]` (xgboost not strictly required for nb04).

Usage: python scripts/smoke_nb04.py
"""
from __future__ import annotations

from pathlib import Path

import nbformat
from nbclient import NotebookClient

NB = Path(__file__).resolve().parents[1] / "examples" \
                  / "04-benchmark-polars-vs-duckdb.ipynb"


def main() -> None:
    nb = nbformat.read(NB, as_version=4)
    NotebookClient(
        nb,
        timeout=900,
        resources={"metadata": {"path": NB.parent}},
    ).execute()
    print("nb04 smoke OK")


if __name__ == "__main__":
    main()
