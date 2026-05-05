"""Smoke-run examples/04-benchmark-polars-vs-duckdb.ipynb via nbclient.

Runs the notebook against ``data_smoke/`` (a tiny self-consistent fixture built
by ``scripts/make_smoke_fixture.py``) so the smoke completes in seconds without
risking OOM.

Usage:
    python scripts/make_smoke_fixture.py     # one-time setup
    python scripts/smoke_nb04.py
"""
from __future__ import annotations

import os
from pathlib import Path

import nbformat
from nbclient import NotebookClient

ROOT = Path(__file__).resolve().parents[1]
NB = ROOT / "examples" / "04-benchmark-polars-vs-duckdb.ipynb"


def main() -> None:
    smoke_root = ROOT / "data_smoke"
    if not (smoke_root / "trades").is_dir():
        raise SystemExit(
            f"smoke fixture missing at {smoke_root}; "
            "run `python scripts/make_smoke_fixture.py` first"
        )
    os.environ["POLY_DATA_ROOT"] = smoke_root.as_posix()

    nb = nbformat.read(NB, as_version=4)
    NotebookClient(
        nb,
        timeout=300,
        resources={"metadata": {"path": NB.parent}},
    ).execute()
    print("nb04 smoke OK")


if __name__ == "__main__":
    main()
