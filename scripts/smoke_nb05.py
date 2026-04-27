"""Smoke-run examples/05-ml-dataset-and-baseline.ipynb via nbclient.

Requires:
- `python update_all.py` has populated `data/trades/` and `data/markets/`.
- `pip install -e .[analysis,dev]` (xgboost + scikit-learn required).

Usage: python scripts/smoke_nb05.py
"""
from __future__ import annotations

from pathlib import Path

import nbformat
from nbclient import NotebookClient

NB = Path(__file__).resolve().parents[1] / "examples" \
                  / "05-ml-dataset-and-baseline.ipynb"


def main() -> None:
    nb = nbformat.read(NB, as_version=4)
    NotebookClient(
        nb,
        timeout=1800,
        resources={"metadata": {"path": NB.parent}},
    ).execute()
    print("nb05 smoke OK")


if __name__ == "__main__":
    main()
