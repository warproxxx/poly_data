from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import numpy as np
import polars as pl


@dataclass
class DatasetSplit:
    X_train: np.ndarray
    y_train: np.ndarray
    X_test: np.ndarray
    y_test: np.ndarray
    feature_names: list[str]
    meta: dict[str, Any]


def load(category: str, *, window_days: int, horizon_days: int,
         data_root: Path | str = Path("data/ml")) -> DatasetSplit:
    """Load a saved ML split eagerly into numpy arrays.

    Eager `pl.read_parquet` is intentional here: the dataset is bounded by
    ``top_n_players`` × ``decision_dates`` × ``markets-in-category`` (typically
    < 100k rows in practice), and downstream consumers (sklearn / xgboost)
    require numpy arrays anyway.
    """
    root = Path(data_root) / category / f"window={window_days}d" \
                            / f"horizon={horizon_days}d"
    if not root.is_dir():
        raise FileNotFoundError(f"dataset directory missing: {root}")
    meta_path = root / "meta.json"
    train_path = root / "train.parquet"
    test_path = root / "test.parquet"
    for p in (meta_path, train_path, test_path):
        if not p.is_file():
            raise FileNotFoundError(f"required file missing: {p}")

    meta = json.loads(meta_path.read_text())
    feature_names: list[str] = list(meta["feature_names"])

    train = pl.read_parquet(train_path)
    test = pl.read_parquet(test_path)

    def _xy(df: pl.DataFrame) -> tuple[np.ndarray, np.ndarray]:
        if df.height == 0:
            return (np.empty((0, len(feature_names)), dtype=np.float64),
                    np.empty((0,), dtype=object))
        X = df.select(feature_names).to_numpy().astype(np.float64)
        y = df["target"].to_numpy()
        return X, y

    X_train, y_train = _xy(train)
    X_test, y_test = _xy(test)
    return DatasetSplit(
        X_train=X_train, y_train=y_train,
        X_test=X_test, y_test=y_test,
        feature_names=feature_names, meta=meta,
    )
