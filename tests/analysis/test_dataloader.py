import json
from pathlib import Path

import numpy as np
import polars as pl
import pytest

from poly_data.analysis.dataloader import DatasetSplit, load


def _write_dataset(root: Path, category: str, window: int, horizon: int):
    out = root / category / f"window={window}d" / f"horizon={horizon}d"
    out.mkdir(parents=True, exist_ok=True)
    train = pl.DataFrame({
        "decision_date": [1, 2],
        "market_id": ["m1", "m2"],
        "feat_a": [0.1, 0.2],
        "feat_b": [None, 0.3],
        "target": ["YES", "NO"],
    })
    test = pl.DataFrame({
        "decision_date": [3],
        "market_id": ["m3"],
        "feat_a": [0.4],
        "feat_b": [0.5],
        "target": ["PASS"],
    })
    train.write_parquet(out / "train.parquet")
    test.write_parquet(out / "test.parquet")
    meta = {
        "category": category,
        "window_days": window,
        "horizon_days": horizon,
        "feature_names": ["feat_a", "feat_b"],
        "target_classes": ["YES", "NO", "PASS"],
    }
    (out / "meta.json").write_text(json.dumps(meta))


def test_load_returns_dataset_split(tmp_path: Path):
    _write_dataset(tmp_path, "sports", 7, 1)
    ds = load("sports", window_days=7, horizon_days=1, data_root=tmp_path)
    assert isinstance(ds, DatasetSplit)
    assert ds.feature_names == ["feat_a", "feat_b"]
    assert ds.X_train.shape == (2, 2)
    assert ds.X_test.shape == (1, 2)
    assert list(ds.y_train) == ["YES", "NO"]
    assert list(ds.y_test) == ["PASS"]
    assert np.isnan(ds.X_train[0, 1])


def test_load_missing_dir_raises(tmp_path: Path):
    with pytest.raises(FileNotFoundError):
        load("nope", window_days=7, horizon_days=1, data_root=tmp_path)
