from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock

import polars as pl
import pytest

from poly_data.io.parquet_store import ParquetStore
from poly_data.distribute.huggingface import push_snapshot


def test_push_snapshot_calls_upload_large_folder(tmp_path: Path,
                                                 sample_orderfilled_df: pl.DataFrame,
                                                 mocker) -> None:
    store = ParquetStore(tmp_path / "data")
    store.append("orderFilled", sample_orderfilled_df)

    api = MagicMock()
    api.upload_large_folder.return_value = "https://hf.co/datasets/u/p/commit/abc"
    mocker.patch("poly_data.distribute.huggingface.HfApi", return_value=api)

    url = push_snapshot(store, repo_id="u/p")
    assert url == "https://hf.co/datasets/u/p/commit/abc"

    args, kwargs = api.upload_large_folder.call_args
    assert kwargs["repo_id"] == "u/p"
    assert kwargs["repo_type"] == "dataset"
    assert Path(kwargs["folder_path"]) == store.root


def test_push_snapshot_passes_allow_patterns_for_sources(tmp_path: Path,
                                                        mocker) -> None:
    store = ParquetStore(tmp_path / "data")

    api = MagicMock()
    api.upload_large_folder.return_value = "u"
    mocker.patch("poly_data.distribute.huggingface.HfApi", return_value=api)

    push_snapshot(store, repo_id="u/p", sources=["orderFilled"])

    _, kwargs = api.upload_large_folder.call_args
    assert kwargs["allow_patterns"] == ["orderFilled/**"]
