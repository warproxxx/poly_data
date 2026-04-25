from __future__ import annotations

import json
from pathlib import Path

import polars as pl
import responses

from poly_data.io.parquet_store import ParquetStore
from poly_data.ingest.markets import update_markets


def _market(i: int, ts: int) -> dict:
    return {
        "id": f"m{i}",
        "createdAt": ts,
        "question": f"Q{i}",
        "outcomes": json.dumps(["YES", "NO"]),
        "clobTokenIds": json.dumps([f"tok_{i}_a", f"tok_{i}_b"]),
        "negRiskAugmented": False,
        "negRiskOther": False,
        "slug": f"q{i}",
        "conditionId": f"c{i}",
        "volume": 100 + i,
        "events": [{"ticker": f"T{i}"}],
        "closedTime": "",
    }


@responses.activate
def test_update_markets_writes_partitioned_parquet(tmp_path: Path) -> None:
    store = ParquetStore(tmp_path / "data")

    responses.add(
        responses.GET,
        "https://gamma-api.polymarket.com/markets",
        json=[_market(1, 1700000000), _market(2, 1700000100)],
        status=200,
    )
    responses.add(
        responses.GET,
        "https://gamma-api.polymarket.com/markets",
        json=[],
        status=200,
    )

    n = update_markets(store, batch_size=2)
    assert n == 2

    df = store.scan("markets").collect().sort("id")
    assert df["id"].to_list() == ["m1", "m2"]
    assert df["token1"].to_list() == ["tok_1_a", "tok_2_a"]


@responses.activate
def test_update_markets_offset_uses_api_count_not_parsed(tmp_path: Path) -> None:
    """Regression: previous code did `current_offset += batch_count` which drifted
    when individual rows failed to parse. Use len(markets) returned by API."""
    store = ParquetStore(tmp_path / "data")

    bad = _market(99, 1700000000)
    bad["clobTokenIds"] = "not json"

    responses.add(
        responses.GET,
        "https://gamma-api.polymarket.com/markets",
        json=[bad, _market(2, 1700000100)],
        status=200,
    )
    responses.add(
        responses.GET,
        "https://gamma-api.polymarket.com/markets",
        json=[],
        status=200,
    )

    n = update_markets(store, batch_size=2)
    assert n == 1

    second = responses.calls[1].request
    assert "offset=2" in second.url


@responses.activate
def test_update_markets_resumes_from_existing_row_count(tmp_path: Path) -> None:
    store = ParquetStore(tmp_path / "data")
    seed = pl.DataFrame([
        {
            "id": "m0", "createdAt": 1690000000, "timestamp": 1690000000,
            "question": "old", "answer1": "Y", "answer2": "N", "neg_risk": False,
            "market_slug": "old", "token1": "x", "token2": "y",
            "condition_id": "c0", "volume": 0, "ticker": "X", "closedTime": "",
        }
    ])
    store.append("markets", seed)

    responses.add(
        responses.GET,
        "https://gamma-api.polymarket.com/markets",
        json=[],
        status=200,
    )

    update_markets(store, batch_size=10)
    assert "offset=1" in responses.calls[0].request.url


@responses.activate
def test_update_missing_tokens_writes_to_missing_markets(tmp_path: Path) -> None:
    store = ParquetStore(tmp_path / "data")

    market = _market(7, 1700000000)
    responses.add(
        responses.GET,
        "https://gamma-api.polymarket.com/markets",
        json=[market],
        status=200,
    )

    from poly_data.ingest.markets import update_missing_tokens
    n = update_missing_tokens(store, ["tok_7_a"])
    assert n == 1

    df = store.scan("missing_markets").collect()
    assert df["id"].to_list() == ["m7"]


@responses.activate
def test_update_missing_tokens_skips_already_present(tmp_path: Path) -> None:
    store = ParquetStore(tmp_path / "data")
    seed = pl.DataFrame([{
        "id": "m7", "createdAt": "1700000000", "question": "x",
        "answer1": "a", "answer2": "b", "neg_risk": False,
        "market_slug": "s", "token1": "tok_7_a", "token2": "tok_7_b",
        "condition_id": "c", "volume": "0", "ticker": "T",
        "closedTime": "", "timestamp": 1700000000,
    }])
    store.append("missing_markets", seed)

    responses.add(
        responses.GET,
        "https://gamma-api.polymarket.com/markets",
        json=[_market(7, 1700000000)],
        status=200,
    )

    from poly_data.ingest.markets import update_missing_tokens
    n = update_missing_tokens(store, ["tok_7_a"])
    assert n == 0
