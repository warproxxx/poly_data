"""Scale regression: ``compute_player_stats`` must stream — peak RSS bounded.

The OOM bug fixed in commit 859a716 was a non-partitionable
``gather(arg_max)`` aggregate that materialised the full per-group price column
in a single in-memory hash table. This test would have caught it: synthesise
~100k fills across 200 markets × 500 players, run ``compute_player_stats``, and
assert peak RSS above baseline stays under a generous 500 MB cap.

If this test starts failing, someone re-introduced a non-streamable aggregate
(or another full-frame materialisation) into the player-stats pipeline. Run
with ``POLARS_VERBOSE=1`` to find the offending operator.
"""
from __future__ import annotations

import gc
import os

import numpy as np
import polars as pl
import psutil
import pytest

from poly_data.analysis.positions import compute_player_stats


PEAK_RSS_LIMIT_MB = 500


def _synth_fills(
    *, n_rows: int, n_markets: int, n_players: int, seed: int = 0
) -> pl.DataFrame:
    rng = np.random.default_rng(seed)
    market_ids = [f"m{i:04d}" for i in range(n_markets)]
    player_ids = [f"0x{i:040x}" for i in range(n_players)]
    df = pl.DataFrame({
        "timestamp": rng.integers(1_700_000_000, 1_750_000_000,
                                  size=n_rows, dtype=np.int64),
        "market_id": rng.choice(market_ids, size=n_rows),
        "maker": rng.choice(player_ids, size=n_rows),
        "taker": rng.choice(player_ids, size=n_rows),
        "nonusdc_side": rng.choice(["token1", "token2"], size=n_rows),
        "maker_direction": rng.choice(["BUY", "SELL"], size=n_rows),
        "taker_direction": rng.choice(["BUY", "SELL"], size=n_rows),
        "price": rng.uniform(0.0, 1.0, size=n_rows),
        "usd_amount": rng.uniform(0.1, 1000.0, size=n_rows),
        "token_amount": rng.uniform(1.0, 10000.0, size=n_rows),
        "transactionHash": [f"0xt{i:062x}" for i in range(n_rows)],
    })
    return df


@pytest.mark.skipif(
    os.environ.get("CI") == "true",
    reason="scale test slow + RSS-sensitive; run locally",
)
def test_compute_player_stats_peak_rss_under_500mb(tmp_path):
    fills = _synth_fills(n_rows=100_000, n_markets=200, n_players=500, seed=42)
    parquet_path = tmp_path / "trades.parquet"
    fills.write_parquet(parquet_path)
    del fills
    gc.collect()

    proc = psutil.Process()
    baseline = proc.memory_info().rss
    peak = baseline

    trades_lf = pl.scan_parquet(str(parquet_path))
    stats = compute_player_stats(trades_lf, player_side="both")

    cur = proc.memory_info().rss
    if cur > peak:
        peak = cur

    delta_mb = (peak - baseline) / (1024 * 1024)
    assert stats.height > 0, "stats should not be empty for synthetic data"
    assert delta_mb < PEAK_RSS_LIMIT_MB, (
        f"compute_player_stats peak RSS delta = {delta_mb:.0f} MB "
        f"(>{PEAK_RSS_LIMIT_MB} MB cap). Likely a non-streamable aggregate "
        "regressed into the pipeline; run with POLARS_VERBOSE=1 to find it."
    )
