import math

import polars as pl
import pytest

from poly_data.analysis.positions import (
    compute_player_stats,
    expand_to_positions,
    label_outcomes,
    market_resolution,
    player_aggregates,
    positions_table,
)


def test_expand_both_doubles_row_count(trades_lf):
    expanded = expand_to_positions(trades_lf, player_side="both").collect()
    raw = trades_lf.collect()
    assert expanded.height == 2 * raw.height


def test_expand_taker_only_matches_row_count(trades_lf):
    expanded = expand_to_positions(trades_lf, player_side="taker").collect()
    raw = trades_lf.collect()
    assert expanded.height == raw.height


def test_positions_table_is_per_player_market_side(trades_lf):
    pos = positions_table(trades_lf, player_side="both")
    expected = {"player", "market_id", "token_side", "net_tokens",
                "net_usd", "n_buys", "n_sells", "first_ts", "last_ts"}
    assert expected.issubset(set(pos.columns))
    assert pos.unique(subset=["player", "market_id", "token_side"]).height \
        == pos.height


def test_market_resolution_classifies_three_markets(trades_lf):
    res = market_resolution(trades_lf).sort("market_id")
    winners = dict(zip(res["market_id"].to_list(), res["winner_token"].to_list()))
    assert winners == {"M1": "token1", "M2": "token2", "M3": "open"}


def test_label_outcomes_assigns_won_lost_open(trades_lf):
    pos = positions_table(trades_lf, player_side="both")
    res = market_resolution(trades_lf)
    last = (
        trades_lf.sort("timestamp")
        .group_by(["market_id", "nonusdc_side"])
        .agg(pl.col("price").last().alias("last_price"))
        .collect()
        .rename({"nonusdc_side": "token_side"})
    )
    labelled = label_outcomes(pos, res, last_prices=last)
    outcomes = set(labelled["outcome"].unique().to_list())
    assert "won" in outcomes
    assert "lost" in outcomes
    assert "open" in outcomes
    open_markets = labelled.filter(pl.col("outcome") == "open")["market_id"]\
        .unique().to_list()
    assert open_markets == ["M3"]


def test_player_aggregates_columns(trades_lf):
    stats = compute_player_stats(trades_lf, player_side="both")
    expected = {"player", "n_won", "n_lost", "n_bets", "win_rate",
                "total_won_usd", "total_lost_usd", "net_usd_pnl", "score_C"}
    assert expected.issubset(set(stats.columns))


def test_player_aggregates_score_c_is_finite(trades_lf):
    stats = compute_player_stats(trades_lf, player_side="both")
    for v in stats["score_C"].to_list():
        if v is None:
            continue
        assert math.isfinite(v)


def test_player_aggregates_n_bets_is_won_plus_lost(trades_lf):
    stats = compute_player_stats(trades_lf, player_side="both")
    for row in stats.iter_rows(named=True):
        assert row["n_bets"] == row["n_won"] + row["n_lost"]


def test_invalid_player_side_raises(trades_lf):
    with pytest.raises(ValueError):
        expand_to_positions(trades_lf, player_side="bogus")
