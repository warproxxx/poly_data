import polars as pl

from poly_data.analysis.positions import compute_player_stats
from poly_data.analysis.ranking import (
    score_C, score_money_ratio, score_total_won, score_win_rate,
    select_top_n,
)


def test_select_top_n_filters_low_n_bets(trades_lf):
    stats = compute_player_stats(trades_lf, player_side="both")
    top = select_top_n(stats, n=10, min_win_rate=0.0, min_n_bets=999)
    assert top.height == 0


def test_select_top_n_filters_min_win_rate(trades_lf):
    stats = compute_player_stats(trades_lf, player_side="both")
    top = select_top_n(stats, n=10, min_win_rate=0.99, min_n_bets=1)
    if top.height > 0:
        assert (top["win_rate"] > 0.99).all()


def test_select_top_n_orders_by_score(trades_lf):
    stats = compute_player_stats(trades_lf, player_side="both")
    top = select_top_n(stats, n=5, min_win_rate=0.0, min_n_bets=1,
                       score_fn=score_C)
    scores = top["score"].to_list()
    assert scores == sorted(scores, reverse=True)


def test_select_top_n_score_money_ratio_returns_score_column(trades_lf):
    stats = compute_player_stats(trades_lf, player_side="both")
    top_money = select_top_n(stats, n=5, min_win_rate=0.0, min_n_bets=1,
                             score_fn=score_money_ratio)
    assert "score" in top_money.columns


def test_score_fns_return_polars_expr(trades_lf):
    stats = compute_player_stats(trades_lf, player_side="both")
    for fn in (score_C, score_money_ratio, score_total_won, score_win_rate):
        expr = fn(pl.col)
        assert isinstance(expr, pl.Expr)
        evaluated = stats.with_columns(expr.alias("s"))
        assert "s" in evaluated.columns
