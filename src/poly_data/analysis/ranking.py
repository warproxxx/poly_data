from __future__ import annotations

from typing import Callable

import polars as pl

ScoreFn = Callable[[Callable[[str], pl.Expr]], pl.Expr]


def score_C(c: Callable[[str], pl.Expr]) -> pl.Expr:
    """win_rate * log(max(1, total_won_usd))."""
    return c("win_rate") * pl.max_horizontal(c("total_won_usd"), pl.lit(1.0)).log()


def score_money_ratio(c: Callable[[str], pl.Expr]) -> pl.Expr:
    """total_won_usd / (total_lost_usd + 1e-6)."""
    return c("total_won_usd") / (c("total_lost_usd") + 1e-6)


def score_win_rate(c: Callable[[str], pl.Expr]) -> pl.Expr:
    return c("win_rate")


def score_total_won(c: Callable[[str], pl.Expr]) -> pl.Expr:
    return c("total_won_usd")


def select_top_n(player_stats: pl.DataFrame, *,
                 n: int = 128,
                 min_win_rate: float = 0.5,
                 min_n_bets: int = 20,
                 score_fn: ScoreFn = score_C) -> pl.DataFrame:
    return (
        player_stats
        .filter(pl.col("win_rate").is_not_null())
        .filter(pl.col("win_rate") > min_win_rate)
        .filter(pl.col("n_bets") >= min_n_bets)
        .with_columns(score_fn(pl.col).alias("score"))
        .sort("score", descending=True, nulls_last=True)
        .head(n)
    )
