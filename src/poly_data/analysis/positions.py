from __future__ import annotations

from typing import Literal

import polars as pl

PlayerSide = Literal["both", "maker", "taker"]
_VALID_SIDES = {"both", "maker", "taker"}


def expand_to_positions(trades: pl.LazyFrame, *,
                        player_side: PlayerSide = "both") -> pl.LazyFrame:
    """Expand each fill to 1 or 2 rows (one per participant), signed for direction.

    Output cols: timestamp, market_id, player, token_side, signed_tokens,
                 signed_usd, direction, price.
    """
    if player_side not in _VALID_SIDES:
        raise ValueError(f"player_side must be one of {_VALID_SIDES}, got {player_side!r}")

    base = trades.select([
        "timestamp", "market_id", "maker", "taker", "nonusdc_side",
        "maker_direction", "taker_direction", "price", "usd_amount",
        "token_amount",
    ])

    maker_rows = base.with_columns([
        pl.col("maker").alias("player"),
        pl.col("nonusdc_side").alias("token_side"),
        pl.when(pl.col("maker_direction") == "BUY")
          .then(pl.col("token_amount"))
          .otherwise(-pl.col("token_amount"))
          .alias("signed_tokens"),
        pl.when(pl.col("maker_direction") == "BUY")
          .then(-pl.col("usd_amount"))
          .otherwise(pl.col("usd_amount"))
          .alias("signed_usd"),
        pl.col("maker_direction").alias("direction"),
    ]).select(["timestamp", "market_id", "player", "token_side",
               "signed_tokens", "signed_usd", "direction", "price"])

    taker_rows = base.with_columns([
        pl.col("taker").alias("player"),
        pl.col("nonusdc_side").alias("token_side"),
        pl.when(pl.col("taker_direction") == "BUY")
          .then(pl.col("token_amount"))
          .otherwise(-pl.col("token_amount"))
          .alias("signed_tokens"),
        pl.when(pl.col("taker_direction") == "BUY")
          .then(-pl.col("usd_amount"))
          .otherwise(pl.col("usd_amount"))
          .alias("signed_usd"),
        pl.col("taker_direction").alias("direction"),
    ]).select(["timestamp", "market_id", "player", "token_side",
               "signed_tokens", "signed_usd", "direction", "price"])

    if player_side == "both":
        return pl.concat([maker_rows, taker_rows])
    if player_side == "maker":
        return maker_rows
    return taker_rows


def positions_table(trades: pl.LazyFrame, *,
                    player_side: PlayerSide = "both") -> pl.DataFrame:
    expanded = expand_to_positions(trades, player_side=player_side)
    return (
        expanded.group_by(["player", "market_id", "token_side"])
        .agg(
            pl.col("signed_tokens").sum().alias("net_tokens"),
            pl.col("signed_usd").sum().alias("net_usd"),
            (pl.col("direction") == "BUY").sum().alias("n_buys"),
            (pl.col("direction") == "SELL").sum().alias("n_sells"),
            pl.col("timestamp").min().alias("first_ts"),
            pl.col("timestamp").max().alias("last_ts"),
        )
        .collect(engine="streaming")
    )


def _last_price_per_side(trades: pl.LazyFrame) -> pl.LazyFrame:
    """Lazy: one row per (market_id, nonusdc_side) with chronologically last price.

    Uses max-timestamp anti-join instead of `gather(arg_max)` because the latter
    is not partitionable in the polars-stream engine and silently falls back to
    a single in-memory hash table that materialises the full price column per
    group — ~2.4 GB on 50M trades. Both group-by passes here ARE partitionable,
    so `.collect(engine="streaming")` actually streams.
    """
    max_ts = (
        trades
        .group_by(["market_id", "nonusdc_side"])
        .agg(pl.col("timestamp").max().alias("_mt"))
    )
    return (
        trades
        .join(
            max_ts,
            left_on=["market_id", "nonusdc_side", "timestamp"],
            right_on=["market_id", "nonusdc_side", "_mt"],
            how="inner",
        )
        .group_by(["market_id", "nonusdc_side"])
        .agg(pl.col("price").first().alias("last_price"))
    )


def _resolution_from_last(last: pl.DataFrame, *,
                          win_threshold: float) -> pl.DataFrame:
    pivoted = last.pivot(
        index="market_id", on="nonusdc_side", values="last_price"
    )
    if "token1" not in pivoted.columns:
        pivoted = pivoted.with_columns(pl.lit(None, dtype=pl.Float64).alias("token1"))
    if "token2" not in pivoted.columns:
        pivoted = pivoted.with_columns(pl.lit(None, dtype=pl.Float64).alias("token2"))
    return pivoted.with_columns([
        pl.when(pl.col("token1") >= win_threshold).then(pl.lit("token1"))
        .when(pl.col("token2") >= win_threshold).then(pl.lit("token2"))
        .otherwise(pl.lit("open"))
        .alias("winner_token")
    ])


def market_resolution(trades: pl.LazyFrame, *,
                      win_threshold: float = 0.98) -> pl.DataFrame:
    """One row per market: market_id, token1, token2 (last prices), winner_token."""
    last = _last_price_per_side(trades).collect(engine="streaming")
    return _resolution_from_last(last, win_threshold=win_threshold)


def label_outcomes(positions: pl.DataFrame,
                   resolutions: pl.DataFrame,
                   *,
                   last_prices: pl.DataFrame) -> pl.DataFrame:
    """Add `outcome` and `pnl_usd` columns to a positions table.

    `last_prices` columns: market_id, token_side, last_price.
    `resolutions` columns: market_id, winner_token (+ side prices ignored).
    """
    out = (
        positions.join(resolutions.select(["market_id", "winner_token"]),
                       on="market_id", how="left")
        .join(last_prices, on=["market_id", "token_side"], how="left")
    )
    out = out.with_columns([
        pl.when(pl.col("net_tokens") == 0).then(pl.lit("flat"))
        .when(pl.col("winner_token") == "open").then(pl.lit("open"))
        .when((pl.col("token_side") == pl.col("winner_token")) & (pl.col("net_tokens") > 0))
        .then(pl.lit("won"))
        .when((pl.col("token_side") != pl.col("winner_token")) & (pl.col("net_tokens") > 0))
        .then(pl.lit("lost"))
        .otherwise(pl.lit("flat"))
        .alias("outcome")
    ])
    return out.with_columns([
        pl.when(pl.col("outcome") == "won")
          .then(pl.col("net_usd") + pl.col("net_tokens") * 1.0)
        .when(pl.col("outcome") == "lost")
          .then(pl.col("net_usd") + pl.col("net_tokens") * 0.0)
        .when(pl.col("outcome") == "open")
          .then(pl.col("net_usd") + pl.col("net_tokens") * pl.col("last_price"))
        .otherwise(pl.col("net_usd"))
        .alias("pnl_usd")
    ])


def player_aggregates(labelled_positions: pl.DataFrame) -> pl.DataFrame:
    decided = labelled_positions.filter(pl.col("outcome").is_in(["won", "lost"]))
    grp = (
        decided.group_by("player")
        .agg(
            (pl.col("outcome") == "won").sum().alias("n_won"),
            (pl.col("outcome") == "lost").sum().alias("n_lost"),
            pl.col("pnl_usd").filter(pl.col("outcome") == "won").sum()
              .alias("total_won_usd"),
            pl.col("pnl_usd").filter(pl.col("outcome") == "lost").sum().abs()
              .alias("total_lost_usd"),
            pl.col("pnl_usd").sum().alias("net_usd_pnl"),
        )
    )
    return grp.with_columns([
        (pl.col("n_won") + pl.col("n_lost")).alias("n_bets"),
        (pl.col("n_won") /
            (pl.col("n_won") + pl.col("n_lost")).cast(pl.Float64)
        ).alias("win_rate"),
    ]).with_columns([
        (pl.col("win_rate") *
            pl.max_horizontal(pl.col("total_won_usd"), pl.lit(1.0)).log()
        ).alias("score_C"),
    ])


def compute_player_stats(trades: pl.LazyFrame, *,
                         player_side: PlayerSide = "both",
                         win_threshold: float = 0.98) -> pl.DataFrame:
    last = _last_price_per_side(trades).collect(engine="streaming")
    res = _resolution_from_last(last, win_threshold=win_threshold)
    pos = positions_table(trades, player_side=player_side)
    last_for_join = last.rename({"nonusdc_side": "token_side"})
    labelled = label_outcomes(pos, res, last_prices=last_for_join)
    return player_aggregates(labelled)
