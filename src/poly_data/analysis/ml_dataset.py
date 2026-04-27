from __future__ import annotations

import math
from datetime import datetime, timedelta, timezone
from typing import Any

import polars as pl

from poly_data.analysis.positions import market_resolution

FEATURE_NAMES: list[str] = [
    "n_players_active",
    "mean_buy_price_t1", "mean_buy_price_t2",
    "mean_sell_price_t1", "mean_sell_price_t2",
    "usd_volume_t1", "usd_volume_t2",
    "net_token1_position",
    "n_buys_t1", "n_sells_t1", "n_buys_t2", "n_sells_t2",
    "last_price_t1",
    "consensus_score",
]


def _parse_iso_to_unix(s: str) -> int:
    if not s:
        return 0
    try:
        return int(datetime.fromisoformat(s.replace("Z", "+00:00"))
                   .astimezone(timezone.utc).timestamp())
    except Exception:
        return 0


def _decision_date_grid(min_ts: int, max_ts: int) -> list[int]:
    """Daily 00:00 UTC strides between min_ts and max_ts inclusive."""
    if min_ts >= max_ts:
        return []
    start = datetime.fromtimestamp(min_ts, tz=timezone.utc)\
        .replace(hour=0, minute=0, second=0, microsecond=0)
    end = datetime.fromtimestamp(max_ts, tz=timezone.utc)\
        .replace(hour=0, minute=0, second=0, microsecond=0)
    out: list[int] = []
    cur = start
    while cur <= end:
        out.append(int(cur.timestamp()))
        cur += timedelta(days=1)
    return out


def _empty_features_df() -> pl.DataFrame:
    schema: dict[str, Any] = {"market_id": pl.Utf8}
    for name in FEATURE_NAMES:
        schema[name] = pl.Float64
    return pl.DataFrame(schema=schema)


def _features_for_window(window_trades: pl.DataFrame,
                         player_set: set[str]) -> pl.DataFrame:
    """Per-market panel features for one decision window."""
    if window_trades.height == 0:
        return _empty_features_df()

    ps = list(player_set)
    df = window_trades.with_columns([
        pl.col("maker").is_in(ps).alias("maker_active"),
        pl.col("taker").is_in(ps).alias("taker_active"),
    ])
    df = df.with_columns(
        (pl.col("maker_active") | pl.col("taker_active")).alias("any_active")
    ).filter(pl.col("any_active"))

    if df.height == 0:
        return _empty_features_df()

    df = df.with_columns([
        pl.when((pl.col("nonusdc_side") == "token1")
                & (pl.col("maker_direction") == "BUY"))
          .then(pl.col("token_amount"))
        .when((pl.col("nonusdc_side") == "token1")
              & (pl.col("maker_direction") == "SELL"))
          .then(-pl.col("token_amount"))
        .otherwise(0.0)
        .alias("signed_t1"),
        pl.when(pl.col("maker_active")).then(pl.col("maker"))
          .when(pl.col("taker_active")).then(pl.col("taker"))
          .otherwise(None)
          .alias("active_player"),
    ])

    by_market = df.group_by("market_id").agg([
        pl.col("active_player").n_unique().alias("n_players_active"),
        pl.col("price").filter(
            (pl.col("nonusdc_side") == "token1") & (pl.col("maker_direction") == "BUY")
        ).mean().alias("mean_buy_price_t1"),
        pl.col("price").filter(
            (pl.col("nonusdc_side") == "token2") & (pl.col("maker_direction") == "BUY")
        ).mean().alias("mean_buy_price_t2"),
        pl.col("price").filter(
            (pl.col("nonusdc_side") == "token1") & (pl.col("maker_direction") == "SELL")
        ).mean().alias("mean_sell_price_t1"),
        pl.col("price").filter(
            (pl.col("nonusdc_side") == "token2") & (pl.col("maker_direction") == "SELL")
        ).mean().alias("mean_sell_price_t2"),
        pl.col("usd_amount").filter(
            pl.col("nonusdc_side") == "token1"
        ).sum().alias("usd_volume_t1"),
        pl.col("usd_amount").filter(
            pl.col("nonusdc_side") == "token2"
        ).sum().alias("usd_volume_t2"),
        pl.col("signed_t1").sum().alias("net_token1_position"),
        ((pl.col("nonusdc_side") == "token1")
         & (pl.col("maker_direction") == "BUY")).sum().alias("n_buys_t1"),
        ((pl.col("nonusdc_side") == "token1")
         & (pl.col("maker_direction") == "SELL")).sum().alias("n_sells_t1"),
        ((pl.col("nonusdc_side") == "token2")
         & (pl.col("maker_direction") == "BUY")).sum().alias("n_buys_t2"),
        ((pl.col("nonusdc_side") == "token2")
         & (pl.col("maker_direction") == "SELL")).sum().alias("n_sells_t2"),
        pl.col("price").filter(pl.col("nonusdc_side") == "token1")
          .last().alias("last_price_t1"),
    ])

    per_player_t1 = (
        df.filter(pl.col("nonusdc_side") == "token1")
          .group_by(["market_id", "active_player"])
          .agg(pl.col("signed_t1").sum().alias("net"))
    )
    consensus = (
        per_player_t1.group_by("market_id")
        .agg(
            ((pl.col("net") > 0).sum().cast(pl.Float64) /
             pl.len().cast(pl.Float64)).alias("consensus_score")
        )
    )
    out = by_market.join(consensus, on="market_id", how="left")
    if "consensus_score" not in out.columns:
        out = out.with_columns(pl.lit(None, dtype=pl.Float64).alias("consensus_score"))
    return out


def build_dataset(
    trades: pl.LazyFrame,
    markets: pl.DataFrame,
    top_n_players: pl.DataFrame,
    *,
    category: str,
    window_days: int = 7,
    horizon_days: int = 1,
    min_active_frac: float = 0.5,
    test_frac: float = 0.20,
) -> tuple[pl.DataFrame, pl.DataFrame, dict[str, Any]]:
    n_players = top_n_players.height
    player_ids = set(top_n_players["player"].to_list())

    markets_in_cat = (markets if category == "*"
                      else markets.filter(pl.col("category") == category))
    market_ids = set(markets_in_cat["id"].to_list())
    if not market_ids or not player_ids:
        return _empty_result(category, window_days, horizon_days,
                             min_active_frac, n_players)

    filtered = (
        trades.filter(pl.col("market_id").is_in(list(market_ids)))
              .filter(pl.col("maker").is_in(list(player_ids))
                      | pl.col("taker").is_in(list(player_ids)))
    ).collect()

    if filtered.height == 0:
        return _empty_result(category, window_days, horizon_days,
                             min_active_frac, n_players)

    min_ts = int(filtered["timestamp"].min())
    max_ts = int(filtered["timestamp"].max())
    horizon_secs = horizon_days * 86400
    window_secs = window_days * 86400
    decision_dates = _decision_date_grid(min_ts + window_secs,
                                         max_ts - horizon_secs)

    winners = market_resolution(trades)
    closed_lookup = (
        markets_in_cat.with_columns(
            pl.col("closedTime").map_elements(_parse_iso_to_unix,
                                              return_dtype=pl.Int64)
              .alias("closed_ts")
        ).select(["id", "closed_ts", "ticker", "question"])
        .rename({"id": "market_id"})
    )

    threshold = math.ceil(min_active_frac * n_players)
    rows: list[pl.DataFrame] = []
    for d_ts in decision_dates:
        slice_ = filtered.filter(
            (pl.col("timestamp") >= d_ts - window_secs)
            & (pl.col("timestamp") < d_ts)
        )
        feat = _features_for_window(slice_, player_ids)
        if feat.height == 0:
            continue
        feat = feat.filter(pl.col("n_players_active") >= threshold)
        if feat.height == 0:
            continue
        feat = feat.with_columns(pl.lit(d_ts).cast(pl.Int64).alias("decision_ts"))
        rows.append(feat)

    if not rows:
        return _empty_result(category, window_days, horizon_days,
                             min_active_frac, n_players)

    panel = pl.concat(rows)

    panel = panel.join(
        winners.select(["market_id", "winner_token"]), on="market_id", how="left"
    ).join(closed_lookup, on="market_id", how="left")

    panel = panel.with_columns([
        pl.from_epoch("decision_ts", time_unit="s").alias("decision_date"),
        pl.from_epoch("closed_ts", time_unit="s").alias("target_resolved_at"),
        pl.from_epoch(pl.col("decision_ts") - window_secs, time_unit="s")
          .alias("window_start"),
        pl.from_epoch("decision_ts", time_unit="s").alias("window_end"),
    ])

    panel = panel.with_columns([
        pl.when(
            pl.col("closed_ts").is_not_null()
            & (pl.col("closed_ts") > 0)
            & (pl.col("closed_ts") >= pl.col("decision_ts"))
            & (pl.col("closed_ts") < pl.col("decision_ts") + horizon_secs)
            & (pl.col("winner_token") == "token1")
        ).then(pl.lit("YES"))
        .when(
            pl.col("closed_ts").is_not_null()
            & (pl.col("closed_ts") > 0)
            & (pl.col("closed_ts") >= pl.col("decision_ts"))
            & (pl.col("closed_ts") < pl.col("decision_ts") + horizon_secs)
            & (pl.col("winner_token") == "token2")
        ).then(pl.lit("NO"))
        .otherwise(pl.lit("PASS"))
        .alias("target")
    ])

    keep_cols = (["decision_date", "market_id", "ticker", "question",
                  "window_start", "window_end"]
                 + FEATURE_NAMES
                 + ["target", "target_resolved_at"])
    panel = panel.select([c for c in keep_cols if c in panel.columns]).sort("decision_date")

    unique_dates = panel["decision_date"].unique().sort().to_list()
    if len(unique_dates) < 2:
        split_idx = max(0, len(unique_dates) - 1)
    else:
        split_idx = max(1, int(len(unique_dates) * (1 - test_frac)))
    split_date = unique_dates[split_idx] if split_idx < len(unique_dates) \
        else unique_dates[-1]

    train = panel.filter(pl.col("decision_date") < split_date)
    test = panel.filter(pl.col("decision_date") >= split_date)

    meta = _meta_dict(
        category=category, window_days=window_days, horizon_days=horizon_days,
        min_active_frac=min_active_frac, n_players=n_players,
        split_date=str(split_date), n_train=train.height, n_test=test.height,
        train=train, test=test,
    )
    return train, test, meta


def _meta_dict(*, category, window_days, horizon_days, min_active_frac,
               n_players, split_date, n_train, n_test,
               train: pl.DataFrame, test: pl.DataFrame) -> dict[str, Any]:
    def _counts(df: pl.DataFrame) -> dict:
        if df.height == 0 or "target" not in df.columns:
            return {"YES": 0, "NO": 0, "PASS": 0}
        d = dict(df.group_by("target").len().iter_rows())
        return {k: int(d.get(k, 0)) for k in ("YES", "NO", "PASS")}

    return {
        "category": category,
        "window_days": window_days,
        "horizon_days": horizon_days,
        "min_active_frac": min_active_frac,
        "n_players": n_players,
        "criterion": "score_C(win_rate * log(max(1, total_won_usd)))",
        "min_win_rate": 0.5,
        "min_n_bets": 20,
        "split_date": split_date,
        "n_train_rows": n_train,
        "n_test_rows": n_test,
        "n_features": len(FEATURE_NAMES),
        "feature_names": list(FEATURE_NAMES),
        "target_classes": ["YES", "NO", "PASS"],
        "target_class_counts_train": _counts(train),
        "target_class_counts_test": _counts(test),
        "data_source": "data/trades + data/markets",
    }


def _empty_result(category, window_days, horizon_days, min_active_frac,
                  n_players):
    schema: dict[str, Any] = {
        "decision_date": pl.Datetime,
        "market_id": pl.Utf8,
        "ticker": pl.Utf8,
        "question": pl.Utf8,
        "window_start": pl.Datetime,
        "window_end": pl.Datetime,
    }
    for name in FEATURE_NAMES:
        schema[name] = pl.Float64
    schema["target"] = pl.Utf8
    schema["target_resolved_at"] = pl.Datetime
    empty = pl.DataFrame(schema=schema)
    meta = _meta_dict(
        category=category,
        window_days=window_days, horizon_days=horizon_days,
        min_active_frac=min_active_frac, n_players=n_players,
        split_date="", n_train=0, n_test=0,
        train=empty, test=empty,
    )
    return empty, empty, meta
