import math

import polars as pl
import pytest

from poly_data.analysis.ml_dataset import build_dataset
from poly_data.analysis.positions import compute_player_stats
from poly_data.analysis.ranking import select_top_n


@pytest.fixture
def top_players(trades_lf) -> pl.DataFrame:
    stats = compute_player_stats(trades_lf, player_side="both")
    return select_top_n(stats, n=10, min_win_rate=0.0, min_n_bets=1)


def test_build_dataset_returns_train_test_meta(trades_lf, markets_df, top_players):
    train, test, meta = build_dataset(
        trades_lf, markets_df, top_players,
        category="Sports",
        window_days=1, horizon_days=10,
        min_active_frac=0.0,
        test_frac=0.5,
    )
    assert isinstance(train, pl.DataFrame)
    assert isinstance(test, pl.DataFrame)
    assert isinstance(meta, dict)


def test_build_dataset_temporal_split(trades_lf, markets_df, top_players):
    train, test, meta = build_dataset(
        trades_lf, markets_df, top_players,
        category="Sports",
        window_days=1, horizon_days=10,
        min_active_frac=0.0,
        test_frac=0.5,
    )
    if train.height > 0 and test.height > 0:
        assert train["decision_date"].max() <= test["decision_date"].min()


def test_build_dataset_target_in_known_classes(trades_lf, markets_df, top_players):
    train, test, _ = build_dataset(
        trades_lf, markets_df, top_players,
        category="Sports",
        window_days=1, horizon_days=10,
        min_active_frac=0.0,
        test_frac=0.5,
    )
    classes = {"YES", "NO", "PASS"}
    for df in (train, test):
        if df.height:
            assert set(df["target"].unique()).issubset(classes)


def test_build_dataset_meta_keys(trades_lf, markets_df, top_players):
    _, _, meta = build_dataset(
        trades_lf, markets_df, top_players,
        category="Sports",
        window_days=1, horizon_days=10,
        min_active_frac=0.0,
        test_frac=0.5,
    )
    required = {"category", "window_days", "horizon_days", "min_active_frac",
                "n_players", "split_date", "n_train_rows", "n_test_rows",
                "n_features", "feature_names", "target_classes",
                "target_class_counts_train", "target_class_counts_test"}
    assert required.issubset(set(meta.keys()))


def test_build_dataset_feature_columns_present(trades_lf, markets_df, top_players):
    train, _, meta = build_dataset(
        trades_lf, markets_df, top_players,
        category="Sports",
        window_days=1, horizon_days=10,
        min_active_frac=0.0,
        test_frac=0.5,
    )
    expected = {"n_players_active", "mean_buy_price_t1", "mean_buy_price_t2",
                "mean_sell_price_t1", "mean_sell_price_t2",
                "usd_volume_t1", "usd_volume_t2", "net_token1_position",
                "n_buys_t1", "n_sells_t1", "n_buys_t2", "n_sells_t2",
                "last_price_t1", "consensus_score"}
    if train.height:
        assert expected.issubset(set(train.columns))
        assert set(meta["feature_names"]) == expected


def test_build_dataset_active_frac_filter(trades_lf, markets_df, top_players):
    train, test, _ = build_dataset(
        trades_lf, markets_df, top_players,
        category="Sports",
        window_days=1, horizon_days=10,
        min_active_frac=1.0,
        test_frac=0.5,
    )
    n_players = top_players.height
    threshold = math.ceil(1.0 * n_players)
    for df in (train, test):
        if df.height:
            assert (df["n_players_active"] >= threshold).all()
