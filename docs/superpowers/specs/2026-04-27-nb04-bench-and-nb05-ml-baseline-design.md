# Spec — nb04 (Polars vs DuckDB Benchmark) + nb05 (ML Dataset + XGBoost Baseline)

**Date:** 2026-04-27
**Status:** Approved (brainstorming)
**Author:** baptiste.boitepoubelle@gmail.com via Claude Code

---

## 1. Goal

Add two new example notebooks to `poly_data`:

- **nb04** — benchmark Polars (lazy/streaming) vs DuckDB on six representative analyst queries, under a strict 2 GB RAM cap so neighbouring ML training jobs are not crashed. Each query runs through both engines; correctness is asserted; time + peak RSS reported per `(query, engine)`.
- **nb05** — prepare a per-category Machine-Learning dataset for predicting bet outcomes from a panel of high-skill players, then fit a baseline XGBoost classifier as a sanity floor.

Both notebooks are **thin orchestrators**. All non-trivial logic lives in a new package, `src/poly_data/analysis/`, and is unit-tested on synthetic fixtures.

## 2. Prerequisites

- User runs `python update_all.py` (CLI: `poly-data update-all`) **after** the markets-parser edit described in §6 to populate:
  - `data/orderFilled/year=YYYY/month=MM/month.parquet` (already present, 151M fills, 2022-11→2025-10).
  - `data/trades/year=YYYY/month=MM/month.parquet` (post-`process`, includes `market_id`, `maker_direction`, `taker_direction`, `price`, `usd_amount`, `token_amount`).
  - `data/markets/...parquet` (post-`update-markets`, includes new `category` column).
- Notebooks fail loudly with a clear assertion if any of the three sources are missing.

## 3. Definitions

### Player

A wallet address (`0x...`). Default `PLAYER_SIDE = "both"` — every fill contributes to **both** the maker's and taker's positions. Switchable to `"maker"` or `"taker"` to study single-sided behaviour.

### Bet (position) granularity

One row per `(player, market_id, token_side ∈ {token1, token2})`. Built by aggregating all fills for that triple:

```text
net_tokens = Σ signed_token_amount
net_usd    = Σ signed_usd_amount
n_buys, n_sells, first_ts, last_ts
```

Polymarket cannot short, so `net_tokens ≥ 0` is invariant.

### Market resolution proxy (last-fill clamp)

Per market, the **winner** is the token whose last observed fill price is `≥ 0.98`. The **loser** is the token whose last observed fill price is `≤ 0.02`. If neither token reaches the threshold, the market is `"open"` and all positions on it are excluded from win/loss ratios.

### Position outcome label

```text
outcome =
  "won"  if winner_token != "open" AND token_side == winner_token AND net_tokens > 0
  "lost" if winner_token != "open" AND token_side == loser_token  AND net_tokens > 0
  "flat" if net_tokens == 0
  "open" otherwise
```

### Position USD PnL

`pnl_usd = net_usd + net_tokens * payout`, with `payout = 1.0` (won) / `0.0` (lost) / `last_fill_price(token_side)` (open). PnL is omitted from won/lost ratios when `outcome == "open"`.

### Player aggregates

```text
n_bets        = count of non-flat positions
n_won, n_lost = count of "won" / "lost" positions
win_rate      = n_won / (n_won + n_lost)         # NaN if n_won + n_lost == 0
total_won_usd = Σ pnl_usd  for outcome == "won"
total_lost_usd= Σ |pnl_usd| for outcome == "lost"
net_usd_pnl   = Σ pnl_usd for outcome != "open"
score_C       = win_rate * log(max(1, total_won_usd))
```

## 4. Module layout

```
src/poly_data/analysis/         (NEW)
  __init__.py
  positions.py     fills + markets → positions table; outcome labels
  ranking.py       player_stats → top-N selection; pluggable scoring fns
  ml_dataset.py    positions + window/horizon → (decision_date, market_id) panel
  dataloader.py    parquet → numpy / pandas for sklearn-style consumers
  bench.py         time + peak-RSS context manager; result table builder
  io.py            scan helpers + 2 GB rss_guard

src/poly_data/ingest/markets.py   (EDIT — add `category` column)

examples/04-benchmark-polars-vs-duckdb.ipynb   (NEW, ~15 cells)
examples/05-ml-dataset-and-baseline.ipynb      (NEW, ~25 cells)

tests/analysis/                   (NEW)
  conftest.py
  test_positions.py
  test_ranking.py
  test_bench.py
  test_ml_dataset.py
  test_dataloader.py

scripts/
  smoke_nb04.py    nbclient-driven CI smoke
  smoke_nb05.py
```

## 5. Markets parser edit

`src/poly_data/ingest/markets.py`:

- Append `"category"` to `MARKET_COLUMNS`.
- In `_parse_market`, set `"category": str(events[0].get("category", "")) if events else str(market.get("category", ""))`.
- Backward-compatible: empty string for any pre-existing rows lacking the field.

This must be applied **before** `update_all.py` is run so the persisted `data/markets/` Parquet has the column.

## 6. nb04 — Polars vs DuckDB benchmark

### Six queries

| # | Question | Output shape |
|---|---|---|
| 1 | Top 20 players by `total_won_usd` | 20 × (player, total_won_usd) |
| 2 | Top 20 players by `n_won` | 20 × (player, n_won) |
| 3 | Top 20 players by `win_rate` | 20 × (player, win_rate, n_bets) |
| 4 | Top 20 players by `score_C` | 20 × (player, score_C, win_rate, total_won_usd) |
| 5 | Distribution of `n_bets` per player + mean | bins + mean |
| 6 | Q1–Q4 restricted to the **top half** of players ranked by `n_bets` (i.e. drop the bottom 50% by bet count, then re-run Q1–Q4 on the survivors) | 4 × 20-row tables |

### Engines

- **Polars** — `pl.scan_parquet("data/trades/**/*.parquet")` lazy, `.collect(streaming=True)`, chunk size 100 k.
- **DuckDB** — view-based, `SET memory_limit='2GB'`, `SET temp_directory='./_duckdb_tmp'`, `SET threads=4`. Hive partition pruning enabled via `read_parquet(..., hive_partitioning=true)`.

### Bench API (`bench.py`)

```python
@dataclass
class BenchResult:
    label: str
    engine: str          # "polars" | "duckdb"
    seconds: float
    peak_rss_mb: float
    rows_out: int

class Bench:
    def __init__(self): self.results: list[BenchResult] = []
    def __call__(self, label, engine) -> _BenchCM: ...   # context manager
    def df(self) -> pl.DataFrame: ...
```

`_BenchCM` polls `psutil.Process().memory_info().rss` on a 50 ms tick in a background thread. Time captured by `time.perf_counter()`. `gc.collect()` + 200 ms idle before each block to subtract baseline.

### Correctness guard

For every query, the Polars and DuckDB outputs are sorted on the same key columns and compared with `assert polars_result.equals(duckdb_result)`. Mismatch raises immediately so we never publish numbers from a wrong implementation.

### Report

`bench.df()` returns one row per `(query, engine)` containing `seconds`, `peak_rss_mb`, `rows_out`. The notebook plots two grouped bar charts (time, memory) and prints a numeric table for archival.

### Notebook cell sequence (≈15 cells)

1. Setup (DuckDB connection, scan_parquet view, Bench instance, helper imports).
2. Markdown: what this notebook measures.
3-8. Q1–Q6 each: one cell that runs both engines back-to-back (`bench("q1", "polars")` then `bench("q1", "duckdb")`).
9. `bench.df()` table.
10-11. Two bar charts.
12. Markdown commentary on how to read the result.
13-15. Cleanup + outro.

## 7. nb05 — ML dataset + baseline

### Top-128 selection (`ranking.select_top_n`)

```python
def select_top_n(player_stats, *, n=128, min_win_rate=0.5, min_n_bets=20,
                 score_fn=score_C):
    return (player_stats
            .filter(pl.col("win_rate") > min_win_rate)
            .filter(pl.col("n_bets") >= min_n_bets)
            .with_columns(score_fn(pl.col).alias("score"))
            .sort("score", descending=True)
            .head(n))
```

Built-in scoring fns: `score_C` (default), `score_money_ratio`, `score_win_rate`, `score_total_won`. User passes any callable returning a Polars expression.

### Dataset builder (`ml_dataset.build_dataset`)

```python
def build_dataset(
    trades: pl.LazyFrame,
    markets: pl.DataFrame,
    top_n_players: pl.DataFrame,
    *,
    category: str,
    window_days: int = 7,
    horizon_days: int = 1,
    min_active_frac: float = 0.5,
    grid: str = "daily",
    test_frac: float = 0.20,
) -> tuple[pl.DataFrame, pl.DataFrame, dict]:
    ...
```

Pipeline (lazy/streaming end-to-end, 2 GB cap):

1. Filter trades to top-128 players in the target category. Predicate pushed into the parquet scan.
2. Build daily decision-date grid from `trades.ts.min()` to `trades.ts.max() - horizon_days`.
3. For each `decision_date`: slice trades to `[decision_date - window_days, decision_date)`. Group by `market_id`. Compute features. Filter to markets where `n_players_active ≥ ceil(0.5 * 128) = 64`.
4. Attach target. For each `(decision_date, market_id)`:
   - "Resolved" = `markets.closedTime` parses to a UTC instant inside `[decision_date, decision_date + horizon_days)` AND the §3 winner proxy classifies the market as non-`open`.
   - If resolved → label by winner: `YES` (token1 won) or `NO` (token2 won).
   - Else → `PASS`.
5. Concatenate per-decision-date rows. Sort by `decision_date`.
6. Train/test split by `decision_date`: split point = 80th percentile date. Strictly temporal — no leakage.
7. Return `(train_df, test_df, meta)`.

### Default features (14)

```text
n_players_active                            i32
mean_buy_price_t1, mean_buy_price_t2        f64  (NaN if no buys on side)
mean_sell_price_t1, mean_sell_price_t2      f64  (NaN if no sells on side)
usd_volume_t1, usd_volume_t2                f64
net_token1_position                         f64
n_buys_t1, n_sells_t1, n_buys_t2, n_sells_t2  i32
last_price_t1                               f64
consensus_score                             f64  (frac of active panel net-long t1)
```

NaNs preserved — XGBoost handles natively.

### Output

```
data/ml/
  <category-slug>/
    window=7d/horizon=1d/
      train.parquet
      test.parquet
      top_n.parquet
      meta.json
    window=30d/horizon=1d/
      train.parquet
      test.parquet
      top_n.parquet
      meta.json
```

Schema of `train.parquet` / `test.parquet` (one row per `(decision_date, market_id)`):

```text
decision_date         datetime[ns, UTC]
market_id             str
ticker                str
question              str
window_start          datetime[ns, UTC]
window_end            datetime[ns, UTC]
<14 features>
target                str ∈ {"YES","NO","PASS"}
target_resolved_at    datetime[ns, UTC]   (null when PASS)
target_winner_price   f64                 (null when PASS)
```

Compression: zstd. Single file per split.

`top_n.parquet`: 128 rows of `(player, score, win_rate, n_bets, n_won, n_lost, total_won_usd, total_lost_usd, net_usd_pnl)`.

`meta.json` records: category, window, horizon, min_active_frac, n_players, criterion description, min_win_rate, min_n_bets, split_date, n_train_rows, n_test_rows, n_features, feature_names, target_classes, train+test class counts, data_source, git_sha, built_at.

### "Biggest category" selection

Default metric: number of markets in `data/markets/` per category (most rows). Parametrisable to `n_fills` or `total_usdc_volume`.

### Notebook cell sequence (≈25 cells)

Part B (≈15 cells):
1. Setup, prerequisite assertions.
2-3. Compute `player_stats` once via DuckDB (cheap on `data/trades`).
4. `select_top_n(...)` → 128-row table; print + save `top_n.parquet`.
5. Pick biggest category. Print top-10 categories.
6-9. `build_dataset(window_days=7, horizon_days=1)` → save train/test/meta to `data/ml/<category>/window=7d/horizon=1d/`.
10. Class-balance plot train vs test.
11-13. Repeat with `window_days=30`.
14-15. Sanity tables (head, shape, target counts).

Part C (≈10 cells):
16. Load latest dataset via `dataloader.load(category, window=7, horizon=1)` → `(X_train, y_train, X_test, y_test, feature_names)`.
17. Fit `XGBClassifier(tree_method='hist', n_estimators=400, max_depth=6, learning_rate=0.05, eval_metric='mlogloss')`.
18. Predict + classification report (precision, recall, F1 per class) on test.
19. Confusion matrix heatmap.
20. Feature importances bar chart.
21. ROC curves per class (one-vs-rest).
22-25. Repeat for window=30d. Compare baselines side-by-side. Outro.

## 8. RAM + time guardrails

- **Polars**: never `pl.read_parquet` on the full store; always `pl.scan_parquet` + `.collect(streaming=True)`.
- **DuckDB**: `memory_limit='2GB'`, `temp_directory='./_duckdb_tmp'`, `threads=4`.
- **Soft RSS guard**: `io.rss_guard(label, cap_mb=2048, sample_ms=50)` — background thread polls, raises `MemoryError` on breach.
- **Hard time cap per benchmark block**: 600 s. Timeout records `seconds=NaN`, `peak_rss_mb=<last sample>` so the report still renders.
- **Cleanup**: `atexit` removes `./_duckdb_tmp` after kernel close.

## 9. Testing

`tests/analysis/` uses pytest, fixtures in `conftest.py`:
- 3 markets (1 token1-wins, 1 token2-wins, 1 still open)
- 5 players, ~200 fills, deterministic via fixed seed
- Schema matches `data/trades`

Coverage:
- `test_positions.py` — fills → positions inventory invariants, won/lost/open labels, `PLAYER_SIDE` toggle.
- `test_ranking.py` — `min_win_rate`/`min_n_bets` filters, `score_C` ordering, pluggable `score_fn`.
- `test_bench.py` — context manager records non-zero time + RSS; `rss_guard(cap_mb=1)` raises on deliberate over-allocation.
- `test_ml_dataset.py` — `build_dataset` shape, active-frac filter, target distribution, temporal-only train/test split.
- `test_dataloader.py` — parquet → numpy/pandas roundtrip preserves dtypes + NaN; column subset matches `meta.json` `feature_names`.

CI smoke: `scripts/smoke_nb04.py`, `scripts/smoke_nb05.py` execute the notebooks via `nbclient` against the synthetic fixture (or first month of real data) on PRs touching `examples/` or `src/poly_data/analysis/`.

Out of scope for tests:
- Performance numbers (environment-dependent).
- Markets API throttling.
- XGBoost accuracy thresholds.

## 10. Open questions / non-goals

- **Production CLI** (`scripts/build_ml_dataset.py`): not built now. Helpers in `src/poly_data/analysis/` make this trivial to add later if the ML team wants a non-notebook entry point.
- **Hyperparameter tuning** for XGBoost: out of scope. Baseline is a floor, not a ceiling.
- **Streaming inference / live prediction**: not addressed.
- **`umaResolutionStatus` capture**: kept on the proxy clamp (≥ 0.98 / ≤ 0.02) for consistency with nb01–nb03. Real resolution data can be added later via a separate markets-parser extension.

## 11. Acceptance criteria

- [ ] `markets.py` edit applied; `update_all.py` re-runnable; `data/markets/` has `category` column.
- [ ] `src/poly_data/analysis/` package created with the six modules listed in §4.
- [ ] `tests/analysis/` ≥ 80% coverage on the new modules; all tests green under `pytest`.
- [ ] `examples/04-benchmark-polars-vs-duckdb.ipynb` executes top-to-bottom under 2 GB RSS, producing the bench table + two charts.
- [ ] `examples/05-ml-dataset-and-baseline.ipynb` executes top-to-bottom under 2 GB RSS, producing `data/ml/<category>/window=7d/horizon=1d/{train,test,top_n}.parquet` + `meta.json`, plus an XGBoost baseline classification report and confusion matrix.
- [ ] Both notebooks pass nbclient smoke in CI.
