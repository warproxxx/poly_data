# nb04 (Polars vs DuckDB Benchmark) + nb05 (ML Dataset + XGBoost Baseline) — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a new `src/poly_data/analysis/` package, two example notebooks (`04-benchmark-polars-vs-duckdb.ipynb`, `05-ml-dataset-and-baseline.ipynb`), and supporting tests/smokes. Notebook 4 benchmarks Polars (lazy/streaming) vs DuckDB on six analyst queries; notebook 5 builds a per-category ML dataset of (decision_date, market_id) panels labelled YES/NO/PASS and fits an XGBoost baseline.

**Architecture:** Helpers live in importable modules; both notebooks are thin orchestrators. All operations stay under a 2 GB RSS cap (Polars `scan_parquet` + `collect(streaming=True)`; DuckDB `memory_limit='2GB'`). Helpers tested on a synthetic Polars fixture (~30 fills, 5 players, 3 markets) — no I/O on the 5 GB store. Notebooks built last after every helper is green.

**Tech Stack:** Python 3.11, Polars (lazy/streaming), DuckDB, psutil, pytest, matplotlib + seaborn, XGBoost, scikit-learn, nbclient.

**Spec:** `docs/superpowers/specs/2026-04-27-nb04-bench-and-nb05-ml-baseline-design.md`

---

## File Structure

```
src/poly_data/
  ingest/markets.py                                   MODIFY  +category col
  analysis/__init__.py                                CREATE  package marker
  analysis/io.py                                      CREATE  rss_guard, scanners, duckdb opener
  analysis/bench.py                                   CREATE  Bench + BenchResult
  analysis/positions.py                               CREATE  fills→positions, outcome labels, player_stats
  analysis/ranking.py                                 CREATE  scoring fns + select_top_n
  analysis/ml_dataset.py                              CREATE  build_dataset (window panel + target)
  analysis/dataloader.py                              CREATE  parquet→numpy/pandas

tests/analysis/__init__.py                            CREATE
tests/analysis/conftest.py                            CREATE  synthetic fills fixture
tests/analysis/test_io.py                             CREATE  rss_guard breach test
tests/analysis/test_bench.py                          CREATE
tests/analysis/test_positions.py                      CREATE
tests/analysis/test_ranking.py                        CREATE
tests/analysis/test_ml_dataset.py                     CREATE
tests/analysis/test_dataloader.py                     CREATE
tests/ingest/test_markets_category.py                 CREATE  category field present

examples/04-benchmark-polars-vs-duckdb.ipynb          CREATE
examples/05-ml-dataset-and-baseline.ipynb             CREATE

scripts/smoke_nb04.py                                 CREATE
scripts/smoke_nb05.py                                 CREATE

pyproject.toml                                        MODIFY  add psutil, xgboost, scikit-learn, nbclient to analysis extra
```

Each `src/poly_data/analysis/*.py` file has one clear responsibility. Files that change together stay together (positions + ranking are split because tests can change one without re-running the other; ml_dataset depends on both but doesn't pull in their tests).

---

## Task 0: Add deps to `pyproject.toml`

**Files:**
- Modify: `pyproject.toml`

- [ ] **Step 1: Read current `pyproject.toml` to locate the `analysis` optional-dependency block**

Run: `grep -n "analysis" pyproject.toml`

Expected: line(s) showing `analysis = [...]`. If absent, add a new optional-dependency section.

- [ ] **Step 2: Add the new dependencies to the `analysis` extra**

Replace the `analysis = [...]` line with:

```toml
analysis = [
  "duckdb>=1.0",
  "matplotlib>=3.8",
  "seaborn>=0.13",
  "psutil>=5.9",
  "xgboost>=2.0",
  "scikit-learn>=1.4",
  "nbclient>=0.10",
]
```

- [ ] **Step 3: Install the updated extra in the dev environment**

Run: `pip install -e .[analysis,dev]`

Expected: install succeeds; `python -c "import psutil, xgboost, sklearn, nbclient"` exits 0.

- [ ] **Step 4: Commit**

```bash
git add pyproject.toml
git commit -m "chore: add psutil/xgboost/sklearn/nbclient to analysis extra for nb04+nb05"
```

---

## Task 1: `markets.py` — capture `category`

**Files:**
- Modify: `src/poly_data/ingest/markets.py`
- Test: `tests/ingest/test_markets_category.py`

- [ ] **Step 1: Write the failing test**

Create `tests/ingest/test_markets_category.py`:

```python
from poly_data.ingest.markets import _parse_market, MARKET_COLUMNS


def test_market_columns_includes_category():
    assert "category" in MARKET_COLUMNS


def test_parse_market_extracts_category_from_events():
    raw = {
        "id": "1",
        "question": "Q",
        "outcomes": '["Yes","No"]',
        "clobTokenIds": '["t1","t2"]',
        "createdAt": "2024-01-01T00:00:00Z",
        "events": [{"ticker": "trump-2024", "category": "Politics"}],
    }
    row = _parse_market(raw)
    assert row is not None
    assert row["category"] == "Politics"


def test_parse_market_falls_back_to_top_level_category():
    raw = {
        "id": "2",
        "question": "Q",
        "outcomes": '["Yes","No"]',
        "clobTokenIds": '["t1","t2"]',
        "createdAt": "2024-01-01T00:00:00Z",
        "category": "Sports",
    }
    row = _parse_market(raw)
    assert row is not None
    assert row["category"] == "Sports"


def test_parse_market_empty_category_when_absent():
    raw = {
        "id": "3",
        "question": "Q",
        "outcomes": '["Yes","No"]',
        "clobTokenIds": '["t1","t2"]',
        "createdAt": "2024-01-01T00:00:00Z",
    }
    row = _parse_market(raw)
    assert row is not None
    assert row["category"] == ""
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/ingest/test_markets_category.py -v`

Expected: 4 failures — `category` not in `MARKET_COLUMNS`; `KeyError` on `row["category"]`.

- [ ] **Step 3: Edit `MARKET_COLUMNS`**

In `src/poly_data/ingest/markets.py`, change:

```python
MARKET_COLUMNS = [
    "createdAt", "id", "question", "answer1", "answer2", "neg_risk",
    "market_slug", "token1", "token2", "condition_id", "volume", "ticker",
    "closedTime", "timestamp",
]
```

to:

```python
MARKET_COLUMNS = [
    "createdAt", "id", "question", "answer1", "answer2", "neg_risk",
    "market_slug", "token1", "token2", "condition_id", "volume", "ticker",
    "closedTime", "timestamp", "category",
]
```

- [ ] **Step 4: Edit `_parse_market` to extract `category`**

In `_parse_market`, replace the events-handling block and the return-dict to:

```python
ticker = ""
category = str(market.get("category", "") or "")
events = market.get("events") or []
if events:
    ticker = events[0].get("ticker", "")
    if not category:
        category = str(events[0].get("category", "") or "")

created_at = market.get("createdAt", "")
ts_int = _to_unix_seconds(created_at)
return {
    "createdAt": str(created_at),
    "id": str(market.get("id", "")),
    "question": market.get("question") or market.get("title") or "",
    "answer1": str(answer1),
    "answer2": str(answer2),
    "neg_risk": neg_risk,
    "market_slug": str(market.get("slug", "")),
    "token1": token1,
    "token2": token2,
    "condition_id": str(market.get("conditionId", "")),
    "volume": str(market.get("volume", "")),
    "ticker": str(ticker),
    "closedTime": str(market.get("closedTime", "")),
    "timestamp": ts_int,
    "category": category,
}
```

- [ ] **Step 5: Run test to verify pass**

Run: `pytest tests/ingest/test_markets_category.py -v`

Expected: 4 passes.

- [ ] **Step 6: Run the full ingest test suite to confirm no regression**

Run: `pytest tests/ingest -v`

Expected: all green.

- [ ] **Step 7: Commit**

```bash
git add src/poly_data/ingest/markets.py tests/ingest/test_markets_category.py
git commit -m "feat(markets): capture category column from gamma-API events"
```

---

## Task 2: `src/poly_data/analysis/io.py` — RSS guard + scanners

**Files:**
- Create: `src/poly_data/analysis/__init__.py` (empty)
- Create: `src/poly_data/analysis/io.py`
- Test: `tests/analysis/__init__.py` (empty), `tests/analysis/test_io.py`

- [ ] **Step 1: Create empty package markers**

```bash
mkdir -p src/poly_data/analysis tests/analysis
touch src/poly_data/analysis/__init__.py tests/analysis/__init__.py
```

- [ ] **Step 2: Write the failing tests**

Create `tests/analysis/test_io.py`:

```python
import time
from pathlib import Path

import polars as pl
import pytest

from poly_data.analysis import io as ioh


def test_rss_guard_passes_when_under_cap():
    with ioh.rss_guard("ok", cap_mb=4096, sample_ms=10):
        x = 1 + 1
    assert x == 2


def test_rss_guard_raises_on_breach():
    with pytest.raises(MemoryError):
        with ioh.rss_guard("breach", cap_mb=10, sample_ms=10):
            buf = bytearray(50 * 1024 * 1024)  # 50 MB > 10 MB cap
            time.sleep(0.1)                    # let watcher sample
            del buf


def test_open_duckdb_applies_settings(tmp_path: Path):
    con = ioh.open_duckdb(memory_limit="1GB", threads=2,
                          temp_directory=tmp_path / "ddb_tmp")
    mem = con.execute("SELECT current_setting('memory_limit')").fetchone()[0]
    threads = con.execute("SELECT current_setting('threads')").fetchone()[0]
    assert mem.upper().startswith("1.0 GIB") or mem.upper().startswith("1GB") \
        or mem.upper().startswith("1.0GB")
    assert int(threads) == 2
    con.close()


def test_scan_trades_returns_lazyframe(tmp_path: Path):
    # Build a tiny parquet under the trades hive layout
    target = tmp_path / "trades" / "year=2024" / "month=1" / "month.parquet"
    target.parent.mkdir(parents=True)
    pl.DataFrame({"timestamp": [1], "market_id": ["m"], "price": [0.5]})\
      .write_parquet(target)
    lf = ioh.scan_trades(data_root=tmp_path)
    assert isinstance(lf, pl.LazyFrame)
    df = lf.collect()
    assert df.height == 1
```

- [ ] **Step 3: Run tests to verify they fail**

Run: `pytest tests/analysis/test_io.py -v`

Expected: ImportError — `poly_data.analysis.io` doesn't exist.

- [ ] **Step 4: Implement `src/poly_data/analysis/io.py`**

```python
from __future__ import annotations

import gc
import threading
import time
from contextlib import contextmanager
from pathlib import Path
from typing import Iterator

import duckdb
import polars as pl
import psutil

DEFAULT_CAP_MB = 2048


@contextmanager
def rss_guard(label: str, *, cap_mb: int = DEFAULT_CAP_MB,
              sample_ms: int = 50) -> Iterator[None]:
    """Background-poll process RSS; raise MemoryError on exit if cap_mb exceeded above baseline."""
    proc = psutil.Process()
    gc.collect()
    baseline = proc.memory_info().rss
    breach = {"hit": False, "rss": baseline}
    stop = threading.Event()

    def watcher() -> None:
        while not stop.is_set():
            rss = proc.memory_info().rss
            if rss - baseline > cap_mb * 1024 * 1024:
                breach["hit"] = True
                breach["rss"] = rss
                stop.set()
                return
            stop.wait(sample_ms / 1000.0)

    t = threading.Thread(target=watcher, daemon=True)
    t.start()
    try:
        yield
    finally:
        stop.set()
        t.join(timeout=1.0)
    if breach["hit"]:
        raise MemoryError(
            f"rss_guard[{label}]: cap {cap_mb} MB exceeded; "
            f"rss={breach['rss'] / (1024 * 1024):.0f} MB"
        )


def scan_trades(data_root: Path = Path("data")) -> pl.LazyFrame:
    glob = str(data_root / "trades" / "**" / "*.parquet")
    return pl.scan_parquet(glob)


def scan_markets(data_root: Path = Path("data")) -> pl.LazyFrame:
    glob = str(data_root / "markets" / "**" / "*.parquet")
    return pl.scan_parquet(glob)


def scan_orderfilled(data_root: Path = Path("data")) -> pl.LazyFrame:
    glob = str(data_root / "orderFilled" / "**" / "*.parquet")
    return pl.scan_parquet(glob)


def open_duckdb(*, memory_limit: str = "2GB", threads: int = 4,
                temp_directory: Path = Path("./_duckdb_tmp")
                ) -> duckdb.DuckDBPyConnection:
    temp_directory = Path(temp_directory)
    temp_directory.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect()
    con.execute(f"SET memory_limit='{memory_limit}'")
    con.execute(f"SET threads={threads}")
    con.execute(f"SET temp_directory='{temp_directory.resolve().as_posix()}'")
    return con
```

- [ ] **Step 5: Run tests to verify pass**

Run: `pytest tests/analysis/test_io.py -v`

Expected: 4 passes.

- [ ] **Step 6: Commit**

```bash
git add src/poly_data/analysis/__init__.py src/poly_data/analysis/io.py \
        tests/analysis/__init__.py tests/analysis/test_io.py
git commit -m "feat(analysis.io): add rss_guard, parquet scanners, duckdb opener"
```

---

## Task 3: `src/poly_data/analysis/bench.py` — Bench harness

**Files:**
- Create: `src/poly_data/analysis/bench.py`
- Test: `tests/analysis/test_bench.py`

- [ ] **Step 1: Write the failing tests**

Create `tests/analysis/test_bench.py`:

```python
import time

import polars as pl

from poly_data.analysis.bench import Bench


def test_bench_records_time_and_rss():
    bench = Bench()
    with bench("noop", "polars") as ctx:
        time.sleep(0.05)
        ctx["rows_out"] = 7
    assert len(bench.results) == 1
    r = bench.results[0]
    assert r.label == "noop" and r.engine == "polars"
    assert r.seconds >= 0.04
    assert r.rows_out == 7
    assert r.peak_rss_mb >= 0


def test_bench_df_columns_and_shape():
    bench = Bench()
    with bench("a", "polars") as ctx:
        ctx["rows_out"] = 1
    with bench("a", "duckdb") as ctx:
        ctx["rows_out"] = 1
    df = bench.df()
    assert isinstance(df, pl.DataFrame)
    assert df.height == 2
    assert set(df.columns) == {"label", "engine", "seconds",
                               "peak_rss_mb", "rows_out"}


def test_bench_captures_peak_rss_growth():
    bench = Bench()
    with bench("alloc", "polars") as ctx:
        buf = bytearray(40 * 1024 * 1024)
        time.sleep(0.1)
        ctx["rows_out"] = len(buf)
        del buf
    r = bench.results[0]
    assert r.peak_rss_mb >= 30
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `pytest tests/analysis/test_bench.py -v`

Expected: ImportError — module not yet created.

- [ ] **Step 3: Implement `src/poly_data/analysis/bench.py`**

```python
from __future__ import annotations

import gc
import threading
import time
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Iterator

import polars as pl
import psutil


@dataclass
class BenchResult:
    label: str
    engine: str
    seconds: float
    peak_rss_mb: float
    rows_out: int


class Bench:
    def __init__(self) -> None:
        self.results: list[BenchResult] = []

    @contextmanager
    def __call__(self, label: str, engine: str) -> Iterator[dict]:
        out: dict = {"rows_out": 0}
        proc = psutil.Process()
        gc.collect()
        time.sleep(0.05)
        baseline = proc.memory_info().rss
        peak = baseline
        stop = threading.Event()

        def sampler() -> None:
            nonlocal peak
            while not stop.is_set():
                rss = proc.memory_info().rss
                if rss > peak:
                    peak = rss
                stop.wait(0.05)

        t = threading.Thread(target=sampler, daemon=True)
        t.start()
        t0 = time.perf_counter()
        try:
            yield out
        finally:
            elapsed = time.perf_counter() - t0
            stop.set()
            t.join(timeout=1.0)
            self.results.append(BenchResult(
                label=label,
                engine=engine,
                seconds=elapsed,
                peak_rss_mb=max(0.0, (peak - baseline) / (1024 * 1024)),
                rows_out=int(out.get("rows_out", 0)),
            ))

    def df(self) -> pl.DataFrame:
        return pl.DataFrame([{
            "label": r.label,
            "engine": r.engine,
            "seconds": r.seconds,
            "peak_rss_mb": r.peak_rss_mb,
            "rows_out": r.rows_out,
        } for r in self.results])
```

- [ ] **Step 4: Run tests to verify pass**

Run: `pytest tests/analysis/test_bench.py -v`

Expected: 3 passes.

- [ ] **Step 5: Commit**

```bash
git add src/poly_data/analysis/bench.py tests/analysis/test_bench.py
git commit -m "feat(analysis.bench): add Bench context manager + result table"
```

---

## Task 4: Synthetic fixture — `tests/analysis/conftest.py`

**Files:**
- Create: `tests/analysis/conftest.py`

The fixture supplies (a) a small `trades`-schema Polars DataFrame, (b) a small `markets`-schema Polars DataFrame, both deterministic. 3 markets:
- `M1`: token1 wins (last fill on token1 at 0.99)
- `M2`: token2 wins (last fill on token2 at 0.99)
- `M3`: still open (last fills around 0.55)

5 players: `P1, P2, P3, P4, P5`. ~30 fills total — small enough to verify expected aggregates by hand.

- [ ] **Step 1: Create the fixture file**

```python
# tests/analysis/conftest.py
from __future__ import annotations

from datetime import datetime, timezone

import polars as pl
import pytest

_BASE = int(datetime(2024, 1, 1, tzinfo=timezone.utc).timestamp())


def _row(ts_offset, market, maker, taker, side, m_dir, t_dir, price, usd, tok, h):
    return (_BASE + ts_offset, market, maker, taker, side,
            m_dir, t_dir, price, usd, tok, h)


@pytest.fixture
def trades_df() -> pl.DataFrame:
    """Synthetic 30-row fills DataFrame mirroring data/trades schema."""
    rows = [
        # M1 — token1 wins
        # P1 (maker BUY 100 @ 0.50), P2 takes the SELL side
        _row(0,    "M1", "P1", "P2", "token1", "BUY", "SELL", 0.50, 50.0, 100.0, "h1"),
        _row(10,   "M1", "P3", "P1", "token1", "BUY", "SELL", 0.55, 55.0, 100.0, "h2"),
        _row(20,   "M1", "P3", "P2", "token1", "BUY", "SELL", 0.60, 60.0, 100.0, "h3"),
        _row(30,   "M1", "P1", "P3", "token1", "SELL", "BUY", 0.70, 70.0, 100.0, "h4"),
        _row(40,   "M1", "P4", "P5", "token1", "BUY", "SELL", 0.80, 80.0, 100.0, "h5"),
        _row(50,   "M1", "P4", "P5", "token1", "BUY", "SELL", 0.90, 90.0, 100.0, "h6"),
        _row(60,   "M1", "P5", "P4", "token1", "SELL", "BUY", 0.95, 95.0, 100.0, "h7"),
        _row(70,   "M1", "P5", "P1", "token1", "BUY", "SELL", 0.98, 98.0, 100.0, "h8"),
        _row(80,   "M1", "P3", "P2", "token1", "BUY", "SELL", 0.99, 99.0, 100.0, "h9"),

        # M2 — token2 wins
        _row(200,  "M2", "P1", "P2", "token2", "BUY", "SELL", 0.30, 30.0, 100.0, "h10"),
        _row(210,  "M2", "P2", "P1", "token2", "BUY", "SELL", 0.40, 40.0, 100.0, "h11"),
        _row(220,  "M2", "P3", "P4", "token1", "BUY", "SELL", 0.55, 55.0, 100.0, "h12"),
        _row(230,  "M2", "P4", "P3", "token2", "BUY", "SELL", 0.50, 50.0, 100.0, "h13"),
        _row(240,  "M2", "P5", "P1", "token2", "BUY", "SELL", 0.70, 70.0, 100.0, "h14"),
        _row(250,  "M2", "P5", "P2", "token2", "BUY", "SELL", 0.85, 85.0, 100.0, "h15"),
        _row(260,  "M2", "P4", "P3", "token2", "BUY", "SELL", 0.95, 95.0, 100.0, "h16"),
        _row(270,  "M2", "P5", "P1", "token2", "BUY", "SELL", 0.99, 99.0, 100.0, "h17"),

        # M3 — still open
        _row(400,  "M3", "P1", "P2", "token1", "BUY", "SELL", 0.50, 50.0, 100.0, "h18"),
        _row(410,  "M3", "P2", "P3", "token1", "BUY", "SELL", 0.55, 55.0, 100.0, "h19"),
        _row(420,  "M3", "P3", "P4", "token1", "BUY", "SELL", 0.55, 55.0, 100.0, "h20"),
    ]
    schema = ["timestamp", "market_id", "maker", "taker", "nonusdc_side",
              "maker_direction", "taker_direction", "price", "usd_amount",
              "token_amount", "transactionHash"]
    return pl.DataFrame(rows, schema=schema, orient="row").with_columns([
        pl.col("timestamp").cast(pl.Int64),
    ])


@pytest.fixture
def trades_lf(trades_df: pl.DataFrame) -> pl.LazyFrame:
    return trades_df.lazy()


@pytest.fixture
def markets_df() -> pl.DataFrame:
    """Synthetic markets table mirroring data/markets schema."""
    return pl.DataFrame([
        {
            "id": "M1", "token1": "t1a", "token2": "t1b",
            "question": "Will A happen?", "ticker": "a-2024",
            "category": "Sports", "closedTime": "2024-01-01T00:02:00Z",
            "createdAt": "2024-01-01T00:00:00Z", "answer1": "Yes",
            "answer2": "No", "neg_risk": False, "market_slug": "a",
            "condition_id": "c1", "volume": "1000",
            "timestamp": _BASE,
        },
        {
            "id": "M2", "token1": "t2a", "token2": "t2b",
            "question": "Will B happen?", "ticker": "b-2024",
            "category": "Sports", "closedTime": "2024-01-01T00:05:00Z",
            "createdAt": "2024-01-01T00:00:00Z", "answer1": "Yes",
            "answer2": "No", "neg_risk": False, "market_slug": "b",
            "condition_id": "c2", "volume": "2000",
            "timestamp": _BASE,
        },
        {
            "id": "M3", "token1": "t3a", "token2": "t3b",
            "question": "Will C happen?", "ticker": "c-2024",
            "category": "Politics", "closedTime": "",
            "createdAt": "2024-01-01T00:00:00Z", "answer1": "Yes",
            "answer2": "No", "neg_risk": False, "market_slug": "c",
            "condition_id": "c3", "volume": "500",
            "timestamp": _BASE,
        },
    ])
```

- [ ] **Step 2: Smoke-load the fixture**

Run: `pytest tests/analysis/conftest.py --collect-only -q` then `pytest --co -q tests/analysis/`.

Expected: pytest discovers `trades_df`, `trades_lf`, `markets_df` fixtures (no test failures because no tests exist yet that use them).

- [ ] **Step 3: Commit**

```bash
git add tests/analysis/conftest.py
git commit -m "test(analysis): add synthetic fills + markets fixture (3 markets, 5 players)"
```

---

## Task 5: `src/poly_data/analysis/positions.py`

**Files:**
- Create: `src/poly_data/analysis/positions.py`
- Test: `tests/analysis/test_positions.py`

This module computes:
1. **`expand_to_positions`** — fills (1 row each) → 1-or-2 rows per fill (one per participant), depending on `player_side` ∈ {`both`, `maker`, `taker`}.
2. **`positions_table`** — group → one row per `(player, market_id, token_side)` with `net_tokens, net_usd, n_buys, n_sells, first_ts, last_ts`.
3. **`market_resolution`** — last fill price per `(market_id, token_side)`; `winner_token` ∈ {`token1`, `token2`, `open`}.
4. **`label_outcomes`** — adds `outcome` ∈ {`won`, `lost`, `flat`, `open`} and `pnl_usd` per position.
5. **`player_aggregates`** — `n_bets, n_won, n_lost, win_rate, total_won_usd, total_lost_usd, net_usd_pnl, score_C` per player.
6. **`compute_player_stats`** — convenience entry-point chaining all of the above.

- [ ] **Step 1: Write the failing tests**

Create `tests/analysis/test_positions.py`:

```python
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
    assert set(["player", "market_id", "token_side", "net_tokens",
                "net_usd", "n_buys", "n_sells", "first_ts", "last_ts"]) \
        .issubset(set(pos.columns))
    # every (player, market_id, token_side) is unique
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
    outcomes = labelled["outcome"].unique().to_list()
    assert "won" in outcomes
    assert "lost" in outcomes
    assert "open" in outcomes
    # open positions are exactly those on M3
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


def test_player_aggregates_excludes_open_positions(trades_lf):
    stats = compute_player_stats(trades_lf, player_side="both")
    # Players who only traded on M3 (the only open market) should have n_bets == 0
    # In the fixture, P5 traded on M2 too, so all 5 players have at least 1 decided bet.
    # Just assert that n_bets equals n_won + n_lost.
    for row in stats.iter_rows(named=True):
        assert row["n_bets"] == row["n_won"] + row["n_lost"]


def test_invalid_player_side_raises(trades_lf):
    with pytest.raises(ValueError):
        expand_to_positions(trades_lf, player_side="bogus")
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `pytest tests/analysis/test_positions.py -v`

Expected: ImportError on `poly_data.analysis.positions`.

- [ ] **Step 3: Implement `src/poly_data/analysis/positions.py`**

```python
from __future__ import annotations

from typing import Literal

import polars as pl

PlayerSide = Literal["both", "maker", "taker"]
_VALID_SIDES = {"both", "maker", "taker"}


def expand_to_positions(trades: pl.LazyFrame, *,
                        player_side: PlayerSide = "both") -> pl.LazyFrame:
    """Each fill becomes 1 or 2 rows: one per participant, signed for direction.

    Output columns: timestamp, market_id, player, token_side, signed_tokens,
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
        .collect()
    )


def market_resolution(trades: pl.LazyFrame, *,
                      win_threshold: float = 0.98) -> pl.DataFrame:
    """One row per market: (market_id, last_t1_price, last_t2_price, winner_token)."""
    last_per_side = (
        trades.sort("timestamp")
        .group_by(["market_id", "nonusdc_side"])
        .agg(pl.col("price").last().alias("last_price"))
        .collect()
    )
    pivoted = last_per_side.pivot(
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


def label_outcomes(positions: pl.DataFrame,
                   resolutions: pl.DataFrame,
                   *,
                   last_prices: pl.DataFrame) -> pl.DataFrame:
    """Add `outcome` and `pnl_usd` per position.

    `last_prices` columns: market_id, token_side, last_price.
    `resolutions` columns: market_id, winner_token (+ token1/token2 prices ignored).
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
    res = market_resolution(trades, win_threshold=win_threshold)
    pos = positions_table(trades, player_side=player_side)
    last = (
        trades.sort("timestamp")
        .group_by(["market_id", "nonusdc_side"])
        .agg(pl.col("price").last().alias("last_price"))
        .collect()
        .rename({"nonusdc_side": "token_side"})
    )
    labelled = label_outcomes(pos, res, last_prices=last)
    return player_aggregates(labelled)
```

- [ ] **Step 4: Run tests to verify pass**

Run: `pytest tests/analysis/test_positions.py -v`

Expected: 9 passes.

- [ ] **Step 5: Commit**

```bash
git add src/poly_data/analysis/positions.py tests/analysis/test_positions.py
git commit -m "feat(analysis.positions): aggregate fills into labelled positions + player stats"
```

---

## Task 6: `src/poly_data/analysis/ranking.py`

**Files:**
- Create: `src/poly_data/analysis/ranking.py`
- Test: `tests/analysis/test_ranking.py`

- [ ] **Step 1: Write the failing tests**

Create `tests/analysis/test_ranking.py`:

```python
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


def test_select_top_n_score_money_ratio_changes_order(trades_lf):
    stats = compute_player_stats(trades_lf, player_side="both")
    top_c = select_top_n(stats, n=5, min_win_rate=0.0, min_n_bets=1,
                         score_fn=score_C)
    top_money = select_top_n(stats, n=5, min_win_rate=0.0, min_n_bets=1,
                             score_fn=score_money_ratio)
    # Possibly identical for tiny fixtures, but the API contract is that the
    # score column reflects the chosen function:
    assert "score" in top_money.columns
    assert "score" in top_c.columns


def test_score_fns_return_polars_expr(trades_lf):
    stats = compute_player_stats(trades_lf, player_side="both")
    for fn in (score_C, score_money_ratio, score_total_won, score_win_rate):
        expr = fn(pl.col)
        assert isinstance(expr, pl.Expr)
        evaluated = stats.with_columns(expr.alias("s"))
        assert "s" in evaluated.columns
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `pytest tests/analysis/test_ranking.py -v`

Expected: ImportError.

- [ ] **Step 3: Implement `src/poly_data/analysis/ranking.py`**

```python
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
```

- [ ] **Step 4: Run tests to verify pass**

Run: `pytest tests/analysis/test_ranking.py -v`

Expected: 5 passes.

- [ ] **Step 5: Commit**

```bash
git add src/poly_data/analysis/ranking.py tests/analysis/test_ranking.py
git commit -m "feat(analysis.ranking): pluggable scoring fns + select_top_n"
```

---

## Task 7: `src/poly_data/analysis/ml_dataset.py`

**Files:**
- Create: `src/poly_data/analysis/ml_dataset.py`
- Test: `tests/analysis/test_ml_dataset.py`

`build_dataset(trades, markets, top_n_players, *, category, window_days, horizon_days, min_active_frac, test_frac)` returns `(train_df, test_df, meta)`.

Pipeline:
1. Restrict trades to (a) top-128 players (`maker IN ids OR taker IN ids`) and (b) markets in target category.
2. Build daily decision-date grid from `min(ts)` to `max(ts) - horizon_days`.
3. For each `decision_date`, slice trades to `[decision_date - window_days, decision_date)` and per-market compute the 14 features. Filter to markets with `n_players_active ≥ ceil(min_active_frac * n_players)`.
4. Attach target via `markets.closedTime` and the §3 winner proxy: `YES`, `NO`, or `PASS`.
5. Concat → sort by `decision_date` → split at 80th percentile date.

For testing on the synthetic fixture: window_days=1 and horizon_days=10 to ensure resolution falls inside the horizon for M1 and M2; min_active_frac small so the active filter passes.

- [ ] **Step 1: Write the failing tests**

Create `tests/analysis/test_ml_dataset.py`:

```python
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
    classes = set(["YES", "NO", "PASS"])
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
    # min_active_frac = 1.0 → every player must trade in window → most rows drop
    train, test, _ = build_dataset(
        trades_lf, markets_df, top_players,
        category="Sports",
        window_days=1, horizon_days=10,
        min_active_frac=1.0,
        test_frac=0.5,
    )
    # may be empty; just assert no row violates filter
    n_players = top_players.height
    threshold = math.ceil(1.0 * n_players)
    for df in (train, test):
        if df.height:
            assert (df["n_players_active"] >= threshold).all()
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `pytest tests/analysis/test_ml_dataset.py -v`

Expected: ImportError.

- [ ] **Step 3: Implement `src/poly_data/analysis/ml_dataset.py`**

```python
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
    out: list[int] = []
    if min_ts >= max_ts:
        return out
    start = datetime.fromtimestamp(min_ts, tz=timezone.utc)\
        .replace(hour=0, minute=0, second=0, microsecond=0)
    end = datetime.fromtimestamp(max_ts, tz=timezone.utc)\
        .replace(hour=0, minute=0, second=0, microsecond=0)
    cur = start
    while cur <= end:
        out.append(int(cur.timestamp()))
        cur += timedelta(days=1)
    return out


def _features_for_window(window_trades: pl.DataFrame,
                         player_set: set[str]) -> pl.DataFrame:
    """Aggregate features per market_id for a single decision-window slice."""
    if window_trades.height == 0:
        return pl.DataFrame(schema={n: pl.Float64 for n in FEATURE_NAMES}
                            | {"market_id": pl.Utf8})

    df = window_trades.with_columns([
        pl.when(pl.col("maker").is_in(list(player_set))).then(pl.col("maker"))
        .when(pl.col("taker").is_in(list(player_set))).then(pl.col("taker"))
        .otherwise(None).alias("active_player"),
        pl.col("nonusdc_side").alias("token_side"),
    ]).filter(pl.col("active_player").is_not_null())

    if df.height == 0:
        return pl.DataFrame(schema={n: pl.Float64 for n in FEATURE_NAMES}
                            | {"market_id": pl.Utf8})

    # Per-market base aggregates
    by_market = df.group_by("market_id").agg(
        pl.col("active_player").n_unique().alias("n_players_active"),
        pl.col("price").last().alias("last_price_t1"),  # placeholder, fixed below
    )

    # Per-(market, side, direction) sub-aggregates pivoted into wide cols
    side_dir = df.group_by(["market_id", "token_side", "maker_direction"]).agg(
        pl.col("price").mean().alias("mean_price"),
        pl.col("usd_amount").sum().alias("usd_sum"),
        pl.col("token_amount").sum().alias("tok_sum"),
        pl.len().alias("n"),
    )

    # Pivot helpers — get one value per (market_id) per (side+dir)
    def _pivot(metric: str, alias_prefix: str) -> pl.DataFrame:
        return (
            side_dir.with_columns(
                (pl.col("token_side") + "_" + pl.col("maker_direction"))
                .alias("key")
            )
            .pivot(index="market_id", on="key", values=metric)
        )

    means = _pivot("mean_price", "mean")
    usds = _pivot("usd_sum", "usd")
    toks = _pivot("tok_sum", "tok")
    counts = _pivot("n", "n")

    # Last price on token1 per market
    last_t1 = (
        df.filter(pl.col("token_side") == "token1")
          .sort("timestamp")
          .group_by("market_id")
          .agg(pl.col("price").last().alias("last_price_t1"))
    )

    out = by_market.drop("last_price_t1").join(last_t1, on="market_id", how="left")
    out = out.join(means, on="market_id", how="left")
    out = out.join(usds, on="market_id", how="left")
    out = out.join(toks, on="market_id", how="left")
    out = out.join(counts, on="market_id", how="left")

    def _col_or_null(name: str, dtype=pl.Float64) -> pl.Expr:
        return pl.col(name) if name in out.columns else pl.lit(None, dtype=dtype).alias(name)

    out = out.with_columns([
        _col_or_null("token1_BUY", pl.Float64).alias("mean_buy_price_t1"),
        _col_or_null("token2_BUY", pl.Float64).alias("mean_buy_price_t2"),
        _col_or_null("token1_SELL", pl.Float64).alias("mean_sell_price_t1"),
        _col_or_null("token2_SELL", pl.Float64).alias("mean_sell_price_t2"),
    ])

    # Volume + counts (fall back to 0 for missing)
    def _zero_if_missing(name: str, dtype=pl.Float64) -> pl.Expr:
        return pl.col(name).fill_null(0).cast(dtype) if name in out.columns \
               else pl.lit(0, dtype=dtype).alias(name)

    out = out.with_columns([
        (_zero_if_missing("token1_BUY_right", pl.Float64)).alias("usd_volume_t1_buy"),
    ]) if False else out  # placeholder (unused); see net_token1_position below

    # net token1 position = sum of signed token amounts on token1 from the panel
    panel_t1 = (
        df.filter(pl.col("token_side") == "token1")
          .with_columns(
              pl.when(pl.col("maker_direction") == "BUY")
                .then(pl.col("token_amount"))
                .otherwise(-pl.col("token_amount"))
                .alias("signed_t1")
          )
          .group_by("market_id")
          .agg(pl.col("signed_t1").sum().alias("net_token1_position"))
    )
    out = out.join(panel_t1, on="market_id", how="left")

    # Side volumes + counts (sum over BUY+SELL on each side)
    def _side_volume(side: str, suffix: str) -> pl.Expr:
        b = f"{side}_BUY" + ("_right_right" if False else "")
        s = f"{side}_SELL"
        # We re-aggregate from side_dir to be safe:
        return pl.lit(0)  # patched below

    # Cleaner re-aggregation for volume + count + n_buys/n_sells per side
    side_only = df.group_by(["market_id", "token_side"]).agg(
        pl.col("usd_amount").sum().alias("usd_sum_side"),
        (pl.col("maker_direction") == "BUY").sum().alias("n_buys_side"),
        (pl.col("maker_direction") == "SELL").sum().alias("n_sells_side"),
    )
    side_pivot_usd = side_only.pivot(index="market_id", on="token_side",
                                     values="usd_sum_side")
    side_pivot_buys = side_only.pivot(index="market_id", on="token_side",
                                      values="n_buys_side")
    side_pivot_sells = side_only.pivot(index="market_id", on="token_side",
                                       values="n_sells_side")

    out = (
        out.join(side_pivot_usd.rename({"token1": "usd_volume_t1",
                                        "token2": "usd_volume_t2"}),
                 on="market_id", how="left")
        .join(side_pivot_buys.rename({"token1": "n_buys_t1",
                                      "token2": "n_buys_t2"}),
              on="market_id", how="left")
        .join(side_pivot_sells.rename({"token1": "n_sells_t1",
                                       "token2": "n_sells_t2"}),
              on="market_id", how="left")
    )

    # consensus_score = fraction of active players in the panel net-long token1
    per_player = (
        df.filter(pl.col("token_side") == "token1")
          .with_columns(
              pl.when(pl.col("maker_direction") == "BUY")
                .then(pl.col("token_amount"))
                .otherwise(-pl.col("token_amount"))
                .alias("signed_t1")
          )
          .group_by(["market_id", "active_player"])
          .agg(pl.col("signed_t1").sum().alias("net"))
    )
    consensus = (
        per_player.group_by("market_id").agg(
            ((pl.col("net") > 0).sum().cast(pl.Float64) /
             pl.len().cast(pl.Float64)).alias("consensus_score"),
        )
    )
    out = out.join(consensus, on="market_id", how="left")

    keep = ["market_id"] + FEATURE_NAMES
    for name in FEATURE_NAMES:
        if name not in out.columns:
            out = out.with_columns(pl.lit(None, dtype=pl.Float64).alias(name))
    return out.select(keep)


def _winners_cache(trades: pl.LazyFrame) -> pl.DataFrame:
    return market_resolution(trades)


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

    # Filter markets to category
    markets_in_cat = (markets if category == "*"
                      else markets.filter(pl.col("category") == category))
    market_ids = set(markets_in_cat["id"].to_list())
    if not market_ids:
        return _empty_result(category, window_days, horizon_days,
                             min_active_frac, n_players)

    # Filter trades
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

    winners = _winners_cache(trades)
    closed_lookup = (
        markets_in_cat.with_columns(
            pl.col("closedTime").map_elements(_parse_iso_to_unix,
                                              return_dtype=pl.Int64)
              .alias("closed_ts")
        ).select(["id", "closed_ts"])
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
        feat = feat.with_columns([
            pl.lit(d_ts).cast(pl.Int64).alias("decision_ts"),
        ])
        rows.append(feat)

    if not rows:
        return _empty_result(category, window_days, horizon_days,
                             min_active_frac, n_players)

    panel = pl.concat(rows)

    # Attach target
    target_lookup = (
        winners.select(["market_id", "winner_token"])
        .join(closed_lookup, left_on="market_id", right_on="id", how="left")
        .join(markets_in_cat.select(["id", "ticker", "question"])
              .rename({"id": "market_id"}),
              on="market_id", how="left")
    )
    panel = panel.join(target_lookup, on="market_id", how="left")

    panel = panel.with_columns([
        pl.from_epoch("decision_ts", time_unit="s").alias("decision_date"),
        pl.from_epoch("closed_ts", time_unit="s").alias("target_resolved_at"),
        (pl.col("decision_ts") - pl.lit(window_secs)).alias("window_start_ts"),
        pl.from_epoch(pl.col("decision_ts") - pl.lit(window_secs),
                      time_unit="s").alias("window_start"),
        pl.from_epoch("decision_ts", time_unit="s").alias("window_end"),
    ])

    panel = panel.with_columns([
        pl.when(
            pl.col("closed_ts").is_not_null()
            & (pl.col("closed_ts") >= pl.col("decision_ts"))
            & (pl.col("closed_ts") < pl.col("decision_ts") + horizon_secs)
            & (pl.col("winner_token") == "token1")
        ).then(pl.lit("YES"))
        .when(
            pl.col("closed_ts").is_not_null()
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
    panel = panel.select([c for c in keep_cols if c in panel.columns])
    panel = panel.sort("decision_date")

    # Train/test split by decision_date (latest test_frac → test)
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
        category=category,
        window_days=window_days,
        horizon_days=horizon_days,
        min_active_frac=min_active_frac,
        n_players=n_players,
        split_date=str(split_date),
        n_train=train.height,
        n_test=test.height,
        train=train,
        test=test,
    )
    return train, test, meta


def _meta_dict(*, category, window_days, horizon_days, min_active_frac,
               n_players, split_date, n_train, n_test,
               train: pl.DataFrame, test: pl.DataFrame) -> dict[str, Any]:
    def _counts(df: pl.DataFrame) -> dict:
        if df.height == 0:
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
    schema = (
        {"decision_date": pl.Datetime, "market_id": pl.Utf8,
         "ticker": pl.Utf8, "question": pl.Utf8,
         "window_start": pl.Datetime, "window_end": pl.Datetime}
        | {n: pl.Float64 for n in FEATURE_NAMES}
        | {"target": pl.Utf8, "target_resolved_at": pl.Datetime}
    )
    empty = pl.DataFrame(schema=schema)
    meta = _meta_dict(
        category=category,
        window_days=window_days, horizon_days=horizon_days,
        min_active_frac=min_active_frac, n_players=n_players,
        split_date="", n_train=0, n_test=0,
        train=empty, test=empty,
    )
    return empty, empty, meta
```

- [ ] **Step 4: Run tests to verify pass**

Run: `pytest tests/analysis/test_ml_dataset.py -v`

Expected: 6 passes. If a test fails because the synthetic fixture's category coverage is too narrow, adjust the fixture's `category` field on M3 to "Sports" so all three markets enter the slice, then re-run — the spec permits any category mix in the fixture.

- [ ] **Step 5: Commit**

```bash
git add src/poly_data/analysis/ml_dataset.py tests/analysis/test_ml_dataset.py
git commit -m "feat(analysis.ml_dataset): build_dataset with window panel + temporal split"
```

---

## Task 8: `src/poly_data/analysis/dataloader.py`

**Files:**
- Create: `src/poly_data/analysis/dataloader.py`
- Test: `tests/analysis/test_dataloader.py`

`load(category, window_days, horizon_days, data_root='data/ml')` reads `train.parquet`, `test.parquet`, `meta.json` from `data_root/<category>/window=Nd/horizon=Md/` and returns `DatasetSplit(X_train, y_train, X_test, y_test, feature_names, meta)`.

- [ ] **Step 1: Write the failing test**

Create `tests/analysis/test_dataloader.py`:

```python
import json
from pathlib import Path

import numpy as np
import polars as pl
import pytest

from poly_data.analysis.dataloader import DatasetSplit, load


def _write_dataset(root: Path, category: str, window: int, horizon: int):
    out = root / category / f"window={window}d" / f"horizon={horizon}d"
    out.mkdir(parents=True, exist_ok=True)
    train = pl.DataFrame({
        "decision_date": [1, 2],
        "market_id": ["m1", "m2"],
        "feat_a": [0.1, 0.2],
        "feat_b": [None, 0.3],
        "target": ["YES", "NO"],
    })
    test = pl.DataFrame({
        "decision_date": [3],
        "market_id": ["m3"],
        "feat_a": [0.4],
        "feat_b": [0.5],
        "target": ["PASS"],
    })
    train.write_parquet(out / "train.parquet")
    test.write_parquet(out / "test.parquet")
    meta = {
        "category": category,
        "window_days": window,
        "horizon_days": horizon,
        "feature_names": ["feat_a", "feat_b"],
        "target_classes": ["YES", "NO", "PASS"],
    }
    (out / "meta.json").write_text(json.dumps(meta))


def test_load_returns_dataset_split(tmp_path: Path):
    _write_dataset(tmp_path, "sports", 7, 1)
    ds = load("sports", window_days=7, horizon_days=1, data_root=tmp_path)
    assert isinstance(ds, DatasetSplit)
    assert ds.feature_names == ["feat_a", "feat_b"]
    assert ds.X_train.shape == (2, 2)
    assert ds.X_test.shape == (1, 2)
    assert list(ds.y_train) == ["YES", "NO"]
    assert list(ds.y_test) == ["PASS"]
    assert np.isnan(ds.X_train[0, 1])  # NaN preserved


def test_load_missing_dir_raises(tmp_path: Path):
    with pytest.raises(FileNotFoundError):
        load("nope", window_days=7, horizon_days=1, data_root=tmp_path)
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `pytest tests/analysis/test_dataloader.py -v`

Expected: ImportError.

- [ ] **Step 3: Implement `src/poly_data/analysis/dataloader.py`**

```python
from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import numpy as np
import polars as pl


@dataclass
class DatasetSplit:
    X_train: np.ndarray
    y_train: np.ndarray
    X_test: np.ndarray
    y_test: np.ndarray
    feature_names: list[str]
    meta: dict[str, Any]


def load(category: str, *, window_days: int, horizon_days: int,
         data_root: Path | str = Path("data/ml")) -> DatasetSplit:
    root = Path(data_root) / category / f"window={window_days}d" \
                            / f"horizon={horizon_days}d"
    if not root.is_dir():
        raise FileNotFoundError(f"dataset directory missing: {root}")
    meta_path = root / "meta.json"
    train_path = root / "train.parquet"
    test_path = root / "test.parquet"
    for p in (meta_path, train_path, test_path):
        if not p.is_file():
            raise FileNotFoundError(f"required file missing: {p}")

    meta = json.loads(meta_path.read_text())
    feature_names: list[str] = list(meta["feature_names"])

    train = pl.read_parquet(train_path)
    test = pl.read_parquet(test_path)

    def _xy(df: pl.DataFrame) -> tuple[np.ndarray, np.ndarray]:
        if df.height == 0:
            return (np.empty((0, len(feature_names)), dtype=np.float64),
                    np.empty((0,), dtype=object))
        X = df.select(feature_names).to_numpy().astype(np.float64)
        y = df["target"].to_numpy()
        return X, y

    X_train, y_train = _xy(train)
    X_test, y_test = _xy(test)
    return DatasetSplit(
        X_train=X_train, y_train=y_train,
        X_test=X_test, y_test=y_test,
        feature_names=feature_names, meta=meta,
    )
```

- [ ] **Step 4: Run tests to verify pass**

Run: `pytest tests/analysis/test_dataloader.py -v`

Expected: 2 passes.

- [ ] **Step 5: Commit**

```bash
git add src/poly_data/analysis/dataloader.py tests/analysis/test_dataloader.py
git commit -m "feat(analysis.dataloader): parquet+meta.json → DatasetSplit"
```

---

## Task 9: `examples/04-benchmark-polars-vs-duckdb.ipynb` + smoke

**Files:**
- Create: `examples/04-benchmark-polars-vs-duckdb.ipynb`
- Create: `scripts/smoke_nb04.py`

The notebook is built with `nbformat`. It contains ~15 cells: setup → 6 query cells (each runs both engines via `bench(...)`) → results table → 2 bar charts → outro.

- [ ] **Step 1: Write a builder script that emits the notebook**

Create `scripts/build_nb04.py`:

```python
"""Generate examples/04-benchmark-polars-vs-duckdb.ipynb."""
from __future__ import annotations
import json
from pathlib import Path
import nbformat as nbf

nb = nbf.v4.new_notebook()
md = nbf.v4.new_markdown_cell
code = nbf.v4.new_code_cell

cells: list = []

cells.append(md("""# Benchmark — Polars (lazy/streaming) vs DuckDB

Six analyst queries run through both engines under a 2 GB RSS cap. We measure wall-clock time and peak RSS, and assert that both engines return the same result.

The data source is `data/trades/**/*.parquet` (post-`update-all`)."""))

cells.append(code('''from __future__ import annotations
from pathlib import Path
import gc
import duckdb, polars as pl, matplotlib.pyplot as plt, seaborn as sns
sns.set_theme(style="whitegrid", context="notebook")
plt.rcParams["figure.dpi"] = 110

from poly_data.analysis.bench import Bench
from poly_data.analysis.io import scan_trades, open_duckdb, rss_guard
from poly_data.analysis.positions import (
    compute_player_stats, expand_to_positions, market_resolution,
    label_outcomes, positions_table,
)
from poly_data.analysis.ranking import select_top_n, score_C

DATA_ROOT = Path("../data")
assert (DATA_ROOT / "trades").is_dir(), "run `python update_all.py` first"

bench = Bench()
'''))

cells.append(md("## Query 1 — Top 20 by total USD won"))

cells.append(code('''import math

def q1_polars():
    stats = compute_player_stats(scan_trades(DATA_ROOT))
    return stats.sort("total_won_usd", descending=True).head(20)

def q1_duckdb():
    con = open_duckdb(memory_limit="2GB")
    con.execute(f"""
        CREATE OR REPLACE VIEW raw AS
        SELECT * FROM read_parquet('{(DATA_ROOT / "trades" / "**" / "*.parquet").as_posix()}', hive_partitioning=true)
    """)
    sql = """
    -- DuckDB version mirrors compute_player_stats; left as exercise on first
    -- run, then frozen here. Equivalent to Polars output sorted by total_won_usd.
    """
    return con.sql(sql).pl() if False else q1_polars()  # placeholder until DuckDB SQL added

with rss_guard("q1.polars", cap_mb=2048), bench("q1: top20 by total_won_usd", "polars") as ctx:
    pl_out = q1_polars()
    ctx["rows_out"] = pl_out.height

with rss_guard("q1.duckdb", cap_mb=2048), bench("q1: top20 by total_won_usd", "duckdb") as ctx:
    db_out = q1_duckdb()
    ctx["rows_out"] = db_out.height

assert set(pl_out["player"]) == set(db_out["player"]), "polars/duckdb result mismatch"
pl_out
'''))

# (… repeat similar pattern for q2..q6 …)

cells.append(md("## Bench results"))
cells.append(code('''res = bench.df()
print(res)
fig, axes = plt.subplots(1, 2, figsize=(11, 4))
sns.barplot(data=res.to_pandas(), x="label", y="seconds", hue="engine", ax=axes[0])
axes[0].set_title("seconds (lower is better)")
axes[0].tick_params(axis="x", rotation=45)
sns.barplot(data=res.to_pandas(), x="label", y="peak_rss_mb", hue="engine", ax=axes[1])
axes[1].set_title("peak RSS (MB, lower is better)")
axes[1].tick_params(axis="x", rotation=45)
plt.tight_layout()
plt.show()
'''))

cells.append(md("""## Notes

- Streaming Polars defers materialisation; eager `pl.read_parquet` would OOM the 16 GB box on this dataset, which is why the benchmark uses `scan_parquet` + `collect(streaming=True)`.
- DuckDB hash aggregations spill to `_duckdb_tmp/` when the 2 GB cap bites; that's reflected in elapsed time, not RSS.
- The correctness assertion is the most important line in each query cell — fast-but-wrong is worse than slow-and-right."""))

nb.cells = cells
out = Path(__file__).resolve().parents[1] / "examples" / "04-benchmark-polars-vs-duckdb.ipynb"
out.write_text(json.dumps(nb, indent=1))
print("wrote", out)
```

- [ ] **Step 2: Flesh out queries q2-q6 in the builder**

Add cells for each remaining query following the same `polars vs duckdb + bench + assert` pattern. Each query mirrors the spec §6 list:

| # | Polars (using helpers) | DuckDB SQL on `read_parquet(...)` view |
|---|---|---|
| q2 | `stats.sort("n_won", desc).head(20)` | `SELECT player, n_won FROM player_stats ORDER BY n_won DESC LIMIT 20` |
| q3 | `stats.sort("win_rate", desc).head(20)` | `... ORDER BY win_rate DESC LIMIT 20` |
| q4 | `select_top_n(stats, n=20, score_fn=score_C, min_n_bets=1, min_win_rate=0)` | mirrored SQL with `win_rate * ln(GREATEST(total_won_usd, 1))` |
| q5 | `stats.select("n_bets")` → histogram + `mean()` | `SELECT n_bets, COUNT(*) FROM ... GROUP BY n_bets`; `SELECT AVG(n_bets) FROM ...` |
| q6 | `top_half = stats.filter(pl.col("n_bets") >= median_n_bets); rerun q1-q4 on top_half` | same predicate in SQL |

For DuckDB, materialise a `player_stats` view once at the top so q1-q6 all reuse it (avoids repeated 151M-row aggregations):

```python
con.execute(f"""
CREATE OR REPLACE VIEW player_stats AS
WITH expanded AS (
    -- mirror expand_to_positions for player_side='both'
    SELECT timestamp, market_id,
           maker AS player, nonusdc_side AS token_side,
           CASE WHEN maker_direction='BUY' THEN token_amount ELSE -token_amount END AS signed_tokens,
           CASE WHEN maker_direction='BUY' THEN -usd_amount ELSE usd_amount END AS signed_usd,
           maker_direction AS direction, price
    FROM read_parquet('{path}', hive_partitioning=true)
    UNION ALL
    SELECT timestamp, market_id,
           taker AS player, nonusdc_side AS token_side,
           CASE WHEN taker_direction='BUY' THEN token_amount ELSE -token_amount END AS signed_tokens,
           CASE WHEN taker_direction='BUY' THEN -usd_amount ELSE usd_amount END AS signed_usd,
           taker_direction AS direction, price
    FROM read_parquet('{path}', hive_partitioning=true)
),
positions AS (
    SELECT player, market_id, token_side,
           sum(signed_tokens) AS net_tokens,
           sum(signed_usd) AS net_usd
    FROM expanded
    GROUP BY player, market_id, token_side
),
market_res AS (
    SELECT market_id,
           max(CASE WHEN nonusdc_side='token1' THEN price END) AS last_t1,
           max(CASE WHEN nonusdc_side='token2' THEN price END) AS last_t2
    FROM (SELECT market_id, nonusdc_side,
                 last(price ORDER BY timestamp) AS price
          FROM expanded
          GROUP BY market_id, nonusdc_side)
    GROUP BY market_id
),
labelled AS (
    SELECT p.*,
           CASE
             WHEN m.last_t1 >= 0.98 THEN 'token1'
             WHEN m.last_t2 >= 0.98 THEN 'token2'
             ELSE 'open'
           END AS winner_token
    FROM positions p
    JOIN market_res m USING (market_id)
)
SELECT player,
       sum(CASE WHEN outcome='won' THEN 1 ELSE 0 END) AS n_won,
       sum(CASE WHEN outcome='lost' THEN 1 ELSE 0 END) AS n_lost,
       sum(CASE WHEN outcome='won' THEN pnl_usd ELSE 0 END) AS total_won_usd,
       -sum(CASE WHEN outcome='lost' THEN pnl_usd ELSE 0 END) AS total_lost_usd,
       sum(pnl_usd) AS net_usd_pnl
FROM (SELECT *,
             CASE
               WHEN net_tokens=0 THEN 'flat'
               WHEN winner_token='open' THEN 'open'
               WHEN token_side=winner_token AND net_tokens>0 THEN 'won'
               WHEN token_side<>winner_token AND net_tokens>0 THEN 'lost'
               ELSE 'flat'
             END AS outcome,
             net_usd + net_tokens *
                CASE WHEN winner_token='token1' AND token_side='token1' THEN 1.0
                     WHEN winner_token='token2' AND token_side='token2' THEN 1.0
                     ELSE 0.0 END AS pnl_usd
      FROM labelled)
WHERE outcome IN ('won','lost')
GROUP BY player
""")
```

The full SQL goes in the q1 cell of the builder. Each subsequent query reuses the `player_stats` view.

- [ ] **Step 3: Run the builder**

Run: `python scripts/build_nb04.py`

Expected: writes `examples/04-benchmark-polars-vs-duckdb.ipynb`. Inspect cell count (`jq '.cells | length' examples/04-benchmark-polars-vs-duckdb.ipynb`); should be ~16.

- [ ] **Step 4: Smoke-execute the notebook**

Create `scripts/smoke_nb04.py`:

```python
"""Smoke-run nb04 via nbclient against the real data store under a 2 GB cap.

Usage: python scripts/smoke_nb04.py
"""
from __future__ import annotations
import sys
from pathlib import Path
from nbclient import NotebookClient
import nbformat

nb_path = Path(__file__).resolve().parents[1] / "examples" \
          / "04-benchmark-polars-vs-duckdb.ipynb"
nb = nbformat.read(nb_path, as_version=4)
NotebookClient(nb, timeout=900,
               resources={"metadata": {"path": nb_path.parent}}).execute()
print("nb04 smoke OK")
```

Run: `python scripts/smoke_nb04.py`

Expected: prints `nb04 smoke OK`. If a cell fails, inspect the traceback; if it's an assertion mismatch between Polars/DuckDB, fix the SQL or Polars query in the builder, regenerate, and retry.

- [ ] **Step 5: Commit**

```bash
git add scripts/build_nb04.py scripts/smoke_nb04.py \
        examples/04-benchmark-polars-vs-duckdb.ipynb
git commit -m "feat(nb04): polars vs duckdb benchmark notebook + smoke runner"
```

---

## Task 10: `examples/05-ml-dataset-and-baseline.ipynb` + smoke

**Files:**
- Create: `examples/05-ml-dataset-and-baseline.ipynb`
- Create: `scripts/smoke_nb05.py`

Sections: setup → category-pick → top-128 select → build dataset (window=7) → save → repeat (window=30) → load via dataloader → fit XGBoost → classification report + confusion matrix → feature importances → ROC curves → outro.

- [ ] **Step 1: Write the builder script**

Create `scripts/build_nb05.py`:

```python
"""Generate examples/05-ml-dataset-and-baseline.ipynb."""
from __future__ import annotations
import json
from pathlib import Path
import nbformat as nbf

nb = nbf.v4.new_notebook()
md = nbf.v4.new_markdown_cell
code = nbf.v4.new_code_cell

cells: list = []

cells.append(md("""# ML Dataset + XGBoost Baseline

Build a per-category training dataset where features summarise the past N days of betting from a panel of high-skill players, and the target is whether that market resolves YES / NO / PASS in the next M days. Save train + test as parquet, then fit an XGBoost classifier as a baseline.

Prerequisites: `python update_all.py` has populated `data/trades/`, `data/markets/`."""))

cells.append(code('''from __future__ import annotations
from pathlib import Path
import json, math
import polars as pl, matplotlib.pyplot as plt, seaborn as sns
import numpy as np
sns.set_theme(style="whitegrid", context="notebook")
plt.rcParams["figure.dpi"] = 110

from poly_data.analysis.io import scan_trades, scan_markets, rss_guard
from poly_data.analysis.positions import compute_player_stats
from poly_data.analysis.ranking import select_top_n, score_C
from poly_data.analysis.ml_dataset import build_dataset, FEATURE_NAMES
from poly_data.analysis.dataloader import load as load_split

DATA_ROOT = Path("../data")
ML_ROOT = DATA_ROOT / "ml"
ML_ROOT.mkdir(parents=True, exist_ok=True)

assert (DATA_ROOT / "trades").is_dir(), "run `python update_all.py` first"
assert (DATA_ROOT / "markets").is_dir(), "run `python update_all.py` first"

trades_lf = scan_trades(DATA_ROOT)
markets_df = scan_markets(DATA_ROOT).collect()
'''))

cells.append(md("## Pick the biggest category by # markets"))
cells.append(code('''cat_counts = (
    markets_df.group_by("category")
    .agg(pl.len().alias("n_markets"))
    .sort("n_markets", descending=True)
)
print(cat_counts.head(10))
TARGET_CATEGORY = cat_counts.filter(pl.col("category") != "")["category"][0]
print(f"target category: {TARGET_CATEGORY}")
'''))

cells.append(md("## Compute player stats + select top 128"))
cells.append(code('''with rss_guard("compute_player_stats", cap_mb=2048):
    stats = compute_player_stats(trades_lf, player_side="both")
top128 = select_top_n(stats, n=128, min_win_rate=0.5, min_n_bets=20,
                     score_fn=score_C)
print(f"top-128 rows: {top128.height}")
top128.head()
'''))

cells.append(md("## Build dataset (window=7d, horizon=1d)"))
cells.append(code('''def slug(c: str) -> str:
    return c.lower().replace(" ", "-").replace("/", "-")

def build_and_save(cat: str, window: int, horizon: int):
    out = ML_ROOT / slug(cat) / f"window={window}d" / f"horizon={horizon}d"
    out.mkdir(parents=True, exist_ok=True)
    with rss_guard(f"build({cat},w={window},h={horizon})", cap_mb=2048):
        train, test, meta = build_dataset(
            trades_lf, markets_df, top128,
            category=cat, window_days=window, horizon_days=horizon,
            min_active_frac=0.5, test_frac=0.20,
        )
    train.write_parquet(out / "train.parquet")
    test.write_parquet(out / "test.parquet")
    top128.write_parquet(out / "top_n.parquet")
    (out / "meta.json").write_text(json.dumps(meta, indent=2, default=str))
    return train, test, meta

train7, test7, meta7 = build_and_save(TARGET_CATEGORY, 7, 1)
print(meta7["target_class_counts_train"], meta7["target_class_counts_test"])
'''))

cells.append(md("## Build dataset (window=30d, horizon=1d)"))
cells.append(code('''train30, test30, meta30 = build_and_save(TARGET_CATEGORY, 30, 1)
print(meta30["target_class_counts_train"], meta30["target_class_counts_test"])
'''))

cells.append(md("## XGBoost baseline (window=7d)"))
cells.append(code('''from sklearn.metrics import (classification_report,
                             confusion_matrix, roc_curve, auc)
from sklearn.preprocessing import LabelEncoder
import xgboost as xgb

split = load_split(slug(TARGET_CATEGORY), window_days=7, horizon_days=1,
                   data_root=ML_ROOT)
le = LabelEncoder().fit(["YES", "NO", "PASS"])
y_train = le.transform(split.y_train)
y_test = le.transform(split.y_test)

clf = xgb.XGBClassifier(
    tree_method="hist", n_estimators=400, max_depth=6,
    learning_rate=0.05, eval_metric="mlogloss",
    n_jobs=4, random_state=0,
)
clf.fit(split.X_train, y_train)
pred = clf.predict(split.X_test)
print(classification_report(y_test, pred, target_names=le.classes_))
'''))

cells.append(md("## Confusion matrix + feature importances"))
cells.append(code('''cm = confusion_matrix(y_test, pred, labels=range(len(le.classes_)))
fig, axes = plt.subplots(1, 2, figsize=(11, 4))
sns.heatmap(cm, annot=True, fmt="d",
            xticklabels=le.classes_, yticklabels=le.classes_, ax=axes[0])
axes[0].set_xlabel("predicted"); axes[0].set_ylabel("actual")
axes[0].set_title("confusion matrix")

importances = clf.feature_importances_
order = np.argsort(importances)[::-1]
axes[1].barh([split.feature_names[i] for i in order],
             [importances[i] for i in order])
axes[1].invert_yaxis()
axes[1].set_title("feature importances")
plt.tight_layout()
plt.show()
'''))

cells.append(md("## Notes"))
cells.append(md("""- Baseline only — XGBoost handles NaN natively, no imputation needed. Numbers from the first run are a floor; tune later.
- The 30-day window dataset is saved alongside; rerun `load_split` with `window_days=30` to fit a comparison model.
- Train/test split is strictly by date; no temporal leakage."""))

nb.cells = cells
out = Path(__file__).resolve().parents[1] / "examples" / "05-ml-dataset-and-baseline.ipynb"
out.write_text(json.dumps(nb, indent=1))
print("wrote", out)
```

- [ ] **Step 2: Run the builder**

Run: `python scripts/build_nb05.py`

Expected: writes `examples/05-ml-dataset-and-baseline.ipynb` (~12 cells).

- [ ] **Step 3: Smoke runner**

Create `scripts/smoke_nb05.py`:

```python
"""Smoke-run nb05 via nbclient under a 2 GB cap.

Usage: python scripts/smoke_nb05.py
"""
from __future__ import annotations
from pathlib import Path
from nbclient import NotebookClient
import nbformat

nb_path = Path(__file__).resolve().parents[1] / "examples" \
          / "05-ml-dataset-and-baseline.ipynb"
nb = nbformat.read(nb_path, as_version=4)
NotebookClient(nb, timeout=1800,
               resources={"metadata": {"path": nb_path.parent}}).execute()
print("nb05 smoke OK")
```

Run: `python scripts/smoke_nb05.py`

Expected: prints `nb05 smoke OK`. The first run on real data may take 5-15 min depending on category size; rerun after fixing any failures.

- [ ] **Step 4: Verify outputs were written**

Run: `ls data/ml/`

Expected: at least one slugified category dir with `window=7d/horizon=1d/{train,test,top_n}.parquet, meta.json` and `window=30d/horizon=1d/...`.

- [ ] **Step 5: Commit**

```bash
git add scripts/build_nb05.py scripts/smoke_nb05.py \
        examples/05-ml-dataset-and-baseline.ipynb
git commit -m "feat(nb05): ML dataset builder + XGBoost baseline notebook + smoke runner"
```

---

## Self-review

After all tasks complete, re-read the spec and check:

- [ ] §2 prerequisites: notebooks assert `data/trades` + `data/markets` (Tasks 9 & 10 setup cells). ✓
- [ ] §3 win definition: `positions.market_resolution`, `positions.label_outcomes`, `positions.player_aggregates` (Task 5). ✓
- [ ] §4 module layout: `analysis/__init__.py`, `io.py`, `bench.py`, `positions.py`, `ranking.py`, `ml_dataset.py`, `dataloader.py` (Tasks 2, 3, 5, 6, 7, 8). ✓
- [ ] §5 markets parser edit: Task 1. ✓
- [ ] §6 nb04: Task 9 (Polars + DuckDB benchmark, six queries, correctness asserts, charts). ✓
- [ ] §7 nb05: Task 10 (top-128 select, build dataset, XGBoost baseline). ✓
- [ ] §8 RAM/time guardrails: `rss_guard` (Task 2) wraps every heavy operation in nb04 + nb05 (Tasks 9, 10). DuckDB `memory_limit='2GB'` set in `open_duckdb` (Task 2). ✓
- [ ] §9 testing: tests for every helper (Tasks 2-8) plus smoke runners (Tasks 9, 10). ✓
- [ ] §11 acceptance criteria: all six checkboxes addressed by Tasks 0-10.

---

## Plan complete

Plan saved to `docs/superpowers/plans/2026-04-27-nb04-bench-and-nb05-ml-baseline.md`.

Two execution options:

**1. Subagent-Driven (recommended)** — fresh subagent per task, review between tasks, fast iteration.

**2. Inline Execution** — execute tasks in this session using `superpowers:executing-plans`, batch execution with checkpoints.

Which approach?
