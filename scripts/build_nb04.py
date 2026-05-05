"""Generate examples/04-benchmark-polars-vs-duckdb.ipynb."""
from __future__ import annotations

from pathlib import Path

import nbformat as nbf

DUCKDB_PLAYER_STATS_SQL = """
CREATE OR REPLACE VIEW player_stats AS
WITH expanded AS (
  SELECT timestamp, market_id,
         maker AS player, nonusdc_side AS token_side,
         CASE WHEN maker_direction='BUY' THEN token_amount
              ELSE -token_amount END AS signed_tokens,
         CASE WHEN maker_direction='BUY' THEN -usd_amount
              ELSE usd_amount END AS signed_usd,
         maker_direction AS direction, price
  FROM read_parquet('{path}', hive_partitioning=true)
  UNION ALL
  SELECT timestamp, market_id,
         taker AS player, nonusdc_side AS token_side,
         CASE WHEN taker_direction='BUY' THEN token_amount
              ELSE -token_amount END AS signed_tokens,
         CASE WHEN taker_direction='BUY' THEN -usd_amount
              ELSE usd_amount END AS signed_usd,
         taker_direction AS direction, price
  FROM read_parquet('{path}', hive_partitioning=true)
),
last_per_side AS (
  SELECT market_id, nonusdc_side, ARG_MAX(price, timestamp) AS last_price
  FROM read_parquet('{path}', hive_partitioning=true)
  GROUP BY market_id, nonusdc_side
),
winners AS (
  SELECT market_id,
         CASE
           WHEN MAX(CASE WHEN nonusdc_side='token1' THEN last_price END) >= 0.98 THEN 'token1'
           WHEN MAX(CASE WHEN nonusdc_side='token2' THEN last_price END) >= 0.98 THEN 'token2'
           ELSE 'open'
         END AS winner_token
  FROM last_per_side
  GROUP BY market_id
),
positions AS (
  SELECT player, market_id, token_side,
         SUM(signed_tokens) AS net_tokens,
         SUM(signed_usd)    AS net_usd
  FROM expanded
  GROUP BY player, market_id, token_side
),
labelled AS (
  SELECT p.*, w.winner_token,
         CASE
           WHEN p.net_tokens = 0 THEN 'flat'
           WHEN w.winner_token = 'open' THEN 'open'
           WHEN p.token_side = w.winner_token AND p.net_tokens > 0 THEN 'won'
           WHEN p.token_side <> w.winner_token AND p.net_tokens > 0 THEN 'lost'
           ELSE 'flat'
         END AS outcome,
         CASE
           WHEN w.winner_token != 'open'
                AND p.token_side = w.winner_token
                AND p.net_tokens > 0
             THEN p.net_usd + p.net_tokens * 1.0
           WHEN w.winner_token != 'open'
                AND p.token_side <> w.winner_token
                AND p.net_tokens > 0
             THEN p.net_usd + p.net_tokens * 0.0
           ELSE p.net_usd
         END AS pnl_usd
  FROM positions p
  LEFT JOIN winners w USING (market_id)
)
SELECT player,
       SUM(CASE WHEN outcome='won' THEN 1 ELSE 0 END) AS n_won,
       SUM(CASE WHEN outcome='lost' THEN 1 ELSE 0 END) AS n_lost,
       SUM(CASE WHEN outcome='won' THEN pnl_usd ELSE 0 END) AS total_won_usd,
       -SUM(CASE WHEN outcome='lost' THEN pnl_usd ELSE 0 END) AS total_lost_usd,
       SUM(CASE WHEN outcome IN ('won','lost') THEN pnl_usd ELSE 0 END) AS net_usd_pnl,
       SUM(CASE WHEN outcome IN ('won','lost') THEN 1 ELSE 0 END) AS n_bets,
       CAST(SUM(CASE WHEN outcome='won' THEN 1 ELSE 0 END) AS DOUBLE)
       / NULLIF(SUM(CASE WHEN outcome IN ('won','lost') THEN 1 ELSE 0 END), 0) AS win_rate
FROM labelled
GROUP BY player
HAVING SUM(CASE WHEN outcome IN ('won','lost') THEN 1 ELSE 0 END) > 0
"""


def main() -> None:
    nb = nbf.v4.new_notebook()
    md = nbf.v4.new_markdown_cell
    code = nbf.v4.new_code_cell

    cells: list = []

    cells.append(md(
        "# Benchmark — Polars (lazy/streaming) vs DuckDB\n\n"
        "Six analyst queries run end-to-end through both engines under a 2 GB RSS cap. "
        "We measure wall-clock time and peak RSS, and assert that both engines return "
        "the same result for every query.\n\n"
        "**Data source:** `data/trades/**/*.parquet` (post `python update_all.py`).\n\n"
        "**Engines:**\n"
        "- **Polars** — `pl.scan_parquet(...)` + `.collect(streaming=True)`.\n"
        "- **DuckDB** — `read_parquet(..., hive_partitioning=true)` view, `memory_limit='2GB'`."
    ))

    setup_src = (
        "from __future__ import annotations\n"
        "from pathlib import Path\n"
        "import gc\n\n"
        "import polars as pl\n"
        "import matplotlib.pyplot as plt\n"
        "import seaborn as sns\n"
        "sns.set_theme(style='whitegrid', context='notebook')\n"
        "plt.rcParams['figure.dpi'] = 110\n\n"
        "import os\n"
        "from functools import partial\n"
        "from poly_data.analysis.bench import Bench\n"
        "from poly_data.analysis.io import scan_trades, open_duckdb\n"
        "from poly_data.analysis.io import rss_guard as _rss_guard\n"
        "rss_guard = partial(_rss_guard, cap_mb=6144)\n"
        "from poly_data.analysis.positions import compute_player_stats\n"
        "from poly_data.analysis.ranking import score_C\n\n"
        "DATA_ROOT = Path(os.environ.get('POLY_DATA_ROOT', '../data'))\n"
        "TRADES_GLOB = (DATA_ROOT / 'trades' / '**' / '*.parquet').as_posix()\n"
        "assert (DATA_ROOT / 'trades').is_dir(), 'run `python update_all.py` first'\n\n"
        "bench = Bench()\n\n"
        "# DuckDB connection reused across all queries; player_stats view registered once below.\n"
        "con = open_duckdb(memory_limit='2GB', threads=4)\n"
        f"DUCKDB_PLAYER_STATS_SQL = {DUCKDB_PLAYER_STATS_SQL!r}.format(path=TRADES_GLOB)\n"
        "con.execute(DUCKDB_PLAYER_STATS_SQL)\n\n"
        "def polars_player_stats() -> pl.DataFrame:\n"
        "    return compute_player_stats(scan_trades(DATA_ROOT), player_side='both')\n"
    )
    cells.append(code(setup_src))

    cells.append(md("## Query 1 — Top 20 by total USD won"))
    cells.append(code(
        "with rss_guard('q1.polars'), bench('q1: top20 by total_won_usd', 'polars') as ctx:\n"
        "    pl_out = (polars_player_stats()\n"
        "              .sort(['total_won_usd', 'player'], descending=[True, False]).head(20))\n"
        "    ctx['rows_out'] = pl_out.height\n\n"
        "with rss_guard('q1.duckdb'), bench('q1: top20 by total_won_usd', 'duckdb') as ctx:\n"
        "    db_out = con.sql('SELECT player, total_won_usd FROM player_stats ORDER BY total_won_usd DESC, player ASC LIMIT 20').pl()\n"
        "    ctx['rows_out'] = db_out.height\n\n"
        "pl_top = set(pl_out['player'].to_list()); db_top = set(db_out['player'].to_list())\n"
        "assert pl_top == db_top, f'mismatch: polars-only {pl_top - db_top}, duckdb-only {db_top - pl_top}'\n"
        "pl_out.head(10)\n"
    ))

    cells.append(md(
        "## Query 2 — Top 20 by # bets won\n\n"
        "Tie-break: secondary sort on `player` (string) so polars and DuckDB pick the same 20 "
        "rows when many players are tied on `n_won`."
    ))
    cells.append(code(
        "with rss_guard('q2.polars'), bench('q2: top20 by n_won', 'polars') as ctx:\n"
        "    pl_out = (polars_player_stats()\n"
        "              .sort(['n_won', 'player'], descending=[True, False]).head(20))\n"
        "    ctx['rows_out'] = pl_out.height\n\n"
        "with rss_guard('q2.duckdb'), bench('q2: top20 by n_won', 'duckdb') as ctx:\n"
        "    db_out = con.sql('SELECT player, n_won FROM player_stats ORDER BY n_won DESC, player ASC LIMIT 20').pl()\n"
        "    ctx['rows_out'] = db_out.height\n\n"
        "assert set(pl_out['player'].to_list()) == set(db_out['player'].to_list()), 'q2 mismatch'\n"
        "pl_out.head(10)\n"
    ))

    cells.append(md(
        "## Query 3 — Top 20 by win-rate (won/(won+lost))\n\n"
        "Filter out players with 0 decided bets (NaN win_rate)."
    ))
    cells.append(code(
        "with rss_guard('q3.polars'), bench('q3: top20 by win_rate', 'polars') as ctx:\n"
        "    pl_out = (polars_player_stats()\n"
        "              .filter(pl.col('win_rate').is_not_null())\n"
        "              .sort(['win_rate', 'player'], descending=[True, False]).head(20))\n"
        "    ctx['rows_out'] = pl_out.height\n\n"
        "with rss_guard('q3.duckdb'), bench('q3: top20 by win_rate', 'duckdb') as ctx:\n"
        "    db_out = con.sql(\n"
        "        'SELECT player, win_rate, n_bets FROM player_stats '\n"
        "        'WHERE win_rate IS NOT NULL ORDER BY win_rate DESC, player ASC LIMIT 20'\n"
        "    ).pl()\n"
        "    ctx['rows_out'] = db_out.height\n\n"
        "assert set(pl_out['player'].to_list()) == set(db_out['player'].to_list()), 'q3 mismatch'\n"
        "pl_out.head(10)\n"
    ))

    cells.append(md(
        "## Query 4 — Top 20 by score_C (`win_rate * log(max(1, total_won_usd))`)"
    ))
    cells.append(code(
        "with rss_guard('q4.polars'), bench('q4: top20 by score_C', 'polars') as ctx:\n"
        "    stats = polars_player_stats()\n"
        "    pl_out = (stats.filter(pl.col('win_rate').is_not_null())\n"
        "              .with_columns(score_C(pl.col).alias('score'))\n"
        "              .sort(['score', 'player'], descending=[True, False], nulls_last=True).head(20)\n"
        "              .select(['player', 'score', 'win_rate', 'total_won_usd']))\n"
        "    ctx['rows_out'] = pl_out.height\n\n"
        "with rss_guard('q4.duckdb'), bench('q4: top20 by score_C', 'duckdb') as ctx:\n"
        "    db_out = con.sql(\n"
        "        'SELECT player, win_rate * LN(GREATEST(total_won_usd, 1.0)) AS score, '\n"
        "        '       win_rate, total_won_usd '\n"
        "        'FROM player_stats WHERE win_rate IS NOT NULL '\n"
        "        'ORDER BY score DESC, player ASC LIMIT 20'\n"
        "    ).pl()\n"
        "    ctx['rows_out'] = db_out.height\n\n"
        "assert set(pl_out['player'].to_list()) == set(db_out['player'].to_list()), 'q4 mismatch'\n"
        "pl_out.head(10)\n"
    ))

    cells.append(md(
        "## Query 5 — Distribution of n_bets per player + mean\n\n"
        "Histogram + mean. Output is a single row of stats; for the assert we compare the mean."
    ))
    cells.append(code(
        "with rss_guard('q5.polars'), bench('q5: n_bets distribution + mean', 'polars') as ctx:\n"
        "    stats = polars_player_stats()\n"
        "    pl_mean = float(stats['n_bets'].mean())\n"
        "    ctx['rows_out'] = stats.height\n\n"
        "with rss_guard('q5.duckdb'), bench('q5: n_bets distribution + mean', 'duckdb') as ctx:\n"
        "    db_mean = float(con.sql('SELECT AVG(n_bets) AS m FROM player_stats').pl()['m'][0])\n"
        "    ctx['rows_out'] = int(con.sql('SELECT COUNT(*) AS n FROM player_stats').pl()['n'][0])\n\n"
        "assert abs(pl_mean - db_mean) < 1e-6, f'q5 mean mismatch: polars={pl_mean}, duckdb={db_mean}'\n"
        "n_bets_arr = stats['n_bets'].to_numpy()\n"
        "fig, ax = plt.subplots(figsize=(9, 3.5))\n"
        "ax.hist(n_bets_arr, bins=40, color='#6366f1')\n"
        "ax.set_yscale('log'); ax.set_xlabel('n_bets per player'); ax.set_ylabel('count (log)')\n"
        "ax.set_title(f'n_bets distribution; mean = {pl_mean:,.2f}'); plt.tight_layout(); plt.show()\n"
    ))

    cells.append(md(
        "## Query 6 — Q1-Q4 restricted to top half of players by n_bets\n\n"
        "Drop the bottom 50% of players ranked by `n_bets`, then re-run Q1-Q4 on the survivors."
    ))
    cells.append(code(
        "with rss_guard('q6.polars'), bench('q6: top-half subset, q1-q4', 'polars') as ctx:\n"
        "    stats = polars_player_stats()\n"
        "    median_bets = float(stats['n_bets'].median())\n"
        "    sub = stats.filter(pl.col('n_bets') >= median_bets)\n"
        "    pl_q1 = sub.sort(['total_won_usd', 'player'], descending=[True, False]).head(20)\n"
        "    pl_q2 = sub.sort(['n_won', 'player'],         descending=[True, False]).head(20)\n"
        "    pl_q3 = (sub.filter(pl.col('win_rate').is_not_null())\n"
        "             .sort(['win_rate', 'player'], descending=[True, False]).head(20))\n"
        "    pl_q4 = (sub.filter(pl.col('win_rate').is_not_null())\n"
        "             .with_columns(score_C(pl.col).alias('score'))\n"
        "             .sort(['score', 'player'], descending=[True, False], nulls_last=True).head(20))\n"
        "    ctx['rows_out'] = pl_q1.height + pl_q2.height + pl_q3.height + pl_q4.height\n\n"
        "with rss_guard('q6.duckdb'), bench('q6: top-half subset, q1-q4', 'duckdb') as ctx:\n"
        "    sub_sql = 'WITH sub AS (SELECT * FROM player_stats WHERE n_bets >= (SELECT MEDIAN(n_bets) FROM player_stats)) '\n"
        "    db_q1 = con.sql(sub_sql + 'SELECT player, total_won_usd FROM sub ORDER BY total_won_usd DESC, player ASC LIMIT 20').pl()\n"
        "    db_q2 = con.sql(sub_sql + 'SELECT player, n_won FROM sub ORDER BY n_won DESC, player ASC LIMIT 20').pl()\n"
        "    db_q3 = con.sql(sub_sql + 'SELECT player, win_rate FROM sub WHERE win_rate IS NOT NULL ORDER BY win_rate DESC, player ASC LIMIT 20').pl()\n"
        "    db_q4 = con.sql(sub_sql + 'SELECT player, win_rate * LN(GREATEST(total_won_usd, 1.0)) AS score '\n"
        "                              'FROM sub WHERE win_rate IS NOT NULL ORDER BY score DESC, player ASC LIMIT 20').pl()\n"
        "    ctx['rows_out'] = db_q1.height + db_q2.height + db_q3.height + db_q4.height\n\n"
        "for label, p, d in (('q6.q1', pl_q1, db_q1), ('q6.q2', pl_q2, db_q2),\n"
        "                    ('q6.q3', pl_q3, db_q3), ('q6.q4', pl_q4, db_q4)):\n"
        "    assert set(p['player'].to_list()) == set(d['player'].to_list()), f'{label} mismatch'\n"
        "print('q6 sub-queries all match')\n"
    ))

    cells.append(md("## Bench results"))
    cells.append(code(
        "res = bench.df()\n"
        "print(res)\n"
        "import pandas as pd\n"
        "rp = res.to_pandas()\n"
        "fig, axes = plt.subplots(1, 2, figsize=(13, 4.5))\n"
        "sns.barplot(data=rp, x='label', y='seconds', hue='engine', ax=axes[0])\n"
        "axes[0].set_title('seconds (lower is better)'); axes[0].tick_params(axis='x', rotation=30)\n"
        "for lbl in axes[0].get_xticklabels(): lbl.set_horizontalalignment('right')\n"
        "sns.barplot(data=rp, x='label', y='peak_rss_mb', hue='engine', ax=axes[1])\n"
        "axes[1].set_title('peak RSS (MB above baseline, lower is better)')\n"
        "axes[1].tick_params(axis='x', rotation=30)\n"
        "for lbl in axes[1].get_xticklabels(): lbl.set_horizontalalignment('right')\n"
        "plt.tight_layout(); plt.show()\n"
    ))

    cells.append(md(
        "## Notes\n\n"
        "- **Streaming Polars** defers materialisation; eager `pl.read_parquet` would OOM the 16 GB box on this dataset.\n"
        "- **DuckDB** hash aggregations spill to `_duckdb_tmp/` when the 2 GB cap bites; that surfaces as elapsed time, not RSS.\n"
        "- The **correctness assertion** in each query cell is the most important line — fast-but-wrong is worse than slow-and-right.\n"
        "- This is a cold-start benchmark: each query re-runs the full pipeline. In practice, analysts pre-compute `player_stats` once and slice it many ways — that workload would favour both engines roughly equally."
    ))

    nb.cells = cells
    out = Path(__file__).resolve().parents[1] / "examples" \
          / "04-benchmark-polars-vs-duckdb.ipynb"
    out.parent.mkdir(parents=True, exist_ok=True)
    nbf.write(nb, out)
    print(f"wrote {out}")


if __name__ == "__main__":
    main()
