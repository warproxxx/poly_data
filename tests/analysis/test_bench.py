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
