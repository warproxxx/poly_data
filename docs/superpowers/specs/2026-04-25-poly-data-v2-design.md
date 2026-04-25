# poly_data v2 — Design Spec

**Date:** 2026-04-25
**Status:** Approved
**Author:** Brainstorm session, Claude Opus 4.7

## Goal

Migrate `poly_data` from monolithic CSV pipeline to partitioned-Parquet data lake. Make ingest, processing, compaction, and distribution work cross-platform (Linux + Windows) and scale to ~2.4 TB raw across four sources.

## Drivers

1. Current `orderFilled.csv` is 6 GB and projected to grow ~100×. `markets.csv`, `missing_markets.csv`, and `trades.csv` will scale similarly.
2. Pipeline relies on `tail`/`head` subprocess calls — broken on Windows.
3. Existing dedup is per-batch only; resume hack rewinds 1 s and produces duplicate rows in CSV.
4. `update_markets` offset increment is wrong (uses parsed count, not API count) — drifts on parse errors.
5. `parallel_sync` merge can corrupt `MAIN_CSV` when file is empty or lacks trailing newline.
6. `process_live` materializes the entire CSV in RAM — OOM at TB scale.
7. Vendored `backtrader_plotting/` fork pollutes graph (2 cycles), is undeclared, unused by pipeline.
8. Future use cases include HuggingFace Datasets push and AI/ML downstream consumption — favour Arrow-native formats.

## Non-goals

- Real-time streaming. Batch ingest remains.
- Schema evolution beyond what Parquet supports natively.
- Web UI / dashboard.
- Removing `backtrader_plotting/` from the repo (separate decision; design accommodates either choice).

## Architecture

Single-source-of-truth = Hive-partitioned Parquet directories under `data/<source>/year=YYYY/month=MM/`. Ingest writes append-only `run-<utc-epoch>.parquet` files per run. Compaction (nightly) rewrites month dirs to `month.parquet`, dedup by `id`, deletes old run files. Reads are always lazy via `polars.scan_parquet` with partition pruning. Cursors and run metadata in `data/<source>/cursor.json` (atomic write).

Distribution layer pushes snapshots to a HuggingFace Hub Dataset. The legacy S3 `.xz` link stays linked from README for backward compatibility.

```
[Polymarket markets API] ──ingest.markets──┐
[Goldsky GraphQL]        ──ingest.goldsky──┼──> ParquetStore.append(source, df)
                                           │       │
                                           │       └──> data/<source>/year=YYYY/month=MM/run-<epoch>.parquet
                                           │
                                           └──> ParquetStore.save_cursor(source, state)

ParquetStore.scan(orderFilled) ──> process.trades ──> ParquetStore.append(trades, df)

nightly:  compact.monthly(source, year, month) ──> rewrites month dir, dedup by id
weekly:   distribute.huggingface ──> hf://datasets/<user>/poly-data
```

## Tech stack

- Python ≥ 3.10 (drop 3.8/3.9 — Polars and pyarrow current versions assume 3.10+)
- `polars >= 0.20` — lazy scan, sink_parquet, hive partitioning
- `pyarrow >= 15` — Parquet engine + HuggingFace Datasets compatibility
- `requests >= 2.31` — Polymarket API client
- `gql[requests] >= 3.4` — Goldsky GraphQL client
- `huggingface-hub >= 0.20` — `HfApi.upload_large_folder` for snapshots
- `rich >= 13` — optional, for `RichHandler` logging on color-capable terminals
- `pytest >= 8`, `pytest-mock`, `responses` — test stack
- `duckdb` — optional, in `[project.optional-dependencies] analysis` only

## Repository layout (target)

```
poly_data/
├── pyproject.toml                       # add: pyarrow, huggingface-hub, rich; bump python>=3.10
├── README.md                            # rewritten for Parquet + cross-platform
├── src/poly_data/                       # src layout
│   ├── __init__.py
│   ├── cli.py                           # argparse entry; replaces update_all.py
│   ├── logging_setup.py                 # configure_logging(level, rich=True)
│   ├── io/
│   │   ├── __init__.py
│   │   ├── parquet_store.py             # ParquetStore: append/scan/compact/cursor
│   │   ├── cursor.py                    # cursor JSON read/write
│   │   └── platform.py                  # last_line, atomic_write, utf8 setup
│   ├── ingest/
│   │   ├── __init__.py
│   │   ├── goldsky.py                   # GoldskyScraper engine (sticky cursor logic, single source)
│   │   ├── markets.py                   # Polymarket markets API client
│   │   └── parallel.py                  # ThreadPool segment runner
│   ├── process/
│   │   ├── __init__.py
│   │   └── trades.py                    # orderFilled → trades, batched per month
│   ├── compact/
│   │   ├── __init__.py
│   │   └── monthly.py                   # rewrite month dir; dedup by id; atomic
│   └── distribute/
│       ├── __init__.py
│       └── huggingface.py               # push partitioned dataset to HF Hub
├── tests/
│   ├── conftest.py                      # tmp_path fixtures; sample dataframes
│   ├── test_parquet_store.py
│   ├── test_cursor.py
│   ├── test_platform.py                 # last_line, atomic_write cross-platform
│   ├── test_goldsky_ingest.py           # mocked gql.Client
│   ├── test_markets_ingest.py
│   ├── test_parallel.py
│   ├── test_trades_process.py
│   ├── test_compact.py
│   ├── test_distribute_huggingface.py
│   └── test_cli.py
├── scripts/
│   └── migrate_csv_to_parquet.py        # one-shot migration of existing CSVs
├── .github/workflows/
│   └── ci.yml                           # matrix: ubuntu-latest, windows-latest
├── docs/superpowers/
│   ├── specs/2026-04-25-poly-data-v2-design.md   (this file)
│   └── plans/2026-04-25-poly-data-v2.md
└── data/                                # gitignored; contains all parquet partitions
```

## Components

### `io.parquet_store.ParquetStore`

Single facade over the data lake. Methods:

- `__init__(self, root: Path)` — `root` defaults to `data/`.
- `append(source: str, df: pl.DataFrame) -> Path` — partitions by `year`/`month` derived from `timestamp` column; writes `run-<utc-epoch-ms>.parquet`. Returns the file path written.
- `scan(source: str, year: int | None = None, month: int | None = None) -> pl.LazyFrame` — returns lazy frame with partition prune. `pl.scan_parquet(root / source / "**/*.parquet", hive_partitioning=True)`.
- `last_cursor(source: str) -> dict | None` — reads `data/<source>/cursor.json`. Returns `None` if missing.
- `save_cursor(source: str, state: dict)` — atomic write via tmp + `os.replace`.
- `compact(source: str, year: int, month: int) -> int` — read all `run-*.parquet` and existing `month.parquet` in the partition, dedup by `id`, sort by `timestamp`, sink to `month.parquet.tmp`, `os.replace` over `month.parquet`, delete run-* files. Returns row count after compact.

### `io.platform`

Cross-platform primitives. No subprocess calls.

- `last_line(path: Path) -> str | None` — reverse-seek bytes from EOF (4 KB chunks), return last non-empty line decoded as UTF-8 with `errors='replace'`. Returns `None` for empty/missing file.
- `atomic_write(path: Path, data: bytes | str)` — write to `path.with_suffix(path.suffix + '.tmp')`, then `os.replace`.
- `setup_utf8_console()` — `sys.stdout.reconfigure(encoding='utf-8', errors='replace')` on Windows; no-op elsewhere. Called once from `cli.main`.

### `io.cursor`

Thin helper over JSON file: `load(path) -> dict | None`, `save(path, state: dict)`. Uses `platform.atomic_write`.

### `ingest.goldsky.GoldskyScraper`

Generic Goldsky GraphQL scraper. One instance per event type.

- `__init__(self, event_type: str, fields: list[str], project_url: str, store: ParquetStore, batch_size: int = 1000, sticky_threshold: int = 100)` — `fields` is the GraphQL selection set; query template is built from it.
- `fetch_range(start_ts: int, end_ts: int, *, worker_id: int = 0, shutdown_event: threading.Event | None = None) -> int` — fetch all events in `(start_ts, end_ts]`, write directly to `store.append(event_type, ...)` per batch. Sticky cursor logic (single timestamp + id-gt) lives here only. Returns row count written.
- `fetch_incremental() -> int` — reads `store.last_cursor(event_type)`, calls `fetch_range(last_ts, now)`. For non-parallel single-stream ingest.

The sticky-cursor algorithm (single source of truth):

1. If batch is full (`len(events) >= batch_size`):
   - All events same timestamp → set sticky to that timestamp, advance by `id_gt`.
   - Mixed timestamps with ≥ `sticky_threshold` events at the boundary timestamp → set sticky to boundary timestamp.
   - Mixed timestamps with < `sticky_threshold` at the boundary → advance to last "safe" timestamp (one before boundary), drop boundary events on this batch (they will be re-fetched next iteration without dup risk because we anti-join by id at compaction).
2. If batch not full and no sticky → advance to `batch_last_ts`. Done if `batch_last_ts >= end_ts`.
3. If batch not full and sticky was set → exit sticky, advance `last_ts = sticky_ts`.

### `ingest.markets`

Polymarket markets API client. Two functions:

- `update_markets(store: ParquetStore, *, batch_size: int = 500) -> int` — paginate with `offset` derived from `store.scan('markets').select(pl.len()).collect().item()`. Increment `current_offset += len(markets)` (count from API response, not parsed count). Append each batch to `store` after parsing outcomes/clobTokenIds.
- `update_missing_tokens(store: ParquetStore, missing_token_ids: list[str]) -> int` — fetch market metadata for tokens discovered during processing. Append to `missing_markets` source. Skip already-present `id`s by anti-joining against `store.scan('missing_markets')`.

### `ingest.parallel.run_segments`

Replaces standalone `parallel_sync.py`. Splits `(start_ts, end_ts)` across N workers; each calls `GoldskyScraper.fetch_range(seg_start, seg_end, worker_id=i)`. Workers write directly to `ParquetStore` — no temp CSVs, no merge step. Cursor saved at end with `last_ts = end_ts`.

Signal handling: `SIGINT`/`SIGTERM` set a shared `threading.Event`; workers check between batches and exit cleanly. Already-written run-* files are kept; re-run resumes from cursor.

### `process.trades`

`process_trades(store: ParquetStore, markets_lazy: pl.LazyFrame) -> int`:

1. Resume cursor: `store.last_cursor('trades')` → `(last_year, last_month, last_id)`. If absent, start from earliest `orderFilled` partition.
2. Iterate `(year, month)` partitions in order from cursor's start.
3. Per partition: lazy-scan `orderFilled` filtered to the partition + `id > last_id`, lazy-scan `markets`, perform the `melt` + join + price calc transformations as in current `get_processed_df`.
4. `sink_parquet` to a temp run file in the corresponding `trades/year=YYYY/month=MM/` partition, then `os.replace` to `run-<epoch>.parquet`.
5. Save cursor `(year, month, max_id)` after each partition.

Memory: each partition typically << 1 GB even at 100× scale; lazy + sink keeps peak bounded.

### `compact.monthly`

`compact_month(store: ParquetStore, source: str, year: int, month: int) -> int`:

```python
month_dir = store.root / source / f"year={year}" / f"month={month}"
files = sorted(month_dir.glob("*.parquet"))
if len(files) <= 1: return 0  # nothing to compact

lf = (
    pl.scan_parquet(month_dir / "*.parquet")
    .unique(subset="id", keep="first", maintain_order=False)
    .sort("timestamp")
)
tmp = month_dir / "month.parquet.tmp"
lf.sink_parquet(tmp, compression="zstd")
final = month_dir / "month.parquet"
os.replace(tmp, final)
for f in files:
    if f != final and f != tmp:
        f.unlink()
return pl.scan_parquet(final).select(pl.len()).collect().item()
```

`compact_all(store, source)` iterates every month dir under the source.

### `distribute.huggingface`

`push_snapshot(store: ParquetStore, repo_id: str, *, sources: list[str] | None = None, snapshot_tag: str | None = None) -> str`:

- Default `sources = ["orderFilled", "markets", "missing_markets", "trades"]`.
- `snapshot_tag` defaults to `datetime.utcnow().strftime("%Y-%m-%d")`.
- Uses `huggingface_hub.HfApi.upload_large_folder(repo_id=repo_id, repo_type="dataset", folder_path=store.root)` — handles chunking and retries internally.
- Tags the commit with `snapshot_tag`.
- Returns the commit URL.

Auth via env var `HF_TOKEN` or `~/.huggingface/token` (handled by `huggingface_hub` automatically).

## CLI

`python -m poly_data <subcommand>` (replaces `update_all.py`):

- `update-markets` → `ingest.markets.update_markets`
- `update-goldsky [--event orderFilled] [--workers N]` → single-stream or parallel
- `process` → `process.trades.process_trades`
- `compact [--source NAME] [--year YYYY] [--month MM]` → defaults to all sources, all months
- `push-hf --repo USER/REPO [--sources ...]` → distribution
- `update-all` → markets → goldsky → process (the legacy default flow)

`update_all.py` kept as a 3-line shim that calls `cli.main(["update-all"])` for backward compat.

## Data flow & cursors

| Source           | Cursor state                                                |
|------------------|-------------------------------------------------------------|
| `markets`        | (none — offset derived from `scan('markets').select(pl.len()).collect().item()`) |
| `missing_markets`| (none — token-id driven, dedup at append)                   |
| `orderFilled`    | `{"last_timestamp": int, "last_id": str?, "sticky_ts": int?}` |
| `trades`         | `{"year": int, "month": int, "last_id": str}`               |

Cursor files (where used): `data/<source>/cursor.json` written atomically.

## Cross-platform fixes (mapping to review issues)

| Review item | Fix |
|-------------|-----|
| Critical 1 — `tail`/`head` subprocess broken on Windows | Replaced by `io.platform.last_line` (pure Python reverse-seek). All call sites use `ParquetStore.last_cursor` or `last_line`. |
| Critical 2 — empty-file merge in parallel_sync | Eliminated; ParquetStore writes per-run files, no merge step. |
| Critical 3 — trailing-newline corruption on append | Eliminated; Parquet not text. |
| Critical 4 — duplicate rows from `last_timestamp - 1` rewind | Cursor is authoritative; sticky id resolves boundary; dedup at compaction by `id`. |
| Critical 5 — `update_markets` offset drift | Use `current_offset += len(markets)` (API count), not parsed count. |
| Important 6 — re-built GraphQL transport per loop | `GoldskyScraper.__init__` builds `Client` once; reused across `fetch_range`. `requests.Session` reused. |
| Important 7 — `process_live` OOM | Per-month batched lazy scan + sink. |
| Important 8 — segment shutdown ≠ cursor advance | Workers write directly to ParquetStore; restart resumes from saved cursor; partial run-* files become extra dedup work but not data loss. |
| Important 9 — `same_timestamp.row(0)[0]` picks first match | Use `(year, month, last_id)` cursor; no more tuple-match. |
| Important 10 — vendored backtrader_plotting | Out of scope; decision deferred. Design does not import from it. |
| Important 11 — `poly_utils` star-import | Module deleted; `src/poly_data/` is the only import root. Explicit `__all__` everywhere. |
| Important 12 — notebooks with outputs | Migrated to `examples/` dir; cleared with `nbstripout` pre-commit hook (added in plan). |
| Minor 13 — pandas + polars mix | All Polars. Pandas dropped. |
| Minor 14 — unused RUNTIME_TIMESTAMP | Removed. |
| Minor 15 — emoji prints crash on cp1252 | `setup_utf8_console` + stdlib `logging`. |
| Minor 16 — bare `print` | Replaced with `logging`. |
| Minor 17 — sticky logic duplicated | Single implementation in `GoldskyScraper`. |
| Minor 18 — `.DS_Store` tracked | `git rm --cached .DS_Store`; gitignore already covers it. |
| Minor 19 — Python 3.8 | Bumped to ≥ 3.10. |
| Minor 20 — `update_utils/__init__.py` asymmetry | Both packages deleted; replaced by `src/poly_data/`. |

## Scale handling

- 100× growth on 4 sources → ~600 GB × 4 = ~2.4 TB raw CSV equivalent.
- Parquet + zstd (level 3) ~ 8–12× compression on this kind of structured data → ~200–300 GB on disk.
- Polars `scan_parquet` with hive partitioning prunes by year/month at planning time → reads touch only relevant months.
- Compaction nightly keeps month dirs at 1 file each → fewer file descriptors, faster scans.
- HuggingFace `upload_large_folder` chunks and retries; handles multi-hundred-GB pushes.
- Per-month processing in `process.trades` keeps peak RAM well under 4 GB even on the largest month.

## Testing strategy

- **Unit tests** use real Parquet writes/reads in `tmp_path`. No filesystem mocks.
- **Network tests** mock `requests.Session` (via `responses`) and `gql.Client.execute` (via `pytest-mock`).
- **Cross-platform** matrix in CI: `ubuntu-latest` + `windows-latest`, Python 3.10/3.11/3.12.
- **Critical platform tests** in `test_platform.py`: `last_line` correctness on `\n`, `\r\n`, UTF-8 BOM, empty file, single-line, file < 4 KB, file with trailing newline, file without trailing newline.
- **Concurrency** in `test_parallel.py`: spawn 3 workers writing distinct ranges to a tmp ParquetStore, verify all rows present + dedup-clean after compact.
- **Sticky-cursor regression** in `test_goldsky_ingest.py`: mock client returns batches with same-timestamp run, verify scraper paginates by id and converges.

## Migration

`scripts/migrate_csv_to_parquet.py` is a one-shot tool. Steps:

1. Detect existing CSVs (`goldsky/orderFilled.csv`, `markets.csv`, `missing_markets.csv`, `processed/trades.csv`).
2. For each: lazy-scan, derive `year`/`month` from timestamp (or createdAt for markets), sink_parquet partitioned to `data/<source>/`.
3. Dedup by `id` during the sink (handles past dup-bug rows from the rewind hack).
4. Move legacy CSVs to `legacy_csv/` (preserve, don't delete).
5. Print row counts before/after for verification.

## Risks

| Risk | Mitigation |
|------|------------|
| Polars `sink_parquet` API churn between minor versions | Pin `polars>=0.20,<2.0`; integration tests catch regressions in CI. |
| HuggingFace upload of 200 GB hits rate limits or timeouts | `upload_large_folder` already handles retries; document `--workers` tunable. |
| Run-file count explosion before compact runs (many parallel workers, no compact for days) | Compact CLI is fast (single-month-at-a-time); document running it after large parallel ingests. |
| Migration of 6 GB legacy CSV is slow | Acceptable — one-shot; user runs once. Lazy-scan keeps memory bounded. |
| Loss of compatibility with consumers reading old CSV paths | README + `legacy_csv/` preserved; CSV→Parquet adapter in README section showing how to re-export a CSV view if needed. |

## Out of scope

- Removing `backtrader_plotting/` from repo (separate decision)
- Real-time streaming ingest
- Schema migration tooling beyond Parquet's native column add/null
- DuckDB pipeline integration (kept as optional analysis dep only)
- Web dashboard / monitoring
