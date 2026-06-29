"""
Pipeline entrypoint — runs the three stages in order:

  1. Markets — full market list from the CLOB API (runs to completion FIRST)
  2. Chain   — OrderFilled events via HyperSync
  3. Process — join orders ↔ markets into labeled trades

Markets is fetched first so the complete list exists before any trade is
scraped or processed. Each stage is independently resumable; subsequent runs
only pull deltas.

Exposed as the `poly-data` console script (see pyproject) so `uv run poly-data`
runs the whole pipeline.
"""

from update_utils.update_markets import update_markets
from update_utils.update_chain import update_chain
from update_utils.process_live import process_live


def _run(name, fn):
    print(f"[{name}] starting")
    try:
        fn()
        print(f"[{name}] done")
    except Exception as e:
        print(f"[{name}] FAILED: {e}")
        raise


def main():
    _run("markets", update_markets)  # 1. full market list, to completion
    _run("chain", update_chain)      # 2. then scrape trades
    print("\n[process] joining orders ↔ markets")
    process_live()                   # 3. then label


if __name__ == "__main__":
    main()
