"""Run the full Polymarket v2 data pipeline (markets → chain → process).

Thin wrapper around update_utils.pipeline so `python update.py` and
`uv run update.py` keep working; `uv run poly-data` runs the same thing.
"""

from update_utils.pipeline import main

if __name__ == "__main__":
    main()
