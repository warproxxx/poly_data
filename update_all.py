"""Backwards-compatible shim — delegates to poly_data.cli."""

from __future__ import annotations

import sys

from poly_data.cli import main

if __name__ == "__main__":
    sys.exit(main(["update-all", *sys.argv[1:]]))
