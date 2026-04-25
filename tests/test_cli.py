from __future__ import annotations

from pathlib import Path

import pytest

from poly_data.cli import main


def test_cli_help_returns_zero(capsys) -> None:
    with pytest.raises(SystemExit) as exc:
        main(["--help"])
    assert exc.value.code == 0
    out = capsys.readouterr().out
    assert "update-all" in out
    assert "compact" in out


def test_cli_compact_invokes_compact_all(tmp_path: Path, mocker) -> None:
    fake = mocker.patch("poly_data.cli.compact_all", return_value={})
    code = main([
        "compact",
        "--data-root", str(tmp_path / "data"),
        "--source", "orderFilled",
    ])
    assert code == 0
    fake.assert_called_once()
    args, _ = fake.call_args
    assert args[1] == "orderFilled"


def test_cli_update_all_runs_pipeline(tmp_path: Path, mocker) -> None:
    mocker.patch("poly_data.cli.update_markets", return_value=0)
    mocker.patch("poly_data.cli.GoldskyScraper")
    fake_inc = mocker.patch(
        "poly_data.cli._scraper_fetch_incremental",
        return_value=0,
    )
    mocker.patch("poly_data.cli.process_trades", return_value=0)

    code = main(["update-all", "--data-root", str(tmp_path / "data")])
    assert code == 0
    fake_inc.assert_called_once()
