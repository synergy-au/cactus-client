import argparse
import sys

import pytest


class _FakeStdin:
    """Minimal stdin stub for importing CLI modules that inspect stdin at import time."""

    def fileno(self) -> int:
        return 0


def _build_run_parser(monkeypatch: pytest.MonkeyPatch) -> argparse.ArgumentParser:
    """Build an isolated parser with only the `cactus run` subcommand registered."""
    monkeypatch.setattr(sys, "stdin", _FakeStdin())

    from cactus_client.cli import run

    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(dest="command")
    run.add_sub_commands(subparsers)
    return parser


def test_run_refetch_delay_ms_is_parsed_as_an_int(monkeypatch: pytest.MonkeyPatch) -> None:
    """`--refetch-delay-ms` should be coerced to an integer before the run action sees it."""
    args = _build_run_parser(monkeypatch).parse_args(["run", "--refetch-delay-ms", "250", "procedure-id", "client-a"])

    assert args.refetch_delay_ms == 250
    assert isinstance(args.refetch_delay_ms, int)


def test_run_refetch_delay_ms_rejects_negative_values(monkeypatch: pytest.MonkeyPatch) -> None:
    """Negative millisecond values should fail fast during CLI parsing."""
    with pytest.raises(SystemExit):
        _build_run_parser(monkeypatch).parse_args(["run", "--refetch-delay-ms", "-1", "procedure-id", "client-a"])


def test_run_action_uses_active_csip_aus_version(monkeypatch: pytest.MonkeyPatch) -> None:
    """Ensure the active csipaus version is being used."""
    parser = _build_run_parser(monkeypatch)

    from cactus_client.cli import run
    from cactus_client.constants import ACTIVE_CSIP_AUS_VERSION
    from cactus_client.model.config import GlobalConfig, RunConfig

    global_config = GlobalConfig()
    versions_used: list[str] = []

    async def fake_run_entrypoint(global_config: GlobalConfig, run_config: RunConfig) -> bool:
        versions_used.append(run_config.csip_aus_version)
        return True

    monkeypatch.setattr(run, "load_config", lambda _: (global_config, None))
    monkeypatch.setattr(GlobalConfig, "get_validation_error", lambda _: None)
    monkeypatch.setattr(run, "run_entrypoint", fake_run_entrypoint)
    args = parser.parse_args(["run", "S-ALL-01", "client-a"])

    with pytest.raises(SystemExit) as exc:
        run.run_action(args)

    assert exc.value.code == 0
    assert versions_used == [ACTIVE_CSIP_AUS_VERSION.value]
