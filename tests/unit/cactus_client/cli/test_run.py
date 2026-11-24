import argparse
import sys
from pathlib import Path

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


def test_run_action_uses_configured_csip_aus_version(monkeypatch: pytest.MonkeyPatch) -> None:
    from cactus_test_definitions import CSIPAusVersion

    from cactus_client.cli import run
    from cactus_client.model.config import GlobalConfig, RunConfig

    parser = _build_run_parser(monkeypatch)
    global_config = GlobalConfig(csip_aus_version=CSIPAusVersion.BETA_1_3_STORAGE)
    versions_used: list[CSIPAusVersion] = []

    async def fake_run_entrypoint(global_config: GlobalConfig, run_config: RunConfig) -> bool:
        versions_used.append(run_config.csip_aus_version)
        return True

    monkeypatch.setattr(run, "load_config", lambda _: (global_config, Path(".cactus.yaml")))
    monkeypatch.setattr(GlobalConfig, "get_validation_error", lambda self: None)
    monkeypatch.setattr(run, "run_entrypoint", fake_run_entrypoint)
    args = parser.parse_args(["run", "S-ALL-01", "client-a"])

    with pytest.raises(SystemExit) as exc:
        run.run_action(args)

    assert exc.value.code == 0
    assert versions_used == [CSIPAusVersion.BETA_1_3_STORAGE]
