"""Regression tests for the `cactus run` CLI argument surface.

`cactus_client.cli.run` pulls in the TUI, whose keypress module reads
`sys.stdin.fileno()` at import time. pytest's stdin capture cannot provide
one, so satisfy it for the duration of the import.
"""

import argparse
import io
import sys


def _add_sub_commands():
    try:
        return __import__("cactus_client.cli.run", fromlist=["add_sub_commands"]).add_sub_commands
    except io.UnsupportedOperation:  # pragma: no cover - depends on runner stdin
        saved_fileno = sys.stdin.fileno
        sys.stdin.fileno = lambda: 0  # type: ignore[method-assign]
        try:
            return __import__("cactus_client.cli.run", fromlist=["add_sub_commands"]).add_sub_commands
        finally:
            del sys.stdin.fileno
            sys.stdin.fileno = saved_fileno



add_sub_commands = _add_sub_commands()


def _run_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="cactus")
    add_sub_commands(parser.add_subparsers())
    return parser


def test_refetch_delay_ms_parses_as_int():
    # --refetch-delay-ms had no type=, so argparse handed the raw string
    # through; "500" is truthy and submit_and_refetch_resource_for_step then
    # crashed on `"500" / 1000` (S-ALL-08 rerun, 2026-10-08).
    args = _run_parser().parse_args(
        ["run", "--refetch-delay-ms", "500", "S-ALL-08", "myclient-agg-1"]
    )
    assert args.refetch_delay_ms == 500
    assert isinstance(args.refetch_delay_ms, int)


def test_refetch_delay_ms_defaults_to_none():
    args = _run_parser().parse_args(["run", "S-ALL-08", "myclient-agg-1"])
    assert args.refetch_delay_ms is None


def test_timeout_parses_as_int():
    args = _run_parser().parse_args(["run", "--timeout", "90", "S-ALL-08", "myclient-agg-1"])
    assert args.timeout == 90
