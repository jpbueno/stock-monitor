"""Minimal command-line configuration boundary."""

from __future__ import annotations

import argparse
import os
import sys
from collections.abc import Sequence
from pathlib import Path

from .config import ConfigurationError, load_settings


def _parser() -> argparse.ArgumentParser:
    return argparse.ArgumentParser(
        prog="stock-monitor",
        description="Validate Stock Monitor's local configuration.",
    )


def main(argv: Sequence[str] | None = None) -> int:
    """Validate configuration behind a secret-safe exit-code boundary."""
    _parser().parse_args(argv)
    try:
        load_settings(Path.cwd(), os.environ)
    except ConfigurationError as error:
        print(f"configuration error: {error}", file=sys.stderr)
        return 2
    return 0


__all__ = ["main"]
