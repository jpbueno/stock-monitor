"""Isolated unattended entry point with literal private-environment loading."""

from __future__ import annotations

import os
import stat
from collections.abc import Iterator, Mapping, Sequence
from pathlib import Path

from . import cli


APPROVED_KEYS = frozenset(
    {
        "APCA_API_KEY_ID",
        "APCA_API_SECRET_KEY",
        "SEC_USER_AGENT",
        "STOCK_MONITOR_HOME",
    }
)
_MAXIMUM_ENVIRONMENT_BYTES = 16384


class LiteralEnvironmentError(RuntimeError):
    """The private environment failed its closed literal-file contract."""


class _LiteralEnvironment(Mapping[str, str]):
    """Read-only approved values with secret-safe implicit representations."""

    __slots__ = ("_entries",)

    def __init__(self, values: Mapping[str, str]) -> None:
        self._entries = tuple((key, values[key]) for key in sorted(values))

    def __getitem__(self, key: str) -> str:
        for candidate, value in self._entries:
            if candidate == key:
                return value
        raise KeyError(key)

    def __iter__(self) -> Iterator[str]:
        return (key for key, _ in self._entries)

    def __len__(self) -> int:
        return len(self._entries)

    def __repr__(self) -> str:
        keys = ", ".join(key for key, _ in self._entries)
        return f"LiteralEnvironment(keys=[{keys}])"

    def __str__(self) -> str:
        return repr(self)

    def __format__(self, format_spec: str) -> str:
        return format(str(self), format_spec)


def read_bounded_descriptor(descriptor: int, *, maximum_bytes: int) -> bytes:
    """Read at most ``maximum_bytes`` from an already validated descriptor."""
    payload = bytearray()
    while True:
        chunk = os.read(
            descriptor,
            min(4096, maximum_bytes + 1 - len(payload)),
        )
        if not chunk:
            return bytes(payload)
        payload.extend(chunk)
        if len(payload) > maximum_bytes:
            raise LiteralEnvironmentError("private environment is too large")


def parse_exact_literal_assignments(
    payload: bytes,
    *,
    approved: frozenset[str],
) -> Mapping[str, str]:
    """Parse one literal non-empty assignment for every approved key."""
    try:
        text = payload.decode("utf-8")
    except UnicodeError:
        raise LiteralEnvironmentError(
            "private environment encoding is invalid"
        ) from None
    if any(
        character != "\n"
        and (ord(character) < 32 or 127 <= ord(character) <= 159)
        for character in text
    ):
        raise LiteralEnvironmentError("private environment control data is invalid")

    result: dict[str, str] = {}
    for line in text.split("\n"):
        if not line or line.startswith("#"):
            continue
        key, separator, value = line.partition("=")
        if separator != "=" or key not in approved or key in result:
            raise LiteralEnvironmentError("private environment entry is invalid")
        if not value or any(
            ord(character) < 32 or 127 <= ord(character) <= 159
            for character in value
        ):
            raise LiteralEnvironmentError("private environment value is invalid")
        result[key] = value

    if result.keys() != approved:
        raise LiteralEnvironmentError("private environment is incomplete")
    return _LiteralEnvironment(result)


def load_literal_environment(path: Path) -> Mapping[str, str]:
    """Open, validate, and parse a private environment through one descriptor."""
    try:
        descriptor = os.open(
            path,
            os.O_RDONLY | os.O_CLOEXEC | os.O_NOFOLLOW,
        )
    except OSError:
        raise LiteralEnvironmentError("private environment is unavailable") from None

    try:
        try:
            metadata = os.fstat(descriptor)
            if not stat.S_ISREG(metadata.st_mode) or metadata.st_uid != os.geteuid():
                raise LiteralEnvironmentError(
                    "private environment ownership is invalid"
                )
            if metadata.st_nlink != 1 or stat.S_IMODE(metadata.st_mode) & 0o077:
                raise LiteralEnvironmentError(
                    "private environment permissions are invalid"
                )
            payload = read_bounded_descriptor(
                descriptor,
                maximum_bytes=_MAXIMUM_ENVIRONMENT_BYTES,
            )
        except LiteralEnvironmentError:
            raise
        except OSError:
            raise LiteralEnvironmentError(
                "private environment is unavailable"
            ) from None
    finally:
        os.close(descriptor)

    return parse_exact_literal_assignments(payload, approved=APPROVED_KEYS)


def main(argv: Sequence[str] | None = None) -> int:
    """Load only the reviewed literal environment and invoke the existing CLI."""
    try:
        root = Path(__file__).resolve(strict=True).parents[2]
        values = load_literal_environment(root / ".env")
        return cli.run(argv, environ=values)
    except LiteralEnvironmentError:
        print("CONFIGURATION REQUIRED\nNo candidate or action was produced.")
        return 2
    except Exception:
        print("INTERNAL ERROR\nNo candidate or action was produced.")
        return 10


if __name__ == "__main__":
    raise SystemExit(main())
