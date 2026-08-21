"""Deterministic, decimal-safe, secret-redacted CSV journal exports."""

from __future__ import annotations

import csv
import json
import os
import re
import sqlite3
import tempfile
from collections.abc import Iterable
from pathlib import Path

from .domain import money_from_micros
from .journal import Journal


_TABLE_NAME = re.compile(r"^[a-z][a-z0-9_]*$")
_AUTHORIZATION_ASSIGNMENT = re.compile(
    r"\b(AUTHORIZATION)\b(\s*[:=]\s*)"
    r"(?:\"[^\"]*\"|'[^']*'|[^,;\r\n]+)",
    flags=re.IGNORECASE,
)
_COOKIE_ASSIGNMENT = re.compile(
    r"\b(COOKIE)\b(\s*[:=]\s*)(?:\"[^\"]*\"|'[^']*'|[^\r\n]+)",
    flags=re.IGNORECASE,
)
_SECRET_ASSIGNMENT = re.compile(
    r"\b(APCA_API_KEY_ID|APCA_API_SECRET_KEY|ACCESS_TOKEN|REFRESH_TOKEN|"
    r"AUTHORIZATION|PASSWORD|COOKIE)\b(\s*[:=]\s*)"
    r"(?:\"[^\"]*\"|'[^']*'|[^,;\s]+)",
    flags=re.IGNORECASE,
)
_SENSITIVE_NAMES = frozenset(
    {
        "access_token",
        "api_key",
        "api_key_id",
        "api_secret",
        "api_secret_key",
        "apca_api_key_id",
        "apca_api_secret_key",
        "authorization",
        "cookie",
        "credentials",
        "password",
        "private_key",
        "refresh_token",
        "secret",
        "secret_key",
        "session_token",
    }
)


class ExportError(OSError):
    """A consistent inspectable export could not be produced."""


def export_tables(journal: Journal, destination: Path) -> tuple[Path, ...]:
    """Export one consistent read snapshot of every owned journal table."""
    if not isinstance(journal, Journal):
        raise TypeError("journal must be a Journal")
    if not isinstance(destination, Path):
        raise TypeError("export destination must be a pathlib.Path")
    export_root = journal.path.parent
    _reject_symbolic_link_ancestors(export_root, destination)
    if destination.is_symlink():
        raise ExportError("export destination cannot be a symbolic link")
    try:
        destination.mkdir(mode=0o700, parents=True, exist_ok=True)
    except OSError as error:
        raise ExportError("export destination could not be created") from error
    if not destination.is_dir():
        raise ExportError("export destination must be a directory")
    _reject_symbolic_link_ancestors(export_root, destination)

    connection: sqlite3.Connection | None = None
    try:
        connection = sqlite3.connect(
            f"{journal.path.resolve().as_uri()}?mode=ro",
            uri=True,
            isolation_level=None,
        )
        connection.execute("PRAGMA query_only = ON")
        connection.execute("BEGIN")
        tables = _table_names(connection)
        paths = tuple(
            _export_table(connection, table, destination) for table in tables
        )
        connection.rollback()
        return paths
    except (OSError, sqlite3.Error, UnicodeError, ValueError) as error:
        if connection is not None and connection.in_transaction:
            connection.rollback()
        if isinstance(error, ExportError):
            raise
        raise ExportError("journal tables could not be exported") from error
    finally:
        if connection is not None:
            connection.close()


def _table_names(connection: sqlite3.Connection) -> tuple[str, ...]:
    rows = connection.execute(
        "SELECT name FROM sqlite_schema "
        "WHERE type = 'table' AND name NOT LIKE 'sqlite_%' "
        "ORDER BY name COLLATE BINARY"
    ).fetchall()
    result = tuple(str(row[0]) for row in rows)
    if not result or any(_TABLE_NAME.fullmatch(name) is None for name in result):
        raise ExportError("journal contains an unsupported table name")
    return result


def _export_table(
    connection: sqlite3.Connection,
    table: str,
    destination: Path,
) -> Path:
    quoted_table = _quote_identifier(table)
    info = connection.execute(f"PRAGMA table_info({quoted_table})").fetchall()
    columns = tuple(str(row[1]) for row in info)
    if not columns:
        raise ExportError(f"journal table has no columns: {table}")
    included = tuple(
        (index, name)
        for index, name in enumerate(columns)
        if not _sensitive_name(name)
    )
    headers = tuple(_export_header(name) for _, name in included)
    if len(headers) != len(set(headers)):
        raise ExportError(f"journal export headers conflict: {table}")
    primary_keys = tuple(
        str(row[1]) for row in sorted(info, key=lambda item: int(item[5])) if row[5]
    )
    order_columns = primary_keys or columns
    order_sql = ", ".join(_quote_identifier(name) for name in order_columns)
    rows = connection.execute(
        f"SELECT * FROM {quoted_table} ORDER BY {order_sql}"
    )

    target = destination / f"{table}.csv"
    if target.is_symlink():
        raise ExportError(f"export target cannot be a symbolic link: {target.name}")
    temporary_path: Path | None = None
    try:
        descriptor, raw_path = tempfile.mkstemp(
            prefix=f".{table}.",
            suffix=".tmp",
            dir=destination,
            text=True,
        )
        temporary_path = Path(raw_path)
        with os.fdopen(
            descriptor,
            "w",
            encoding="utf-8",
            newline="",
        ) as stream:
            writer = csv.writer(stream, lineterminator="\n")
            writer.writerow(headers)
            for row in rows:
                writer.writerow(
                    _export_value(name, row[index]) for index, name in included
                )
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary_path, target)
        temporary_path = None
        return target
    except (OSError, UnicodeError, ValueError, TypeError) as error:
        raise ExportError(f"journal table could not be exported: {table}") from error
    finally:
        if temporary_path is not None:
            try:
                temporary_path.unlink(missing_ok=True)
            except OSError:
                pass


def _export_header(column: str) -> str:
    if column.endswith("_micros"):
        return f"{column[:-7]}_decimal"
    return column


def _export_value(column: str, value: object) -> object:
    if value is None:
        return ""
    if column.endswith("_micros"):
        if type(value) is not int:
            raise ExportError(f"micro-unit column is not an integer: {column}")
        return format(money_from_micros(value), "f")
    if isinstance(value, bytes):
        return value.hex()
    if isinstance(value, str):
        if column.endswith("_json"):
            return _redacted_json(value)
        return _redact_text(value)
    if type(value) in (int, float):
        return str(value)
    raise ExportError(f"journal value has an unsupported type: {column}")


def _redacted_json(value: str) -> str:
    try:
        document = json.loads(value)
    except json.JSONDecodeError:
        return _redact_text(value)
    redacted = _redact_document(document)
    return json.dumps(
        redacted,
        ensure_ascii=True,
        separators=(",", ":"),
        sort_keys=True,
    )


def _redact_document(value: object) -> object:
    if isinstance(value, dict):
        return {
            str(key): (
                "[REDACTED]"
                if _sensitive_name(str(key))
                else _redact_document(item)
            )
            for key, item in sorted(value.items(), key=lambda item: str(item[0]))
        }
    if isinstance(value, list):
        return [_redact_document(item) for item in value]
    if isinstance(value, str):
        return _redact_text(value)
    return value


def _redact_text(value: str) -> str:
    redacted = _AUTHORIZATION_ASSIGNMENT.sub(
        lambda match: f"{match.group(1)}{match.group(2)}[REDACTED]",
        value,
    )
    redacted = _COOKIE_ASSIGNMENT.sub(
        lambda match: f"{match.group(1)}{match.group(2)}[REDACTED]",
        redacted,
    )
    return _SECRET_ASSIGNMENT.sub(
        lambda match: f"{match.group(1)}{match.group(2)}[REDACTED]",
        redacted,
    )


def _sensitive_name(value: str) -> bool:
    normalized = re.sub(r"[^a-z0-9]+", "_", value.lower()).strip("_")
    return normalized in _SENSITIVE_NAMES or normalized.endswith(
        ("_password", "_secret", "_secret_key", "_access_token", "_refresh_token")
    )


def _quote_identifier(value: str) -> str:
    if _TABLE_NAME.fullmatch(value) is None:
        raise ExportError("journal identifier is unsupported")
    return f'"{value}"'


def _reject_symbolic_link_ancestors(root: Path, destination: Path) -> None:
    normalized_root = Path(os.path.abspath(root))
    normalized_destination = Path(os.path.abspath(destination))
    try:
        relative = normalized_destination.relative_to(normalized_root)
    except ValueError as error:
        raise ExportError("export destination must remain under the journal root") from error
    current = normalized_root
    for component in ("", *relative.parts):
        if component:
            current /= component
        if current.is_symlink():
            raise ExportError("export destination cannot traverse a symbolic link")
