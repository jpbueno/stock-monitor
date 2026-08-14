"""Secret-safe project and runtime configuration loading."""

from __future__ import annotations

import ipaddress
import json
import re
import tomllib
from collections.abc import Mapping
from dataclasses import dataclass, field
from pathlib import Path
from urllib.parse import urlsplit, urlunsplit

from .domain import ConfigurationError
from .policy import Policy


_ENVIRONMENT_NAMES = (
    "APCA_API_KEY_ID",
    "APCA_API_SECRET_KEY",
    "SEC_USER_AGENT",
    "STOCK_MONITOR_HOME",
)
_SOURCE_FIELDS = frozenset(
    {
        "alpaca_market_data_url",
        "sec_submissions_url",
        "sec_archives_url",
        "reference_hosts",
    }
)
_APPROVED_ALPACA_MARKET_DATA_HOST = "data.alpaca.markets"
_ALPACA_DOMAIN = "alpaca.markets"
_PROHIBITED_BROKER_TOKEN = "robinhood"
_DNS_LABEL = re.compile(r"[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?\Z")
_IPV4_COMPONENT = re.compile(r"(?:[0-9]+|0x[0-9a-f]+)\Z")


@dataclass(frozen=True)
class Sources:
    alpaca_market_data_url: str
    sec_submissions_url: str
    sec_archives_url: str
    reference_hosts: tuple[str, ...]


@dataclass(frozen=True)
class Settings:
    project_root: Path
    config_root: Path
    state_root: Path
    reports_root: Path
    journal_path: Path
    cache_root: Path
    locks_root: Path
    fees_path: Path
    policy: Policy
    sources: Sources
    sec_user_agent: str
    alpaca_api_key_id: str = field(repr=False)
    alpaca_api_secret_key: str = field(repr=False)


def _load_toml(path: Path, label: str) -> dict[str, object]:
    try:
        document = tomllib.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, tomllib.TOMLDecodeError) as error:
        raise ConfigurationError(f"cannot load {label}: {type(error).__name__}") from None
    if not isinstance(document, dict):
        raise ConfigurationError(f"{label} must contain a TOML document")
    return document


def _canonical_dns_hostname(name: str, value: object) -> str:
    if not isinstance(value, str):
        raise ConfigurationError(f"sources field {name} must be a DNS hostname")
    try:
        ascii_value = value.encode("ascii").decode("ascii")
    except UnicodeError:
        raise ConfigurationError(
            f"sources field {name} must be an ASCII DNS hostname"
        ) from None

    canonical = ascii_value.lower()
    if canonical.endswith("."):
        canonical = canonical[:-1]
    try:
        ipaddress.ip_address(canonical)
    except ValueError:
        pass
    else:
        raise ConfigurationError(f"sources field {name} cannot use an IP literal")

    labels = canonical.split(".")
    if labels and all(_IPV4_COMPONENT.fullmatch(label) for label in labels):
        raise ConfigurationError(f"sources field {name} cannot use an IP literal")

    if (
        len(canonical) > 253
        or len(labels) < 2
        or any(not _DNS_LABEL.fullmatch(label) for label in labels)
    ):
        raise ConfigurationError(f"sources field {name} must be a DNS hostname")
    if _PROHIBITED_BROKER_TOKEN in canonical:
        raise ConfigurationError(f"sources field {name} uses a prohibited host")
    if (
        canonical == _ALPACA_DOMAIN
        or canonical.endswith(f".{_ALPACA_DOMAIN}")
    ) and canonical != _APPROVED_ALPACA_MARKET_DATA_HOST:
        raise ConfigurationError(f"sources field {name} uses a prohibited host")
    return canonical


def _https_url(name: str, value: object) -> str:
    if not isinstance(value, str):
        raise ConfigurationError(f"sources field {name} must be a URL string")
    if (
        "\\" in value
        or "?" in value
        or "#" in value
        or any(ord(character) <= 32 for character in value)
    ):
        raise ConfigurationError(f"sources field {name} must be a safe HTTPS URL")
    try:
        parsed = urlsplit(value)
    except ValueError:
        raise ConfigurationError(f"sources field {name} is not a valid URL") from None
    try:
        port = parsed.port
    except ValueError:
        raise ConfigurationError(f"sources field {name} has an invalid port") from None
    if (
        parsed.scheme != "https"
        or parsed.hostname is None
        or parsed.username is not None
        or parsed.password is not None
        or port not in (None, 443)
        or parsed.query
        or parsed.fragment
    ):
        raise ConfigurationError(
            f"sources field {name} must be a credential-free HTTPS URL"
        )
    hostname = _canonical_dns_hostname(name, parsed.hostname)
    return urlunsplit(("https", hostname, parsed.path, "", ""))


def _alpaca_market_data_url(value: object) -> str:
    canonical = _https_url("alpaca_market_data_url", value)
    parsed = urlsplit(canonical)
    if (
        parsed.hostname != _APPROVED_ALPACA_MARKET_DATA_HOST
        or parsed.path not in ("", "/")
    ):
        raise ConfigurationError(
            "sources field alpaca_market_data_url must use the approved market-data origin"
        )
    return f"https://{_APPROVED_ALPACA_MARKET_DATA_HOST}"


def _load_sources(path: Path) -> Sources:
    document = _load_toml(path, "sources.toml")
    if document.get("schema_version") != 1:
        raise ConfigurationError("sources.toml is missing supported schema_version 1")
    table = document.get("sources")
    if not isinstance(table, dict):
        raise ConfigurationError("sources.toml is missing the sources table")

    names = set(table)
    missing = sorted(_SOURCE_FIELDS - names)
    unknown = sorted(names - _SOURCE_FIELDS)
    if missing:
        raise ConfigurationError(
            "sources.toml is missing fields: " + ", ".join(missing)
        )
    if unknown:
        raise ConfigurationError(
            "sources.toml contains unknown fields: " + ", ".join(unknown)
        )

    configured_reference_hosts = table["reference_hosts"]
    if (
        not isinstance(configured_reference_hosts, list)
        or not configured_reference_hosts
    ):
        raise ConfigurationError(
            "sources field reference_hosts must be a non-empty hostname list"
        )
    reference_hosts = tuple(
        _canonical_dns_hostname("reference_hosts", host)
        for host in configured_reference_hosts
    )
    if len(reference_hosts) != len(set(reference_hosts)):
        raise ConfigurationError("sources field reference_hosts contains duplicates")

    return Sources(
        alpaca_market_data_url=_alpaca_market_data_url(
            table["alpaca_market_data_url"]
        ),
        sec_submissions_url=_https_url(
            "sec_submissions_url", table["sec_submissions_url"]
        ),
        sec_archives_url=_https_url("sec_archives_url", table["sec_archives_url"]),
        reference_hosts=reference_hosts,
    )


def _validate_fees_file(path: Path) -> None:
    try:
        document = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as error:
        raise ConfigurationError(f"cannot load fees.json: {type(error).__name__}") from None
    if not isinstance(document, dict) or document.get("schema_version") != 1:
        raise ConfigurationError("fees.json is missing supported schema_version 1")


def _operator_root(project_root: Path, configured_home: str) -> Path:
    if not configured_home.strip():
        return project_root
    candidate = Path(configured_home).expanduser()
    if not candidate.is_absolute():
        candidate = project_root / candidate
    return candidate.resolve()


def load_settings(project_root: Path, environ: Mapping[str, str]) -> Settings:
    """Load versioned non-secret config and four approved environment values."""
    environment = {name: environ.get(name, "") for name in _ENVIRONMENT_NAMES}
    for required in (
        "APCA_API_KEY_ID",
        "APCA_API_SECRET_KEY",
        "SEC_USER_AGENT",
    ):
        if not environment[required].strip():
            raise ConfigurationError(f"required environment variable {required} is missing")

    resolved_project_root = Path(project_root).expanduser().resolve()
    config_root = resolved_project_root / "config"
    policy = Policy.from_toml(config_root / "policy.toml")
    sources = _load_sources(config_root / "sources.toml")
    fees_path = config_root / "fees.json"
    _validate_fees_file(fees_path)

    operator_root = _operator_root(
        resolved_project_root,
        environment["STOCK_MONITOR_HOME"],
    )
    state_root = operator_root / ".stock-monitor"
    reports_root = operator_root / "reports"

    return Settings(
        project_root=resolved_project_root,
        config_root=config_root,
        state_root=state_root,
        reports_root=reports_root,
        journal_path=state_root / "journal.sqlite3",
        cache_root=state_root / "cache",
        locks_root=state_root / "locks",
        fees_path=fees_path,
        policy=policy,
        sources=sources,
        sec_user_agent=environment["SEC_USER_AGENT"],
        alpaca_api_key_id=environment["APCA_API_KEY_ID"],
        alpaca_api_secret_key=environment["APCA_API_SECRET_KEY"],
    )


__all__ = ["ConfigurationError", "Settings", "Sources", "load_settings"]
