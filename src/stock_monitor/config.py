"""Secret-safe project and runtime configuration loading."""

from __future__ import annotations

import ipaddress
import hashlib
import json
import re
import tomllib
from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from pathlib import Path
from threading import RLock
from urllib.parse import parse_qsl, unquote, urlsplit, urlunsplit
from weakref import ReferenceType, ref

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
        "reference_feeds",
        "reference_roles",
        "reference_urls",
    }
)
_APPROVED_ALPACA_MARKET_DATA_HOST = "data.alpaca.markets"
_APPROVED_SEC_SUBMISSIONS_ORIGIN = "https://data.sec.gov/submissions/"
_APPROVED_SEC_ARCHIVES_ORIGIN = "https://www.sec.gov/Archives/"
_ALPACA_DOMAIN = "alpaca.markets"
_PROHIBITED_BROKER_TOKEN = "robinhood"
_DNS_LABEL = re.compile(r"[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?\Z")
_IPV4_COMPONENT = re.compile(r"(?:[0-9]+|0x[0-9a-f]+)\Z")
_EMAIL_ATOM = r"[A-Za-z0-9!#$%&'*+/=?^_`{|}~-]+"
_EMAIL_CONTACT = re.compile(
    rf"(?<![A-Za-z0-9.!#$%&'*+/=?^_`{{|}}~-])"
    rf"{_EMAIL_ATOM}(?:\.{_EMAIL_ATOM})*@"
    r"[A-Za-z0-9](?:[A-Za-z0-9-]{0,61}[A-Za-z0-9])?"
    r"(?:\.[A-Za-z0-9](?:[A-Za-z0-9-]{0,61}[A-Za-z0-9])?)+"
    r"(?![A-Za-z0-9.-])"
)
_ALPACA_CREDENTIAL_MAX_LENGTH = 256
_SEC_USER_AGENT_MAX_LENGTH = 512
_BASE_REFERENCE_ROLES = {
    "PRIMARY_HALT_FEED": (
        "https://www.nasdaqtrader.com/rss.aspx?feed=tradehalts",
        "primary-halt-feed",
    ),
    "TRADER_ALERT_HALT": (
        "https://www.nasdaqtrader.com/rss.aspx?categorylist=2&feed=currentheadlines",
        "trader-alert-halt",
    ),
    "CROSS_CHECK_CALENDAR": (
        "https://www.nasdaqtrader.com/Trader.aspx?id=Calendar",
        "cross-check-calendar",
    ),
    "OPERATIONAL_STATUS": (
        "https://www.nyse.com/api/notifications/public/alerts?2=3",
        "operational-status",
    ),
    "PRIMARY_CALENDAR": (
        "https://www.nyse.com/trade/hours-calendars",
        "primary-calendar",
    ),
}
_SCOPED_REFERENCE_ROLE = re.compile(
    r"(?:ISSUER_IR|CORPORATE_ACTION):[A-Z][A-Z0-9.-]{0,14}\Z"
)
_REFERENCE_FEED = re.compile(r"[a-z0-9][a-z0-9._-]{0,63}\Z")
_FEE_SCHEDULE_ID = re.compile(r"[A-Z][A-Z0-9_]{0,63}\Z")
_LOWER_SHA256 = re.compile(r"[0-9a-f]{64}\Z")
_REVIEWED_FEE_FIELDS = frozenset(
    {
        "schema_version",
        "status",
        "schedule_id",
        "effective_session",
        "reviewed_at",
        "currency",
        "contract_multiplier",
        "entry_fee_per_contract_micros",
        "exit_fee_per_contract_micros",
        "close_fee_reserve_per_contract_micros",
        "source_sha256",
    }
)
_FEE_REVIEW_MARKER_FIELDS = frozenset({"schema_version", "status", "purpose"})


@dataclass(frozen=True)
class ReferenceSource:
    url: str
    role: str
    feed: str


@dataclass(frozen=True)
class Sources:
    alpaca_market_data_url: str
    sec_submissions_url: str
    sec_archives_url: str
    reference_hosts: tuple[str, ...]
    reference_urls: tuple[str, ...]
    reference_sources: tuple[ReferenceSource, ...]


@dataclass(frozen=True, slots=True, weakref_slot=True)
class FeeSchedule:
    """Exact reviewed per-contract fee inputs for Phase 2 paper accounting."""

    schedule_id: str
    effective_session: date
    reviewed_at: datetime
    currency: str
    contract_multiplier: int
    entry_fee_per_contract_micros: int
    exit_fee_per_contract_micros: int
    close_fee_reserve_per_contract_micros: int
    source_sha256: str
    digest: str


@dataclass(frozen=True, slots=True)
class _IssuedFeeSchedule:
    reference: ReferenceType[FeeSchedule]
    fingerprint: tuple[object, ...]
    reviewed_bytes: bytes


_ISSUED_FEE_SCHEDULES: dict[int, _IssuedFeeSchedule] = {}
_ISSUED_FEE_SCHEDULES_LOCK = RLock()


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


def _pinned_origin(name: str, value: object, expected: str) -> str:
    canonical = _https_url(name, value)
    if canonical != expected:
        raise ConfigurationError(f"sources field {name} must use its official exact origin")
    return canonical


def _reference_url(
    value: object,
    reference_hosts: tuple[str, ...],
    *,
    allow_root: bool,
) -> str:
    if not isinstance(value, str) or not value or "\\" in value or any(
        ord(character) <= 32 for character in value
    ):
        raise ConfigurationError("sources field reference_urls must contain safe URLs")
    try:
        parsed = urlsplit(value)
        port = parsed.port
    except ValueError:
        raise ConfigurationError(
            "sources field reference_urls contains an invalid URL"
        ) from None
    if (
        parsed.scheme != "https"
        or parsed.hostname is None
        or parsed.username is not None
        or parsed.password is not None
        or port not in (None, 443)
        or not parsed.path.startswith("/")
        or (parsed.path == "/" and not allow_root)
        or parsed.path.startswith("//")
        or parsed.fragment
    ):
        raise ConfigurationError(
            "sources field reference_urls requires credential-free exact HTTPS URLs"
        )
    hostname = _canonical_dns_hostname("reference_urls", parsed.hostname)
    if hostname not in reference_hosts:
        raise ConfigurationError(
            "sources field reference_urls contains a host outside reference_hosts"
        )
    try:
        query = urlsplit(value).query
        query_pairs = parse_qsl(
            query,
            keep_blank_values=True,
            strict_parsing=True,
        )
        if query and (
            any(not name or not item for name, item in query_pairs)
            or any(
                name.casefold().endswith("key")
                or any(
                    part in name.casefold()
                    for part in (
                        "authorization",
                        "credential",
                        "password",
                        "secret",
                        "signature",
                        "token",
                    )
                )
                or any(
                    re.sub(r"[^a-z0-9]", "", name.casefold()).endswith(part)
                    for part in (
                        "apikey",
                        "authorization",
                        "credential",
                        "keyid",
                        "password",
                        "secret",
                        "signature",
                        "token",
                    )
                )
                for name, _ in query_pairs
            )
        ):
            raise ValueError
        if not (allow_root and parsed.path == "/") and any(
            unquote(segment).casefold() in {"", ".", ".."}
            for segment in parsed.path.split("/")[1:]
        ):
            raise ValueError
    except ValueError:
        raise ConfigurationError(
            "sources field reference_urls contains a malformed query"
        ) from None
    return urlunsplit(("https", hostname, parsed.path, query, ""))


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

    configured_reference_urls = table["reference_urls"]
    configured_reference_roles = table["reference_roles"]
    configured_reference_feeds = table["reference_feeds"]
    if (
        not isinstance(configured_reference_urls, list)
        or not configured_reference_urls
        or not isinstance(configured_reference_roles, list)
        or not isinstance(configured_reference_feeds, list)
        or len(configured_reference_urls) != len(configured_reference_roles)
        or len(configured_reference_urls) != len(configured_reference_feeds)
    ):
        raise ConfigurationError(
            "reference URLs, roles, and feeds must be aligned non-empty lists"
        )
    roles: list[str] = []
    feeds: list[str] = []
    for role, feed in zip(
        configured_reference_roles,
        configured_reference_feeds,
        strict=True,
    ):
        if not isinstance(role, str) or (
            role not in _BASE_REFERENCE_ROLES
            and not _SCOPED_REFERENCE_ROLE.fullmatch(role)
        ):
            raise ConfigurationError("sources field reference_roles is unsupported")
        if role not in _BASE_REFERENCE_ROLES:
            raise ConfigurationError(
                "scoped reference roles require a separately reviewed pinned manifest"
            )
        if not isinstance(feed, str) or not _REFERENCE_FEED.fullmatch(feed):
            raise ConfigurationError("sources field reference_feeds is malformed")
        roles.append(role)
        feeds.append(feed)
    if len(roles) != len(set(roles)):
        raise ConfigurationError("sources field reference_roles contains duplicates")
    if not set(_BASE_REFERENCE_ROLES).issubset(roles):
        raise ConfigurationError("sources field reference_roles is incomplete")
    reference_urls = tuple(
        _reference_url(
            url,
            reference_hosts,
            allow_root=role.startswith(("ISSUER_IR:", "CORPORATE_ACTION:")),
        )
        for url, role in zip(configured_reference_urls, roles, strict=True)
    )
    if len(reference_urls) != len(set(reference_urls)):
        raise ConfigurationError("sources field reference_urls contains duplicates")
    for url, role, feed in zip(reference_urls, roles, feeds, strict=True):
        if role in _BASE_REFERENCE_ROLES and _BASE_REFERENCE_ROLES[role] != (
            url,
            feed,
        ):
            raise ConfigurationError(
                "base reference role must use its exact official URL and feed"
            )
    reference_sources = tuple(
        ReferenceSource(url=url, role=role, feed=feed)
        for url, role, feed in zip(reference_urls, roles, feeds, strict=True)
    )

    return Sources(
        alpaca_market_data_url=_alpaca_market_data_url(
            table["alpaca_market_data_url"]
        ),
        sec_submissions_url=_pinned_origin(
            "sec_submissions_url",
            table["sec_submissions_url"],
            _APPROVED_SEC_SUBMISSIONS_ORIGIN,
        ),
        sec_archives_url=_pinned_origin(
            "sec_archives_url",
            table["sec_archives_url"],
            _APPROVED_SEC_ARCHIVES_ORIGIN,
        ),
        reference_hosts=reference_hosts,
        reference_urls=reference_urls,
        reference_sources=reference_sources,
    )


def _fee_schedule_fingerprint(schedule: FeeSchedule) -> tuple[object, ...]:
    return (
        schedule.schedule_id,
        schedule.effective_session,
        schedule.reviewed_at,
        schedule.currency,
        schedule.contract_multiplier,
        schedule.entry_fee_per_contract_micros,
        schedule.exit_fee_per_contract_micros,
        schedule.close_fee_reserve_per_contract_micros,
        schedule.source_sha256,
        schedule.digest,
    )


def _fee_document(path: Path) -> dict[str, object]:
    try:
        document = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as error:
        raise ConfigurationError(f"cannot load fees.json: {type(error).__name__}") from None
    if (
        not isinstance(document, dict)
        or type(document.get("schema_version")) is not int
        or document.get("schema_version") != 1
    ):
        raise ConfigurationError("fees.json is missing supported schema_version 1")
    return document


def _fee_integer(document: Mapping[str, object], name: str) -> int:
    value = document.get(name)
    if type(value) is not int or value < 0:
        raise ConfigurationError(f"fees.json field {name} must be a nonnegative integer")
    return value


def _fee_date(value: object) -> date:
    if not isinstance(value, str):
        raise ConfigurationError("fees.json effective_session must be a canonical date")
    try:
        parsed = date.fromisoformat(value)
    except ValueError:
        raise ConfigurationError(
            "fees.json effective_session must be a canonical date"
        ) from None
    if parsed.isoformat() != value:
        raise ConfigurationError("fees.json effective_session must be a canonical date")
    return parsed


def _fee_timestamp(value: object) -> datetime:
    if not isinstance(value, str):
        raise ConfigurationError("fees.json reviewed_at must be a canonical UTC timestamp")
    try:
        parsed = datetime.strptime(value, "%Y-%m-%dT%H:%M:%S.%fZ").replace(
            tzinfo=timezone.utc
        )
    except ValueError:
        raise ConfigurationError(
            "fees.json reviewed_at must be a canonical UTC timestamp"
        ) from None
    canonical = parsed.isoformat(timespec="microseconds").replace("+00:00", "Z")
    if canonical != value:
        raise ConfigurationError("fees.json reviewed_at must be a canonical UTC timestamp")
    return parsed


def _parse_reviewed_fee_schedule(document: Mapping[str, object]) -> FeeSchedule:
    if (
        type(document.get("schema_version")) is not int
        or document.get("schema_version") != 1
    ):
        raise ConfigurationError("fees.json is missing supported schema_version 1")
    if document.get("status") != "reviewed":
        raise ConfigurationError("Phase 2 fee schedule requires explicit operator review")
    if set(document) != _REVIEWED_FEE_FIELDS:
        raise ConfigurationError("fees.json reviewed schedule fields are not exact")
    schedule_id = document.get("schedule_id")
    if not isinstance(schedule_id, str) or _FEE_SCHEDULE_ID.fullmatch(schedule_id) is None:
        raise ConfigurationError("fees.json schedule_id is malformed")
    if document.get("currency") != "USD":
        raise ConfigurationError("fees.json currency must be USD")
    contract_multiplier = _fee_integer(document, "contract_multiplier")
    if contract_multiplier != 100:
        raise ConfigurationError("fees.json contract_multiplier must be 100")
    entry_fee = _fee_integer(document, "entry_fee_per_contract_micros")
    exit_fee = _fee_integer(document, "exit_fee_per_contract_micros")
    if exit_fee == 0:
        raise ConfigurationError(
            "fees.json exit fee must be positive because FEE confirmations are positive"
        )
    close_reserve = _fee_integer(
        document,
        "close_fee_reserve_per_contract_micros",
    )
    if close_reserve < exit_fee:
        raise ConfigurationError(
            "fees.json close fee reserve cannot be below the reviewed exit fee"
        )
    source_sha256 = document.get("source_sha256")
    if (
        not isinstance(source_sha256, str)
        or _LOWER_SHA256.fullmatch(source_sha256) is None
    ):
        raise ConfigurationError("fees.json source_sha256 is malformed")
    digest = hashlib.sha256(
        json.dumps(
            document,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("utf-8")
    ).hexdigest()
    return FeeSchedule(
        schedule_id=schedule_id,
        effective_session=_fee_date(document.get("effective_session")),
        reviewed_at=_fee_timestamp(document.get("reviewed_at")),
        currency="USD",
        contract_multiplier=contract_multiplier,
        entry_fee_per_contract_micros=entry_fee,
        exit_fee_per_contract_micros=exit_fee,
        close_fee_reserve_per_contract_micros=close_reserve,
        source_sha256=source_sha256,
        digest=digest,
    )


def _validate_fees_file(path: Path) -> None:
    document = _fee_document(path)
    status = document.get("status")
    if status == "operator_review_required":
        if (
            set(document) != _FEE_REVIEW_MARKER_FIELDS
            or not isinstance(document.get("purpose"), str)
            or not str(document["purpose"]).strip()
        ):
            raise ConfigurationError("fees.json operator review marker is malformed")
        return
    _parse_reviewed_fee_schedule(document)


def _canonical_reviewed_fee_bytes(document: Mapping[str, object]) -> bytes:
    return json.dumps(
        document,
        ensure_ascii=True,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")


def load_fee_schedule(path: Path) -> FeeSchedule:
    """Issue an identity-bound fee authority only from exact reviewed bytes."""
    document = _fee_document(Path(path))
    schedule = _parse_reviewed_fee_schedule(document)
    reviewed_bytes = _canonical_reviewed_fee_bytes(document)
    identity = id(schedule)

    def discard(dead: ReferenceType[FeeSchedule]) -> None:
        with _ISSUED_FEE_SCHEDULES_LOCK:
            current = _ISSUED_FEE_SCHEDULES.get(identity)
            if current is not None and current.reference is dead:
                _ISSUED_FEE_SCHEDULES.pop(identity, None)

    issued = _IssuedFeeSchedule(
        reference=ref(schedule, discard),
        fingerprint=_fee_schedule_fingerprint(schedule),
        reviewed_bytes=reviewed_bytes,
    )
    with _ISSUED_FEE_SCHEDULES_LOCK:
        _ISSUED_FEE_SCHEDULES[identity] = issued
    return schedule


def is_reviewed_fee_schedule(schedule: object) -> bool:
    """Return whether *schedule* is the current untampered issued object."""
    if not isinstance(schedule, FeeSchedule):
        return False
    with _ISSUED_FEE_SCHEDULES_LOCK:
        issued = _ISSUED_FEE_SCHEDULES.get(id(schedule))
        return (
            issued is not None
            and issued.reference() is schedule
            and issued.fingerprint == _fee_schedule_fingerprint(schedule)
        )


def _read_reviewed_fee_schedule_bytes(schedule: object) -> bytes:
    """Return canonical reviewed bytes only for one exact issued schedule."""
    if not is_reviewed_fee_schedule(schedule):
        raise ConfigurationError("fee schedule authority is unverified")
    with _ISSUED_FEE_SCHEDULES_LOCK:
        issued = _ISSUED_FEE_SCHEDULES.get(id(schedule))
        if issued is None or issued.reference() is not schedule:
            raise ConfigurationError("fee schedule authority is unverified")
        return bytes(issued.reviewed_bytes)


def _reissue_archived_fee_schedule(source: object) -> FeeSchedule:
    """Reissue only from an exact current Journal fee-schedule capability."""
    try:
        from .journal import (
            Phase2FeeScheduleSource,
            is_verified_phase2_fee_schedule_source,
        )
    except (ImportError, AttributeError):
        raise ConfigurationError("archived fee schedule authority is unavailable") from None
    if type(source) is not Phase2FeeScheduleSource or not (
        is_verified_phase2_fee_schedule_source(source)
    ):
        raise ConfigurationError("archived fee schedule authority is unverified")
    try:
        current = source._is_current_phase2_fee_schedule_source()
    except Exception:
        current = False
    if type(current) is not bool or not current:
        raise ConfigurationError("archived fee schedule authority is unverified")
    reviewed_bytes = source.reviewed_bytes
    if type(reviewed_bytes) is not bytes or not reviewed_bytes:
        raise ConfigurationError("archived fee schedule bytes are invalid")
    try:
        text = reviewed_bytes.decode("utf-8")
        document = json.loads(text)
    except (UnicodeError, json.JSONDecodeError):
        raise ConfigurationError("archived fee schedule bytes are invalid") from None
    if not isinstance(document, dict) or _canonical_reviewed_fee_bytes(document) != reviewed_bytes:
        raise ConfigurationError("archived fee schedule bytes are not canonical")
    schedule = _parse_reviewed_fee_schedule(document)
    expected = (
        source.schedule_id,
        source.effective_session,
        source.reviewed_at,
        source.currency,
        source.contract_multiplier,
        source.entry_fee_per_contract_micros,
        source.exit_fee_per_contract_micros,
        source.close_fee_reserve_per_contract_micros,
        source.source_sha256,
        source.schedule_digest,
    )
    if _fee_schedule_fingerprint(schedule) != expected:
        raise ConfigurationError("archived fee schedule fields are inconsistent")
    identity = id(schedule)

    def discard(dead: ReferenceType[FeeSchedule]) -> None:
        with _ISSUED_FEE_SCHEDULES_LOCK:
            current = _ISSUED_FEE_SCHEDULES.get(identity)
            if current is not None and current.reference is dead:
                _ISSUED_FEE_SCHEDULES.pop(identity, None)

    issued = _IssuedFeeSchedule(
        reference=ref(schedule, discard),
        fingerprint=_fee_schedule_fingerprint(schedule),
        reviewed_bytes=reviewed_bytes,
    )
    with _ISSUED_FEE_SCHEDULES_LOCK:
        _ISSUED_FEE_SCHEDULES[identity] = issued
    return schedule


def _operator_root(project_root: Path, configured_home: str) -> Path:
    if not configured_home.strip():
        return project_root
    candidate = Path(configured_home).expanduser()
    if not candidate.is_absolute():
        candidate = project_root / candidate
    return candidate.resolve()


def _http_environment_value(name: str, value: object, max_length: int) -> str:
    if (
        not isinstance(value, str)
        or not value
        or value != value.strip()
        or len(value) > max_length
        or not value.isascii()
        or not value.isprintable()
    ):
        raise ConfigurationError(f"required environment variable {name} is invalid")
    return value


def _sec_user_agent(value: object) -> str:
    user_agent = _http_environment_value(
        "SEC_USER_AGENT",
        value,
        _SEC_USER_AGENT_MAX_LENGTH,
    )
    contacts = tuple(_EMAIL_CONTACT.finditer(user_agent))
    identity_parts: list[str] = []
    cursor = 0
    for contact in contacts:
        identity_parts.append(user_agent[cursor : contact.start()])
        cursor = contact.end()
    identity_parts.append(user_agent[cursor:])
    identity = "".join(identity_parts)
    if not contacts or not any(character.isalpha() for character in identity):
        raise ConfigurationError(
            "required environment variable SEC_USER_AGENT needs application identity and email"
        )
    return user_agent


def load_settings(project_root: Path, environ: Mapping[str, str]) -> Settings:
    """Load versioned non-secret config and four approved environment values."""
    environment = {name: environ.get(name, "") for name in _ENVIRONMENT_NAMES}
    environment["APCA_API_KEY_ID"] = _http_environment_value(
        "APCA_API_KEY_ID",
        environment["APCA_API_KEY_ID"],
        _ALPACA_CREDENTIAL_MAX_LENGTH,
    )
    environment["APCA_API_SECRET_KEY"] = _http_environment_value(
        "APCA_API_SECRET_KEY",
        environment["APCA_API_SECRET_KEY"],
        _ALPACA_CREDENTIAL_MAX_LENGTH,
    )
    environment["SEC_USER_AGENT"] = _sec_user_agent(
        environment["SEC_USER_AGENT"]
    )

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


__all__ = [
    "ConfigurationError",
    "FeeSchedule",
    "Settings",
    "Sources",
    "is_reviewed_fee_schedule",
    "load_fee_schedule",
    "load_settings",
]
