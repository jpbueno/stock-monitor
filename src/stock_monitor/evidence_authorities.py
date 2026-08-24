"""Compiled exact-source policy for subject-scoped evidence discovery."""

from __future__ import annotations

import re
from collections.abc import Mapping
from dataclasses import dataclass
from types import MappingProxyType
from urllib.parse import parse_qsl, urlsplit


_CIK = re.compile(r"[0-9]{10}\Z")
_STOCK_SYMBOLS = frozenset({"AAPL", "AMD", "NVDA"})
_ETF_SYMBOLS = frozenset({"QQQ", "SPY", "VTI", "XLK"})
_SEC_PUBLISHER = "U.S. Securities and Exchange Commission"
_SENSITIVE_QUERY_PARTS = (
    "authorization",
    "credential",
    "password",
    "secret",
    "signature",
    "token",
)
_SENSITIVE_QUERY_SUFFIXES = (
    "apikey",
    "authorization",
    "credential",
    "keyid",
    "password",
    "secret",
    "signature",
    "token",
)


@dataclass(frozen=True, slots=True)
class EvidenceAuthority:
    symbol: str
    issuer_cik: str | None
    requested_url: str
    allowed_final_urls: frozenset[str]
    publisher: str
    role: str
    event_class: str
    purpose: str = "FACT_DISCOVERY"
    timestamp_rule: str = "PRIMARY_ITEM_METADATA"
    clear_capable: bool = False


def _source(
    *,
    symbol: str,
    issuer_cik: str | None,
    requested_url: str,
    publisher: str,
    role: str,
    event_class: str,
) -> EvidenceAuthority:
    return EvidenceAuthority(
        symbol=symbol,
        issuer_cik=issuer_cik,
        requested_url=requested_url,
        allowed_final_urls=frozenset({requested_url}),
        publisher=publisher,
        role=role,
        event_class=event_class,
    )


EVIDENCE_AUTHORITIES: tuple[EvidenceAuthority, ...] = (
    _source(
        symbol="AAPL",
        issuer_cik="0000320193",
        requested_url="https://data.sec.gov/submissions/CIK0000320193.json",
        publisher=_SEC_PUBLISHER,
        role="SEC_SUBMISSIONS:AAPL",
        event_class="BINARY_EVENT",
    ),
    _source(
        symbol="AAPL",
        issuer_cik="0000320193",
        requested_url="https://investor.apple.com/investor-relations/default.aspx",
        publisher="Apple Inc.",
        role="ISSUER_IR:AAPL",
        event_class="BINARY_EVENT",
    ),
    _source(
        symbol="AAPL",
        issuer_cik="0000320193",
        requested_url="https://www.apple.com/newsroom/rss-feed.rss",
        publisher="Apple Inc.",
        role="ISSUER_IR:AAPL",
        event_class="BINARY_EVENT",
    ),
    _source(
        symbol="AMD",
        issuer_cik="0000002488",
        requested_url="https://data.sec.gov/submissions/CIK0000002488.json",
        publisher=_SEC_PUBLISHER,
        role="SEC_SUBMISSIONS:AMD",
        event_class="BINARY_EVENT",
    ),
    _source(
        symbol="AMD",
        issuer_cik="0000002488",
        requested_url="https://ir.amd.com/news-events/ir-calendar",
        publisher="Advanced Micro Devices, Inc.",
        role="ISSUER_IR:AMD",
        event_class="BINARY_EVENT",
    ),
    _source(
        symbol="AMD",
        issuer_cik="0000002488",
        requested_url="https://ir.amd.com/news-events/press-releases/rss",
        publisher="Advanced Micro Devices, Inc.",
        role="ISSUER_IR:AMD",
        event_class="BINARY_EVENT",
    ),
    _source(
        symbol="NVDA",
        issuer_cik="0001045810",
        requested_url="https://data.sec.gov/submissions/CIK0001045810.json",
        publisher=_SEC_PUBLISHER,
        role="SEC_SUBMISSIONS:NVDA",
        event_class="BINARY_EVENT",
    ),
    _source(
        symbol="NVDA",
        issuer_cik="0001045810",
        requested_url="https://investor.nvidia.com/rss/Event.aspx?LanguageId=1",
        publisher="NVIDIA Corporation",
        role="ISSUER_IR:NVDA",
        event_class="BINARY_EVENT",
    ),
    _source(
        symbol="NVDA",
        issuer_cik="0001045810",
        requested_url="https://nvidianews.nvidia.com/cats/press_release.xml",
        publisher="NVIDIA Corporation",
        role="ISSUER_IR:NVDA",
        event_class="BINARY_EVENT",
    ),
    _source(
        symbol="QQQ",
        issuer_cik=None,
        requested_url="https://www.invesco.com/qqq-etf/en/home.html",
        publisher="Invesco",
        role="ISSUER_IR:QQQ",
        event_class="ETF_ACTION",
    ),
    _source(
        symbol="QQQ",
        issuer_cik=None,
        requested_url="https://www.invesco.com/us/en/newsroom.html",
        publisher="Invesco",
        role="ISSUER_IR:QQQ",
        event_class="ETF_ACTION",
    ),
    _source(
        symbol="SPY",
        issuer_cik=None,
        requested_url=(
            "https://www.ssga.com/us/en/intermediary/etfs/"
            "state-street-spdr-sp-500-etf-trust-spy"
        ),
        publisher="State Street Global Advisors",
        role="ISSUER_IR:SPY",
        event_class="ETF_ACTION",
    ),
    _source(
        symbol="SPY",
        issuer_cik=None,
        requested_url=(
            "https://www.ssga.com/us/en/intermediary/resources/"
            "authorized-participants"
        ),
        publisher="State Street Global Advisors",
        role="ISSUER_IR:SPY",
        event_class="ETF_ACTION",
    ),
    _source(
        symbol="VTI",
        issuer_cik=None,
        requested_url=(
            "https://investor.vanguard.com/investment-products/etfs/profile/vti"
        ),
        publisher="Vanguard",
        role="ISSUER_IR:VTI",
        event_class="ETF_ACTION",
    ),
    _source(
        symbol="VTI",
        issuer_cik=None,
        requested_url=(
            "https://corporate.vanguard.com/content/corporatesite/us/en/corp/"
            "who-we-are/pressroom/index.html.html"
        ),
        publisher="Vanguard",
        role="ISSUER_IR:VTI",
        event_class="ETF_ACTION",
    ),
    _source(
        symbol="XLK",
        issuer_cik=None,
        requested_url=(
            "https://www.ssga.com/us/en/intermediary/etfs/"
            "state-street-technology-select-sector-spdr-etf-xlk"
        ),
        publisher="State Street Global Advisors",
        role="ISSUER_IR:XLK",
        event_class="ETF_ACTION",
    ),
    _source(
        symbol="XLK",
        issuer_cik=None,
        requested_url=(
            "https://www.ssga.com/us/en/intermediary/resources/"
            "authorized-participants"
        ),
        publisher="State Street Global Advisors",
        role="ISSUER_IR:XLK",
        event_class="ETF_ACTION",
    ),
)

# Loader-only compatibility for already reviewed source bytes. These URLs are
# deliberately absent from EVIDENCE_AUTHORITIES and cannot authorize retrieval
# or redirects in the evidence-source adapter.
_LOADER_COMPATIBILITY_AUTHORITIES: Mapping[
    str,
    tuple[str | None, frozenset[tuple[str, str]]],
] = MappingProxyType(
    {
        "ISSUER_IR:AAPL": (
            "0000320193",
            frozenset(
                {
                    (
                        "https://investor.apple.com/investor-relations/"
                        "faq/default.aspx",
                        "Apple Inc.",
                    ),
                }
            ),
        ),
        "ISSUER_IR:AMD": (
            "0000002488",
            frozenset(
                {
                    (
                        "https://ir.amd.com/contacts-faq/faq",
                        "Advanced Micro Devices, Inc.",
                    ),
                }
            ),
        ),
        "ISSUER_IR:NVDA": (
            "0001045810",
            frozenset(
                {
                    (
                        "https://investor.nvidia.com/investor-resources/"
                        "faqs/default.aspx",
                        "NVIDIA Corporation",
                    ),
                }
            ),
        ),
        "ISSUER_IR:VTI": (
            None,
            frozenset(
                {
                    (
                        "https://personal1.vanguard.com/pub/Pdf/p961.pdf",
                        "Vanguard",
                    ),
                }
            ),
        ),
    }
)


def authorities_for(symbol: str) -> tuple[EvidenceAuthority, ...]:
    return tuple(source for source in EVIDENCE_AUTHORITIES if source.symbol == symbol)


def scoped_reference_authorities() -> dict[
    str,
    tuple[str | None, frozenset[tuple[str, str]]],
]:
    grouped: dict[str, set[tuple[str, str]]] = {}
    issuer_by_role: dict[str, str | None] = {}

    def add(
        role: str,
        issuer_cik: str | None,
        sources: frozenset[tuple[str, str]],
    ) -> None:
        if role in issuer_by_role and issuer_by_role[role] != issuer_cik:
            raise RuntimeError("evidence role has conflicting issuer identity")
        issuer_by_role[role] = issuer_cik
        grouped.setdefault(role, set()).update(sources)

    for source in EVIDENCE_AUTHORITIES:
        if not source.role.startswith(("ISSUER_IR:", "CORPORATE_ACTION:")):
            continue
        add(
            source.role,
            source.issuer_cik,
            frozenset(
                (url, source.publisher) for url in source.allowed_final_urls
            ),
        )
    for role, (issuer_cik, sources) in _LOADER_COMPATIBILITY_AUTHORITIES.items():
        if role.startswith(("ISSUER_IR:", "CORPORATE_ACTION:")):
            add(role, issuer_cik, sources)
    return {
        role: (issuer_by_role[role], frozenset(values))
        for role, values in sorted(grouped.items())
    }


CLEAR_COVERAGE_AUTHORITY_BUNDLES: Mapping[
    tuple[str, str],
    frozenset[frozenset[tuple[str, str, str]]],
] = MappingProxyType({})


def _origin(url: str) -> tuple[str, str, int]:
    parsed = urlsplit(url)
    try:
        port = parsed.port
    except ValueError as exc:
        raise RuntimeError("evidence authority URL port is malformed") from exc
    if (
        parsed.scheme != "https"
        or parsed.hostname is None
        or parsed.username is not None
        or parsed.password is not None
        or parsed.fragment
        or not url.isascii()
        or not url.isprintable()
    ):
        raise RuntimeError("evidence authority URL must be credential-free HTTPS")
    try:
        query = parse_qsl(
            parsed.query,
            keep_blank_values=True,
            strict_parsing=True,
        )
    except ValueError as exc:
        raise RuntimeError("evidence authority URL query is malformed") from exc
    for key, _ in query:
        folded = key.casefold()
        normalized = re.sub(r"[^a-z0-9]", "", folded)
        if folded.endswith("key") or any(
            part in folded for part in _SENSITIVE_QUERY_PARTS
        ) or any(
            normalized.endswith(suffix) for suffix in _SENSITIVE_QUERY_SUFFIXES
        ):
            raise RuntimeError("evidence authority URL contains credentials")
    return parsed.scheme, parsed.hostname, port or 443


def _validate_catalog() -> None:
    seen: set[tuple[str, str, str]] = set()
    for source in EVIDENCE_AUTHORITIES:
        if type(source) is not EvidenceAuthority:
            raise RuntimeError("evidence authority must use the exact policy type")
        if source.symbol in _STOCK_SYMBOLS:
            if source.issuer_cik is None or _CIK.fullmatch(source.issuer_cik) is None:
                raise RuntimeError("stock evidence authority CIK must be ten digits")
            expected_sec_url = (
                "https://data.sec.gov/submissions/"
                f"CIK{source.issuer_cik}.json"
            )
            claims_sec_identity = (
                source.publisher == _SEC_PUBLISHER
                or source.role.startswith("SEC_SUBMISSIONS:")
                or urlsplit(source.requested_url).hostname == "data.sec.gov"
            )
            if claims_sec_identity:
                if (
                    source.requested_url != expected_sec_url
                    or source.publisher != _SEC_PUBLISHER
                    or source.role != f"SEC_SUBMISSIONS:{source.symbol}"
                ):
                    raise RuntimeError(
                        "SEC evidence authority identity is malformed"
                    )
                role_prefix = "SEC_SUBMISSIONS"
            else:
                role_prefix = "ISSUER_IR"
            expected_event_class = "BINARY_EVENT"
        elif source.symbol in _ETF_SYMBOLS:
            if source.issuer_cik is not None:
                raise RuntimeError("ETF evidence authority cannot have an issuer CIK")
            if (
                source.publisher == _SEC_PUBLISHER
                or source.role.startswith("SEC_SUBMISSIONS:")
                or urlsplit(source.requested_url).hostname == "data.sec.gov"
            ):
                raise RuntimeError("ETF evidence authority cannot claim SEC identity")
            role_prefix = "ISSUER_IR"
            expected_event_class = "ETF_ACTION"
        else:
            raise RuntimeError("evidence authority subject is outside the universe")
        if source.role != f"{role_prefix}:{source.symbol}":
            raise RuntimeError("evidence authority role is not subject scoped")
        if source.event_class != expected_event_class:
            raise RuntimeError("evidence authority event class is malformed")
        if (
            source.purpose != "FACT_DISCOVERY"
            or source.timestamp_rule != "PRIMARY_ITEM_METADATA"
            or source.clear_capable is not False
        ):
            raise RuntimeError("evidence authority can only discover facts")
        if (
            type(source.allowed_final_urls) is not frozenset
            or not source.allowed_final_urls
            or source.requested_url not in source.allowed_final_urls
        ):
            raise RuntimeError("evidence authority final URL policy is malformed")
        requested_origin = _origin(source.requested_url)
        for url in source.allowed_final_urls:
            if _origin(url) != requested_origin:
                raise RuntimeError("evidence authority final URL changed origin")
            identity = (source.symbol, url, source.purpose)
            if identity in seen:
                raise RuntimeError("evidence authority source identity is duplicated")
            seen.add(identity)


_validate_catalog()


__all__ = [
    "CLEAR_COVERAGE_AUTHORITY_BUNDLES",
    "EVIDENCE_AUTHORITIES",
    "EvidenceAuthority",
    "authorities_for",
    "scoped_reference_authorities",
]
