"""Exact-catalog GET retrieval for unreviewed evidence proposals."""

from __future__ import annotations

import hashlib
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime

from stock_monitor.domain import require_aware_timestamp
from stock_monitor.evidence_authorities import (
    EVIDENCE_AUTHORITIES,
    EvidenceAuthority,
)

from .http import EgressPolicy, GetTransport, get_with_redirects
from .sec import validate_contact_user_agent


_ACCEPT = "application/json,application/xml,text/html,text/plain"
_ALLOWED_CONTENT_TYPES = frozenset(
    {
        "application/atom+xml",
        "application/json",
        "application/rss+xml",
        "application/xml",
        "text/html",
        "text/plain",
        "text/xml",
    }
)
_MAX_BYTES = 4_194_304


@dataclass(frozen=True, slots=True)
class ProposalSourceObservation:
    observation_id: str
    symbol: str
    issuer_cik: str | None
    url: str
    publisher: str
    role: str
    event_class: str
    retrieved_at: datetime
    published_at: datetime | None
    timestamp_source: str
    content_sha256: str
    body: bytes = field(repr=False)


def _catalog_host(url: str) -> str:
    host = url.removeprefix("https://").split("/", 1)[0]
    return host.removesuffix(":443")


def _require_exact_target(authority: EvidenceAuthority, target: str) -> None:
    if target not in authority.allowed_final_urls:
        raise ValueError("evidence source target is not the compiled authority")


def _identity_digest(
    symbol: str,
    role: str,
    final_url: str,
    retrieved_at: datetime,
    body: bytes,
) -> str:
    return hashlib.sha256(
        b"\0".join(
            (
                symbol.encode("ascii"),
                role.encode("ascii"),
                final_url.encode("ascii"),
                retrieved_at.isoformat(timespec="microseconds").encode("ascii"),
                body,
            )
        )
    ).hexdigest()


class EvidenceSourceClient:
    """Fetch only exact objects from the compiled evidence-source catalog."""

    def __init__(
        self,
        *,
        transport: GetTransport,
        now: Callable[[], datetime],
        user_agent: str,
    ) -> None:
        self._transport = transport
        self._now = now
        self._user_agent = validate_contact_user_agent(user_agent)
        self._identity_fingerprints: dict[
            str,
            tuple[str, str, str, str, str],
        ] = {}

    def fetch(self, authority: EvidenceAuthority) -> ProposalSourceObservation:
        if not any(authority is item for item in EVIDENCE_AUTHORITIES):
            raise ValueError("evidence source is not the compiled authority")

        policy = EgressPolicy(
            _catalog_host(url) for url in authority.allowed_final_urls
        )
        response = get_with_redirects(
            self._transport,
            policy,
            authority.requested_url,
            {"Accept": _ACCEPT, "User-Agent": self._user_agent},
            allowed_content_types=_ALLOWED_CONTENT_TYPES,
            max_bytes=_MAX_BYTES,
            exact_url_validator=lambda target: _require_exact_target(
                authority,
                target,
            ),
        )
        retrieved_at = require_aware_timestamp(
            self._now(),
            "evidence retrieval time",
        ).astimezone(UTC)
        content_sha256 = hashlib.sha256(response.body).hexdigest()
        identity = _identity_digest(
            authority.symbol,
            authority.role,
            response.url,
            retrieved_at,
            response.body,
        )
        observation_id = f"proposal-{identity[:24]}"
        fingerprint = (
            authority.symbol,
            authority.role,
            response.url,
            retrieved_at.isoformat(timespec="microseconds"),
            content_sha256,
        )
        prior = self._identity_fingerprints.get(observation_id)
        if prior is not None and prior != fingerprint:
            raise ValueError("evidence source observation identity collision")
        self._identity_fingerprints[observation_id] = fingerprint

        return ProposalSourceObservation(
            observation_id=observation_id,
            symbol=authority.symbol,
            issuer_cik=authority.issuer_cik,
            url=response.url,
            publisher=authority.publisher,
            role=authority.role,
            event_class=authority.event_class,
            retrieved_at=retrieved_at,
            published_at=None,
            timestamp_source="UNAVAILABLE",
            content_sha256=content_sha256,
            body=response.body,
        )


__all__ = ["EvidenceSourceClient", "ProposalSourceObservation"]
