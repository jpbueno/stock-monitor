"""Explicit, idempotent Phase 1 validation-window bootstrap."""

from __future__ import annotations

import hashlib
import json
import re
from datetime import date, datetime, timezone
from decimal import Decimal

from .journal import InvalidJournalValue, Journal, StoredPhase1ValidationWindow
from .risk import SessionCalendarResolver, _calendar_digest


_STARTING_CAPITAL = Decimal("5000.00")
_STARTING_CAPITAL_MICROS = 5_000_000_000
_LOWER_SHA256 = re.compile(r"[0-9a-f]{64}\Z")


def phase1_validation_window_id(
    *,
    session_date: date,
    calendar_digest: str,
    starting_capital_micros: int,
) -> str:
    """Return the receipt-time-independent identity for one reviewed genesis."""
    if type(session_date) is not date:
        raise InvalidJournalValue("Phase 1 session must be an exact date")
    if (
        type(calendar_digest) is not str
        or _LOWER_SHA256.fullmatch(calendar_digest) is None
    ):
        raise InvalidJournalValue(
            "Phase 1 calendar digest must be a lowercase SHA-256 digest"
        )
    if type(starting_capital_micros) is not int:
        raise InvalidJournalValue("Phase 1 starting capital must be integer micros")
    material = {
        "namespace": "stock-monitor/phase1-validation-window/v1",
        "session_date": session_date.isoformat(),
        "calendar_digest": calendar_digest,
        "starting_capital_micros": starting_capital_micros,
    }
    return hashlib.sha256(
        json.dumps(
            material,
            ensure_ascii=True,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("utf-8")
    ).hexdigest()


def bootstrap_phase1(
    journal: Journal,
    *,
    session_date: date,
    calendar_resolver: SessionCalendarResolver,
    received_at: datetime,
) -> StoredPhase1ValidationWindow:
    """Start or verify the single canonical $5,000 Phase 1 window."""
    if type(session_date) is not date:
        raise InvalidJournalValue("Phase 1 session must be an exact date")
    if type(received_at) is not datetime or received_at.tzinfo is None:
        raise InvalidJournalValue(
            "Phase 1 receipt must be a timezone-aware datetime"
        )
    try:
        if received_at.utcoffset() is None:
            raise InvalidJournalValue(
                "Phase 1 receipt must be a timezone-aware datetime"
            )
        received_utc = received_at.astimezone(timezone.utc)
    except (OverflowError, ValueError) as error:
        raise InvalidJournalValue("Phase 1 receipt is invalid") from error

    session = calendar_resolver.session(session_date)
    started_at = datetime.combine(
        session_date,
        session.close_time,
        session.timezone,
    )
    if received_utc < started_at.astimezone(timezone.utc):
        raise InvalidJournalValue("Phase 1 genesis session is not complete")

    calendar_digest = _calendar_digest(calendar_resolver)
    window_id = phase1_validation_window_id(
        session_date=session_date,
        calendar_digest=calendar_digest,
        starting_capital_micros=_STARTING_CAPITAL_MICROS,
    )
    return journal.start_or_read_phase1_validation_window(
        window_id=window_id,
        started_session=session_date,
        starting_capital=_STARTING_CAPITAL,
        started_at=started_at,
        received_at=received_at,
        calendar_resolver=calendar_resolver,
    )


__all__ = ["bootstrap_phase1", "phase1_validation_window_id"]
