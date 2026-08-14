"""Shared dependency-free domain invariants."""

from __future__ import annotations

from datetime import datetime
from decimal import Decimal


MICRODOLLARS_PER_DOLLAR = 1_000_000
_MICRODOLLARS_PER_DOLLAR_DECIMAL = Decimal(MICRODOLLARS_PER_DOLLAR)


class ConfigurationError(ValueError):
    """Configuration is absent, malformed, or violates a locked policy."""


class DomainValidationError(ValueError):
    """A value violates a shared domain invariant."""


def money_to_micros(value: Decimal) -> int:
    """Convert an exact decimal dollar value to integer microdollars."""
    if not isinstance(value, Decimal) or not value.is_finite():
        raise DomainValidationError("money must be a finite Decimal")
    scaled = value * _MICRODOLLARS_PER_DOLLAR_DECIMAL
    integral = scaled.to_integral_value()
    if scaled != integral:
        raise DomainValidationError("money supports at most six decimal places")
    return int(integral)


def money_from_micros(value: int) -> Decimal:
    """Convert integer microdollars to an exact decimal dollar value."""
    if type(value) is not int:
        raise DomainValidationError("microdollars must be an integer")
    return Decimal(value) / _MICRODOLLARS_PER_DOLLAR_DECIMAL


def require_aware_timestamp(value: datetime, name: str = "timestamp") -> datetime:
    """Return *value* after confirming it carries a usable UTC offset."""
    if not isinstance(value, datetime) or value.tzinfo is None:
        raise DomainValidationError(f"{name} must be timezone-aware")
    try:
        offset = value.utcoffset()
    except (OverflowError, ValueError):
        offset = None
    if offset is None:
        raise DomainValidationError(f"{name} must be timezone-aware")
    return value


def require_positive_decimal(value: Decimal, name: str = "value") -> Decimal:
    """Return a finite, strictly positive Decimal."""
    if (
        not isinstance(value, Decimal)
        or not value.is_finite()
        or value <= Decimal("0")
    ):
        raise DomainValidationError(f"{name} must be a positive Decimal")
    return value


def require_positive_int(value: int, name: str = "value") -> int:
    """Return a strictly positive integer, excluding booleans."""
    if type(value) is not int or value <= 0:
        raise DomainValidationError(f"{name} must be a positive integer")
    return value
