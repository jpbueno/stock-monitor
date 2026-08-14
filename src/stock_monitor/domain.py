"""Shared dependency-free domain invariants."""

from __future__ import annotations

from datetime import datetime
from decimal import Decimal


MICRODOLLARS_PER_DOLLAR = 1_000_000


class ConfigurationError(ValueError):
    """Configuration is absent, malformed, or violates a locked policy."""


class DomainValidationError(ValueError):
    """A value violates a shared domain invariant."""


def money_to_micros(value: Decimal) -> int:
    """Convert an exact decimal dollar value to integer microdollars."""
    if not isinstance(value, Decimal) or not value.is_finite():
        raise DomainValidationError("money must be a finite Decimal")

    sign, digits, exponent = value.as_tuple()
    coefficient = 0
    for digit in digits:
        coefficient = coefficient * 10 + digit
    if coefficient == 0:
        return 0

    micro_exponent = exponent + 6
    if micro_exponent >= 0:
        micros = coefficient * (10**micro_exponent)
    else:
        micros, remainder = divmod(coefficient, 10 ** (-micro_exponent))
        if remainder:
            raise DomainValidationError("money supports at most six decimal places")
    if sign:
        micros = -micros
    return micros


def money_from_micros(value: int) -> Decimal:
    """Convert integer microdollars to an exact decimal dollar value."""
    if type(value) is not int:
        raise DomainValidationError("microdollars must be an integer")

    sign = int(value < 0)
    remaining = abs(value)
    reversed_digits: list[int] = []
    while remaining:
        remaining, digit = divmod(remaining, 10)
        reversed_digits.append(digit)
    digits = tuple(reversed(reversed_digits)) if reversed_digits else (0,)
    return Decimal((sign, digits, -6))


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
