"""Shared dependency-free domain invariants."""

from __future__ import annotations

from datetime import datetime
from decimal import Decimal


MICRODOLLARS_PER_DOLLAR = 1_000_000
MIN_MICRODOLLARS = -(2**63)
MAX_MICRODOLLARS = 2**63 - 1
_MAX_MICRODOLLAR_DIGITS = 19


class ConfigurationError(ValueError):
    """Configuration is absent, malformed, or violates a locked policy."""


class DomainValidationError(ValueError):
    """A value violates a shared domain invariant."""


def money_to_micros(value: Decimal) -> int:
    """Convert an exact decimal dollar value to integer microdollars."""
    if not isinstance(value, Decimal) or not value.is_finite():
        raise DomainValidationError("money must be a finite Decimal")

    sign, digits, exponent = value.as_tuple()
    if not any(digits):
        return 0

    micro_exponent = exponent + 6
    integral_digits = digits
    if micro_exponent < 0:
        discarded_count = -micro_exponent
        if discarded_count > len(digits) or any(digits[-discarded_count:]):
            raise DomainValidationError("money supports at most six decimal places")
        integral_digits = digits[:-discarded_count]
        micro_exponent = 0

    first_nonzero = next(
        (
            index
            for index, digit in enumerate(integral_digits)
            if digit != 0
        ),
        len(integral_digits),
    )
    integral_digits = integral_digits[first_nonzero:]
    if not integral_digits:
        return 0

    if len(integral_digits) + micro_exponent > _MAX_MICRODOLLAR_DIGITS:
        raise DomainValidationError(
            "money is outside the SQLite signed 64-bit microdollar range"
        )

    coefficient = 0
    for digit in integral_digits:
        coefficient = coefficient * 10 + digit
    micros = coefficient * (10**micro_exponent)
    limit = -MIN_MICRODOLLARS if sign else MAX_MICRODOLLARS
    if micros > limit:
        raise DomainValidationError(
            "money is outside the SQLite signed 64-bit microdollar range"
        )
    if sign:
        micros = -micros
    return micros


def money_from_micros(value: int) -> Decimal:
    """Convert integer microdollars to an exact decimal dollar value."""
    if type(value) is not int:
        raise DomainValidationError("microdollars must be an integer")
    if not MIN_MICRODOLLARS <= value <= MAX_MICRODOLLARS:
        raise DomainValidationError(
            "microdollars are outside the SQLite signed 64-bit range"
        )

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
