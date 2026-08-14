"""Locked validation policy loaded from versioned TOML."""

from __future__ import annotations

import tomllib
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import ClassVar

from .domain import ConfigurationError


_DECIMAL_FIELDS = (
    "capital",
    "max_live_exposure",
    "max_position_risk",
    "max_combined_risk",
    "max_monthly_drawdown",
    "max_weekly_drawdown",
    "disagreement_tolerance",
)
_INTEGER_FIELDS = (
    "max_positions",
    "max_entries_per_session",
    "min_score",
    "universe_max_age_days",
    "live_quote_max_age_seconds",
)
_POLICY_FIELDS = frozenset((*_DECIMAL_FIELDS, *_INTEGER_FIELDS))


def _decimal_field(name: str, value: object) -> Decimal:
    if isinstance(value, bool) or not isinstance(value, (str, int)):
        raise ConfigurationError(f"policy field {name} must be an exact decimal string")
    try:
        result = Decimal(str(value))
    except (InvalidOperation, ValueError):
        raise ConfigurationError(f"policy field {name} is not a valid decimal") from None
    if not result.is_finite():
        raise ConfigurationError(f"policy field {name} must be finite")
    return result


def _integer_field(name: str, value: object) -> int:
    if type(value) is not int:
        raise ConfigurationError(f"policy field {name} must be an integer")
    return value


@dataclass(frozen=True)
class Policy:
    capital: Decimal
    max_live_exposure: Decimal
    max_position_risk: Decimal
    max_combined_risk: Decimal
    max_positions: int
    max_entries_per_session: int
    min_score: int
    max_monthly_drawdown: Decimal
    max_weekly_drawdown: Decimal
    universe_max_age_days: int
    live_quote_max_age_seconds: int
    disagreement_tolerance: Decimal

    _FIXED_VALUES: ClassVar[dict[str, Decimal | int]] = {
        "capital": Decimal("5000"),
        "max_live_exposure": Decimal("1000"),
        "max_position_risk": Decimal("25"),
        "max_combined_risk": Decimal("50"),
        "max_positions": 2,
        "max_entries_per_session": 1,
        "min_score": 80,
        "max_monthly_drawdown": Decimal("250"),
        "max_weekly_drawdown": Decimal("100"),
        "universe_max_age_days": 31,
        "live_quote_max_age_seconds": 300,
        "disagreement_tolerance": Decimal("0.005"),
    }

    @classmethod
    def from_toml(cls, path: Path) -> Policy:
        """Load, type-check, and validate a versioned policy document."""
        try:
            document = tomllib.loads(path.read_text(encoding="utf-8"))
        except (OSError, UnicodeError, tomllib.TOMLDecodeError) as error:
            raise ConfigurationError(f"cannot load policy.toml: {type(error).__name__}") from None

        if document.get("schema_version") != 1:
            raise ConfigurationError("policy.toml is missing supported schema_version 1")
        table = document.get("policy")
        if not isinstance(table, dict):
            raise ConfigurationError("policy.toml is missing the policy table")

        names = set(table)
        missing = sorted(_POLICY_FIELDS - names)
        unknown = sorted(names - _POLICY_FIELDS)
        if missing:
            raise ConfigurationError(
                "policy.toml is missing fields: " + ", ".join(missing)
            )
        if unknown:
            raise ConfigurationError(
                "policy.toml contains unknown fields: " + ", ".join(unknown)
            )

        values: dict[str, Decimal | int] = {}
        for name in _DECIMAL_FIELDS:
            values[name] = _decimal_field(name, table[name])
        for name in _INTEGER_FIELDS:
            values[name] = _integer_field(name, table[name])

        policy = cls(**values)  # type: ignore[arg-type]
        policy.validate()
        return policy

    def validate(self) -> None:
        """Reject any alteration to the approved validation boundary."""
        for name, fixed_value in self._FIXED_VALUES.items():
            configured_value = getattr(self, name)
            if (
                type(configured_value) is not type(fixed_value)
                or configured_value != fixed_value
            ):
                raise ConfigurationError(
                    f"policy field {name} is fixed during validation"
                )


__all__ = ["ConfigurationError", "Policy"]
