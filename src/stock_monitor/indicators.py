"""Exact completed-session indicators used by the pure screening engine."""

from __future__ import annotations

from collections.abc import Sequence
from decimal import Decimal, localcontext
from typing import Protocol


_ZERO = Decimal("0")
_ONE = Decimal("1")


class IndicatorError(ValueError):
    """Indicator input is missing, inexact, or outside the approved bar contract."""


class BarLike(Protocol):
    """Narrow structural contract shared with normalized provider bars."""

    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    volume: int
    adjustment: str


def _period(value: int, name: str = "period") -> int:
    if type(value) is not int or value <= 0:
        raise IndicatorError(f"{name} must be a positive integer")
    return value


def _decimal_values(values: Sequence[Decimal], *, minimum: int) -> tuple[Decimal, ...]:
    if isinstance(values, (str, bytes)):
        raise IndicatorError("indicator values must be a Decimal sequence")
    result = tuple(values)
    if len(result) < minimum:
        raise IndicatorError("indicator history is insufficient")
    if any(
        not isinstance(value, Decimal) or not value.is_finite()
        for value in result
    ):
        raise IndicatorError("indicator values must be finite Decimals")
    return result


def _bars(values: Sequence[BarLike], *, minimum: int) -> tuple[BarLike, ...]:
    if isinstance(values, (str, bytes)):
        raise IndicatorError("bars must be a sequence")
    result = tuple(values)
    if len(result) < minimum:
        raise IndicatorError("bar history is insufficient")
    for bar in result:
        try:
            prices = (bar.open, bar.high, bar.low, bar.close)
            volume = bar.volume
            adjustment = bar.adjustment
        except AttributeError:
            raise IndicatorError("bar does not satisfy the normalized contract") from None
        if any(
            not isinstance(price, Decimal)
            or not price.is_finite()
            or price <= _ZERO
            for price in prices
        ):
            raise IndicatorError("bar prices must be positive finite Decimals")
        if bar.low > min(bar.open, bar.close) or bar.high < max(bar.open, bar.close):
            raise IndicatorError("bar OHLC values are internally inconsistent")
        if bar.low > bar.high:
            raise IndicatorError("bar low exceeds high")
        if type(volume) is not int or volume <= 0:
            raise IndicatorError("bar volume must be a positive integer")
        if adjustment != "split":
            raise IndicatorError("only split-adjusted bars are accepted")
    return result


def sma(values: Sequence[Decimal], period: int) -> Decimal:
    """Return the trailing simple moving average for *period* observations."""
    window_size = _period(period)
    checked = _decimal_values(values, minimum=window_size)
    with localcontext() as context:
        context.prec = 50
        return sum(checked[-window_size:], _ZERO) / Decimal(window_size)


def ema(values: Sequence[Decimal], period: int) -> Decimal:
    """Return a conventional EMA seeded by the first period's SMA."""
    window_size = _period(period)
    checked = _decimal_values(values, minimum=window_size)
    with localcontext() as context:
        context.prec = 50
        current = sum(checked[:window_size], _ZERO) / Decimal(window_size)
        alpha = Decimal("2") / Decimal(window_size + 1)
        for value in checked[window_size:]:
            current = current + alpha * (value - current)
        return current


def wilder_atr(bars: Sequence[BarLike], period: int) -> Decimal:
    """Return Wilder ATR with a simple-average true-range seed."""
    window_size = _period(period)
    checked = _bars(bars, minimum=window_size)
    with localcontext() as context:
        context.prec = 50
        true_ranges: list[Decimal] = []
        previous_close: Decimal | None = None
        for bar in checked:
            if previous_close is None:
                true_range = bar.high - bar.low
            else:
                true_range = max(
                    bar.high - bar.low,
                    abs(bar.high - previous_close),
                    abs(bar.low - previous_close),
                )
            true_ranges.append(true_range)
            previous_close = bar.close
        current = sum(true_ranges[:window_size], _ZERO) / Decimal(window_size)
        for true_range in true_ranges[window_size:]:
            current = (
                current * Decimal(window_size - 1) + true_range
            ) / Decimal(window_size)
        return current


def session_return(values: Sequence[Decimal], sessions: int) -> Decimal:
    """Return close-to-close total return across an exact session offset."""
    offset = _period(sessions, "sessions")
    checked = _decimal_values(values, minimum=offset + 1)
    start = checked[-offset - 1]
    end = checked[-1]
    if start <= _ZERO or end <= _ZERO:
        raise IndicatorError("return prices must be positive")
    with localcontext() as context:
        context.prec = 50
        return end / start - _ONE


def five_session_return(values: Sequence[Decimal]) -> Decimal:
    return session_return(values, 5)


def twenty_session_return(values: Sequence[Decimal]) -> Decimal:
    return session_return(values, 20)


def average_dollar_volume(bars: Sequence[BarLike], period: int = 20) -> Decimal:
    """Return trailing mean of split-adjusted close times share volume."""
    window_size = _period(period)
    checked = _bars(bars, minimum=window_size)
    with localcontext() as context:
        context.prec = 50
        total = sum(
            (bar.close * Decimal(bar.volume) for bar in checked[-window_size:]),
            _ZERO,
        )
        return total / Decimal(window_size)


def median_share_volume(bars: Sequence[BarLike], period: int = 20) -> Decimal:
    """Return the exact trailing median share volume."""
    window_size = _period(period)
    checked = _bars(bars, minimum=window_size)
    ordered = sorted(bar.volume for bar in checked[-window_size:])
    midpoint = window_size // 2
    with localcontext() as context:
        context.prec = 50
        if window_size % 2:
            return Decimal(ordered[midpoint])
        return (
            Decimal(ordered[midpoint - 1]) + Decimal(ordered[midpoint])
        ) / Decimal("2")


def max_relative_volume(
    bars: Sequence[BarLike],
    period: int = 20,
    lookback: int = 3,
) -> Decimal:
    """Return the largest volume/SMA(volume) ratio in the trailing lookback."""
    window_size = _period(period)
    trailing = _period(lookback, "lookback")
    checked = _bars(bars, minimum=window_size + trailing - 1)
    ratios: list[Decimal] = []
    with localcontext() as context:
        context.prec = 50
        for index in range(len(checked) - trailing, len(checked)):
            start = index - window_size + 1
            average = sum(
                (Decimal(bar.volume) for bar in checked[start : index + 1]),
                _ZERO,
            ) / Decimal(window_size)
            if average <= _ZERO:
                raise IndicatorError("relative-volume average is nonpositive")
            ratios.append(Decimal(checked[index].volume) / average)
        return max(ratios)


def directional_volume_means(
    bars: Sequence[BarLike],
    period: int = 10,
) -> tuple[Decimal | None, Decimal | None]:
    """Return mean up-session and down-session volume; unchanged days are omitted."""
    window_size = _period(period)
    checked = _bars(bars, minimum=window_size + 1)
    up: list[int] = []
    down: list[int] = []
    for index in range(len(checked) - window_size, len(checked)):
        current = checked[index]
        previous = checked[index - 1]
        if current.close > previous.close:
            up.append(current.volume)
        elif current.close < previous.close:
            down.append(current.volume)
    with localcontext() as context:
        context.prec = 50
        up_mean = (
            sum((Decimal(value) for value in up), _ZERO) / Decimal(len(up))
            if up
            else None
        )
        down_mean = (
            sum((Decimal(value) for value in down), _ZERO) / Decimal(len(down))
            if down
            else None
        )
        return up_mean, down_mean


__all__ = [
    "BarLike",
    "IndicatorError",
    "average_dollar_volume",
    "directional_volume_means",
    "ema",
    "five_session_return",
    "max_relative_volume",
    "median_share_volume",
    "session_return",
    "sma",
    "twenty_session_return",
    "wilder_atr",
]
