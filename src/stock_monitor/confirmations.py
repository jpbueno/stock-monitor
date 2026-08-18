"""Strict, side-effect-free parsing for manual confirmation messages."""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal, InvalidOperation
from enum import Enum
from zoneinfo import ZoneInfo

from .domain import (
    DomainValidationError,
    MAX_MICRODOLLARS,
    money_from_micros,
    money_to_micros,
    require_aware_timestamp,
)


_ET = ZoneInfo("America/New_York")
_MONEY = r"[0-9]+(?:\.[0-9]{1,6})?"
_SIGNED_MONEY = rf"[+-]{_MONEY}"
_COUNT = r"[0-9]+"
_SIGNED_COUNT = r"[+-][0-9]+"
_SYMBOL = r"[A-Za-z](?:[A-Za-z0-9]|[.-](?=[A-Za-z0-9])){0,14}"
_OCC = r"[A-Z]{1,6}[0-9]{6}[CP][0-9]{8}"
_ORDER_ID = r"[A-Za-z0-9][A-Za-z0-9._:-]{0,127}"
_ISO_WITH_OFFSET = re.compile(
    r"^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}"
    r"(?:\.[0-9]{1,6})?(?:Z|[+-][0-9]{2}:[0-9]{2})$"
)
_SAME_DAY_ET = re.compile(r"^(?P<hour>[0-9]{2}):(?P<minute>[0-9]{2}) ET$")
_TICKER = re.compile(rf"^{_SYMBOL}$")
_OCC_SYMBOL = re.compile(rf"^(?P<root>[A-Z]{{1,6}})(?P<expiry>[0-9]{{6}})[CP](?P<strike>[0-9]{{8}})$")
MAX_CONFIRMATION_ACTIONS = 64


class ConfirmationKind(str, Enum):
    PENDING_CLARIFICATION = "PENDING_CLARIFICATION"
    ACCOUNT_CHECK = "ACCOUNT_CHECK"
    BUY = "BOUGHT"
    STOP_UPDATED = "STOP_UPDATED"
    STOP_FILLED = "STOP_FILLED"
    SOLD = "SOLD"
    SKIPPED = "SKIPPED"
    OPTION_WINDOW_START = "OPTION_PAPER_WINDOW_START"
    OPTION_OPEN = "OPTION_PAPER_OPEN"
    OPTION_MARK = "OPTION_PAPER_MARK"
    OPTION_CLOSE = "OPTION_PAPER_CLOSE"
    RECONCILE_CASH = "RECONCILE_CASH"
    RECONCILE_UNRELATED_POSITION = "RECONCILE_UNRELATED_POSITION"
    RECONCILE_PENDING_ORDERS = "RECONCILE_PENDING_ORDERS"
    FEE = "FEE"
    PARTIAL_FILL = "PARTIAL_FILL"


class ConfirmationParseError(ValueError):
    """The complete input did not match exactly one supported grammar form."""

    def __init__(self, reason_code: str = "UNRECOGNIZED_CONFIRMATION") -> None:
        self.reason_code = reason_code
        self.code = reason_code
        super().__init__(reason_code)


@dataclass(frozen=True, slots=True)
class ConfirmationEnvelope:
    """One source message with separate message and local receipt knowledge time."""

    message_id: str
    message_time: datetime
    received_at: datetime
    text: str
    session_date: date

    def __post_init__(self) -> None:
        if type(self.message_id) is not str or not self.message_id or "\x00" in self.message_id:
            raise ValueError("INVALID_MESSAGE_ID")
        try:
            message_time = require_aware_timestamp(self.message_time, "message time")
            received_at = require_aware_timestamp(self.received_at, "received at")
        except DomainValidationError as error:
            raise ValueError("INVALID_ENVELOPE_TIME") from error
        if message_time > received_at:
            raise ValueError("CONFIRMATION_SOURCE_TIME_OUT_OF_ORDER")
        if type(self.session_date) is not date:
            raise ValueError("INVALID_SESSION_DATE")
        if message_time.astimezone(_ET).date() != self.session_date:
            raise ValueError("SESSION_DATE_MESSAGE_TIME_MISMATCH")
        if type(self.text) is not str or not self.text:
            raise ValueError("INVALID_CONFIRMATION_TEXT")
        parsed = parse_confirmation_batch_or_pending(
            self.text,
            session_date=self.session_date,
        )
        if isinstance(parsed, tuple) and any(
            action.event_time is not None and action.event_time > message_time
            for action in parsed
        ):
            raise ValueError("CONFIRMATION_SOURCE_TIME_OUT_OF_ORDER")


@dataclass(frozen=True, slots=True)
class ParsedConfirmation:
    """One immutable, normalized action parsed from one complete input line."""

    kind: ConfirmationKind
    raw_text: str
    event_time: datetime | None
    event_time_basis: str = "EXPLICIT"
    symbol: str | None = None
    quantity: int | None = None
    price: Decimal | None = None
    bid: Decimal | None = None
    ask: Decimal | None = None
    stop: Decimal | None = None
    settled_cash: Decimal | None = None
    pending_orders: int | None = None
    unlogged_positions: int | None = None
    occ_symbol: str | None = None
    delta: Decimal | None = None
    open_interest: int | None = None
    volume: int | None = None
    amount: Decimal | None = None
    signed_shares: int | None = None
    reason: str | None = None
    asset_id: str | None = None
    parent_order_id: str | None = None
    fill_group_planned_shares: int | None = None
    missing_fields: frozenset[str] = frozenset()

    def __post_init__(self) -> None:
        if not isinstance(self.kind, ConfirmationKind):
            raise ValueError("INVALID_CONFIRMATION_KIND")
        if type(self.raw_text) is not str or not self.raw_text or "\x00" in self.raw_text:
            raise ValueError("INVALID_CONFIRMATION_TEXT")
        if self.event_time is not None:
            try:
                require_aware_timestamp(self.event_time, "event time")
            except DomainValidationError as error:
                raise ValueError("INVALID_CONFIRMATION_TIME") from error
        if self.event_time_basis not in {"EXPLICIT", "MESSAGE_TIME_OBSERVATION"}:
            raise ValueError("INVALID_EVENT_TIME_BASIS")
        if self.symbol is not None and (
            type(self.symbol) is not str
            or self.symbol != self.symbol.upper()
            or not _is_valid_equity_symbol(self.symbol)
        ):
            raise ValueError("INVALID_CONFIRMATION_SYMBOL")
        if self.quantity is not None:
            _validate_int(self.quantity, positive=True)
        for name in ("price", "bid", "ask", "stop"):
            value = getattr(self, name)
            if value is not None:
                object.__setattr__(self, name, _normalize_money(value, positive=True))
        if self.settled_cash is not None:
            object.__setattr__(
                self,
                "settled_cash",
                _normalize_money(self.settled_cash, nonnegative=True),
            )
        for name in ("pending_orders", "unlogged_positions", "open_interest", "volume"):
            value = getattr(self, name)
            if value is not None:
                _validate_int(value, nonnegative=True)
        if self.delta is not None:
            object.__setattr__(self, "delta", _normalize_delta(self.delta))
        if self.amount is not None:
            amount = _normalize_money(self.amount)
            if amount == 0:
                raise ValueError("INVALID_CONFIRMATION_AMOUNT")
            object.__setattr__(self, "amount", amount)
        if self.signed_shares is not None:
            _validate_int(self.signed_shares)
            if self.signed_shares == 0:
                raise ValueError("INVALID_CONFIRMATION_SHARES")
        if self.occ_symbol is not None:
            _validate_occ(self.occ_symbol)
        if self.asset_id is not None:
            if self.asset_id != self.asset_id.upper():
                raise ValueError("INVALID_ASSET_ID")
            if _OCC_SYMBOL.fullmatch(self.asset_id) is not None:
                _validate_occ(self.asset_id)
            elif not _is_valid_equity_symbol(self.asset_id):
                raise ValueError("INVALID_ASSET_ID")
        if self.reason is not None and (
            type(self.reason) is not str
            or not self.reason
            or self.reason != self.reason.strip()
            or any(not character.isprintable() for character in self.reason)
        ):
            raise ValueError("INVALID_RECONCILIATION_REASON")
        if (self.parent_order_id is None) != (
            self.fill_group_planned_shares is None
        ):
            raise ValueError("INCOMPLETE_FILL_GROUP")
        if self.parent_order_id is not None:
            if re.fullmatch(_ORDER_ID, self.parent_order_id) is None:
                raise ValueError("INVALID_PARENT_ORDER_ID")
            _validate_int(self.fill_group_planned_shares, positive=True)
            if self.quantity is None or self.fill_group_planned_shares < self.quantity:
                raise ValueError("INVALID_FILL_GROUP_SHARES")
        missing = frozenset(self.missing_fields)
        if any(type(field) is not str or not field for field in missing):
            raise ValueError("INVALID_MISSING_FIELDS")
        object.__setattr__(self, "missing_fields", missing)
        if self.bid is not None and self.ask is not None and self.ask < self.bid:
            raise ValueError("INVALID_CONFIRMED_SPREAD")
        self._validate_shape()

    def _validate_shape(self) -> None:
        values = {
            "event_time": self.event_time,
            "symbol": self.symbol,
            "quantity": self.quantity,
            "price": self.price,
            "bid": self.bid,
            "ask": self.ask,
            "stop": self.stop,
            "settled_cash": self.settled_cash,
            "pending_orders": self.pending_orders,
            "unlogged_positions": self.unlogged_positions,
            "occ_symbol": self.occ_symbol,
            "delta": self.delta,
            "open_interest": self.open_interest,
            "volume": self.volume,
            "amount": self.amount,
            "signed_shares": self.signed_shares,
            "reason": self.reason,
            "asset_id": self.asset_id,
            "parent_order_id": self.parent_order_id,
            "fill_group_planned_shares": self.fill_group_planned_shares,
        }

        def exact(required: set[str], optional: set[str] = set()) -> None:
            if any(values[name] is None for name in required):
                raise ValueError("INCOMPLETE_CONFIRMATION")
            allowed = required | optional
            if any(value is not None and name not in allowed for name, value in values.items()):
                raise ValueError("INCONSISTENT_CONFIRMATION")

        if self.kind is ConfirmationKind.BUY:
            exact(
                {"event_time", "symbol", "quantity", "price"},
                {"bid", "ask", "stop"},
            )
            quote_fields = (self.bid, self.ask, self.stop)
            if all(value is None for value in quote_fields):
                expected_missing = frozenset({"bid", "ask", "stop"})
            elif all(value is not None for value in quote_fields):
                expected_missing = frozenset()
            else:
                raise ValueError("INCOMPLETE_BUY")
            if self.missing_fields != expected_missing:
                raise ValueError("INVALID_MISSING_FIELDS")
        elif self.kind is ConfirmationKind.PARTIAL_FILL:
            exact(
                {"event_time", "symbol", "quantity", "price"},
                {"parent_order_id", "fill_group_planned_shares"},
            )
            expected = (
                frozenset({"parent_order_id", "fill_group_planned_shares"})
                if self.parent_order_id is None
                else frozenset()
            )
            if self.missing_fields != expected:
                raise ValueError("INVALID_MISSING_FIELDS")
        elif self.kind is ConfirmationKind.ACCOUNT_CHECK:
            exact(
                {
                    "event_time",
                    "settled_cash",
                    "pending_orders",
                    "unlogged_positions",
                }
            )
        elif self.kind is ConfirmationKind.STOP_UPDATED:
            exact({"event_time", "symbol", "stop"})
        elif self.kind in {ConfirmationKind.STOP_FILLED, ConfirmationKind.SOLD}:
            exact({"event_time", "symbol", "quantity", "price"})
        elif self.kind is ConfirmationKind.SKIPPED:
            exact({"symbol"})
            if self.event_time is not None or self.event_time_basis != "MESSAGE_TIME_OBSERVATION":
                raise ValueError("INVALID_SKIPPED_TIME_BASIS")
        elif self.kind is ConfirmationKind.OPTION_WINDOW_START:
            exact({"event_time"})
        elif self.kind is ConfirmationKind.OPTION_OPEN:
            exact(
                {
                    "event_time",
                    "occ_symbol",
                    "bid",
                    "ask",
                    "delta",
                    "open_interest",
                    "volume",
                }
            )
        elif self.kind in {ConfirmationKind.OPTION_MARK, ConfirmationKind.OPTION_CLOSE}:
            exact({"event_time", "occ_symbol", "bid", "ask"})
        elif self.kind is ConfirmationKind.RECONCILE_CASH:
            exact({"event_time", "amount", "reason"})
        elif self.kind is ConfirmationKind.RECONCILE_UNRELATED_POSITION:
            exact({"event_time", "symbol", "price", "signed_shares"})
        elif self.kind is ConfirmationKind.RECONCILE_PENDING_ORDERS:
            exact({"event_time", "pending_orders"})
        elif self.kind is ConfirmationKind.FEE:
            exact({"event_time", "amount", "asset_id"})
        elif self.kind is ConfirmationKind.PENDING_CLARIFICATION:
            raise ValueError("PENDING_IS_NOT_A_PARSED_ACTION")
        if self.kind not in {ConfirmationKind.BUY, ConfirmationKind.PARTIAL_FILL} and self.missing_fields:
            raise ValueError("INVALID_MISSING_FIELDS")
        if self.kind is not ConfirmationKind.SKIPPED and self.event_time_basis != "EXPLICIT":
            raise ValueError("INVALID_EVENT_TIME_BASIS")


@dataclass(frozen=True, slots=True)
class PendingConfirmation:
    raw_text: str
    reason_code: str = "UNRECOGNIZED_CONFIRMATION"

    def __post_init__(self) -> None:
        if type(self.raw_text) is not str:
            raise ValueError("INVALID_CONFIRMATION_TEXT")
        if type(self.reason_code) is not str or not self.reason_code:
            raise ValueError("INVALID_PENDING_REASON")


def _normalize_decimal(value: Decimal) -> Decimal:
    if not isinstance(value, Decimal) or not value.is_finite():
        raise ValueError("INVALID_DECIMAL")
    return value


def _normalize_delta(value: Decimal) -> Decimal:
    delta = _normalize_decimal(value)
    try:
        canonical = delta.quantize(Decimal("0.000001"))
    except InvalidOperation as error:
        raise ValueError("INVALID_OPTION_DELTA") from error
    if canonical != delta or not Decimal("0") < canonical < Decimal("1"):
        raise ValueError("INVALID_OPTION_DELTA")
    return canonical


def _normalize_money(
    value: Decimal,
    *,
    positive: bool = False,
    nonnegative: bool = False,
) -> Decimal:
    try:
        normalized = money_from_micros(money_to_micros(value))
    except DomainValidationError as error:
        raise ValueError("INVALID_MONEY") from error
    if positive and normalized <= 0:
        raise ValueError("INVALID_MONEY")
    if nonnegative and normalized < 0:
        raise ValueError("INVALID_MONEY")
    return normalized


def _validate_int(
    value: object,
    *,
    positive: bool = False,
    nonnegative: bool = False,
) -> int:
    if type(value) is not int or not -(2**63) <= value <= MAX_MICRODOLLARS:
        raise ValueError("INVALID_INTEGER")
    if positive and value <= 0:
        raise ValueError("INVALID_INTEGER")
    if nonnegative and value < 0:
        raise ValueError("INVALID_INTEGER")
    return value


def _decimal(text: str) -> Decimal:
    try:
        return Decimal(text)
    except InvalidOperation as error:
        raise ConfirmationParseError("INVALID_DECIMAL") from error


def _positive_money(text: str) -> Decimal:
    try:
        return _normalize_money(_decimal(text), positive=True)
    except ValueError as error:
        raise ConfirmationParseError("INVALID_MONEY") from error


def _nonnegative_money(text: str) -> Decimal:
    try:
        return _normalize_money(_decimal(text), nonnegative=True)
    except ValueError as error:
        raise ConfirmationParseError("INVALID_MONEY") from error


def _integer(text: str, *, positive: bool = False, nonnegative: bool = False) -> int:
    try:
        return _validate_int(int(text), positive=positive, nonnegative=nonnegative)
    except (ValueError, OverflowError) as error:
        raise ConfirmationParseError("INVALID_INTEGER") from error


def _parse_time(text: str, session_date: date) -> datetime:
    if type(session_date) is not date:
        raise ConfirmationParseError("INVALID_SESSION_DATE")
    same_day = _SAME_DAY_ET.fullmatch(text)
    if same_day is not None:
        hour = int(same_day.group("hour"))
        minute = int(same_day.group("minute"))
        if hour > 23 or minute > 59:
            raise ConfirmationParseError("INVALID_CONFIRMATION_TIME")
        return datetime(
            session_date.year,
            session_date.month,
            session_date.day,
            hour,
            minute,
            tzinfo=_ET,
        )
    if _ISO_WITH_OFFSET.fullmatch(text) is None:
        raise ConfirmationParseError("INVALID_CONFIRMATION_TIME")
    offset = re.search(r"[+-](?P<hour>[0-9]{2}):(?P<minute>[0-9]{2})$", text)
    if offset is not None and (
        int(offset.group("hour")) > 23 or int(offset.group("minute")) > 59
    ):
        raise ConfirmationParseError("INVALID_CONFIRMATION_TIME")
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
        return require_aware_timestamp(parsed, "event time")
    except (DomainValidationError, ValueError) as error:
        raise ConfirmationParseError("INVALID_CONFIRMATION_TIME") from error


def _validate_symbol(text: str) -> str:
    symbol = text.upper()
    if not _is_valid_equity_symbol(symbol):
        raise ConfirmationParseError("INVALID_SYMBOL")
    return symbol


def _is_valid_equity_symbol(symbol: str) -> bool:
    return (
        len(symbol) <= 15
        and _TICKER.fullmatch(symbol) is not None
        and _OCC_SYMBOL.fullmatch(symbol) is None
    )


def _has_valid_confirmation_characters(text: str) -> bool:
    if any(not character.isprintable() for character in text):
        return False
    if text.isascii():
        return True
    return (
        re.fullmatch(
            rf"RECONCILE CASH {_SIGNED_MONEY} REASON .+ AT [\x20-\x7e]+",
            text,
        )
        is not None
    )


def _validate_occ(text: str) -> str:
    match = _OCC_SYMBOL.fullmatch(text)
    if match is None:
        raise ValueError("INVALID_OCC_SYMBOL")
    try:
        datetime.strptime(match.group("expiry"), "%y%m%d")
    except ValueError as error:
        raise ValueError("INVALID_OCC_SYMBOL") from error
    if int(match.group("strike")) <= 0:
        raise ValueError("INVALID_OCC_SYMBOL")
    return text


def _match(pattern: str, text: str) -> re.Match[str] | None:
    return re.fullmatch(pattern, text)


def _parse_confirmation(text: str, session_date: date) -> ParsedConfirmation:
    """Parse exactly one complete grammar action; never consume a prefix."""
    if type(session_date) is not date:
        raise ConfirmationParseError("INVALID_SESSION_DATE")
    if (
        type(text) is not str
        or not text
        or not _has_valid_confirmation_characters(text)
    ):
        raise ConfirmationParseError()

    match = _match(
        rf"BOUGHT (?P<symbol>{_SYMBOL}) (?P<shares>{_COUNT}) shares @ (?P<price>{_MONEY}) "
        rf"AT (?P<at>.+); BID (?P<bid>{_MONEY}) ASK (?P<ask>{_MONEY}); "
        rf"STOP SET @ (?P<stop>{_MONEY})",
        text,
    )
    if match is not None:
        return ParsedConfirmation(
            ConfirmationKind.BUY,
            text,
            _parse_time(match["at"], session_date),
            symbol=_validate_symbol(match["symbol"]),
            quantity=_integer(match["shares"], positive=True),
            price=_positive_money(match["price"]),
            bid=_positive_money(match["bid"]),
            ask=_positive_money(match["ask"]),
            stop=_positive_money(match["stop"]),
        )

    match = _match(
        rf"BOUGHT (?P<symbol>{_SYMBOL}) (?P<shares>{_COUNT}) shares @ (?P<price>{_MONEY}) AT (?P<at>.+)",
        text,
    )
    if match is not None:
        return ParsedConfirmation(
            ConfirmationKind.BUY,
            text,
            _parse_time(match["at"], session_date),
            symbol=_validate_symbol(match["symbol"]),
            quantity=_integer(match["shares"], positive=True),
            price=_positive_money(match["price"]),
            missing_fields=frozenset({"bid", "ask", "stop"}),
        )

    match = _match(
        rf"ACCOUNT CHECK settled_cash (?P<cash>{_MONEY}) pending_orders (?P<pending>{_COUNT}) "
        rf"unlogged_positions (?P<unlogged>{_COUNT}) AT (?P<at>.+)",
        text,
    )
    if match is not None:
        return ParsedConfirmation(
            ConfirmationKind.ACCOUNT_CHECK,
            text,
            _parse_time(match["at"], session_date),
            settled_cash=_nonnegative_money(match["cash"]),
            pending_orders=_integer(match["pending"], nonnegative=True),
            unlogged_positions=_integer(match["unlogged"], nonnegative=True),
        )

    for prefix, kind in (
        ("STOP UPDATED", ConfirmationKind.STOP_UPDATED),
    ):
        match = _match(
            rf"{prefix} (?P<symbol>{_SYMBOL}) @ (?P<price>{_MONEY}) AT (?P<at>.+)",
            text,
        )
        if match is not None:
            return ParsedConfirmation(
                kind,
                text,
                _parse_time(match["at"], session_date),
                symbol=_validate_symbol(match["symbol"]),
                stop=_positive_money(match["price"]),
            )

    for prefix, kind in (
        ("STOP FILLED", ConfirmationKind.STOP_FILLED),
        ("SOLD", ConfirmationKind.SOLD),
    ):
        match = _match(
            rf"{prefix} (?P<symbol>{_SYMBOL}) (?P<shares>{_COUNT}) shares @ "
            rf"(?P<price>{_MONEY}) AT (?P<at>.+)",
            text,
        )
        if match is not None:
            return ParsedConfirmation(
                kind,
                text,
                _parse_time(match["at"], session_date),
                symbol=_validate_symbol(match["symbol"]),
                quantity=_integer(match["shares"], positive=True),
                price=_positive_money(match["price"]),
            )

    match = _match(rf"SKIPPED (?P<symbol>{_SYMBOL})", text)
    if match is not None:
        return ParsedConfirmation(
            ConfirmationKind.SKIPPED,
            text,
            None,
            event_time_basis="MESSAGE_TIME_OBSERVATION",
            symbol=_validate_symbol(match["symbol"]),
        )

    match = _match(r"OPTION PAPER WINDOW START AT (?P<at>.+)", text)
    if match is not None:
        return ParsedConfirmation(
            ConfirmationKind.OPTION_WINDOW_START,
            text,
            _parse_time(match["at"], session_date),
        )

    match = _match(
        rf"OPTION PAPER OPEN (?P<occ>{_OCC}) BID (?P<bid>{_MONEY}) ASK (?P<ask>{_MONEY}) "
        rf"DELTA (?P<delta>{_MONEY}) OI (?P<oi>{_COUNT}) VOLUME (?P<volume>{_COUNT}) AT (?P<at>.+)",
        text,
    )
    if match is not None:
        try:
            occ = _validate_occ(match["occ"])
        except ValueError as error:
            raise ConfirmationParseError("INVALID_OCC_SYMBOL") from error
        return ParsedConfirmation(
            ConfirmationKind.OPTION_OPEN,
            text,
            _parse_time(match["at"], session_date),
            occ_symbol=occ,
            bid=_positive_money(match["bid"]),
            ask=_positive_money(match["ask"]),
            delta=_decimal(match["delta"]),
            open_interest=_integer(match["oi"], nonnegative=True),
            volume=_integer(match["volume"], nonnegative=True),
        )

    for prefix, kind in (
        ("OPTION PAPER MARK", ConfirmationKind.OPTION_MARK),
        ("OPTION PAPER CLOSE", ConfirmationKind.OPTION_CLOSE),
    ):
        match = _match(
            rf"{prefix} (?P<occ>{_OCC}) BID (?P<bid>{_MONEY}) ASK (?P<ask>{_MONEY}) AT (?P<at>.+)",
            text,
        )
        if match is not None:
            try:
                occ = _validate_occ(match["occ"])
            except ValueError as error:
                raise ConfirmationParseError("INVALID_OCC_SYMBOL") from error
            return ParsedConfirmation(
                kind,
                text,
                _parse_time(match["at"], session_date),
                occ_symbol=occ,
                bid=_positive_money(match["bid"]),
                ask=_positive_money(match["ask"]),
            )

    match = _match(
        rf"RECONCILE CASH (?P<amount>{_SIGNED_MONEY}) REASON (?P<reason>.+) AT (?P<at>.+)",
        text,
    )
    if match is not None:
        amount = _decimal(match["amount"])
        try:
            amount = _normalize_money(amount)
        except ValueError as error:
            raise ConfirmationParseError("INVALID_MONEY") from error
        return ParsedConfirmation(
            ConfirmationKind.RECONCILE_CASH,
            text,
            _parse_time(match["at"], session_date),
            amount=amount,
            reason=match["reason"],
        )

    match = _match(
        rf"RECONCILE UNRELATED POSITION (?P<symbol>{_SYMBOL}) (?P<shares>{_SIGNED_COUNT}) "
        rf"shares @ (?P<price>{_MONEY}) AT (?P<at>.+)",
        text,
    )
    if match is not None:
        return ParsedConfirmation(
            ConfirmationKind.RECONCILE_UNRELATED_POSITION,
            text,
            _parse_time(match["at"], session_date),
            symbol=_validate_symbol(match["symbol"]),
            price=_positive_money(match["price"]),
            signed_shares=_integer(match["shares"]),
        )

    match = _match(
        rf"RECONCILE PENDING ORDERS (?P<count>{_COUNT}) AT (?P<at>.+)",
        text,
    )
    if match is not None:
        return ParsedConfirmation(
            ConfirmationKind.RECONCILE_PENDING_ORDERS,
            text,
            _parse_time(match["at"], session_date),
            pending_orders=_integer(match["count"], nonnegative=True),
        )

    match = _match(
        rf"FEE (?P<asset>{_SYMBOL}|{_OCC}) (?P<amount>{_MONEY}) AT (?P<at>.+)",
        text,
    )
    if match is not None:
        asset = match["asset"].upper()
        if _OCC_SYMBOL.fullmatch(asset) is not None:
            try:
                _validate_occ(asset)
            except ValueError as error:
                raise ConfirmationParseError("INVALID_ASSET_ID") from error
        elif not _is_valid_equity_symbol(asset):
            raise ConfirmationParseError("INVALID_ASSET_ID")
        return ParsedConfirmation(
            ConfirmationKind.FEE,
            text,
            _parse_time(match["at"], session_date),
            amount=_positive_money(match["amount"]),
            asset_id=asset,
        )

    match = _match(
        rf"PARTIAL FILL (?P<symbol>{_SYMBOL}) (?P<shares>{_COUNT}) shares @ "
        rf"(?P<price>{_MONEY}) AT (?P<at>.+); ORDER (?P<order>{_ORDER_ID}) "
        rf"TOTAL (?P<total>{_COUNT}) shares",
        text,
    )
    if match is not None:
        return ParsedConfirmation(
            ConfirmationKind.PARTIAL_FILL,
            text,
            _parse_time(match["at"], session_date),
            symbol=_validate_symbol(match["symbol"]),
            quantity=_integer(match["shares"], positive=True),
            price=_positive_money(match["price"]),
            parent_order_id=match["order"],
            fill_group_planned_shares=_integer(match["total"], positive=True),
        )

    match = _match(
        rf"PARTIAL FILL (?P<symbol>{_SYMBOL}) (?P<shares>{_COUNT}) shares @ "
        rf"(?P<price>{_MONEY}) AT (?P<at>.+)",
        text,
    )
    if match is not None:
        return ParsedConfirmation(
            ConfirmationKind.PARTIAL_FILL,
            text,
            _parse_time(match["at"], session_date),
            symbol=_validate_symbol(match["symbol"]),
            quantity=_integer(match["shares"], positive=True),
            price=_positive_money(match["price"]),
            missing_fields=frozenset(
                {"parent_order_id", "fill_group_planned_shares"}
            ),
        )

    raise ConfirmationParseError()


def parse_confirmation(text: str, session_date: date) -> ParsedConfirmation:
    """Parse one action and expose one stable parse-error family to callers."""
    try:
        return _parse_confirmation(text, session_date)
    except ConfirmationParseError:
        raise
    except (DomainValidationError, ValueError) as error:
        reason = str(error) if str(error) else "INVALID_CONFIRMATION"
        raise ConfirmationParseError(reason) from error


def parse_confirmation_or_pending(
    text: str,
    session_date: date,
) -> ParsedConfirmation | PendingConfirmation:
    try:
        return parse_confirmation(text, session_date)
    except ConfirmationParseError as error:
        return PendingConfirmation(text, error.reason_code)


def parse_confirmation_batch(
    text: str,
    *,
    session_date: date,
) -> tuple[ParsedConfirmation, ...]:
    """Parse exact actions separated by LF; only reconciliation reasons allow Unicode."""
    if (
        type(text) is not str
        or not text
        or "\r" in text
        or text.startswith("\n")
        or text.endswith("\n")
    ):
        raise ConfirmationParseError()
    lines = text.split("\n")
    if any(not line for line in lines):
        raise ConfirmationParseError()
    if len(lines) > MAX_CONFIRMATION_ACTIONS:
        raise ConfirmationParseError("CONFIRMATION_BATCH_TOO_LARGE")
    return tuple(parse_confirmation(line, session_date) for line in lines)


def parse_confirmation_batch_or_pending(
    text: str,
    *,
    session_date: date,
) -> tuple[ParsedConfirmation, ...] | PendingConfirmation:
    try:
        return parse_confirmation_batch(text, session_date=session_date)
    except ConfirmationParseError as error:
        if error.reason_code == "CONFIRMATION_BATCH_TOO_LARGE":
            raise
        return PendingConfirmation(text, error.reason_code)


__all__ = [
    "ConfirmationKind",
    "ConfirmationEnvelope",
    "ConfirmationParseError",
    "MAX_CONFIRMATION_ACTIONS",
    "ParsedConfirmation",
    "PendingConfirmation",
    "parse_confirmation",
    "parse_confirmation_batch",
    "parse_confirmation_batch_or_pending",
    "parse_confirmation_or_pending",
]
