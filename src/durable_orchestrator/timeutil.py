from __future__ import annotations

import re
from datetime import datetime, timedelta, timezone

_DURATION = re.compile(r"^\s*(\d+(?:\.\d+)?)\s*([smhdw])\s*$")
_UNITS = {"s": "seconds", "m": "minutes", "h": "hours", "d": "days", "w": "weeks"}


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def parse_duration(value: str | int | float) -> timedelta:
    """'10d', '90s', '2h', '1.5w' or a number of seconds."""
    if isinstance(value, (int, float)):
        return timedelta(seconds=value)
    m = _DURATION.match(value)
    if not m:
        raise ValueError(f"Bad duration {value!r}; use e.g. '10d', '2h', '90s'")
    return timedelta(**{_UNITS[m.group(2)]: float(m.group(1))})


def parse_ts(value: str) -> datetime:
    dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
    return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)


def iso(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).isoformat(timespec="seconds")
