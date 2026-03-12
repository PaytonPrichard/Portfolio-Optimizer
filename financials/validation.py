"""Shared input-validation helpers used across route blueprints."""

import re

_TICKER_RE = re.compile(r"^[A-Z0-9\^.\-]{1,10}$")


def validate_ticker(ticker):
    """Return cleaned ticker or None if invalid."""
    t = (ticker or "").strip().upper()
    if _TICKER_RE.match(t):
        return t
    return None
