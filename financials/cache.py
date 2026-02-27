"""Simple in-memory TTL cache for financial data."""

import time
import threading

_cache = {}
_lock = threading.Lock()

DEFAULT_TTL = 300     # 5 minutes for standard data
PEERS_TTL = 600       # 10 minutes for slow peer fetches
QUOTE_TTL = 60        # 1 minute for live quotes
SECTOR_MOMENTUM_TTL = 1800  # 30 minutes for sector ETF data
ESG_TTL = 1800              # 30 minutes for ESG / sustainability data
HISTORY_INTRADAY_TTL = 300  # 5 min for 1D intraday data
HISTORY_TTL = 900           # 15 min for 1M/1Y historical data


def get(key: str):
    """Return cached value if not expired, else None."""
    with _lock:
        entry = _cache.get(key)
        if entry and time.time() < entry["expires"]:
            return entry["value"]
        if entry:
            del _cache[key]
    return None


def put(key: str, value, ttl: int = DEFAULT_TTL):
    """Store a value with a TTL in seconds."""
    with _lock:
        _cache[key] = {"value": value, "expires": time.time() + ttl}


def clear():
    """Clear all cache entries."""
    with _lock:
        _cache.clear()
