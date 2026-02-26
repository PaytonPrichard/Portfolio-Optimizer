"""Shared formatting helpers."""

import pandas as pd


def fmt_money(value) -> str:
    """Format a dollar value into readable billions / millions."""
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return "N/A"
    try:
        value = float(value)
    except (TypeError, ValueError):
        return "N/A"
    if abs(value) >= 1e12:
        return f"${value / 1e12:.2f}T"
    if abs(value) >= 1e9:
        return f"${value / 1e9:.2f}B"
    if abs(value) >= 1e6:
        return f"${value / 1e6:.2f}M"
    return f"${value:,.0f}"


def fmt_val(value, prefix="", suffix="", decimals=2) -> str:
    """Generic value formatter."""
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return "N/A"
    try:
        return f"{prefix}{float(value):.{decimals}f}{suffix}"
    except (TypeError, ValueError):
        return "N/A"
