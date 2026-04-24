"""Point-in-time Alpha-Lite reconstruction for the backtest harness.

Reconstructs a subset of the 13 Alpha sub-scores using yfinance price-based
data that's natively point-in-time: momentum, technical, industry_cycle,
macro, earnings_surprise. The other 8 sub-scores (value, quality, growth,
analyst, insider, buyback, analyst_momentum, institutional) are held at a
neutral 50.

The reconstruction calls the SAME scoring functions used in production
(`_score_momentum`, `score_technical`, etc.). We only reconstruct their
INPUTS at the historical date, so score logic drift is impossible.

Layered API:
- Low-level per-symbol call: `reconstruct_alpha_lite(symbol, as_of_date, ctx)`
- High-level batch: `prefetch_backtest_context(symbols, start, end)` returns
  a ctx with pre-loaded closes/macro/sector data to avoid redundant yfinance
  calls across dates.
"""

from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Optional

import yfinance as yf

from . import cache
from .alpha import (
    _score_momentum, _score_industry_cycle,
    _get_factor_weights,
)
from .alpha_signals import (
    _compute_rsi, _compute_macd,
    score_technical, score_macro, score_earnings_surprise,
)
from .portfolio_widgets import SECTOR_ETFS

RECONSTRUCTED_FACTORS = (
    "momentum", "technical", "industry_cycle", "macro", "earnings_surprise",
)
NEUTRAL_SCORE = 50
HIST_CACHE_TTL = 86400  # 24h — historical values don't change

# Factors we don't reconstruct get held at neutral. Track which ones so the
# backtest can filter IC computation to reconstructed signals only.
NON_RECONSTRUCTED_FACTORS = (
    "value", "quality", "growth", "analyst", "insider",
    "buyback", "analyst_momentum", "institutional",
)


@dataclass
class BacktestContext:
    """Pre-loaded historical data for an entire backtest run. Keeps yfinance
    calls low by fetching each symbol's full history once, then slicing per
    as_of_date.
    """
    closes: dict = field(default_factory=dict)  # symbol -> list of (date, close) tuples
    macro_history: Optional[dict] = None  # {"vix": [(date, val)], "yield10y": ..., "yield3m": ...}
    sector_etf_closes: dict = field(default_factory=dict)  # etf -> list of (date, close)
    earnings_raw: dict = field(default_factory=dict)  # symbol -> yfinance earnings_history DataFrame


def _daily_closes_up_to(series_list, as_of_date):
    """From a list of (date, close) tuples (sorted ascending), return only
    the closes on or before as_of_date as a plain list of floats. Date is a
    naive datetime."""
    out = []
    for d, c in series_list:
        if d <= as_of_date:
            out.append(c)
        else:
            break
    return out


def _fetch_full_closes(symbol, years=10):
    """Fetch max history up to today, once. Returns list of (naive datetime, close).
    Cached per-symbol for the session.
    """
    cache_key = f"bt_closes:{symbol}:{years}"
    cached = cache.get(cache_key)
    if cached is not None:
        return cached
    try:
        period = f"{years}y" if years <= 10 else "max"
        hist = yf.Ticker(symbol).history(period=period)
        if hist is None or hist.empty:
            cache.put(cache_key, [], ttl=HIST_CACHE_TTL)
            return []
        closes = hist["Close"].dropna()
        try:
            closes.index = closes.index.tz_localize(None)
        except (TypeError, AttributeError):
            pass
        result = [(d.to_pydatetime(), float(c)) for d, c in closes.items()]
        cache.put(cache_key, result, ttl=HIST_CACHE_TTL)
        return result
    except Exception:
        cache.put(cache_key, [], ttl=HIST_CACHE_TTL)
        return []


def _fetch_macro_history(years=10):
    """Fetch VIX, 10Y, 3M Treasury series. Returns dict with lists of
    (date, value). Cached globally."""
    cache_key = f"bt_macro:{years}"
    cached = cache.get(cache_key)
    if cached is not None:
        return cached

    def _one(sym):
        try:
            period = f"{years}y" if years <= 10 else "max"
            h = yf.Ticker(sym).history(period=period)
            if h is None or h.empty:
                return []
            closes = h["Close"].dropna()
            try:
                closes.index = closes.index.tz_localize(None)
            except (TypeError, AttributeError):
                pass
            return [(d.to_pydatetime(), float(c)) for d, c in closes.items()]
        except Exception:
            return []

    with ThreadPoolExecutor(max_workers=3) as pool:
        fv = pool.submit(_one, "^VIX")
        f10 = pool.submit(_one, "^TNX")
        f3m = pool.submit(_one, "^IRX")
        result = {
            "vix": fv.result(timeout=30),
            "yield10y": f10.result(timeout=30),
            "yield3m": f3m.result(timeout=30),
        }
    cache.put(cache_key, result, ttl=HIST_CACHE_TTL)
    return result


def _value_at(series_list, as_of_date):
    """Most recent value on or before as_of_date from a (date, val) series."""
    last = None
    for d, v in series_list:
        if d > as_of_date:
            break
        last = v
    return last


def _macro_data_at(macro_hist, as_of_date):
    """Assemble the macro-signal dict that score_macro expects, point-in-time."""
    vix = _value_at(macro_hist["vix"], as_of_date) if macro_hist else None
    y10 = _value_at(macro_hist["yield10y"], as_of_date) if macro_hist else None
    y3m = _value_at(macro_hist["yield3m"], as_of_date) if macro_hist else None
    spread = (y10 - y3m) if (y10 is not None and y3m is not None) else None
    return {"vix": vix, "yield10y": y10, "yield3m": y3m, "yieldSpread": spread}


def _technical_data_at(closes):
    """Build the technical-signal dict that score_technical expects."""
    if not closes or len(closes) < 50:
        return None
    rsi = _compute_rsi(closes, 14)
    macd_signal = _compute_macd(closes)
    sma50 = sum(closes[-50:]) / 50
    current = closes[-1]
    above_50 = current > sma50
    sma200 = None
    above_200 = None
    golden = None
    if len(closes) >= 200:
        sma200 = sum(closes[-200:]) / 200
        above_200 = current > sma200
        golden = sma50 > sma200
    return {
        "rsi14": rsi,
        "macdSignal": macd_signal,
        "sma50": round(sma50, 2),
        "sma200": round(sma200, 2) if sma200 is not None else None,
        "aboveSma50": above_50,
        "aboveSma200": above_200,
        "goldenCross": golden,
        "price": round(current, 2),
    }


def _sector_cycles_at(ctx, as_of_date):
    """Replicate alpha._compute_sector_cycles but with closes truncated to
    `as_of_date`. Returns same shape: {sector_name: {phase, return_1y, ...}}.
    """
    results = {}
    for etf, sector in SECTOR_ETFS.items():
        series = ctx.sector_etf_closes.get(etf)
        if not series:
            continue
        closes = _daily_closes_up_to(series, as_of_date)
        if len(closes) < 252:
            continue
        current = closes[-1]

        def _ret(n):
            if len(closes) > n:
                old = closes[-n - 1]
                return round((current - old) / old * 100, 2) if old > 0 else None
            return None

        r_1y = _ret(252)
        r_6m = _ret(126)
        r_3m = _ret(63)
        if r_1y is not None and r_6m is not None:
            if r_1y > 15 and r_6m > 5:
                phase = "expansion"
            elif r_1y > 0 and r_6m <= 0:
                phase = "peak"
            elif r_1y < -5 and r_6m < 0:
                phase = "contraction"
            elif r_1y < 0 and r_6m > 0:
                phase = "recovery"
            else:
                phase = "neutral"
        else:
            phase = "unknown"
        results[sector] = {
            "sector": sector, "etf": etf, "price": round(current, 2),
            "return_1m": _ret(21), "return_3m": r_3m,
            "return_6m": r_6m, "return_1y": r_1y, "phase": phase,
        }

    # SPY-relative overlay.
    spy_series = ctx.sector_etf_closes.get("SPY") or []
    spy_closes = _daily_closes_up_to(spy_series, as_of_date)
    if len(spy_closes) >= 252:
        spy_1y = (spy_closes[-1] - spy_closes[-252]) / spy_closes[-252] * 100
        for sec in results.values():
            if sec.get("return_1y") is not None:
                sec["relative_to_spy"] = round(sec["return_1y"] - spy_1y, 2)
    return results


def _earnings_data_at(earnings_df, as_of_date):
    """Reshape a yfinance earnings DataFrame into the dict shape that
    score_earnings_surprise expects, filtered to events before as_of_date.

    yfinance's `earnings_history` only returns the 4 most recent quarters
    (all from the current year), which misses everything in the backtest
    window. `earnings_dates` goes back ~5 years with 'Surprise(%)' in
    percent form (matching the thresholds in score_earnings_surprise).
    """
    if earnings_df is None or earnings_df.empty:
        return None
    try:
        df = earnings_df.copy()
        try:
            df.index = df.index.tz_localize(None)
        except (TypeError, AttributeError):
            pass
        df = df[df.index <= as_of_date]
        # Different yfinance endpoints use different column names. earnings_dates
        # uses 'Surprise(%)' (percent); earnings_history uses 'surprisePercent'
        # (decimal, needs *100 scale-up). Handle both so we don't break if the
        # caller passes in either shape.
        if "Surprise(%)" in df.columns:
            surp_col = "Surprise(%)"
            scale = 1.0
        elif "surprisePercent" in df.columns:
            surp_col = "surprisePercent"
            scale = 100.0
        else:
            return None
        df = df.dropna(subset=[surp_col])
        if df.empty:
            return None
        # Most recent first.
        df = df.sort_index(ascending=False).head(8)
        history = []
        beats = 0
        surprises = []
        for _idx, row in df.iterrows():
            raw = row.get(surp_col)
            try:
                surp_f = float(raw) * scale
            except (TypeError, ValueError):
                continue
            history.append({"surprise": surp_f})
            surprises.append(surp_f)
            if surp_f > 0:
                beats += 1
        if not history:
            return None
        return {
            "beatRate": beats / len(history),
            "avgSurprise": sum(surprises) / len(surprises),
            "history": history,
        }
    except Exception:
        return None


def prefetch_backtest_context(symbols, as_of_start=None, as_of_end=None):
    """Fetch all historical data needed for a backtest in batch.

    Pulls once per symbol then slices per as_of_date inside the loop. This
    keeps the backtest tractable when running 40+ dates × 100+ symbols.

    as_of_start/as_of_end ignored for now (full-series fetch); reserved for
    future optimization where we could trim the fetched range.
    """
    ctx = BacktestContext()
    universe = list(set(symbols))

    def _fetch_one_closes(sym):
        return sym, _fetch_full_closes(sym)

    # Closes for every symbol in the backtest universe.
    with ThreadPoolExecutor(max_workers=8) as pool:
        futures = [pool.submit(_fetch_one_closes, s) for s in universe]
        for fut in as_completed(futures, timeout=600):
            try:
                sym, closes = fut.result(timeout=30)
                ctx.closes[sym] = closes
            except Exception:
                pass

    # Sector ETFs + SPY for industry cycle.
    sector_universe = list(SECTOR_ETFS.keys()) + ["SPY"]
    with ThreadPoolExecutor(max_workers=6) as pool:
        futures = [pool.submit(_fetch_one_closes, s) for s in sector_universe]
        for fut in as_completed(futures, timeout=300):
            try:
                sym, closes = fut.result(timeout=30)
                ctx.sector_etf_closes[sym] = closes
            except Exception:
                pass

    # Macro.
    ctx.macro_history = _fetch_macro_history()

    # Earnings history. earnings_dates goes back ~5 years with EPS surprises
    # in percent form. earnings_history (used in production) only returns the
    # 4 most recent quarters, all after the backtest window.
    def _fetch_one_earnings(sym):
        cache_key = f"bt_earnings:{sym}"
        cached = cache.get(cache_key)
        if cached is not None:
            return sym, cached
        try:
            df = yf.Ticker(sym).earnings_dates
            cache.put(cache_key, df, ttl=HIST_CACHE_TTL)
            return sym, df
        except Exception:
            cache.put(cache_key, None, ttl=HIST_CACHE_TTL)
            return sym, None

    with ThreadPoolExecutor(max_workers=6) as pool:
        futures = [pool.submit(_fetch_one_earnings, s) for s in universe]
        for fut in as_completed(futures, timeout=600):
            try:
                sym, df = fut.result(timeout=30)
                if df is not None:
                    ctx.earnings_raw[sym] = df
            except Exception:
                pass

    return ctx


def reconstruct_alpha_lite(symbol, as_of_date, ctx, sector_hint=None):
    """Reconstruct an Alpha-Lite score at `as_of_date` for one symbol.

    Returns dict with same shape as the production compute_alpha_score output:
      {alphaScore, subScores}
    where non-reconstructed factors are held at NEUTRAL_SCORE.

    `ctx` must be a BacktestContext (use prefetch_backtest_context to build).
    `sector_hint` is the GICS sector string (e.g., "Technology") for this
    symbol so industry_cycle can look up the matching ETF.
    """
    closes_series = ctx.closes.get(symbol, [])
    closes = _daily_closes_up_to(closes_series, as_of_date)

    sub_scores = {name: NEUTRAL_SCORE for name in NON_RECONSTRUCTED_FACTORS}

    # Momentum.
    price_hist = {"closes": closes} if closes else None
    sub_scores["momentum"] = _score_momentum(price_hist)

    # Technical.
    tech_data = _technical_data_at(closes)
    sub_scores["technical"] = score_technical(tech_data)

    # Industry cycle.
    sector_cycles = _sector_cycles_at(ctx, as_of_date)
    snapshot = {"sector": sector_hint or "Unknown"}
    sub_scores["industry_cycle"] = _score_industry_cycle(snapshot, sector_cycles)

    # Macro.
    macro_data = _macro_data_at(ctx.macro_history, as_of_date) if ctx.macro_history else None
    sub_scores["macro"] = score_macro(macro_data)

    # Earnings surprise.
    earn_data = _earnings_data_at(ctx.earnings_raw.get(symbol), as_of_date)
    sub_scores["earnings_surprise"] = score_earnings_surprise(earn_data)

    # Composite via the current factor weights (same formula as alpha.py:822).
    try:
        weights = _get_factor_weights()
    except Exception:
        weights = {}
    total_weight = sum(weights.values())
    if total_weight > 0:
        alpha = sum(sub_scores.get(f, NEUTRAL_SCORE) * weights.get(f, 0)
                    for f in weights) / total_weight
    else:
        alpha = sum(sub_scores.values()) / len(sub_scores)
    alpha = round(max(0, min(100, alpha)))

    return {"alphaScore": alpha, "subScores": sub_scores}
