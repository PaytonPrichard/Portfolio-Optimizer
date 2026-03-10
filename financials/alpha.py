"""Alpha Score engine — proprietary composite stock scoring system.

Combines value, quality, momentum, analyst sentiment, growth, and
industry cycle signals into a single 0-100 score. Backed by a SQLite
database that stores historical metric snapshots and forward returns
for backtesting and continuous learning.
"""

import math
import os
import sqlite3
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta

import yfinance as yf

from . import cache
from .portfolio_widgets import SECTOR_ETFS
from .alpha_signals import (
    fetch_all_signals,
    score_insider,
    score_earnings_surprise,
    score_buyback,
    score_analyst_momentum,
    score_institutional,
    score_technical,
    score_macro,
)

_PROJECT_ROOT = os.path.dirname(os.path.dirname(__file__))
_DEFAULT_DB_DIR = os.path.join(_PROJECT_ROOT, "data")
# On read-only filesystems (e.g. Vercel), fall back to /tmp
if os.access(_DEFAULT_DB_DIR, os.W_OK) or not os.path.exists(_DEFAULT_DB_DIR):
    DB_PATH = os.path.join(_DEFAULT_DB_DIR, "alpha.db")
else:
    DB_PATH = os.path.join("/tmp", "alpha.db")
ALPHA_CACHE_TTL = 600  # 10 min
_db_lock = threading.Lock()


# ── Database setup ────────────────────────────────────────────────────

def _get_db():
    """Get a thread-local SQLite connection."""
    conn = sqlite3.connect(DB_PATH, timeout=10)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def init_db():
    """Create tables if they don't exist."""
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    conn = _get_db()
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS metric_snapshots (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            symbol TEXT NOT NULL,
            snapshot_date TEXT NOT NULL,
            sector TEXT,
            industry TEXT,
            market_cap REAL,
            price REAL,
            trailing_pe REAL,
            forward_pe REAL,
            price_to_book REAL,
            ev_to_ebitda REAL,
            peg_ratio REAL,
            gross_margins REAL,
            operating_margins REAL,
            profit_margins REAL,
            roe REAL,
            roa REAL,
            debt_to_equity REAL,
            current_ratio REAL,
            revenue_growth REAL,
            earnings_growth REAL,
            fcf_yield REAL,
            dividend_yield REAL,
            beta REAL,
            analyst_rating TEXT,
            analyst_target REAL,
            analyst_count INTEGER,
            shares_outstanding REAL,
            -- Forward returns (filled in later by backfill jobs)
            fwd_return_3m REAL,
            fwd_return_6m REAL,
            fwd_return_1y REAL,
            fwd_return_3y REAL,
            fwd_return_5y REAL,
            UNIQUE(symbol, snapshot_date)
        );

        CREATE TABLE IF NOT EXISTS sector_cycles (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            sector TEXT NOT NULL,
            cycle_date TEXT NOT NULL,
            etf_symbol TEXT,
            price REAL,
            return_1m REAL,
            return_3m REAL,
            return_6m REAL,
            return_1y REAL,
            relative_to_spy_1y REAL,
            cycle_phase TEXT,
            UNIQUE(sector, cycle_date)
        );

        CREATE TABLE IF NOT EXISTS factor_weights (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            factor_name TEXT NOT NULL UNIQUE,
            weight REAL NOT NULL DEFAULT 1.0,
            last_updated TEXT
        );

        CREATE INDEX IF NOT EXISTS idx_snapshots_symbol ON metric_snapshots(symbol);
        CREATE INDEX IF NOT EXISTS idx_snapshots_date ON metric_snapshots(snapshot_date);
        CREATE INDEX IF NOT EXISTS idx_snapshots_sector ON metric_snapshots(sector);
        CREATE INDEX IF NOT EXISTS idx_sector_cycles ON sector_cycles(sector, cycle_date);
    """)

    # Migrate: add new signal columns (safe if already exist)
    _new_cols = [
        ("insider_buy_count", "INTEGER"),
        ("insider_sell_count", "INTEGER"),
        ("earnings_beat_rate", "REAL"),
        ("earnings_avg_surprise", "REAL"),
        ("shares_change_1y", "REAL"),
        ("upgrades_6m", "INTEGER"),
        ("downgrades_6m", "INTEGER"),
        ("institutional_pct", "REAL"),
        ("rsi_14", "REAL"),
        ("macd_signal", "TEXT"),
    ]
    for col_name, col_type in _new_cols:
        try:
            conn.execute(f"ALTER TABLE metric_snapshots ADD COLUMN {col_name} {col_type}")
        except Exception:
            pass  # column already exists

    # Upsert factor weights — ensures new factors are added to existing DBs
    defaults = [
        ("value", 0.10),
        ("quality", 0.10),
        ("momentum", 0.08),
        ("analyst", 0.05),
        ("growth", 0.08),
        ("industry_cycle", 0.07),
        ("insider", 0.12),
        ("earnings_surprise", 0.08),
        ("buyback", 0.07),
        ("analyst_momentum", 0.05),
        ("institutional", 0.07),
        ("technical", 0.06),
        ("macro", 0.07),
    ]
    now = datetime.now().isoformat()
    for name, weight in defaults:
        conn.execute(
            "INSERT OR IGNORE INTO factor_weights (factor_name, weight, last_updated) VALUES (?, ?, ?)",
            (name, weight, now),
        )
    conn.commit()
    conn.close()


# Initialize on import — wrapped to never crash the app on startup
try:
    init_db()
except Exception:
    pass  # DB init failure should not prevent the app from starting


# ── Data collection ──────────────────────────────────────────────────

def _safe_float(val):
    if val is None:
        return None
    try:
        f = float(val)
        return None if (f != f or math.isinf(f)) else f
    except (TypeError, ValueError):
        return None


def collect_snapshot(symbol):
    """Collect and store a metric snapshot for a symbol. Returns the snapshot dict."""
    cache_key = f"alpha_snapshot:{symbol}"
    cached = cache.get(cache_key)
    if cached is not None:
        return cached

    try:
        ticker = yf.Ticker(symbol)
        info = ticker.info or {}
    except Exception:
        return None

    price = _safe_float(info.get("currentPrice") or info.get("regularMarketPrice"))
    if not price:
        return None

    mcap = _safe_float(info.get("marketCap"))
    fcf = _safe_float(info.get("freeCashflow"))
    fcf_yield = (fcf / mcap) if fcf and mcap and mcap > 0 else None

    # Extract domain for logo favicon
    _website = info.get("website") or ""
    _logo_domain = ""
    if _website:
        try:
            from urllib.parse import urlparse
            _logo_domain = (urlparse(_website).netloc or "").replace("www.", "")
        except Exception:
            pass

    snapshot = {
        "symbol": symbol.upper(),
        "company_name": info.get("longName") or info.get("shortName") or symbol.upper(),
        "logo_domain": _logo_domain,
        "snapshot_date": datetime.now().strftime("%Y-%m-%d"),
        "sector": info.get("sector") or "Unknown",
        "industry": info.get("industry") or "Unknown",
        "market_cap": mcap,
        "price": price,
        "trailing_pe": _safe_float(info.get("trailingPE")),
        "forward_pe": _safe_float(info.get("forwardPE")),
        "price_to_book": _safe_float(info.get("priceToBook")),
        "ev_to_ebitda": _safe_float(info.get("enterpriseToEbitda")),
        "peg_ratio": _safe_float(info.get("trailingPegRatio") or info.get("pegRatio")),
        "gross_margins": _safe_float(info.get("grossMargins")),
        "operating_margins": _safe_float(info.get("operatingMargins")),
        "profit_margins": _safe_float(info.get("profitMargins")),
        "roe": _safe_float(info.get("returnOnEquity")),
        "roa": _safe_float(info.get("returnOnAssets")),
        "debt_to_equity": _safe_float(info.get("debtToEquity")),
        "current_ratio": _safe_float(info.get("currentRatio")),
        "revenue_growth": _safe_float(info.get("revenueGrowth")),
        "earnings_growth": _safe_float(info.get("earningsGrowth")),
        "fcf_yield": _safe_float(fcf_yield),
        "dividend_yield": _safe_float(info.get("dividendYield")),
        "beta": _safe_float(info.get("beta")),
        "analyst_rating": info.get("recommendationKey") or "N/A",
        "analyst_target": _safe_float(info.get("targetMeanPrice")),
        "analyst_count": info.get("numberOfAnalystOpinions"),
        "shares_outstanding": _safe_float(info.get("sharesOutstanding")),
    }

    # Fetch new signal data and merge summary metrics into snapshot
    try:
        signals = fetch_all_signals(symbol)
        insider = signals.get("insider") or {}
        earnings = signals.get("earnings") or {}
        buyback = signals.get("buyback") or {}
        analyst_m = signals.get("analyst_momentum") or {}
        inst = signals.get("institutional") or {}
        tech = signals.get("technical") or {}

        snapshot["insider_buy_count"] = insider.get("buyCount")
        snapshot["insider_sell_count"] = insider.get("sellCount")
        snapshot["earnings_beat_rate"] = earnings.get("beatRate")
        snapshot["earnings_avg_surprise"] = earnings.get("avgSurprise")
        snapshot["shares_change_1y"] = buyback.get("sharesChange1y")
        snapshot["upgrades_6m"] = analyst_m.get("upgrades6m")
        snapshot["downgrades_6m"] = analyst_m.get("downgrades6m")
        snapshot["institutional_pct"] = inst.get("institutionalPct")
        snapshot["rsi_14"] = tech.get("rsi14")
        snapshot["macd_signal"] = tech.get("macdSignal")
    except Exception:
        pass  # signal fetch failure shouldn't block snapshot storage

    # Store in database (exclude non-DB fields)
    _non_db_fields = {"company_name", "logo_domain"}
    with _db_lock:
        conn = _get_db()
        try:
            cols = [k for k in snapshot if k not in _non_db_fields]
            placeholders = ", ".join(["?"] * len(cols))
            col_names = ", ".join(cols)
            conn.execute(
                f"INSERT OR REPLACE INTO metric_snapshots ({col_names}) VALUES ({placeholders})",
                [snapshot[k] for k in cols],
            )
            conn.commit()
        finally:
            conn.close()

    cache.put(cache_key, snapshot, ttl=ALPHA_CACHE_TTL)
    return snapshot


def _fetch_price_history(symbol, years=5):
    """Get price history for momentum and historical P/E calculation."""
    cache_key = f"alpha_hist:{symbol}:{years}"
    cached = cache.get(cache_key)
    if cached is not None:
        return cached

    try:
        period = f"{years}y" if years <= 10 else "max"
        hist = yf.Ticker(symbol).history(period=period)
        if hist is None or hist.empty:
            return None
        closes = hist["Close"].dropna()
        result = {
            "dates": [d.strftime("%Y-%m-%d") for d in closes.index],
            "closes": [float(c) for c in closes.values],
        }
        cache.put(cache_key, result, ttl=ALPHA_CACHE_TTL)
        return result
    except Exception:
        return None


# ── Sector cycle analysis ────────────────────────────────────────────

def _compute_sector_cycles():
    """Analyze current sector cycle positions using ETF history."""
    cache_key = "alpha_sector_cycles"
    cached = cache.get(cache_key)
    if cached is not None:
        return cached

    results = {}

    def _analyze_sector(etf_sym, sector_name):
        try:
            hist = yf.Ticker(etf_sym).history(period="5y")
            if hist is None or hist.empty or len(hist) < 252:
                return None
            closes = hist["Close"].dropna()
            current = float(closes.iloc[-1])

            def _ret(n):
                if len(closes) > n:
                    old = float(closes.iloc[-n - 1])
                    return round((current - old) / old * 100, 2) if old > 0 else None
                return None

            # 1-year rolling return
            r_1y = _ret(252)

            # Determine cycle phase from 6m and 1y momentum
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

            return {
                "sector": sector_name,
                "etf": etf_sym,
                "price": round(current, 2),
                "return_1m": _ret(21),
                "return_3m": r_3m,
                "return_6m": r_6m,
                "return_1y": r_1y,
                "phase": phase,
            }
        except Exception:
            return None

    with ThreadPoolExecutor(max_workers=6) as pool:
        futures = {pool.submit(_analyze_sector, etf, sec): sec
                   for etf, sec in SECTOR_ETFS.items()}
        for future in as_completed(futures, timeout=30):
            sec = futures[future]
            try:
                result = future.result(timeout=10)
                if result:
                    results[sec] = result
            except Exception:
                pass

    # Also get SPY for relative comparison
    try:
        spy_hist = yf.Ticker("SPY").history(period="1y")
        if spy_hist is not None and not spy_hist.empty and len(spy_hist) > 252:
            spy_close = spy_hist["Close"].dropna()
            spy_1y = (float(spy_close.iloc[-1]) - float(spy_close.iloc[0])) / float(spy_close.iloc[0]) * 100
            for sec in results:
                r = results[sec].get("return_1y")
                if r is not None:
                    results[sec]["relative_to_spy"] = round(r - spy_1y, 2)
    except Exception:
        pass

    cache.put(cache_key, results, ttl=1800)
    return results


# ── Historical percentile ranks ──────────────────────────────────────

def _percentile_rank(value, history_values):
    """Compute percentile rank (0-100) of value within history."""
    if not history_values or value is None:
        return None
    below = sum(1 for v in history_values if v < value)
    return round(below / len(history_values) * 100, 1)


def _compute_historical_context(symbol, snapshot, price_history):
    """Compute where current metrics sit relative to history."""
    context = {}

    if not price_history:
        return context

    closes = price_history["closes"]
    current = closes[-1] if closes else None

    if current and len(closes) > 252:
        # 52-week range percentile
        year_prices = closes[-252:]
        low_52 = min(year_prices)
        high_52 = max(year_prices)
        if high_52 > low_52:
            context["range52wPct"] = round((current - low_52) / (high_52 - low_52) * 100, 1)

        # 5-year range percentile
        low_5y = min(closes)
        high_5y = max(closes)
        if high_5y > low_5y:
            context["range5yPct"] = round((current - low_5y) / (high_5y - low_5y) * 100, 1)

    # Historical P/E context from database
    with _db_lock:
        conn = _get_db()
        try:
            rows = conn.execute(
                "SELECT trailing_pe FROM metric_snapshots WHERE symbol = ? AND trailing_pe IS NOT NULL ORDER BY snapshot_date",
                (symbol,)
            ).fetchall()
            if rows and snapshot.get("trailing_pe"):
                pe_history = [r["trailing_pe"] for r in rows]
                context["pePctRank"] = _percentile_rank(snapshot["trailing_pe"], pe_history)
                context["peHistory"] = pe_history[-20:]  # last 20 snapshots

            # Industry P/E comparison
            industry = snapshot.get("industry")
            if industry and industry != "Unknown":
                peer_rows = conn.execute(
                    "SELECT trailing_pe FROM metric_snapshots WHERE industry = ? AND trailing_pe IS NOT NULL AND snapshot_date >= date('now', '-1 year')",
                    (industry,)
                ).fetchall()
                if peer_rows and snapshot.get("trailing_pe"):
                    peer_pes = [r["trailing_pe"] for r in peer_rows]
                    context["peIndustryPctRank"] = _percentile_rank(snapshot["trailing_pe"], peer_pes)
                    context["peIndustryMedian"] = round(sorted(peer_pes)[len(peer_pes) // 2], 1)
        finally:
            conn.close()

    return context


# ── Sub-scores (each 0-100) ──────────────────────────────────────────

def _score_value(snapshot, context):
    """Value score: lower valuations = higher score."""
    scores = []

    pe = snapshot.get("trailing_pe")
    if pe and pe > 0:
        # Absolute P/E scoring
        if pe < 12:
            scores.append(90)
        elif pe < 18:
            scores.append(70)
        elif pe < 25:
            scores.append(50)
        elif pe < 35:
            scores.append(30)
        else:
            scores.append(10)

        # Industry-relative P/E
        ind_rank = context.get("peIndustryPctRank")
        if ind_rank is not None:
            scores.append(max(0, 100 - ind_rank))

    pb = snapshot.get("price_to_book")
    if pb and pb > 0:
        if pb < 1.5:
            scores.append(85)
        elif pb < 3:
            scores.append(65)
        elif pb < 6:
            scores.append(40)
        else:
            scores.append(15)

    peg = snapshot.get("peg_ratio")
    if peg and peg > 0:
        if peg < 1:
            scores.append(90)
        elif peg < 1.5:
            scores.append(70)
        elif peg < 2.5:
            scores.append(45)
        else:
            scores.append(15)

    ev = snapshot.get("ev_to_ebitda")
    if ev and ev > 0:
        if ev < 8:
            scores.append(85)
        elif ev < 14:
            scores.append(65)
        elif ev < 22:
            scores.append(40)
        else:
            scores.append(15)

    return round(sum(scores) / len(scores)) if scores else 50


def _score_quality(snapshot):
    """Quality score: profitability + balance sheet health."""
    scores = []

    roe = snapshot.get("roe")
    if roe is not None:
        if roe > 0.30:
            scores.append(95)
        elif roe > 0.20:
            scores.append(80)
        elif roe > 0.12:
            scores.append(60)
        elif roe > 0.05:
            scores.append(40)
        else:
            scores.append(15)

    gm = snapshot.get("gross_margins")
    if gm is not None:
        if gm > 0.60:
            scores.append(90)
        elif gm > 0.40:
            scores.append(70)
        elif gm > 0.25:
            scores.append(50)
        else:
            scores.append(25)

    om = snapshot.get("operating_margins")
    if om is not None:
        if om > 0.30:
            scores.append(90)
        elif om > 0.18:
            scores.append(70)
        elif om > 0.08:
            scores.append(50)
        else:
            scores.append(25)

    dte = snapshot.get("debt_to_equity")
    if dte is not None:
        if dte < 20:
            scores.append(90)
        elif dte < 60:
            scores.append(70)
        elif dte < 120:
            scores.append(45)
        else:
            scores.append(15)

    cr = snapshot.get("current_ratio")
    if cr is not None:
        if cr > 2.5:
            scores.append(85)
        elif cr > 1.5:
            scores.append(70)
        elif cr > 1.0:
            scores.append(45)
        else:
            scores.append(20)

    fcf = snapshot.get("fcf_yield")
    if fcf is not None:
        if fcf > 0.08:
            scores.append(90)
        elif fcf > 0.05:
            scores.append(75)
        elif fcf > 0.02:
            scores.append(55)
        elif fcf > 0:
            scores.append(35)
        else:
            scores.append(10)

    return round(sum(scores) / len(scores)) if scores else 50


def _score_momentum(price_history):
    """Momentum score: recent price performance at 3m/6m/12m horizons."""
    if not price_history:
        return 50

    closes = price_history["closes"]
    if len(closes) < 63:
        return 50

    current = closes[-1]
    scores = []

    for n_days, weight in [(63, 1.0), (126, 1.0), (252, 1.0)]:
        if len(closes) > n_days and closes[-n_days - 1] > 0:
            ret = (current - closes[-n_days - 1]) / closes[-n_days - 1] * 100
            # Score: +30% = 90, +15% = 75, 0% = 50, -15% = 25, -30% = 10
            score = max(5, min(95, 50 + ret * 1.5))
            scores.append(score)

    # Trend consistency bonus: is 3m > 6m > 12m momentum? (acceleration)
    if len(scores) == 3:
        if scores[0] > scores[1] > scores[2]:
            scores.append(min(95, max(scores) + 10))  # accelerating
        elif scores[0] < scores[1] < scores[2]:
            scores.append(max(5, min(scores) - 10))  # decelerating

    return round(sum(scores) / len(scores)) if scores else 50


def _score_analyst(snapshot):
    """Analyst sentiment score: ratings + target upside + coverage."""
    scores = []

    rating = (snapshot.get("analyst_rating") or "").lower().replace(" ", "_")
    rating_map = {"strong_buy": 95, "buy": 80, "hold": 50, "sell": 20, "strong_sell": 5}
    if rating in rating_map:
        scores.append(rating_map[rating])

    target = snapshot.get("analyst_target")
    price = snapshot.get("price")
    if target and price and price > 0:
        upside = (target - price) / price * 100
        # +30% upside = 90, +15% = 75, 0% = 50, -15% = 25
        score = max(5, min(95, 50 + upside * 1.5))
        scores.append(score)

    count = snapshot.get("analyst_count") or 0
    if count >= 20:
        scores.append(80)
    elif count >= 10:
        scores.append(65)
    elif count >= 5:
        scores.append(50)
    elif count >= 1:
        scores.append(35)

    return round(sum(scores) / len(scores)) if scores else 50


def _score_growth(snapshot):
    """Growth score: revenue and earnings growth rates."""
    scores = []

    rg = snapshot.get("revenue_growth")
    if rg is not None:
        if rg > 0.30:
            scores.append(95)
        elif rg > 0.15:
            scores.append(75)
        elif rg > 0.05:
            scores.append(55)
        elif rg > 0:
            scores.append(40)
        else:
            scores.append(15)

    eg = snapshot.get("earnings_growth")
    if eg is not None:
        if eg > 0.30:
            scores.append(95)
        elif eg > 0.15:
            scores.append(75)
        elif eg > 0.05:
            scores.append(55)
        elif eg > 0:
            scores.append(40)
        else:
            scores.append(15)

    # PEG bonus (growth at reasonable price)
    peg = snapshot.get("peg_ratio")
    if peg and 0 < peg < 1.5:
        scores.append(85)
    elif peg and peg < 2.5:
        scores.append(55)

    return round(sum(scores) / len(scores)) if scores else 50


def _score_industry_cycle(snapshot, sector_cycles):
    """Industry cycle score: favor sectors in recovery/early expansion."""
    sector = snapshot.get("sector")
    if not sector or sector == "Unknown" or not sector_cycles:
        return 50

    cycle = sector_cycles.get(sector)
    if not cycle:
        return 50

    phase = cycle.get("phase", "neutral")
    phase_scores = {
        "recovery": 85,      # Best time to buy
        "expansion": 70,     # Still good
        "neutral": 50,
        "peak": 30,          # Late stage, risky
        "contraction": 40,   # Bargain hunting opportunity
        "unknown": 50,
    }
    score = phase_scores.get(phase, 50)

    # Boost/penalize based on relative performance
    rel = cycle.get("relative_to_spy")
    if rel is not None:
        if rel > 10:
            score = min(95, score + 10)
        elif rel < -10:
            score = max(5, score - 10)

    return score


# ── Composite Alpha Score ────────────────────────────────────────────

def _get_factor_weights():
    """Load factor weights from database."""
    with _db_lock:
        conn = _get_db()
        try:
            rows = conn.execute("SELECT factor_name, weight FROM factor_weights").fetchall()
            return {r["factor_name"]: r["weight"] for r in rows}
        finally:
            conn.close()


def compute_alpha_score(symbol):
    """Compute the full Alpha Score for a single stock.

    Returns dict with: alphaScore (0-100), subScores, snapshot,
    historicalContext, sectorCycle, breakdown
    """
    symbol = symbol.upper().strip()

    # Collect current snapshot
    snapshot = collect_snapshot(symbol)
    if not snapshot:
        return None

    # Fetch price history
    price_history = _fetch_price_history(symbol, years=5)

    # Historical context
    context = _compute_historical_context(symbol, snapshot, price_history)

    # Sector cycles
    sector_cycles = _compute_sector_cycles()

    # Fetch new alpha signals (parallel)
    signals = fetch_all_signals(symbol, price_history)

    # Compute sub-scores — original 6
    value = _score_value(snapshot, context)
    quality = _score_quality(snapshot)
    momentum = _score_momentum(price_history)
    analyst = _score_analyst(snapshot)
    growth = _score_growth(snapshot)
    industry_cycle = _score_industry_cycle(snapshot, sector_cycles)

    # New sub-scores from alpha signals
    insider = score_insider(signals.get("insider"))
    earnings_surprise = score_earnings_surprise(signals.get("earnings"))
    buyback = score_buyback(signals.get("buyback"))
    analyst_mom = score_analyst_momentum(signals.get("analyst_momentum"))
    institutional = score_institutional(signals.get("institutional"))
    technical = score_technical(signals.get("technical"))
    macro = score_macro(signals.get("macro"), snapshot)

    sub_scores = {
        "value": value,
        "quality": quality,
        "momentum": momentum,
        "analyst": analyst,
        "growth": growth,
        "industry_cycle": industry_cycle,
        "insider": insider,
        "earnings_surprise": earnings_surprise,
        "buyback": buyback,
        "analyst_momentum": analyst_mom,
        "institutional": institutional,
        "technical": technical,
        "macro": macro,
    }

    # Weighted composite
    weights = _get_factor_weights()
    total_weight = sum(weights.values())
    if total_weight > 0:
        alpha = sum(sub_scores.get(f, 50) * weights.get(f, 0)
                    for f in weights) / total_weight
    else:
        alpha = sum(sub_scores.values()) / len(sub_scores)

    alpha = round(max(0, min(100, alpha)))

    # Determine conviction level
    if alpha >= 75:
        conviction = "Strong Buy"
    elif alpha >= 60:
        conviction = "Buy"
    elif alpha >= 45:
        conviction = "Hold"
    elif alpha >= 30:
        conviction = "Underweight"
    else:
        conviction = "Avoid"

    # Key insights (top 5 notable signals)
    insights = _generate_insights(snapshot, sub_scores, context, sector_cycles, signals)

    # Sector cycle data for this stock
    sector = snapshot.get("sector", "Unknown")
    cycle_data = sector_cycles.get(sector, {})

    _ld = snapshot.get("logo_domain", "")
    logo_url = f"https://www.google.com/s2/favicons?domain={_ld}&sz=128" if _ld else ""

    return {
        "symbol": symbol,
        "companyName": snapshot.get("company_name", symbol),
        "logoUrl": logo_url,
        "alphaScore": alpha,
        "conviction": conviction,
        "subScores": sub_scores,
        "weights": weights,
        "snapshot": snapshot,
        "historicalContext": context,
        "sectorCycle": cycle_data,
        "insights": insights,
        "price": snapshot.get("price"),
        "sector": sector,
        "industry": snapshot.get("industry", "Unknown"),
        "marketCap": snapshot.get("market_cap"),
        # New signal data for display
        "insiderData": signals.get("insider"),
        "earningsData": signals.get("earnings"),
        "buybackData": signals.get("buyback"),
        "analystMomentum": signals.get("analyst_momentum"),
        "institutionalData": signals.get("institutional"),
        "technicalData": signals.get("technical"),
        "macroData": signals.get("macro"),
    }


def _generate_insights(snapshot, sub_scores, context, sector_cycles, signals=None):
    """Generate human-readable key insights."""
    insights = []
    signals = signals or {}

    # Value insight
    pe = snapshot.get("trailing_pe")
    ind_median = context.get("peIndustryMedian")
    if pe and ind_median:
        if pe < ind_median * 0.8:
            insights.append({
                "type": "positive",
                "text": f"Trades at {pe:.1f}x earnings vs industry median of {ind_median:.1f}x — potential value opportunity",
            })
        elif pe > ind_median * 1.3:
            insights.append({
                "type": "caution",
                "text": f"Premium valuation at {pe:.1f}x earnings vs industry median of {ind_median:.1f}x",
            })

    # Quality insight
    roe = snapshot.get("roe")
    if roe and roe > 0.25:
        insights.append({
            "type": "positive",
            "text": f"Exceptional return on equity at {roe*100:.1f}% — strong competitive position",
        })
    elif roe and roe < 0.05:
        insights.append({
            "type": "caution",
            "text": f"Weak return on equity at {roe*100:.1f}% — low profitability",
        })

    # Momentum insight
    if sub_scores.get("momentum", 50) >= 75:
        insights.append({
            "type": "positive",
            "text": "Strong price momentum across multiple timeframes",
        })
    elif sub_scores.get("momentum", 50) <= 25:
        insights.append({
            "type": "caution",
            "text": "Weak price momentum — downtrend across timeframes",
        })

    # Growth insight
    rg = snapshot.get("revenue_growth")
    eg = snapshot.get("earnings_growth")
    if rg and eg and rg > 0.15 and eg > 0.15:
        insights.append({
            "type": "positive",
            "text": f"Double-digit growth: revenue +{rg*100:.0f}%, earnings +{eg*100:.0f}%",
        })

    # Analyst insight
    rating = (snapshot.get("analyst_rating") or "").lower()
    count = snapshot.get("analyst_count") or 0
    target = snapshot.get("analyst_target")
    price = snapshot.get("price")
    if rating in ("buy", "strong_buy") and count >= 10 and target and price:
        upside = (target - price) / price * 100
        if upside > 10:
            insights.append({
                "type": "positive",
                "text": f"{count} analysts rate {rating.upper()} with {upside:.0f}% upside to ${target:.0f} target",
            })

    # Industry cycle insight
    sector = snapshot.get("sector")
    if sector and sector_cycles:
        cycle = sector_cycles.get(sector, {})
        phase = cycle.get("phase")
        if phase == "recovery":
            insights.append({
                "type": "positive",
                "text": f"{sector} sector is in recovery phase — historically favorable entry point",
            })
        elif phase == "peak":
            insights.append({
                "type": "caution",
                "text": f"{sector} sector appears near peak — late-cycle risk",
            })

    # FCF yield insight
    fcf_y = snapshot.get("fcf_yield")
    if fcf_y and fcf_y > 0.06:
        insights.append({
            "type": "positive",
            "text": f"Strong free cash flow yield of {fcf_y*100:.1f}% — cash-generative business",
        })

    # Debt insight
    dte = snapshot.get("debt_to_equity")
    if dte and dte > 200:
        insights.append({
            "type": "caution",
            "text": f"High leverage: debt-to-equity ratio of {dte:.0f}",
        })

    # Insider trading insight
    ins_data = signals.get("insider")
    if ins_data:
        buys = ins_data.get("buyCount", 0)
        if buys >= 3:
            insights.append({
                "type": "positive",
                "text": f"Cluster insider buying: {buys} purchases in 6 months — one of the strongest alpha signals",
            })
        elif buys >= 1:
            insights.append({
                "type": "positive",
                "text": f"Insider buying detected: {buys} open-market purchase(s) in 6 months",
            })
        sells = ins_data.get("sellCount", 0)
        if sells >= 5 and buys == 0:
            insights.append({
                "type": "caution",
                "text": f"Heavy insider selling: {sells} sales with no insider buying in 6 months",
            })

    # Earnings surprise insight
    earn_data = signals.get("earnings")
    if earn_data:
        beat_rate = earn_data.get("beatRate", 0)
        avg_surp = earn_data.get("avgSurprise", 0)
        if beat_rate >= 1.0 and avg_surp > 3:
            insights.append({
                "type": "positive",
                "text": f"Perfect earnings record: beat estimates every quarter (avg +{avg_surp:.1f}%)",
            })
        elif beat_rate <= 0.25:
            insights.append({
                "type": "caution",
                "text": f"Weak earnings execution: missed estimates {earn_data.get('beatsOf', '')}",
            })

    # Buyback insight
    bb_data = signals.get("buyback")
    if bb_data:
        change = bb_data.get("sharesChange1y")
        if change is not None and change < -3:
            insights.append({
                "type": "positive",
                "text": f"Aggressive buyback: shares outstanding reduced {abs(change):.1f}% in 1 year",
            })
        elif change is not None and change > 3:
            insights.append({
                "type": "caution",
                "text": f"Share dilution: outstanding shares increased {change:.1f}% in 1 year",
            })

    # Technical insight
    tech_data = signals.get("technical")
    if tech_data:
        macd = tech_data.get("macdSignal", "")
        golden = tech_data.get("goldenCross")
        if macd == "bullish_cross":
            insights.append({
                "type": "positive",
                "text": "MACD bullish crossover — momentum shifting positive",
            })
        elif macd == "bearish_cross":
            insights.append({
                "type": "caution",
                "text": "MACD bearish crossover — momentum shifting negative",
            })
        if golden is True and tech_data.get("aboveSma50") and tech_data.get("aboveSma200"):
            insights.append({
                "type": "positive",
                "text": "Golden cross (50-day > 200-day SMA) with price above both — strong uptrend",
            })
        elif golden is False and not tech_data.get("aboveSma50") and not tech_data.get("aboveSma200"):
            insights.append({
                "type": "caution",
                "text": "Death cross with price below both moving averages — sustained downtrend",
            })

    # Macro insight
    macro_data = signals.get("macro")
    if macro_data:
        ys = macro_data.get("yieldSignal")
        if ys == "inverted":
            insights.append({
                "type": "caution",
                "text": "Yield curve inverted — historically precedes recessions within 12-18 months",
            })

    return insights[:7]  # Top 7 insights


# ── Batch scoring (for portfolio widget) ─────────────────────────────

def compute_alpha_scores_batch(symbols):
    """Compute Alpha Scores for multiple symbols in parallel."""
    results = {}

    with ThreadPoolExecutor(max_workers=6) as pool:
        futures = {pool.submit(compute_alpha_score, sym): sym for sym in symbols}
        for future in as_completed(futures, timeout=60):
            sym = futures[future]
            try:
                result = future.result(timeout=20)
                if result:
                    results[sym] = result
            except Exception:
                pass

    return results


# ── Database stats and history ───────────────────────────────────────

def get_db_stats():
    """Return database statistics."""
    with _db_lock:
        conn = _get_db()
        try:
            snap_count = conn.execute("SELECT COUNT(*) FROM metric_snapshots").fetchone()[0]
            unique_symbols = conn.execute("SELECT COUNT(DISTINCT symbol) FROM metric_snapshots").fetchone()[0]
            oldest = conn.execute("SELECT MIN(snapshot_date) FROM metric_snapshots").fetchone()[0]
            newest = conn.execute("SELECT MAX(snapshot_date) FROM metric_snapshots").fetchone()[0]
            sector_count = conn.execute("SELECT COUNT(*) FROM sector_cycles").fetchone()[0]
            return {
                "totalSnapshots": snap_count,
                "uniqueSymbols": unique_symbols,
                "oldestSnapshot": oldest,
                "newestSnapshot": newest,
                "sectorCycleRecords": sector_count,
            }
        finally:
            conn.close()


def get_symbol_history(symbol, limit=50):
    """Get historical snapshots for a symbol."""
    with _db_lock:
        conn = _get_db()
        try:
            rows = conn.execute(
                "SELECT * FROM metric_snapshots WHERE symbol = ? ORDER BY snapshot_date DESC LIMIT ?",
                (symbol.upper(), limit),
            ).fetchall()
            return [dict(r) for r in rows]
        finally:
            conn.close()


def backfill_forward_returns():
    """Update forward returns for historical snapshots where price data is available.

    This should be run periodically (e.g., weekly) to keep the database current.
    Returns count of updated rows.
    """
    updated = 0
    with _db_lock:
        conn = _get_db()
        try:
            # Find snapshots that need forward return filling
            rows = conn.execute("""
                SELECT id, symbol, snapshot_date, price
                FROM metric_snapshots
                WHERE fwd_return_1y IS NULL AND price IS NOT NULL
                AND snapshot_date <= date('now', '-90 days')
            """).fetchall()

            for row in rows:
                sym = row["symbol"]
                snap_date = row["snapshot_date"]
                snap_price = row["price"]
                if not snap_price or snap_price <= 0:
                    continue

                try:
                    hist = _fetch_price_history(sym, years=6)
                    if not hist:
                        continue

                    dates = hist["dates"]
                    closes = hist["closes"]

                    # Find price at various forward intervals
                    updates = {}
                    for label, days in [("fwd_return_3m", 63), ("fwd_return_6m", 126),
                                        ("fwd_return_1y", 252), ("fwd_return_3y", 756),
                                        ("fwd_return_5y", 1260)]:
                        target_date = (datetime.strptime(snap_date, "%Y-%m-%d") +
                                       timedelta(days=days)).strftime("%Y-%m-%d")
                        # Find closest date in history
                        for i, d in enumerate(dates):
                            if d >= target_date:
                                fwd_price = closes[i]
                                updates[label] = round((fwd_price - snap_price) / snap_price * 100, 2)
                                break

                    if updates:
                        set_clause = ", ".join(f"{k} = ?" for k in updates)
                        conn.execute(
                            f"UPDATE metric_snapshots SET {set_clause} WHERE id = ?",
                            list(updates.values()) + [row["id"]],
                        )
                        updated += 1
                except Exception:
                    continue

            conn.commit()
        finally:
            conn.close()

    return updated


# Dow Jones Industrial Average 30 components (as of early 2026)
_SOTD_CURATED = [
    "AMGN", "AMZN", "AAPL", "BA", "CAT", "CSCO", "CVX", "DIS",
    "GS", "HD", "HON", "IBM", "JNJ", "JPM", "KO", "MCD",
    "MMM", "MRK", "MSFT", "NKE", "NVDA", "PG", "CRM", "SHW",
    "TRV", "UNH", "V", "VZ", "WMT", "WBA",
]


def get_stock_of_the_day_symbol():
    """Return the ticker symbol for today's Stock of the Day.

    Deterministic by day-of-year. Uses the Dow 30 curated list directly
    to avoid any DB queries on page load (keeps the landing page fast
    and crash-proof on serverless).
    """
    today = datetime.now().timetuple().tm_yday
    return _SOTD_CURATED[today % len(_SOTD_CURATED)]
