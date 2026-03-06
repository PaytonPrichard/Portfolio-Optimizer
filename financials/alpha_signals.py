"""Alpha signal data fetching and scoring — new alpha-heavy signals.

Fetches insider trading, earnings surprises, share buybacks, analyst
momentum, institutional flows, technical indicators, and macro regime
data.  Each signal has a scoring function (0-100) and returns detailed
data for the UI.
"""

import math
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta

import yfinance as yf

from . import cache

_MACRO_TTL = 3600  # 1 hour — same for all stocks
_SIGNAL_TTL = 600  # 10 min per stock


# ── Utility ───────────────────────────────────────────────────────────

def _sf(val):
    """Safe float."""
    if val is None:
        return None
    try:
        f = float(val)
        return None if (f != f or math.isinf(f)) else f
    except (TypeError, ValueError):
        return None


# =====================================================================
#  DATA FETCHING
# =====================================================================

def fetch_all_signals(symbol, price_history=None):
    """Fetch all alpha signal data in parallel.

    Returns dict with keys: insider, earnings, buyback, analyst_momentum,
    institutional, technical, macro.
    """
    cache_key = f"alpha_signals:{symbol}"
    cached = cache.get(cache_key)
    if cached is not None:
        return cached

    ticker = yf.Ticker(symbol)
    results = {}

    def _get_insider():
        try:
            df = ticker.insider_transactions
            if df is None or df.empty:
                return None
            return df
        except Exception:
            return None

    def _get_earnings():
        try:
            df = ticker.earnings_history
            if df is None or df.empty:
                return None
            return df
        except Exception:
            return None

    def _get_shares():
        try:
            s = ticker.get_shares_full(start=(datetime.now() - timedelta(days=1100)).strftime("%Y-%m-%d"))
            if s is None or s.empty:
                return None
            return s
        except Exception:
            return None

    def _get_upgrades():
        try:
            df = ticker.upgrades_downgrades
            if df is None or df.empty:
                return None
            return df
        except Exception:
            return None

    def _get_institutional():
        try:
            major = ticker.major_holders
            top = ticker.institutional_holders
            return {"major": major, "top": top}
        except Exception:
            return None

    tasks = {
        "insider_raw": _get_insider,
        "earnings_raw": _get_earnings,
        "shares_raw": _get_shares,
        "upgrades_raw": _get_upgrades,
        "institutional_raw": _get_institutional,
    }

    raw = {}
    with ThreadPoolExecutor(max_workers=5) as pool:
        futures = {pool.submit(fn): key for key, fn in tasks.items()}
        for f in as_completed(futures, timeout=25):
            key = futures[f]
            try:
                raw[key] = f.result(timeout=15)
            except Exception:
                raw[key] = None

    # Process raw data into structured signal dicts
    results["insider"] = _process_insider(raw.get("insider_raw"))
    results["earnings"] = _process_earnings(raw.get("earnings_raw"))
    results["buyback"] = _process_buyback(raw.get("shares_raw"))
    results["analyst_momentum"] = _process_upgrades(raw.get("upgrades_raw"))
    results["institutional"] = _process_institutional(raw.get("institutional_raw"))
    results["technical"] = _compute_technical(price_history)
    results["macro"] = _fetch_macro()

    cache.put(cache_key, results, ttl=_SIGNAL_TTL)
    return results


# ── Insider processing ────────────────────────────────────────────────

def _process_insider(df):
    if df is None:
        return None

    six_months_ago = (datetime.now() - timedelta(days=180)).strftime("%Y-%m-%d")
    buys = 0
    sells = 0
    net_shares = 0
    recent = []

    for _, row in df.iterrows():
        text = str(row.get("Text", "")).lower()
        shares = _sf(row.get("Shares")) or 0
        date_val = row.get("Start Date")
        name = row.get("Insider", "Unknown")

        if date_val is not None:
            try:
                d_str = str(date_val)[:10]
            except Exception:
                d_str = ""
        else:
            d_str = ""

        is_buy = "purchase" in text or "buy" in text
        is_sell = "sale" in text or "sell" in text
        is_gift = "gift" in text
        is_option = "option" in text or "exercise" in text

        # Skip gifts and option exercises for scoring
        if is_gift or is_option:
            tx_type = "Gift" if is_gift else "Option Exercise"
        elif is_buy:
            tx_type = "Purchase"
            if d_str >= six_months_ago:
                buys += 1
                net_shares += shares
        elif is_sell:
            tx_type = "Sale"
            if d_str >= six_months_ago:
                sells += 1
                net_shares -= shares
        else:
            tx_type = "Other"

        if len(recent) < 8:
            value = _sf(row.get("Value"))
            recent.append({
                "name": str(name),
                "type": tx_type,
                "shares": int(shares) if shares else 0,
                "date": d_str,
                "value": value,
            })

    return {
        "buyCount": buys,
        "sellCount": sells,
        "netShares": int(net_shares),
        "recent": recent,
    }


# ── Earnings surprise processing ─────────────────────────────────────

def _process_earnings(df):
    if df is None:
        return None

    history = []
    for idx, row in df.iterrows():
        actual = _sf(row.get("epsActual"))
        estimate = _sf(row.get("epsEstimate"))
        surprise = _sf(row.get("surprisePercent"))

        quarter = str(idx) if idx is not None else ""
        if len(quarter) > 10:
            quarter = quarter[:10]

        if actual is not None and estimate is not None:
            history.append({
                "quarter": quarter,
                "actual": round(actual, 2),
                "estimate": round(estimate, 2),
                "surprise": round((surprise or 0) * 100, 1),
                "beat": actual > estimate,
            })

    if not history:
        return None

    beats = sum(1 for h in history if h["beat"])
    surprises = [h["surprise"] for h in history if h["surprise"] != 0]

    return {
        "beatRate": round(beats / len(history), 2) if history else 0,
        "beatsOf": f"{beats}/{len(history)}",
        "avgSurprise": round(sum(surprises) / len(surprises), 1) if surprises else 0,
        "history": history[:8],
    }


# ── Buyback / dilution processing ────────────────────────────────────

def _process_buyback(shares_series):
    if shares_series is None:
        return None

    try:
        quarterly = shares_series.resample("QE").last().dropna()
        if len(quarterly) < 2:
            return None

        current = float(quarterly.iloc[-1])
        if current <= 0:
            return None

        # 1-year change
        change_1y = None
        if len(quarterly) >= 5:
            old = float(quarterly.iloc[-5])
            if old > 0:
                change_1y = round((current - old) / old * 100, 2)

        # 3-year change
        change_3y = None
        if len(quarterly) >= 13:
            old = float(quarterly.iloc[-13])
            if old > 0:
                change_3y = round((current - old) / old * 100, 2)

        if change_1y is not None:
            if change_1y < -1:
                signal = "Buyback"
            elif change_1y > 1:
                signal = "Dilution"
            else:
                signal = "Stable"
        else:
            signal = "Unknown"

        return {
            "sharesChange1y": change_1y,
            "sharesChange3y": change_3y,
            "signal": signal,
            "currentShares": current,
        }
    except Exception:
        return None


# ── Analyst upgrade/downgrade processing ──────────────────────────────

def _process_upgrades(df):
    if df is None:
        return None

    six_months_ago = (datetime.now() - timedelta(days=180)).strftime("%Y-%m-%d")
    three_months_ago = (datetime.now() - timedelta(days=90)).strftime("%Y-%m-%d")

    upgrades_6m = 0
    downgrades_6m = 0
    upgrades_3m = 0
    downgrades_3m = 0
    recent = []

    for idx, row in df.iterrows():
        try:
            d_str = str(idx)[:10]
        except Exception:
            continue

        action = str(row.get("Action", "")).lower()
        firm = row.get("Firm", "")
        to_grade = row.get("ToGrade", "")
        from_grade = row.get("FromGrade", "")

        is_upgrade = action in ("up", "upgrade", "init")
        is_downgrade = action in ("down", "downgrade")
        is_reit = action in ("reit", "main", "reiterated", "maintains")

        if d_str >= six_months_ago:
            if is_upgrade:
                upgrades_6m += 1
            elif is_downgrade:
                downgrades_6m += 1

        if d_str >= three_months_ago:
            if is_upgrade:
                upgrades_3m += 1
            elif is_downgrade:
                downgrades_3m += 1

        if len(recent) < 8:
            recent.append({
                "date": d_str,
                "firm": str(firm),
                "action": str(row.get("Action", "")),
                "to": str(to_grade),
                "from": str(from_grade),
            })

    net_6m = upgrades_6m - downgrades_6m
    if net_6m > 2:
        direction = "positive"
    elif net_6m < -2:
        direction = "negative"
    else:
        direction = "neutral"

    return {
        "upgrades6m": upgrades_6m,
        "downgrades6m": downgrades_6m,
        "upgrades3m": upgrades_3m,
        "downgrades3m": downgrades_3m,
        "netDirection": direction,
        "recent": recent,
    }


# ── Institutional ownership processing ────────────────────────────────

def _process_institutional(data):
    if data is None:
        return None

    result = {}
    major = data.get("major")
    top = data.get("top")

    if major is not None and not major.empty:
        try:
            for _, row in major.iterrows():
                key = str(row.iloc[0]) if len(row) > 0 else ""
                val = row.iloc[1] if len(row) > 1 else None
                key_l = key.lower()
                if "insider" in key_l and "percent" in key_l:
                    result["insiderPct"] = round(float(val) * 100, 2) if val else None
                elif "institution" in key_l and "percent" in key_l and "float" not in key_l:
                    result["institutionalPct"] = round(float(val) * 100, 2) if val else None
                elif "institution" in key_l and "count" in key_l:
                    result["institutionCount"] = int(float(val)) if val else None
        except Exception:
            pass

    top_holders = []
    if top is not None and not top.empty:
        try:
            for _, row in top.head(5).iterrows():
                holder = {
                    "name": str(row.get("Holder", ""))[:30],
                    "pct": round(float(row.get("pctHeld", 0)) * 100, 2),
                    "shares": int(float(row.get("Shares", 0))),
                    "change": round(float(row.get("pctChange", 0)) * 100, 1),
                }
                top_holders.append(holder)
        except Exception:
            pass

    result["topHolders"] = top_holders
    return result if (result.get("institutionalPct") or top_holders) else None


# ── Technical indicators ──────────────────────────────────────────────

def _compute_technical(price_history):
    if not price_history:
        return None

    closes = price_history.get("closes", [])
    if len(closes) < 50:
        return None

    result = {}

    # RSI-14
    result["rsi14"] = _compute_rsi(closes, 14)

    # MACD signal
    result["macdSignal"] = _compute_macd(closes)

    # Moving averages
    sma50 = sum(closes[-50:]) / 50
    result["sma50"] = round(sma50, 2)

    if len(closes) >= 200:
        sma200 = sum(closes[-200:]) / 200
        result["sma200"] = round(sma200, 2)
        result["goldenCross"] = sma50 > sma200
    else:
        result["sma200"] = None
        result["goldenCross"] = None

    current = closes[-1]
    result["aboveSma50"] = current > sma50
    result["aboveSma200"] = current > result["sma200"] if result["sma200"] else None
    result["price"] = round(current, 2)

    return result


def _compute_rsi(closes, period=14):
    if len(closes) < period + 1:
        return None

    gains = []
    losses = []
    for i in range(len(closes) - period, len(closes)):
        change = closes[i] - closes[i - 1]
        gains.append(change if change > 0 else 0)
        losses.append(-change if change < 0 else 0)

    avg_gain = sum(gains) / period
    avg_loss = sum(losses) / period
    if avg_loss == 0:
        return 100.0
    rs = avg_gain / avg_loss
    return round(100 - (100 / (1 + rs)), 1)


def _compute_macd(closes):
    if len(closes) < 35:
        return "neutral"

    def _ema(data, span):
        k = 2 / (span + 1)
        result = [data[0]]
        for i in range(1, len(data)):
            result.append(data[i] * k + result[-1] * (1 - k))
        return result

    ema12 = _ema(closes, 12)
    ema26 = _ema(closes, 26)
    macd_line = [a - b for a, b in zip(ema12, ema26)]

    if len(macd_line) < 35:
        return "neutral"

    signal_line = _ema(macd_line[25:], 9)
    if not signal_line:
        return "neutral"

    histogram = macd_line[-1] - signal_line[-1]
    # Check crossover direction (last 3 bars)
    if len(signal_line) >= 3 and len(macd_line) >= 28:
        prev_hist = macd_line[-2] - signal_line[-2]
        if histogram > 0 and prev_hist <= 0:
            return "bullish_cross"
        elif histogram < 0 and prev_hist >= 0:
            return "bearish_cross"

    if histogram > 0:
        return "bullish"
    elif histogram < 0:
        return "bearish"
    return "neutral"


# ── Macro regime ──────────────────────────────────────────────────────

def _fetch_macro():
    cache_key = "alpha_macro_regime"
    cached = cache.get(cache_key)
    if cached is not None:
        return cached

    result = {}

    def _get_vix():
        try:
            h = yf.Ticker("^VIX").history(period="5d")
            if h is not None and not h.empty:
                return float(h["Close"].iloc[-1])
        except Exception:
            pass
        return None

    def _get_yield(sym):
        try:
            h = yf.Ticker(sym).history(period="5d")
            if h is not None and not h.empty:
                return float(h["Close"].iloc[-1])
        except Exception:
            pass
        return None

    with ThreadPoolExecutor(max_workers=3) as pool:
        fv = pool.submit(_get_vix)
        f10 = pool.submit(_get_yield, "^TNX")
        f2 = pool.submit(_get_yield, "^IRX")

        try:
            result["vix"] = fv.result(timeout=10)
        except Exception:
            result["vix"] = None
        try:
            result["yield10y"] = f10.result(timeout=10)
        except Exception:
            result["yield10y"] = None
        try:
            result["yield2y"] = f2.result(timeout=10)
        except Exception:
            result["yield2y"] = None

    # Classify
    vix = result.get("vix")
    if vix is not None:
        if vix < 15:
            result["vixLevel"] = "low"
        elif vix < 20:
            result["vixLevel"] = "normal"
        elif vix < 30:
            result["vixLevel"] = "elevated"
        else:
            result["vixLevel"] = "high"

    y10 = result.get("yield10y")
    y2 = result.get("yield2y")
    if y10 is not None and y2 is not None:
        spread = round(y10 - y2, 3)
        result["yieldSpread"] = spread
        if spread < 0:
            result["yieldSignal"] = "inverted"
        elif spread < 0.25:
            result["yieldSignal"] = "flat"
        elif spread < 1.5:
            result["yieldSignal"] = "normal"
        else:
            result["yieldSignal"] = "steep"

    cache.put(cache_key, result, ttl=_MACRO_TTL)
    return result


# =====================================================================
#  SCORING FUNCTIONS (each returns 0-100)
# =====================================================================

def score_insider(data):
    """Insider trading score: cluster buying is the strongest signal."""
    if not data:
        return 50

    buys = data.get("buyCount", 0)
    sells = data.get("sellCount", 0)

    if buys >= 3:
        score = 90    # Cluster buying — very strong
    elif buys >= 2:
        score = 80
    elif buys >= 1:
        score = 70
    elif sells == 0 and buys == 0:
        score = 50    # No activity = neutral
    elif sells <= 2:
        score = 40    # Some selling (could be routine)
    elif sells <= 4:
        score = 30    # Moderate selling
    else:
        score = 15    # Heavy selling

    # Bonus if buys significantly outweigh sells
    if buys > sells and buys >= 2:
        score = min(95, score + 5)

    return score


def score_earnings_surprise(data):
    """Earnings surprise: consistent beat rate + surprise magnitude."""
    if not data:
        return 50

    beat_rate = data.get("beatRate", 0)
    avg_surprise = data.get("avgSurprise", 0)
    history = data.get("history", [])

    if not history:
        return 50

    # Beat rate scoring (dominant factor)
    n = len(history)
    if beat_rate >= 1.0:
        score = 90
    elif beat_rate >= 0.75:
        score = 75
    elif beat_rate >= 0.5:
        score = 55
    elif beat_rate >= 0.25:
        score = 35
    else:
        score = 15

    # Surprise magnitude bonus
    if avg_surprise > 10:
        score = min(95, score + 10)
    elif avg_surprise > 5:
        score = min(95, score + 5)
    elif avg_surprise < -5:
        score = max(5, score - 10)

    # Trend bonus: are surprises increasing?
    if len(history) >= 3:
        recent = [h["surprise"] for h in history[:3]]
        if all(recent[i] >= recent[i + 1] for i in range(len(recent) - 1)):
            score = min(95, score + 5)  # accelerating beats

    return score


def score_buyback(data):
    """Buyback/dilution: share shrinkage = management conviction."""
    if not data:
        return 50

    change_1y = data.get("sharesChange1y")
    if change_1y is None:
        return 50

    if change_1y < -5:
        score = 90    # Aggressive buyback
    elif change_1y < -3:
        score = 80
    elif change_1y < -1:
        score = 70
    elif change_1y < 1:
        score = 50    # Flat
    elif change_1y < 3:
        score = 35    # Mild dilution
    elif change_1y < 5:
        score = 25
    else:
        score = 10    # Heavy dilution

    # 3-year trend bonus
    change_3y = data.get("sharesChange3y")
    if change_3y is not None:
        if change_3y < -10:
            score = min(95, score + 5)
        elif change_3y > 10:
            score = max(5, score - 5)

    return score


def score_analyst_momentum(data):
    """Analyst momentum: direction of opinion changes > static consensus."""
    if not data:
        return 50

    upgrades = data.get("upgrades6m", 0)
    downgrades = data.get("downgrades6m", 0)
    net = upgrades - downgrades

    # Recent 3-month activity weighted more
    upgrades_3m = data.get("upgrades3m", 0)
    downgrades_3m = data.get("downgrades3m", 0)
    net_recent = upgrades_3m - downgrades_3m

    if net > 3 and net_recent > 0:
        score = 90    # Strong upgrade momentum
    elif net > 1:
        score = 75
    elif net == 0 or (upgrades == 0 and downgrades == 0):
        score = 50    # No changes
    elif net > -2:
        score = 35
    else:
        score = 15    # Downgrade wave

    # Acceleration bonus
    if net_recent > net / 2 and net > 0:
        score = min(95, score + 5)

    return score


def score_institutional(data):
    """Institutional ownership: smart money positioning."""
    if not data:
        return 50

    inst_pct = data.get("institutionalPct")
    top_holders = data.get("topHolders", [])

    score = 50

    # Institutional ownership level
    if inst_pct is not None:
        if inst_pct > 80:
            score = 65    # Very high (well-covered)
        elif inst_pct > 60:
            score = 60
        elif inst_pct > 40:
            score = 55
        elif inst_pct > 20:
            score = 45
        else:
            score = 40    # Low institutional interest

    # Net change signal from top holders
    if top_holders:
        changes = [h.get("change", 0) for h in top_holders if h.get("change") is not None]
        if changes:
            avg_change = sum(changes) / len(changes)
            if avg_change > 3:
                score = min(95, score + 15)    # Institutions accumulating
            elif avg_change > 0:
                score = min(95, score + 5)
            elif avg_change < -5:
                score = max(5, score - 15)    # Institutions dumping
            elif avg_change < 0:
                score = max(5, score - 5)

    return score


def score_technical(data):
    """Technical analysis: RSI, MACD, moving averages."""
    if not data:
        return 50

    scores = []

    # RSI scoring
    rsi = data.get("rsi14")
    if rsi is not None:
        if rsi > 80:
            scores.append(25)     # Very overbought
        elif rsi > 70:
            scores.append(35)     # Overbought
        elif rsi > 55:
            scores.append(70)     # Bullish momentum
        elif rsi > 45:
            scores.append(55)     # Neutral
        elif rsi > 30:
            scores.append(45)     # Weak
        elif rsi > 20:
            scores.append(60)     # Oversold bounce opportunity
        else:
            scores.append(55)     # Extreme oversold — contrarian

    # MACD scoring
    macd = data.get("macdSignal", "neutral")
    macd_scores = {
        "bullish_cross": 85,
        "bullish": 70,
        "neutral": 50,
        "bearish": 30,
        "bearish_cross": 15,
    }
    scores.append(macd_scores.get(macd, 50))

    # Moving average positioning
    above_50 = data.get("aboveSma50")
    above_200 = data.get("aboveSma200")
    golden = data.get("goldenCross")

    if above_50 is not None and above_200 is not None:
        if above_50 and above_200 and golden:
            scores.append(85)     # Strong uptrend
        elif above_50 and above_200:
            scores.append(70)
        elif above_50:
            scores.append(55)
        elif not above_50 and not above_200 and golden is False:
            scores.append(15)     # Death cross + below both
        elif not above_200:
            scores.append(30)
        else:
            scores.append(40)

    return round(sum(scores) / len(scores)) if scores else 50


def score_macro(data, snapshot=None):
    """Macro regime: how favorable is the environment for equities."""
    if not data:
        return 50

    score = 50

    # VIX component
    vix = data.get("vix")
    if vix is not None:
        if vix < 15:
            score += 15
        elif vix < 20:
            score += 5
        elif vix < 25:
            score -= 5
        elif vix < 30:
            score -= 10
        else:
            score -= 20

    # Yield curve component
    spread = data.get("yieldSpread")
    if spread is not None:
        if spread > 1.5:
            score += 10     # Steep — expansion
        elif spread > 0.5:
            score += 5      # Normal
        elif spread > 0:
            pass             # Flat
        else:
            score -= 15     # Inverted — recession risk

    # Stock-specific adjustment
    if snapshot:
        dte = snapshot.get("debt_to_equity")
        pm = snapshot.get("profit_margins")

        # High debt + high VIX = extra risky
        if dte and dte > 100 and vix and vix > 25:
            score -= 10

        # Quality companies hold up better in vol
        if pm and pm > 0.15 and vix and vix > 25:
            score += 5

    return max(5, min(95, score))
