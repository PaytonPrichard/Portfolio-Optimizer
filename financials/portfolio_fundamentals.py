"""Portfolio fundamental analysis — financial statement scoring,
trend detection, factor exposure, and combined Advisor Score."""

from concurrent.futures import ThreadPoolExecutor, as_completed

import yfinance as yf

from . import cache

FUNDAMENTALS_TTL = 900  # 15 min


# ── Per-stock fundamental fetcher ─────────────────────────────────────

def _fetch_fundamentals(symbol):
    """Fetch fundamental metrics + quarterly trends for a single stock."""
    cache_key = f"fundamentals:{symbol}"
    cached = cache.get(cache_key)
    if cached is not None:
        return cached

    try:
        ticker = yf.Ticker(symbol)
        info = ticker.info or {}

        result = {
            "grossMargins": info.get("grossMargins"),
            "operatingMargins": info.get("operatingMargins"),
            "profitMargins": info.get("profitMargins"),
            "returnOnEquity": info.get("returnOnEquity"),
            "returnOnAssets": info.get("returnOnAssets"),
            "debtToEquity": info.get("debtToEquity"),
            "currentRatio": info.get("currentRatio"),
            "quickRatio": info.get("quickRatio"),
            "totalDebt": info.get("totalDebt"),
            "totalCash": info.get("totalCash"),
            "freeCashflow": info.get("freeCashflow"),
            "operatingCashflow": info.get("operatingCashflow"),
            "marketCap": info.get("marketCap"),
            "revenueGrowth": info.get("revenueGrowth"),
            "earningsGrowth": info.get("earningsGrowth"),
            "earningsQuarterlyGrowth": info.get("earningsQuarterlyGrowth"),
            "trailingPE": info.get("trailingPE"),
            "forwardPE": info.get("forwardPE"),
            "priceToBook": info.get("priceToBook"),
            "pegRatio": info.get("trailingPegRatio"),
            "payoutRatio": info.get("payoutRatio"),
            "dividendYield": info.get("dividendYield"),
        }

        # Quarterly revenue and net income trends
        try:
            qf = ticker.quarterly_financials
            if qf is not None and not qf.empty:
                for target, key in [("revenue", "quarterlyRevenues"),
                                    ("net income", "quarterlyNetIncome")]:
                    row = None
                    for idx in qf.index:
                        low = str(idx).lower()
                        if target == "revenue" and "revenue" in low and "total" in low:
                            row = idx
                            break
                        if target == "net income" and "net income" in low:
                            row = idx
                            break
                    if row is None:
                        for idx in qf.index:
                            if target in str(idx).lower():
                                row = idx
                                break
                    if row is not None:
                        vals = []
                        for col in qf.columns[:4]:
                            val = qf.loc[row, col]
                            if val is not None and not (isinstance(val, float) and val != val):
                                vals.append(float(val))
                        if vals:
                            result[key] = vals
        except Exception:
            pass

        cache.put(cache_key, result, ttl=FUNDAMENTALS_TTL)
        return result
    except Exception:
        cache.put(cache_key, {}, ttl=FUNDAMENTALS_TTL)
        return {}


# ── Scoring functions (0-25 each, total 0-100) ──────────────────────

def _score_profitability(data):
    score = 0
    count = 0
    for val, thresholds in [
        (data.get("grossMargins"), [(0.50, 7), (0.35, 5), (0.20, 3), (0, 1)]),
        (data.get("operatingMargins"), [(0.25, 7), (0.15, 5), (0.05, 3), (0, 1)]),
        (data.get("profitMargins"), [(0.20, 7), (0.10, 5), (0.05, 3), (0, 1)]),
        (data.get("returnOnEquity"), [(0.25, 7), (0.15, 5), (0.08, 3), (0, 1)]),
    ]:
        if val is not None:
            count += 1
            for threshold, pts in thresholds:
                if val >= threshold:
                    score += pts
                    break
    if count == 0:
        return None
    return min(round(score / count * 25 / 7), 25)


def _score_leverage(data):
    score = 0
    count = 0

    dte = data.get("debtToEquity")
    if dte is not None:
        count += 1
        if dte < 30:
            score += 7
        elif dte < 80:
            score += 5
        elif dte < 150:
            score += 3
        else:
            score += 1

    cr = data.get("currentRatio")
    if cr is not None:
        count += 1
        if cr > 2.0:
            score += 7
        elif cr > 1.5:
            score += 5
        elif cr > 1.0:
            score += 3
        else:
            score += 1

    cash = data.get("totalCash")
    debt = data.get("totalDebt")
    if cash is not None and debt is not None and debt > 0:
        count += 1
        ratio = cash / debt
        if ratio > 1.0:
            score += 7
        elif ratio > 0.5:
            score += 5
        elif ratio > 0.2:
            score += 3
        else:
            score += 1

    if count == 0:
        return None
    return min(round(score / count * 25 / 7), 25)


def _score_cashflow(data):
    score = 0
    count = 0

    fcf = data.get("freeCashflow")
    mcap = data.get("marketCap")
    ocf = data.get("operatingCashflow")

    if fcf is not None and mcap and mcap > 0:
        count += 1
        fcf_yield = fcf / mcap
        if fcf_yield > 0.08:
            score += 7
        elif fcf_yield > 0.05:
            score += 5
        elif fcf_yield > 0.02:
            score += 3
        elif fcf_yield > 0:
            score += 1

    if fcf is not None and ocf and ocf > 0:
        count += 1
        conversion = fcf / ocf
        if conversion > 0.8:
            score += 7
        elif conversion > 0.6:
            score += 5
        elif conversion > 0.3:
            score += 3
        else:
            score += 1

    if fcf is not None:
        count += 1
        score += 5 if fcf > 0 else 0

    if count == 0:
        return None
    return min(round(score / count * 25 / 7), 25)


def _score_growth(data):
    score = 0
    count = 0

    for val in [data.get("revenueGrowth"), data.get("earningsGrowth")]:
        if val is not None:
            count += 1
            if val > 0.25:
                score += 7
            elif val > 0.10:
                score += 5
            elif val > 0.03:
                score += 3
            elif val > 0:
                score += 2

    # Quarterly revenue trend (most recent first in yfinance)
    revs = data.get("quarterlyRevenues", [])
    if len(revs) >= 3:
        count += 1
        # revs[0] = most recent quarter
        ups = sum(1 for i in range(1, len(revs)) if revs[i - 1] > revs[i])
        if ups >= len(revs) - 1:
            score += 7
        elif ups >= len(revs) // 2:
            score += 4
        else:
            score += 1

    if count == 0:
        return None
    return min(round(score / count * 25 / 7), 25)


def _detect_trend(data):
    """Detect fundamental trend: improving, stable, or deteriorating."""
    signals = []

    revs = data.get("quarterlyRevenues", [])
    if len(revs) >= 3:
        if revs[0] > revs[1] > revs[2]:
            signals.append("improving")
        elif revs[0] < revs[1] < revs[2]:
            signals.append("deteriorating")
        else:
            signals.append("stable")

    nis = data.get("quarterlyNetIncome", [])
    if len(nis) >= 3:
        if nis[0] > nis[1] > nis[2]:
            signals.append("improving")
        elif nis[0] < nis[1] < nis[2]:
            signals.append("deteriorating")
        else:
            signals.append("stable")

    eg = data.get("earningsGrowth")
    if eg is not None:
        signals.append("improving" if eg > 0.10 else
                        "deteriorating" if eg < -0.05 else "stable")

    if not signals:
        return "unknown"

    imp = signals.count("improving")
    det = signals.count("deteriorating")
    if imp > det and imp >= 2:
        return "improving"
    elif det > imp and det >= 2:
        return "deteriorating"
    return "stable"


def _pct(val):
    if val is None:
        return None
    return round(float(val) * 100, 1)


# ── Portfolio Fundamental Analysis ───────────────────────────────────

def analyze_portfolio_fundamentals(holdings):
    """Full fundamental analysis with per-holding scores, trends, and alerts."""
    stocks = [h for h in holdings
              if not h.get("isFund") and not h.get("sectorWeights")
              and (h.get("currentValue") or 0) > 0]

    if not stocks:
        return {
            "holdings": [], "portfolioScore": 0,
            "profitability": 0, "leverage": 0, "cashflow": 0, "growth": 0,
            "alerts": [], "scoredCount": 0, "totalCount": 0,
        }

    total_value = sum(h.get("currentValue", 0) for h in stocks)

    # Fetch fundamentals in parallel
    fund_map = {}
    with ThreadPoolExecutor(max_workers=6) as pool:
        futures = {pool.submit(_fetch_fundamentals, h["symbol"]): h["symbol"]
                   for h in stocks}
        for future in as_completed(futures, timeout=45):
            sym = futures[future]
            try:
                fund_map[sym] = future.result(timeout=15)
            except Exception:
                fund_map[sym] = {}

    scored_holdings = []
    w_prof = 0.0
    w_lev = 0.0
    w_cf = 0.0
    w_gr = 0.0
    w_total = 0.0
    scored_value = 0.0
    alerts = []

    for h in stocks:
        sym = h["symbol"]
        data = fund_map.get(sym, {})
        weight = h.get("currentValue", 0) / total_value if total_value > 0 else 0

        prof = _score_profitability(data)
        lev = _score_leverage(data)
        cf = _score_cashflow(data)
        gr = _score_growth(data)

        components = [s for s in [prof, lev, cf, gr] if s is not None]
        total_score = round(sum(components) / len(components) * 4) if components else None

        trend = _detect_trend(data)

        fcf = data.get("freeCashflow")
        mcap = data.get("marketCap")

        entry = {
            "symbol": sym,
            "name": h.get("name", ""),
            "currentValue": h.get("currentValue", 0),
            "weight": round(weight * 100, 1),
            "profitability": prof,
            "leverage": lev,
            "cashflow": cf,
            "growth": gr,
            "totalScore": total_score,
            "trend": trend,
            "grossMargins": _pct(data.get("grossMargins")),
            "operatingMargins": _pct(data.get("operatingMargins")),
            "profitMargins": _pct(data.get("profitMargins")),
            "returnOnEquity": _pct(data.get("returnOnEquity")),
            "debtToEquity": round(data["debtToEquity"], 1) if data.get("debtToEquity") is not None else None,
            "currentRatio": round(data["currentRatio"], 2) if data.get("currentRatio") is not None else None,
            "revenueGrowth": _pct(data.get("revenueGrowth")),
            "earningsGrowth": _pct(data.get("earningsGrowth")),
            "fcfYield": _pct(fcf / mcap) if fcf and mcap else None,
        }
        scored_holdings.append(entry)

        if total_score is not None:
            scored_value += h.get("currentValue", 0)
            if prof is not None:
                w_prof += prof * weight
            if lev is not None:
                w_lev += lev * weight
            if cf is not None:
                w_cf += cf * weight
            if gr is not None:
                w_gr += gr * weight
            w_total += total_score * weight

        # Alerts
        if trend == "deteriorating" and weight > 0.05:
            alerts.append({
                "type": "warning", "symbol": sym,
                "message": f"{sym} fundamentals are deteriorating ({round(weight*100,1)}% of portfolio)",
            })
        if data.get("debtToEquity") and data["debtToEquity"] > 200 and weight > 0.05:
            alerts.append({
                "type": "warning", "symbol": sym,
                "message": f"{sym} has high leverage (D/E: {data['debtToEquity']:.0f})",
            })
        if data.get("freeCashflow") and data["freeCashflow"] < 0 and weight > 0.05:
            alerts.append({
                "type": "caution", "symbol": sym,
                "message": f"{sym} has negative free cash flow",
            })
        if total_score is not None and total_score >= 80 and trend == "improving":
            alerts.append({
                "type": "positive", "symbol": sym,
                "message": f"{sym} has strong fundamentals with improving trend (score: {total_score})",
            })

    scored_holdings.sort(key=lambda x: x.get("totalScore") or 0, reverse=True)

    return {
        "holdings": scored_holdings,
        "portfolioScore": round(w_total),
        "profitability": round(w_prof),
        "leverage": round(w_lev),
        "cashflow": round(w_cf),
        "growth": round(w_gr),
        "alerts": alerts,
        "scoredCount": len([h for h in scored_holdings if h["totalScore"] is not None]),
        "totalCount": len(stocks),
    }


# ── Factor Exposure ──────────────────────────────────────────────────

def compute_factor_exposure(holdings):
    """Analyze portfolio factor tilts using enrichment data.

    No additional API calls — uses data already available from portfolio enrichment.
    """
    total_value = sum(h.get("currentValue", 0) for h in holdings)
    if total_value <= 0:
        return {"style": "Unknown", "weightedPE": None, "sizeMix": {},
                "portfolioYield": 0, "holdings": [], "factors": {}}

    factor_data = []
    pe_weighted = []
    size_cats = {}
    yield_total = 0.0

    for h in holdings:
        val = h.get("currentValue", 0)
        if val <= 0:
            continue
        weight = val / total_value

        pe = h.get("trailingPE")
        mcap = h.get("marketCap")
        div_yield = h.get("dividendYield")
        beta = h.get("beta")

        # Value label
        value_label = None
        if pe and pe > 0:
            if pe < 15:
                value_label = "Deep Value"
            elif pe < 22:
                value_label = "Value"
            elif pe < 35:
                value_label = "Blend"
            else:
                value_label = "Growth"

        # Size category
        size = "Unknown"
        if mcap:
            if mcap >= 200e9:
                size = "Mega Cap"
            elif mcap >= 10e9:
                size = "Large Cap"
            elif mcap >= 2e9:
                size = "Mid Cap"
            else:
                size = "Small Cap"

        # Volatility label
        vol_label = None
        if beta is not None:
            if beta < 0.7:
                vol_label = "Low Vol"
            elif beta < 1.1:
                vol_label = "Market"
            elif beta < 1.5:
                vol_label = "High Vol"
            else:
                vol_label = "Very High Vol"

        factor_data.append({
            "symbol": h["symbol"],
            "name": h.get("name", ""),
            "weight": round(weight * 100, 1),
            "pe": round(pe, 1) if pe else None,
            "valueLabel": value_label,
            "size": size,
            "marketCap": mcap,
            "beta": round(beta, 2) if beta is not None else None,
            "volLabel": vol_label,
            "dividendYield": round(div_yield * 100, 2) if div_yield else None,
            "isFund": h.get("isFund", False),
        })

        if pe and pe > 0:
            pe_weighted.append((pe, weight))
        if size != "Unknown":
            size_cats[size] = size_cats.get(size, 0) + weight * 100
        if div_yield:
            yield_total += div_yield * weight

    weighted_pe = sum(pe * w for pe, w in pe_weighted) if pe_weighted else None

    # Portfolio style
    if weighted_pe:
        if weighted_pe < 18:
            style = "Value"
        elif weighted_pe < 28:
            style = "Blend"
        else:
            style = "Growth"
    else:
        style = "Unknown"

    # Factor scores (0-100 scale)
    factors = {}
    if weighted_pe:
        # Value: inverse of PE (lower PE = more value)
        factors["value"] = max(0, min(100, round((40 - weighted_pe) / 30 * 100)))
        factors["growth"] = max(0, min(100, 100 - factors["value"]))
    else:
        factors["value"] = 50
        factors["growth"] = 50

    # Yield score
    portfolio_yield = yield_total * 100
    factors["yield"] = max(0, min(100, round(portfolio_yield / 5 * 100)))

    # Volatility (from weighted beta)
    betas = [(h["beta"], h["weight"] / 100) for h in factor_data
             if h["beta"] is not None]
    if betas:
        weighted_beta = sum(b * w for b, w in betas)
        factors["volatility"] = max(0, min(100, round(weighted_beta / 1.5 * 100)))
        factors["weightedBeta"] = round(weighted_beta, 2)
    else:
        factors["volatility"] = 50
        factors["weightedBeta"] = None

    # Size score (large = 0, small = 100)
    large_pct = size_cats.get("Mega Cap", 0) + size_cats.get("Large Cap", 0)
    factors["size"] = max(0, min(100, round(100 - large_pct)))

    return {
        "style": style,
        "weightedPE": round(weighted_pe, 1) if weighted_pe else None,
        "sizeMix": {k: round(v, 1) for k, v in sorted(size_cats.items(),
                    key=lambda x: x[1], reverse=True)},
        "portfolioYield": round(portfolio_yield, 2),
        "holdings": factor_data,
        "factors": factors,
    }
