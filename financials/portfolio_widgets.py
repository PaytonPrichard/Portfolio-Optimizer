"""Portfolio insight widgets — async data fetchers for sector momentum,
news digest, AI commentary, peer valuations, and ESG analysis."""

import os
from concurrent.futures import ThreadPoolExecutor, as_completed

import yfinance as yf

from . import cache
from .data import fetch_recent_news, fetch_industry_peers


# ── Defensive type helpers ──────────────────────────────────────────────

def _safe_float(val):
    """Convert value to float, returning None for NaN/None/bad types."""
    if val is None:
        return None
    try:
        f = float(val)
        if f != f:  # NaN check
            return None
        return f
    except (TypeError, ValueError):
        return None


def _safe_int(val):
    """Convert value to int, returning None for bad values."""
    f = _safe_float(val)
    if f is None:
        return None
    return int(f)


def _is_true(val):
    """Check if a sustainability boolean-like value is truthy."""
    if val is None:
        return False
    if isinstance(val, bool):
        return val
    if isinstance(val, (int, float)):
        return val > 0
    if isinstance(val, str):
        return val.lower() in ("true", "yes", "1")
    return False


# ── Controversial product categories ────────────────────────────────────

_CONTROVERSIAL_PRODUCTS = [
    ("tobacco", "Tobacco"),
    ("alcoholic", "Alcohol"),
    ("gambling", "Gambling"),
    ("nuclear", "Nuclear"),
    ("militaryContract", "Military Contracts"),
    ("smallArms", "Small Arms"),
    ("controversialWeapons", "Controversial Weapons"),
    ("thermalCoal", "Thermal Coal"),
    ("animalTesting", "Animal Testing"),
    ("palmOil", "Palm Oil"),
    ("pesticides", "Pesticides"),
    ("gmo", "GMO"),
    ("furLeather", "Fur & Leather"),
]

# Sector ETF tickers mapped to sector names
SECTOR_ETFS = {
    "XLK": "Technology",
    "XLF": "Financial Services",
    "XLV": "Healthcare",
    "XLY": "Consumer Cyclical",
    "XLC": "Communication Services",
    "XLI": "Industrials",
    "XLP": "Consumer Defensive",
    "XLE": "Energy",
    "XLRE": "Real Estate",
    "XLB": "Basic Materials",
    "XLU": "Utilities",
}



def _fetch_etf_returns(etf_symbol: str) -> dict:
    """Fetch 1W, 1M, 3M returns for a single sector ETF."""
    try:
        hist = yf.Ticker(etf_symbol).history(period="3mo")
        if hist.empty or len(hist) < 2:
            return None
        close = hist["Close"]
        current = float(close.iloc[-1])

        def _pct(n_days):
            if len(close) > n_days:
                old = float(close.iloc[-n_days - 1])
                return round((current - old) / old * 100, 2) if old else None
            return None

        return {
            "etf": etf_symbol,
            "sector": SECTOR_ETFS[etf_symbol],
            "price": round(current, 2),
            "w1": _pct(5),
            "m1": _pct(21),
            "m3": _pct(63),
        }
    except Exception:
        return None


def fetch_sector_momentum(portfolio_sectors: dict = None) -> list:
    """Fetch sector momentum for all 11 sector ETFs. Cached 30 min.

    Args:
        portfolio_sectors: dict mapping sector name -> portfolio weight %
            (e.g. {"Technology": 35.2, "Healthcare": 12.1})

    Returns:
        list of dicts with etf, sector, price, w1, m1, m3, portfolioWeight
    """
    cache_key = "sector_momentum:all"
    cached = cache.get(cache_key)
    if cached is not None:
        results = cached
    else:
        results = []
        with ThreadPoolExecutor(max_workers=6) as pool:
            futures = {pool.submit(_fetch_etf_returns, etf): etf
                       for etf in SECTOR_ETFS}
            for future in as_completed(futures, timeout=30):
                try:
                    r = future.result(timeout=10)
                    if r:
                        results.append(r)
                except Exception:
                    pass
        # Sort by sector name
        results.sort(key=lambda x: x["sector"])
        cache.put(cache_key, results, ttl=cache.SECTOR_MOMENTUM_TTL)

    # Annotate with portfolio weights
    portfolio_sectors = portfolio_sectors or {}
    for r in results:
        r["portfolioWeight"] = portfolio_sectors.get(r["sector"], 0)

    return results


def compute_analyst_overview(holdings: list) -> dict:
    """Aggregate analyst recommendation data from enriched holdings.

    No additional API calls — uses data already in holdings from _enrich_one().

    Returns dict with:
        buys, holds, sells, totalCovered, totalHoldings,
        coverageRatio, weightedUpside, holdingsByUpside
    """
    buys = 0
    holds = 0
    sells = 0
    total_covered = 0
    upside_data = []

    for h in holdings:
        rec = (h.get("recommendationKey") or "").lower().replace(" ", "_")
        n = h.get("nAnalysts") or 0
        if n == 0 or rec in ("n/a", ""):
            continue

        total_covered += 1
        if rec in ("buy", "strong_buy"):
            buys += 1
        elif rec == "hold":
            holds += 1
        elif rec in ("sell", "underperform", "strong_sell"):
            sells += 1

        # Compute implied upside
        target = h.get("targetMeanPrice")
        current = h.get("currentPrice") or h.get("lastPrice")
        upside_pct = None
        if target and current and current > 0:
            upside_pct = round((target - current) / current * 100, 1)

        upside_data.append({
            "symbol": h["symbol"],
            "name": h.get("name", ""),
            "recommendationKey": h.get("recommendationKey", "N/A"),
            "nAnalysts": n,
            "targetMeanPrice": target,
            "currentPrice": current,
            "upsidePct": upside_pct,
            "currentValue": h.get("currentValue", 0),
            "pctOfAccount": h.get("pctOfAccount", 0),
        })

    # Weighted average upside (weighted by portfolio value)
    total_value = sum(d["currentValue"] for d in upside_data if d["upsidePct"] is not None)
    weighted_upside = None
    if total_value > 0:
        weighted_upside = round(
            sum(d["upsidePct"] * d["currentValue"]
                for d in upside_data if d["upsidePct"] is not None)
            / total_value, 1)

    # Sort by upside descending
    holdings_by_upside = sorted(
        [d for d in upside_data if d["upsidePct"] is not None],
        key=lambda x: x["upsidePct"], reverse=True)

    return {
        "buys": buys,
        "holds": holds,
        "sells": sells,
        "totalCovered": total_covered,
        "totalHoldings": len(holdings),
        "coverageRatio": round(total_covered / len(holdings) * 100, 0) if holdings else 0,
        "weightedUpside": weighted_upside,
        "holdingsByUpside": holdings_by_upside,
    }


def fetch_holdings_news(holdings: list, max_stocks: int = 8,
                        max_per_stock: int = 3, max_total: int = 20) -> list:
    """Fetch recent news for the top holdings by value.

    Reuses existing fetch_recent_news(). Returns a merged timeline.

    Args:
        holdings: enriched holdings list (sorted by value desc)
        max_stocks: number of top holdings to fetch news for
        max_per_stock: max headlines per stock
        max_total: overall cap on returned items

    Returns:
        list of dicts with symbol, title, publisher, date
    """
    top_symbols = []
    for h in holdings[:max_stocks]:
        if not h.get("isFund"):
            top_symbols.append(h["symbol"])
        if len(top_symbols) >= max_stocks:
            break

    all_news = []
    with ThreadPoolExecutor(max_workers=4) as pool:
        futures = {pool.submit(fetch_recent_news, sym, max_per_stock): sym
                   for sym in top_symbols}
        for future in as_completed(futures, timeout=20):
            sym = futures[future]
            try:
                items = future.result(timeout=10) or []
                for item in items[:max_per_stock]:
                    item["symbol"] = sym
                    all_news.append(item)
            except Exception:
                pass

    # Sort by date string (recent first) — best effort since dates are formatted strings
    all_news.sort(key=lambda x: x.get("date", ""), reverse=True)
    return all_news[:max_total]


def generate_portfolio_ai_commentary(holdings: list, by_sector: list,
                                     concentration: list,
                                     analyst_overview: dict) -> str:
    """Generate 3-5 sentence AI commentary about portfolio composition.

    Uses Claude Haiku. Falls back to rule-based summary without API key.
    """
    # Build context for the prompt
    total_value = sum(h.get("currentValue", 0) for h in holdings)
    top_5 = holdings[:5]

    sector_lines = []
    for s in by_sector[:6]:
        sector_lines.append(f"  {s['sector']}: {s['pct']}%")

    top_lines = []
    for h in top_5:
        pct = h.get("pctOfAccount", 0)
        top_lines.append(f"  {h['symbol']}: {pct}% of portfolio")

    conc_lines = []
    for c in concentration:
        conc_lines.append(f"  {c['symbol']}: {c['pct']}%")

    overview = analyst_overview or {}
    upside = overview.get("weightedUpside")
    upside_str = f"{'+' if upside >= 0 else ''}{upside}%" if upside is not None else "N/A"

    data_block = "\n".join(filter(None, [
        f"Total portfolio value: ${total_value:,.0f}",
        f"Number of holdings: {len(holdings)}",
        f"Sectors covered: {len(by_sector)}",
        "Top sector allocations:",
        "\n".join(sector_lines),
        "Largest holdings:",
        "\n".join(top_lines),
        f"Concentration risks (>15%): {len(concentration)} holdings" if concentration else "No concentration risk (all <15%)",
        "\n".join(conc_lines) if conc_lines else "",
        f"Analyst coverage: {overview.get('totalCovered', 0)}/{overview.get('totalHoldings', 0)} holdings",
        f"Consensus: {overview.get('buys', 0)} Buy, {overview.get('holds', 0)} Hold, {overview.get('sells', 0)} Sell",
        f"Weighted implied upside: {upside_str}",
    ]))

    # Try AI first
    api_key = os.environ.get("ANTHROPIC_API_KEY")
    if api_key:
        try:
            import anthropic
            client = anthropic.Anthropic(api_key=api_key)
            prompt = (
                "You are a portfolio analyst writing a brief assessment for an investor's dashboard.\n\n"
                "Based on this portfolio data, write exactly 3-5 sentences in a single paragraph. Cover:\n"
                "1. Overall sector tilts and what they suggest about the investor's strategy\n"
                "2. Any concentration risks worth noting\n"
                "3. One forward-looking observation based on analyst consensus\n\n"
                "Rules: be specific with numbers from the data. No bullet points, no headers. "
                "Keep each sentence under 40 words. Be direct and professional.\n\n"
                f"Portfolio data:\n{data_block}"
            )
            response = client.messages.create(
                model="claude-haiku-4-5-20251001",
                max_tokens=400,
                messages=[{"role": "user", "content": prompt}],
            )
            return response.content[0].text.strip()
        except Exception:
            pass

    # Rule-based fallback
    return _rule_based_commentary(holdings, by_sector, concentration, analyst_overview)


def _rule_based_commentary(holdings, by_sector, concentration, overview):
    """Generate simple rule-based portfolio commentary (no AI needed)."""
    sentences = []
    total_value = sum(h.get("currentValue", 0) for h in holdings)

    # Sector tilt
    if by_sector:
        top = by_sector[0]
        sentences.append(
            f"Your portfolio is most heavily weighted toward {top['sector']} "
            f"at {top['pct']}% of total value ({len(by_sector)} sectors represented)."
        )

    # Concentration
    if concentration:
        names = ", ".join(c["symbol"] for c in concentration)
        sentences.append(
            f"Concentration risk: {names} "
            f"{'each exceed' if len(concentration) > 1 else 'exceeds'} "
            f"15% of your portfolio and may warrant rebalancing."
        )
    else:
        sentences.append("No individual holding exceeds 15% of portfolio value, indicating good diversification.")

    # Analyst consensus
    overview = overview or {}
    buys = overview.get("buys", 0)
    total = overview.get("totalCovered", 0)
    upside = overview.get("weightedUpside")
    if total > 0 and upside is not None:
        sentences.append(
            f"Analyst consensus across {total} covered holdings shows "
            f"{buys} Buy ratings with a weighted implied upside of "
            f"{'+' if upside >= 0 else ''}{upside}%."
        )

    return " ".join(sentences) if sentences else "Upload a portfolio to see AI-generated commentary."


def fetch_peer_valuations(holdings: list, max_holdings: int = 3) -> list:
    """Compare top individual stock holdings to their industry peers.

    Reuses fetch_industry_peers(). Returns comparison data for each holding.

    Args:
        holdings: enriched holdings sorted by value desc
        max_holdings: number of holdings to compare

    Returns:
        list of dicts with: symbol, name, industry, peers (list), verdict
    """
    # Pick top individual stocks (skip funds)
    targets = []
    for h in holdings:
        if h.get("isFund") or h.get("sectorWeights"):
            continue
        if not h.get("industryKey"):
            continue
        targets.append(h)
        if len(targets) >= max_holdings:
            break

    results = []
    for h in targets:
        try:
            info_dict = {
                "industryKey": h.get("industryKey"),
                "industry": h.get("industry"),
            }
            peers = fetch_industry_peers(h["symbol"], info_dict, max_peers=6)
            if not peers:
                continue

            # Find the target holding in peers list
            target_data = None
            peer_list = []
            for p in peers:
                if p.get("is_target"):
                    target_data = p
                else:
                    peer_list.append(p)

            # Compute verdict
            verdict = _compute_verdict(target_data, peer_list)

            results.append({
                "symbol": h["symbol"],
                "name": h.get("name", ""),
                "industry": h.get("industry", "Unknown"),
                "currentValue": h.get("currentValue", 0),
                "pctOfAccount": h.get("pctOfAccount", 0),
                "target": target_data,
                "peers": peer_list[:5],
                "verdict": verdict,
            })
        except Exception:
            continue

    return results


def _compute_verdict(target: dict, peers: list) -> str:
    """Simple verdict comparing the target holding to peers."""
    if not target or not peers:
        return "Insufficient peer data for comparison."

    points = []

    # P/E comparison
    t_pe = target.get("trailingPE")
    peer_pes = [p["trailingPE"] for p in peers if p.get("trailingPE")]
    if t_pe and peer_pes:
        avg_pe = sum(peer_pes) / len(peer_pes)
        if t_pe < avg_pe * 0.8:
            points.append("trades at a discount to peers on P/E")
        elif t_pe > avg_pe * 1.2:
            points.append("trades at a premium to peers on P/E")
        else:
            points.append("P/E roughly in line with peers")

    # Margin comparison
    t_margin = target.get("grossMargins")
    peer_margins = [p["grossMargins"] for p in peers if p.get("grossMargins")]
    if t_margin and peer_margins:
        avg_margin = sum(peer_margins) / len(peer_margins)
        if t_margin > avg_margin:
            points.append("higher gross margins than peer average")
        else:
            points.append("lower gross margins than peer average")

    # Revenue growth
    t_growth = target.get("revenueGrowth")
    peer_growths = [p["revenueGrowth"] for p in peers if p.get("revenueGrowth")]
    if t_growth and peer_growths:
        avg_growth = sum(peer_growths) / len(peer_growths)
        if t_growth > avg_growth:
            points.append("faster revenue growth")
        else:
            points.append("slower revenue growth")

    if points:
        return target.get("symbol", "This holding") + " " + ", ".join(points) + "."
    return "Comparable to industry peers on key metrics."


# ── ESG / Ethical Investing ─────────────────────────────────────────────

def _fetch_esg_data(symbol: str) -> dict:
    """Fetch ESG sustainability data for a single ticker. Cached per-symbol."""
    cache_key = f"esg:{symbol}"
    cached = cache.get(cache_key)
    if cached is not None:
        # {} sentinel means "no data available" — don't retry
        return cached

    try:
        ticker = yf.Ticker(symbol)
        sust = ticker.sustainability
        if sust is None or sust.empty:
            cache.put(cache_key, {}, ttl=cache.ESG_TTL)
            return {}

        # sustainability is a DataFrame with a single column — flatten to dict
        raw = {}
        for idx in sust.index:
            val = sust.loc[idx].values[0] if len(sust.loc[idx].values) else None
            raw[idx] = val

        result = {
            "totalEsg": _safe_float(raw.get("totalEsg")),
            "environmentScore": _safe_float(raw.get("environmentScore")),
            "socialScore": _safe_float(raw.get("socialScore")),
            "governanceScore": _safe_float(raw.get("governanceScore")),
            "esgPerformance": raw.get("esgPerformance"),
            "controversyLevel": _safe_int(raw.get("highestControversy")),
        }

        # Extract controversial product flags
        flags = []
        for key, label in _CONTROVERSIAL_PRODUCTS:
            if _is_true(raw.get(key)):
                flags.append(label)
        result["flags"] = flags

        cache.put(cache_key, result, ttl=cache.ESG_TTL)
        return result
    except Exception:
        cache.put(cache_key, {}, ttl=cache.ESG_TTL)
        return {}


# ── Historical Performance ─────────────────────────────────────────────

_VALID_PERIODS = {"1d", "1mo", "1y"}


def _fetch_ticker_history(symbol: str, period: str) -> dict:
    """Fetch price history for a single ticker. Cached per symbol+period."""
    cache_key = f"history:{symbol}:{period}"
    cached = cache.get(cache_key)
    if cached is not None:
        return cached  # may be None sentinel

    ttl = cache.HISTORY_INTRADAY_TTL if period == "1d" else cache.HISTORY_TTL
    try:
        hist = yf.Ticker(symbol).history(period=period)
        if hist is None or hist.empty:
            cache.put(cache_key, None, ttl=ttl)
            return None
        closes = hist["Close"].dropna()
        if len(closes) < 1:
            cache.put(cache_key, None, ttl=ttl)
            return None
        result = {
            "symbol": symbol,
            "dates": [d.strftime("%Y-%m-%d") for d in closes.index],
            "closes": [round(float(c), 4) for c in closes.values],
            "currentPrice": round(float(closes.iloc[-1]), 4),
        }
        cache.put(cache_key, result, ttl=ttl)
        return result
    except Exception:
        cache.put(cache_key, None, ttl=ttl)
        return None


def _interpolate_value(symbol, date_str, current_value, history_map):
    """Compute holding value on a given date using price-ratio approach.

    Falls back to current_value if no history available.
    """
    hist = history_map.get(symbol)
    if not hist:
        return current_value

    dates = hist["dates"]
    closes = hist["closes"]
    curr_price = hist["currentPrice"]
    if not curr_price or curr_price <= 0:
        return current_value

    # Exact match
    if date_str in dates:
        idx = dates.index(date_str)
        return current_value * (closes[idx] / curr_price)

    # Find most recent date before date_str
    best_price = None
    for i, d in enumerate(dates):
        if d <= date_str:
            best_price = closes[i]
    if best_price is not None:
        return current_value * (best_price / curr_price)

    return current_value


def _empty_performance(period, market_closed=False):
    """Return zeroed-out performance dict for edge cases."""
    return {
        "period": period,
        "dates": [],
        "portfolioValues": [],
        "startValue": 0,
        "endValue": 0,
        "periodReturn": 0,
        "periodReturnDollar": 0,
        "bestPerformer": None,
        "worstPerformer": None,
        "holdingReturns": [],
        "marketClosed": market_closed,
    }


def fetch_portfolio_performance(holdings: list, period: str = "1mo") -> dict:
    """Fetch historical performance for the entire portfolio.

    Uses price-ratio approach: pastValue = currentValue * (histPrice / currPrice).
    Aggregates into portfolio-level daily time series.
    """
    if period not in _VALID_PERIODS:
        period = "1mo"

    # Filter holdings with positive current value
    valid = [h for h in holdings if (h.get("currentValue") or 0) > 0]
    if not valid:
        return _empty_performance(period)

    # Fetch history in parallel
    history_map = {}
    with ThreadPoolExecutor(max_workers=6) as pool:
        futures = {pool.submit(_fetch_ticker_history, h["symbol"], period): h["symbol"]
                   for h in valid}
        for future in as_completed(futures, timeout=30):
            sym = futures[future]
            try:
                result = future.result(timeout=10)
                if result:
                    history_map[sym] = result
            except Exception:
                pass

    if not history_map:
        # Check if 1d and possibly market is closed
        mc = period == "1d"
        return _empty_performance(period, market_closed=mc)

    # Build union of all trading dates
    all_dates = set()
    for hist in history_map.values():
        all_dates.update(hist["dates"])
    all_dates = sorted(all_dates)

    if len(all_dates) < 2:
        return _empty_performance(period, market_closed=(period == "1d"))

    # Compute portfolio value for each date
    portfolio_values = []
    for date_str in all_dates:
        day_total = 0
        for h in valid:
            day_total += _interpolate_value(
                h["symbol"], date_str, h["currentValue"], history_map)
        portfolio_values.append(round(day_total, 2))

    start_value = portfolio_values[0]
    end_value = portfolio_values[-1]
    period_return_dollar = round(end_value - start_value, 2)
    period_return = round((end_value - start_value) / start_value * 100, 2) if start_value else 0

    # Per-holding returns
    holding_returns = []
    for h in valid:
        sym = h["symbol"]
        cv = h["currentValue"]
        hist = history_map.get(sym)
        if hist and hist["dates"] and hist["closes"]:
            start_price = hist["closes"][0]
            end_price = hist["closes"][-1]
            if start_price and start_price > 0:
                ret = round((end_price - start_price) / start_price * 100, 2)
            else:
                ret = 0
            start_val = cv * (start_price / hist["currentPrice"]) if hist["currentPrice"] else cv
        else:
            ret = 0
            start_val = cv
        holding_returns.append({
            "symbol": sym,
            "name": h.get("name", ""),
            "startValue": round(start_val, 2),
            "endValue": round(cv, 2),
            "returnPct": ret,
            "weight": h.get("pctOfAccount", 0),
        })

    # Best / worst performers
    holding_returns.sort(key=lambda x: x["returnPct"], reverse=True)
    best = holding_returns[0] if holding_returns else None
    worst = holding_returns[-1] if holding_returns else None

    # ── SPY benchmark overlay ────────────────────────────────────────
    benchmark_values = []
    benchmark_return = None
    try:
        spy_hist = _fetch_ticker_history("SPY", period)
        if spy_hist and spy_hist["dates"] and spy_hist["closes"]:
            spy_start = spy_hist["closes"][0]
            if spy_start and spy_start > 0:
                # Normalize SPY to start at portfolio start value
                norm = start_value / spy_start
                # Interpolate SPY onto portfolio's date axis
                spy_date_map = dict(zip(spy_hist["dates"], spy_hist["closes"]))
                last_spy = spy_start
                for date_str in all_dates:
                    if date_str in spy_date_map:
                        last_spy = spy_date_map[date_str]
                    benchmark_values.append(round(last_spy * norm, 2))
                spy_end = spy_hist["closes"][-1]
                benchmark_return = round((spy_end - spy_start) / spy_start * 100, 2)
    except Exception:
        pass

    return {
        "period": period,
        "dates": all_dates,
        "portfolioValues": portfolio_values,
        "startValue": start_value,
        "endValue": end_value,
        "periodReturn": period_return,
        "periodReturnDollar": period_return_dollar,
        "bestPerformer": best,
        "worstPerformer": worst,
        "holdingReturns": holding_returns,
        "marketClosed": False,
        "benchmarkValues": benchmark_values,
        "benchmarkReturn": benchmark_return,
    }


def compute_correlation_matrix(holdings: list, max_holdings: int = 8) -> dict:
    """Compute Pearson correlation matrix for top holdings by value.

    Uses 3-month daily returns. Pure Python implementation (no numpy).

    Args:
        holdings: enriched holdings sorted by value desc
        max_holdings: max number of stocks to include

    Returns:
        dict with symbols, matrix (NxN), highCorrelations list
    """
    # Filter to top individual stocks by value (skip funds)
    targets = []
    for h in holdings:
        if h.get("isFund") or h.get("sectorWeights"):
            continue
        if (h.get("currentValue") or 0) <= 0:
            continue
        targets.append(h)
        if len(targets) >= max_holdings:
            break

    if len(targets) < 2:
        return {"symbols": [], "matrix": [], "highCorrelations": []}

    # Fetch 3-month history in parallel
    history_map = {}
    with ThreadPoolExecutor(max_workers=6) as pool:
        futures = {pool.submit(_fetch_ticker_history, h["symbol"], "3mo"): h["symbol"]
                   for h in targets}
        for future in as_completed(futures, timeout=30):
            sym = futures[future]
            try:
                result = future.result(timeout=10)
                if result and len(result["closes"]) >= 5:
                    history_map[sym] = result
            except Exception:
                pass

    # Filter to only symbols with history
    symbols = [h["symbol"] for h in targets if h["symbol"] in history_map]
    if len(symbols) < 2:
        return {"symbols": [], "matrix": [], "highCorrelations": []}

    # Compute daily returns for each symbol
    returns_map = {}
    for sym in symbols:
        closes = history_map[sym]["closes"]
        rets = []
        for i in range(1, len(closes)):
            if closes[i - 1] > 0:
                rets.append((closes[i] - closes[i - 1]) / closes[i - 1])
        returns_map[sym] = rets

    # Align return series to same length (min length)
    min_len = min(len(returns_map[s]) for s in symbols)
    for sym in symbols:
        returns_map[sym] = returns_map[sym][:min_len]

    # Pearson correlation (pure Python)
    def _pearson(xs, ys):
        n = len(xs)
        if n < 3:
            return 0.0
        mean_x = sum(xs) / n
        mean_y = sum(ys) / n
        cov = sum((xs[i] - mean_x) * (ys[i] - mean_y) for i in range(n))
        std_x = (sum((xs[i] - mean_x) ** 2 for i in range(n))) ** 0.5
        std_y = (sum((ys[i] - mean_y) ** 2 for i in range(n))) ** 0.5
        if std_x == 0 or std_y == 0:
            return 0.0
        return round(cov / (std_x * std_y), 4)

    # Build NxN correlation matrix
    n = len(symbols)
    matrix = [[0.0] * n for _ in range(n)]
    high_corrs = []

    for i in range(n):
        for j in range(n):
            if i == j:
                matrix[i][j] = 1.0
            elif j > i:
                corr = _pearson(returns_map[symbols[i]], returns_map[symbols[j]])
                matrix[i][j] = corr
                matrix[j][i] = corr
                if abs(corr) > 0.8:
                    high_corrs.append({
                        "pair": symbols[i] + "/" + symbols[j],
                        "corr": corr,
                    })

    # Sort high correlations by absolute value desc
    high_corrs.sort(key=lambda x: abs(x["corr"]), reverse=True)

    return {
        "symbols": symbols,
        "matrix": matrix,
        "highCorrelations": high_corrs,
    }


def fetch_ethical_analysis(holdings: list) -> dict:
    """Analyze portfolio ESG risk and controversial product involvement.

    Args:
        holdings: enriched holdings list from portfolio analysis

    Returns:
        dict with portfolioEsg, portfolioE, portfolioS, portfolioG,
        holdingsEsg, controversies, coveredCount, totalCount, skippedFunds
    """
    # Separate individual stocks from funds/ETFs
    stocks = []
    skipped_funds = []
    for h in holdings:
        if h.get("isFund") or h.get("sectorWeights"):
            skipped_funds.append(h.get("symbol", "?"))
        else:
            stocks.append(h)

    # Fetch ESG data in parallel
    esg_map = {}
    with ThreadPoolExecutor(max_workers=6) as pool:
        futures = {pool.submit(_fetch_esg_data, h["symbol"]): h["symbol"]
                   for h in stocks}
        for future in as_completed(futures, timeout=30):
            sym = futures[future]
            try:
                esg_map[sym] = future.result(timeout=10)
            except Exception:
                esg_map[sym] = {}
    # Fill any that didn't complete
    for h in stocks:
        if h["symbol"] not in esg_map:
            esg_map[h["symbol"]] = {}

    # Build per-holding ESG list and compute weighted portfolio scores
    holdings_esg = []
    weighted_total = 0.0
    weighted_e = 0.0
    weighted_s = 0.0
    weighted_g = 0.0
    total_covered_value = 0.0
    covered_count = 0

    for h in stocks:
        sym = h["symbol"]
        esg = esg_map.get(sym, {})
        value = h.get("currentValue", 0) or 0

        entry = {
            "symbol": sym,
            "name": h.get("name", ""),
            "currentValue": value,
            "pctOfAccount": h.get("pctOfAccount", 0),
            "totalEsg": esg.get("totalEsg"),
            "environmentScore": esg.get("environmentScore"),
            "socialScore": esg.get("socialScore"),
            "governanceScore": esg.get("governanceScore"),
            "esgPerformance": esg.get("esgPerformance"),
            "controversyLevel": esg.get("controversyLevel"),
            "flags": esg.get("flags", []),
        }
        holdings_esg.append(entry)

        # Accumulate weighted scores for covered holdings
        if esg.get("totalEsg") is not None and value > 0:
            covered_count += 1
            total_covered_value += value
            weighted_total += esg["totalEsg"] * value
            if esg.get("environmentScore") is not None:
                weighted_e += esg["environmentScore"] * value
            if esg.get("socialScore") is not None:
                weighted_s += esg["socialScore"] * value
            if esg.get("governanceScore") is not None:
                weighted_g += esg["governanceScore"] * value

    # Compute portfolio-level weighted averages
    portfolio_esg = None
    portfolio_e = None
    portfolio_s = None
    portfolio_g = None
    if total_covered_value > 0:
        portfolio_esg = round(weighted_total / total_covered_value, 1)
        portfolio_e = round(weighted_e / total_covered_value, 1)
        portfolio_s = round(weighted_s / total_covered_value, 1)
        portfolio_g = round(weighted_g / total_covered_value, 1)

    # Sort holdings: covered sorted by risk desc, then N/A at bottom
    covered = [h for h in holdings_esg if h["totalEsg"] is not None]
    uncovered = [h for h in holdings_esg if h["totalEsg"] is None]
    covered.sort(key=lambda x: x["totalEsg"], reverse=True)
    uncovered.sort(key=lambda x: x["symbol"])
    holdings_esg = covered + uncovered

    # Aggregate controversial product exposure
    controversies = {}
    for h in holdings_esg:
        for flag in h.get("flags", []):
            if flag not in controversies:
                controversies[flag] = {"category": flag, "symbols": [], "value": 0.0, "pct": 0.0}
            controversies[flag]["symbols"].append(h["symbol"])
            controversies[flag]["value"] += h.get("currentValue", 0)

    total_portfolio_value = sum(h.get("currentValue", 0) for h in holdings) or 1
    for cat in controversies.values():
        cat["pct"] = round(cat["value"] / total_portfolio_value * 100, 1)

    # Sort controversies by exposure value desc
    controversies_list = sorted(controversies.values(), key=lambda x: x["value"], reverse=True)

    return {
        "portfolioEsg": portfolio_esg,
        "portfolioE": portfolio_e,
        "portfolioS": portfolio_s,
        "portfolioG": portfolio_g,
        "holdingsEsg": holdings_esg,
        "controversies": controversies_list,
        "coveredCount": covered_count,
        "totalCount": len(stocks),
        "skippedFunds": skipped_funds,
    }
