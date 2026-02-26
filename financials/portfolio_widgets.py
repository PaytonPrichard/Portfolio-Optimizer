"""Portfolio insight widgets — async data fetchers for sector momentum,
news digest, AI commentary, and peer valuations."""

import os
from concurrent.futures import ThreadPoolExecutor, as_completed

import yfinance as yf

from . import cache
from .data import fetch_recent_news, fetch_industry_peers

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
            for future in as_completed(futures):
                try:
                    r = future.result()
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
        for future in as_completed(futures):
            sym = futures[future]
            try:
                items = future.result() or []
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
