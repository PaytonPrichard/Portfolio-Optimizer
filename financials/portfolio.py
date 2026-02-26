"""Fidelity CSV portfolio parser and analysis engine."""

import io
import math
import re
from concurrent.futures import ThreadPoolExecutor, as_completed

import pandas as pd
import yfinance as yf

from . import cache
from .data import fetch_industry_picks, ALLOWED_INDUSTRIES, INDUSTRY_LABELS
from .portfolio_widgets import compute_analyst_overview


def _sanitize_for_json(obj):
    """Recursively replace NaN/Inf floats with None so tojson produces valid JSON."""
    if isinstance(obj, float):
        return None if (math.isnan(obj) or math.isinf(obj)) else obj
    if isinstance(obj, dict):
        return {k: _sanitize_for_json(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_sanitize_for_json(v) for v in obj]
    return obj


# Symbols to skip (cash, money market, pending activity)
_SKIP_SYMBOLS = {"FCASH", "PENDING ACTIVITY"}
_SKIP_PATTERN = re.compile(r"\*")


def _clean_money(val):
    """Strip $, +, %, commas from a value and convert to float."""
    if pd.isna(val) or val is None:
        return None
    s = str(val).strip()
    if s in ("", "--", "n/a", "N/A"):
        return None
    s = s.replace("$", "").replace("%", "").replace(",", "").replace("+", "")
    try:
        return float(s)
    except ValueError:
        return None


# Only these columns are needed.  PII (Account Number, Account Name) is
# stripped client-side in portfolio.js before upload.  This server-side
# allowlist is a defense-in-depth layer in case the JS stripping is bypassed.
# Includes both known Fidelity naming variants for cost basis columns.
_KEEP_COLUMNS = {
    "Symbol", "Description", "Quantity", "Last Price", "Current Value",
    "Cost Basis Total", "Average Cost Basis",
    "Cost Basis", "Cost Basis Per Share",
    "Total Gain/Loss Dollar", "Total Gain/Loss Percent", "Percent Of Account",
}


def parse_fidelity_csv(file_stream) -> list:
    """Parse a Fidelity positions CSV export into a list of holding dicts.

    Security: PII columns are stripped client-side before upload (portfolio.js).
    As defense-in-depth, the server also drops any columns not in _KEEP_COLUMNS.

    Args:
        file_stream: file-like object (from request.files or open())

    Returns:
        list of dicts with keys: symbol, name, quantity, lastPrice,
        currentValue, costBasis, costBasisPerShare, totalGainDollar,
        totalGainPct, pctOfAccount
    """
    content = file_stream.read()
    if isinstance(content, bytes):
        content = content.decode("utf-8-sig")

    # Fidelity appends a legal disclaimer after a blank line — strip it
    # so pandas doesn't choke on non-CSV text.  Also strip trailing commas
    # from each line — Fidelity rows end with "Cash," which creates an extra
    # empty column that shifts pandas column alignment.
    clean_lines = []
    for line in content.splitlines():
        if line.strip() == "":
            break
        clean_lines.append(line.rstrip(","))
    content = "\n".join(clean_lines)

    df = pd.read_csv(io.StringIO(content))

    # Normalize column names (strip whitespace)
    df.columns = df.columns.str.strip()

    # Drop sensitive / unused columns immediately (Account Number, Account Name, etc.)
    cols_to_drop = [c for c in df.columns if c not in _KEEP_COLUMNS]
    df.drop(columns=cols_to_drop, inplace=True, errors="ignore")

    holdings = []
    for _, row in df.iterrows():
        symbol = str(row.get("Symbol", "")).strip().upper()

        # Skip empty, cash, money market, pending
        if not symbol or symbol in _SKIP_SYMBOLS:
            continue
        if _SKIP_PATTERN.search(symbol):
            continue
        if symbol.startswith("PENDING"):
            continue

        name = str(row.get("Description", "")).strip()
        quantity = _clean_money(row.get("Quantity"))
        last_price = _clean_money(row.get("Last Price"))
        current_value = _clean_money(row.get("Current Value"))
        # Fidelity uses "Cost Basis Total" / "Average Cost Basis" but older
        # exports or other brokers may use "Cost Basis" / "Cost Basis Per Share"
        cost_basis = _clean_money(
            row.get("Cost Basis Total") or row.get("Cost Basis"))
        cost_basis_per_share = _clean_money(
            row.get("Average Cost Basis") or row.get("Cost Basis Per Share"))
        total_gain_dollar = _clean_money(row.get("Total Gain/Loss Dollar"))
        total_gain_pct = _clean_money(row.get("Total Gain/Loss Percent"))
        pct_of_account = _clean_money(row.get("Percent Of Account"))

        # Must have at least a symbol and some value
        if current_value is None and quantity is None:
            continue

        holdings.append({
            "symbol": symbol,
            "name": name,
            "quantity": quantity,
            "lastPrice": last_price,
            "currentValue": current_value or 0,
            "costBasis": cost_basis,
            "costBasisPerShare": cost_basis_per_share,
            "totalGainDollar": total_gain_dollar,
            "totalGainPct": total_gain_pct,
            "pctOfAccount": pct_of_account,
        })

    # Consolidate duplicate tickers (same stock held across multiple accounts).
    # Sum additive fields, keep last price from first occurrence.
    merged = {}
    for h in holdings:
        sym = h["symbol"]
        if sym not in merged:
            merged[sym] = dict(h)
        else:
            m = merged[sym]
            m["quantity"] = (m["quantity"] or 0) + (h["quantity"] or 0)
            m["currentValue"] = (m["currentValue"] or 0) + (h.get("currentValue") or 0)
            m["costBasis"] = (m["costBasis"] or 0) + (h.get("costBasis") or 0)
            m["totalGainDollar"] = (m["totalGainDollar"] or 0) + (h.get("totalGainDollar") or 0)

    holdings = list(merged.values())

    # Recalculate derived fields after consolidation
    total_value = sum(h.get("currentValue") or 0 for h in holdings)
    for h in holdings:
        val = h.get("currentValue") or 0
        cost = h.get("costBasis")
        qty = h.get("quantity")
        h["pctOfAccount"] = round(val / total_value * 100, 2) if total_value > 0 else 0
        h["totalGainPct"] = round((val - cost) / cost * 100, 2) if cost else None
        h["costBasisPerShare"] = round(cost / qty, 2) if cost and qty else None

    return holdings


_ENRICHMENT_FALLBACK = {
    "sector": "Unknown",
    "sectorKey": "",
    "industry": "Unknown",
    "industryKey": "",
    "marketCap": None,
    "currentPrice": None,
    "targetMeanPrice": None,
    "nAnalysts": None,
    "recommendationKey": "N/A",
    "sectorWeights": None,
    "isFund": False,
}

# yfinance funds_data uses snake_case sector keys; map to title case for display
_SECTOR_KEY_MAP = {
    "technology": "Technology",
    "financial_services": "Financial Services",
    "healthcare": "Healthcare",
    "consumer_cyclical": "Consumer Cyclical",
    "communication_services": "Communication Services",
    "industrials": "Industrials",
    "consumer_defensive": "Consumer Defensive",
    "energy": "Energy",
    "realestate": "Real Estate",
    "basic_materials": "Basic Materials",
    "utilities": "Utilities",
}


def _enrich_one(symbol: str) -> dict:
    """Fetch enrichment data for a single ticker (called in thread pool).

    For individual stocks, fetches sector/industry from .info.
    For ETFs/mutual funds (where sector is absent), fetches fund sector
    weightings so we can do look-through sector analysis.
    """
    cache_key = f"portfolio_enrich:{symbol}"
    cached = cache.get(cache_key)
    if cached is not None:
        return cached

    try:
        ticker = yf.Ticker(symbol)
        info = ticker.info or {}
        sector = info.get("sector") or ""
        enrichment = {
            "sector": sector or "Unknown",
            "sectorKey": info.get("sectorKey") or "",
            "industry": info.get("industry") or "Unknown",
            "industryKey": info.get("industryKey") or "",
            "marketCap": info.get("marketCap"),
            "currentPrice": info.get("currentPrice") or info.get("regularMarketPrice"),
            "targetMeanPrice": info.get("targetMeanPrice"),
            "nAnalysts": info.get("numberOfAnalystOpinions"),
            "recommendationKey": info.get("recommendationKey") or "N/A",
            "sectorWeights": None,
            "isFund": False,
        }

        # If no sector, this is likely an ETF or mutual fund — try fund data
        if not sector:
            try:
                sw = ticker.funds_data.sector_weightings
                if sw and isinstance(sw, dict) and len(sw) > 0:
                    # Normalize keys to title case
                    weights = {}
                    for k, v in sw.items():
                        label = _SECTOR_KEY_MAP.get(k, k.replace("_", " ").title())
                        weights[label] = round(float(v), 4)
                    enrichment["sectorWeights"] = weights
                    enrichment["isFund"] = True
                    enrichment["sector"] = "Fund/ETF"
            except Exception:
                pass

        cache.put(cache_key, enrichment, ttl=cache.DEFAULT_TTL)
        return enrichment
    except Exception:
        return dict(_ENRICHMENT_FALLBACK)


def enrich_holdings(holdings: list) -> list:
    """Add sector, industry, and analyst data to each holding via yfinance.

    Uses a thread pool (up to 8 workers) to fetch in parallel.
    Results are cached with 5-min TTL.
    """
    # Fetch all unique tickers in parallel
    symbols = list({h["symbol"] for h in holdings})
    results = {}

    with ThreadPoolExecutor(max_workers=8) as pool:
        futures = {pool.submit(_enrich_one, sym): sym for sym in symbols}
        for future in as_completed(futures):
            sym = futures[future]
            try:
                results[sym] = future.result()
            except Exception:
                results[sym] = dict(_ENRICHMENT_FALLBACK)

    # Apply results to holdings
    for h in holdings:
        h.update(results.get(h["symbol"], _ENRICHMENT_FALLBACK))

    return holdings


def analyze_portfolio(holdings: list) -> dict:
    """Run full portfolio analysis on enriched holdings.

    Returns dict with: holdings, totalValue, totalGain, totalGainPct,
    bySector, byIndustry, concentration, gaps, opportunities
    """
    # Sort by current value descending
    holdings.sort(key=lambda h: h.get("currentValue") or 0, reverse=True)

    total_value = sum(h.get("currentValue") or 0 for h in holdings)
    total_cost = sum(h.get("costBasis") or 0 for h in holdings)
    total_gain = sum(h.get("totalGainDollar") or 0 for h in holdings)
    total_gain_pct = ((total_value - total_cost) / total_cost * 100) if total_cost > 0 else 0

    # Sector breakdown — with look-through for ETFs/mutual funds.
    # If a holding has sectorWeights (fund), distribute its value proportionally
    # across sectors.  Otherwise use the single sector from .info.
    sector_map = {}
    for h in holdings:
        val = h.get("currentValue") or 0
        weights = h.get("sectorWeights")
        if weights and isinstance(weights, dict):
            # Look-through: split fund value across sectors
            for sec, w in weights.items():
                if sec not in sector_map:
                    sector_map[sec] = {"sector": sec, "value": 0, "count": 0}
                sector_map[sec]["value"] += val * w
            # Count the fund once in its largest sector
            top_sec = max(weights, key=weights.get)
            sector_map[top_sec]["count"] += 1
        else:
            sec = h.get("sector") or "Unknown"
            if sec == "Fund/ETF":
                sec = "Unknown"
            if sec not in sector_map:
                sector_map[sec] = {"sector": sec, "value": 0, "count": 0}
            sector_map[sec]["value"] += val
            sector_map[sec]["count"] += 1
    by_sector = sorted(sector_map.values(), key=lambda s: s["value"], reverse=True)
    for s in by_sector:
        s["pct"] = round(s["value"] / total_value * 100, 1) if total_value > 0 else 0

    # Industry breakdown — funds don't have industry-level detail, so show
    # individual stocks by industry and funds as a single "Fund/ETF" group.
    industry_map = {}
    for h in holdings:
        if h.get("isFund") or h.get("sectorWeights"):
            ind = "Fund/ETF (look-through in sector view)"
            ind_key = ""
            sec = "Fund/ETF"
        else:
            ind = h.get("industry") or "Unknown"
            ind_key = h.get("industryKey") or ""
            sec = h.get("sector") or "Unknown"
        if ind not in industry_map:
            industry_map[ind] = {
                "industry": ind,
                "industryKey": ind_key,
                "sector": sec,
                "value": 0,
                "count": 0,
            }
        industry_map[ind]["value"] += h.get("currentValue") or 0
        industry_map[ind]["count"] += 1
    by_industry = sorted(industry_map.values(), key=lambda i: i["value"], reverse=True)
    for i in by_industry:
        i["pct"] = round(i["value"] / total_value * 100, 1) if total_value > 0 else 0

    # Concentration risk: holdings > 15% of portfolio
    concentration = []
    for h in holdings:
        pct = h.get("pctOfAccount")
        if pct is None and total_value > 0:
            pct = (h.get("currentValue") or 0) / total_value * 100
        if pct and pct > 15:
            concentration.append({
                "symbol": h["symbol"],
                "name": h.get("name", ""),
                "pct": round(pct, 1),
                "currentValue": h.get("currentValue") or 0,
            })

    # Diversification gaps: ALLOWED_INDUSTRIES not represented
    portfolio_industry_keys = {h.get("industryKey") for h in holdings if h.get("industryKey")}
    gap_industries = []
    for ind_key in sorted(ALLOWED_INDUSTRIES):
        if ind_key not in portfolio_industry_keys:
            gap_industries.append(ind_key)

    # Opportunities: for each gap, fetch top picks (parallelized)
    _MAX_GAP_FETCHES = 12  # cap to keep response time reasonable

    def _fetch_gap_opportunity(ind_key):
        picks = fetch_industry_picks(ind_key)
        hot = []
        for p in sorted(picks, key=lambda x: x.get("upsidePct", 0), reverse=True):
            if (p.get("nAnalysts", 0) >= 5
                    and p.get("upsidePct", 0) > 0
                    and not p.get("lowCoverage", False)):
                hot.append(p)
            if len(hot) >= 3:
                break
        if hot:
            return {
                "industryKey": ind_key,
                "industryLabel": INDUSTRY_LABELS.get(ind_key, ind_key),
                "picks": hot,
            }
        return None

    opportunities = []
    fetch_gaps = gap_industries[:_MAX_GAP_FETCHES]
    with ThreadPoolExecutor(max_workers=6) as pool:
        futures = {pool.submit(_fetch_gap_opportunity, k): k for k in fetch_gaps}
        for future in as_completed(futures):
            try:
                result = future.result()
                if result:
                    opportunities.append(result)
            except Exception:
                pass
    opportunities.sort(key=lambda o: o["industryLabel"])

    # Widget 2: Analyst Consensus Overview (inline, no extra API calls)
    analyst_overview = compute_analyst_overview(holdings)

    # Sector weights dict for widget metadata (sector name -> portfolio %)
    portfolio_sectors = {s["sector"]: s["pct"] for s in by_sector}

    # Widget metadata for async JS loaders — minimal data needed by endpoints.
    # Holdings are trimmed to only the fields the widget endpoints need,
    # keeping the JSON payload small.
    widget_holdings = []
    for h in holdings:
        widget_holdings.append({
            "symbol": h["symbol"],
            "name": h.get("name", ""),
            "currentValue": h.get("currentValue", 0),
            "pctOfAccount": h.get("pctOfAccount", 0),
            "isFund": h.get("isFund", False),
            "industryKey": h.get("industryKey", ""),
            "industry": h.get("industry", ""),
            "sectorWeights": h.get("sectorWeights"),
        })

    widget_meta = _sanitize_for_json({
        "holdings": widget_holdings,
        "portfolioSectors": portfolio_sectors,
        "bySector": by_sector,
        "concentration": concentration,
        "analystOverview": analyst_overview,
    })

    return {
        "holdings": holdings,
        "totalValue": total_value,
        "totalCost": total_cost,
        "totalGain": total_gain,
        "totalGainPct": round(total_gain_pct, 2),
        "bySector": by_sector,
        "byIndustry": by_industry,
        "concentration": concentration,
        "gaps": gap_industries,
        "opportunities": opportunities,
        "industryLabels": INDUSTRY_LABELS,
        "analystOverview": analyst_overview,
        "widgetMeta": widget_meta,
    }
