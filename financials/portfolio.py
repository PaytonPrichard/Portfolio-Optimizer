"""Multi-broker CSV portfolio parser and analysis engine.

Supports CSV exports from Fidelity, Schwab, Vanguard, E*Trade,
TD Ameritrade, Robinhood, Interactive Brokers, Merrill Edge, and
other brokers with standard column naming.
"""

import io
import math
import re
from concurrent.futures import ThreadPoolExecutor, as_completed

import pandas as pd
import yfinance as yf

from . import cache
from .data import fetch_industry_picks, ALLOWED_INDUSTRIES, INDUSTRY_LABELS
from .portfolio_widgets import compute_analyst_overview


# Analyst recommendation to numeric score for composite ranking
_RATING_SCORES = {
    "strong_buy": 5, "buy": 4, "hold": 3,
    "sell": 2, "underperform": 2, "strong_sell": 1,
}


def _sanitize_for_json(obj):
    """Recursively replace NaN/Inf floats with None so tojson produces valid JSON."""
    if isinstance(obj, float):
        return None if (math.isnan(obj) or math.isinf(obj)) else obj
    if isinstance(obj, dict):
        return {k: _sanitize_for_json(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_sanitize_for_json(v) for v in obj]
    return obj


# Symbols to skip (cash, money market, pending activity, sweep accounts)
_SKIP_SYMBOLS = {
    "FCASH", "PENDING ACTIVITY", "CASH", "CASH & CASH INVESTMENTS",
    "SPAXX", "SWVXX", "VMFXX", "FDRXX",  # common money market funds
    "ACCOUNT TOTAL", "TOTAL",
}
_SKIP_PATTERN = re.compile(r"\*")


def _clean_money(val):
    """Strip $, +, %, commas, parens (negative) from a value and convert to float."""
    if pd.isna(val) or val is None:
        return None
    s = str(val).strip()
    if s in ("", "--", "n/a", "N/A", "—", "-"):
        return None
    # Handle parenthetical negatives: ($1,234.56) -> -1234.56
    negative = False
    if s.startswith("(") and s.endswith(")"):
        s = s[1:-1]
        negative = True
    s = s.replace("$", "").replace("%", "").replace(",", "").replace("+", "")
    try:
        result = float(s)
        return -result if negative else result
    except ValueError:
        return None


# ---------------------------------------------------------------------------
# Column name normalization
# ---------------------------------------------------------------------------
# Maps various brokerage column names to canonical names used internally.
# Keys are lowercased for case-insensitive matching.
_COLUMN_ALIASES = {
    # Symbol
    "symbol":           "Symbol",
    "ticker":           "Symbol",
    "ticker symbol":    "Symbol",

    # Description / Name
    "description":          "Description",
    "name":                 "Description",
    "security name":        "Description",
    "security description": "Description",
    "investment name":      "Description",
    "security":             "Description",
    "holding":              "Description",

    # Quantity / Shares
    "quantity":   "Quantity",
    "shares":     "Quantity",
    "qty":        "Quantity",
    "share count": "Quantity",

    # Last Price
    "last price":           "Last Price",
    "price":                "Last Price",
    "share price":          "Last Price",
    "close price":          "Last Price",
    "closing price":        "Last Price",
    "last":                 "Last Price",
    "market price":         "Last Price",
    "current price":        "Last Price",

    # Current Value / Market Value
    "current value":   "Current Value",
    "market value":    "Current Value",
    "total value":     "Current Value",
    "value":           "Current Value",
    "mkt value":       "Current Value",
    "account value":   "Current Value",
    "equity":          "Current Value",

    # Cost Basis (total)
    "cost basis total":   "Cost Basis Total",
    "cost basis":         "Cost Basis Total",
    "total cost":         "Cost Basis Total",
    "total cost basis":   "Cost Basis Total",
    "cost":               "Cost Basis Total",
    "book value":         "Cost Basis Total",
    "purchase value":     "Cost Basis Total",

    # Cost Basis Per Share
    "average cost basis":    "Average Cost Basis",
    "cost basis per share":  "Average Cost Basis",
    "avg cost":              "Average Cost Basis",
    "average cost":          "Average Cost Basis",
    "avg cost/share":        "Average Cost Basis",
    "avg price":             "Average Cost Basis",
    "unit cost":             "Average Cost Basis",

    # Gain/Loss Dollar
    "total gain/loss dollar": "Total Gain/Loss Dollar",
    "gain/loss dollar":       "Total Gain/Loss Dollar",
    "gain/loss $":            "Total Gain/Loss Dollar",
    "gain loss $":            "Total Gain/Loss Dollar",
    "unrealized gain/loss":   "Total Gain/Loss Dollar",
    "unrealized p&l":         "Total Gain/Loss Dollar",
    "gain/loss":              "Total Gain/Loss Dollar",
    "p&l":                    "Total Gain/Loss Dollar",
    "total gain/loss":        "Total Gain/Loss Dollar",

    # Gain/Loss Percent
    "total gain/loss percent": "Total Gain/Loss Percent",
    "gain/loss percent":       "Total Gain/Loss Percent",
    "gain/loss %":             "Total Gain/Loss Percent",
    "gain loss %":             "Total Gain/Loss Percent",
    "unrealized gain/loss %":  "Total Gain/Loss Percent",
    "% gain/loss":             "Total Gain/Loss Percent",

    # Percent of Account
    "percent of account": "Percent Of Account",
    "% of account":       "Percent Of Account",
    "% of portfolio":     "Percent Of Account",
    "weight":             "Percent Of Account",
    "portfolio %":        "Percent Of Account",
    "allocation":         "Percent Of Account",
    "allocation %":       "Percent Of Account",
}

# Canonical column names we actually use
_CANONICAL_COLUMNS = {
    "Symbol", "Description", "Quantity", "Last Price", "Current Value",
    "Cost Basis Total", "Average Cost Basis",
    "Total Gain/Loss Dollar", "Total Gain/Loss Percent", "Percent Of Account",
}


def _normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Rename brokerage-specific column names to canonical names."""
    rename_map = {}
    for col in df.columns:
        lower = col.strip().lower()
        canonical = _COLUMN_ALIASES.get(lower)
        if canonical and canonical not in rename_map.values():
            rename_map[col] = canonical
    if rename_map:
        df = df.rename(columns=rename_map)
    return df


def parse_portfolio_csv(file_stream) -> list:
    """Parse a brokerage positions CSV export into a list of holding dicts.

    Supports CSV exports from Fidelity, Schwab, Vanguard, E*Trade,
    TD Ameritrade, Robinhood, Interactive Brokers, Merrill Edge, and others.

    Security: PII columns are stripped client-side before upload (portfolio.js).
    As defense-in-depth, the server also drops any columns not recognized.

    Args:
        file_stream: file-like object (from request.files or open())

    Returns:
        list of dicts with keys: symbol, name, quantity, lastPrice,
        currentValue, costBasis, costBasisPerShare, totalGainDollar,
        totalGainPct, pctOfAccount
    """
    content = file_stream.read()
    if isinstance(content, bytes):
        # Try UTF-8 first (handles BOM via utf-8-sig), fall back to cp1252
        try:
            content = content.decode("utf-8-sig")
        except UnicodeDecodeError:
            content = content.decode("cp1252", errors="replace")

    # Many brokers append disclaimers/totals after blank lines — strip
    # trailing disclaimer sections while preserving data rows.
    # Strategy: collect all non-blank lines, but stop after 2+ consecutive
    # blank lines (signaling end of data / start of disclaimers).
    # Single blank lines within data are tolerated (multi-account CSVs).
    clean_lines = []
    consecutive_blanks = 0
    for line in content.splitlines():
        stripped = line.strip()
        if stripped == "":
            if clean_lines:
                consecutive_blanks += 1
                if consecutive_blanks >= 2:
                    break  # Two consecutive blanks = end of data
            continue
        consecutive_blanks = 0
        clean_lines.append(line.rstrip(","))
    content = "\n".join(clean_lines)

    df = pd.read_csv(io.StringIO(content))

    # Normalize column names (strip whitespace, then map aliases)
    df.columns = df.columns.str.strip()
    df = _normalize_columns(df)

    # Drop sensitive / unused columns (Account Number, Account Name, etc.)
    cols_to_drop = [c for c in df.columns if c not in _CANONICAL_COLUMNS]
    df.drop(columns=cols_to_drop, inplace=True, errors="ignore")

    holdings = []
    for _, row in df.iterrows():
        symbol = str(row.get("Symbol", "")).strip().upper()

        # Clean common symbol artifacts from various brokers
        symbol = symbol.replace("*", "").replace("+", "").strip()

        # Skip empty, cash, money market, pending, totals
        if not symbol or symbol in _SKIP_SYMBOLS:
            continue
        if _SKIP_PATTERN.search(symbol):
            continue
        if symbol.startswith("PENDING"):
            continue
        # Skip summary/total rows that some brokers include
        if any(kw in symbol for kw in ("TOTAL", "CASH & CASH")):
            continue

        name = str(row.get("Description", "")).strip()
        quantity = _clean_money(row.get("Quantity"))
        last_price = _clean_money(row.get("Last Price"))
        current_value = _clean_money(row.get("Current Value"))
        cost_basis = _clean_money(row.get("Cost Basis Total"))
        cost_basis_per_share = _clean_money(row.get("Average Cost Basis"))
        total_gain_dollar = _clean_money(row.get("Total Gain/Loss Dollar"))
        total_gain_pct = _clean_money(row.get("Total Gain/Loss Percent"))
        pct_of_account = _clean_money(row.get("Percent Of Account"))

        # If market value is missing but we have price and quantity, compute it
        if current_value is None and last_price is not None and quantity is not None:
            current_value = round(last_price * quantity, 2)

        # If cost basis is missing but we have per-share cost and quantity, compute it
        if cost_basis is None and cost_basis_per_share is not None and quantity is not None:
            cost_basis = round(cost_basis_per_share * quantity, 2)

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


# Keep backward-compatible alias
parse_fidelity_csv = parse_portfolio_csv


def build_holdings_from_manual(entries: list) -> list:
    """Convert manual entry list into canonical holdings format.

    Args:
        entries: list of dicts with keys: symbol (str), shares (number),
                 costPerShare (optional number)

    Returns:
        list of holding dicts matching parse_portfolio_csv() output format.
    """
    raw = []
    for entry in entries:
        symbol = str(entry.get("symbol", "")).strip().upper()
        if not symbol or not re.match(r"^[A-Z]{1,10}$", symbol):
            continue
        if symbol in _SKIP_SYMBOLS:
            continue

        shares = entry.get("shares")
        try:
            shares = float(shares)
        except (TypeError, ValueError):
            continue
        if shares <= 0:
            continue

        cost_per_share = entry.get("costPerShare")
        if cost_per_share is not None:
            try:
                cost_per_share = float(cost_per_share)
                if cost_per_share < 0:
                    cost_per_share = None
            except (TypeError, ValueError):
                cost_per_share = None

        cost_basis = round(cost_per_share * shares, 2) if cost_per_share else None

        raw.append({
            "symbol": symbol,
            "name": "",
            "quantity": shares,
            "lastPrice": None,
            "currentValue": 0,
            "costBasis": cost_basis,
            "costBasisPerShare": cost_per_share,
            "totalGainDollar": None,
            "totalGainPct": None,
            "pctOfAccount": 0,
        })

    # Consolidate duplicate symbols (sum shares, weighted-average cost)
    merged = {}
    for h in raw:
        sym = h["symbol"]
        if sym not in merged:
            merged[sym] = dict(h)
        else:
            m = merged[sym]
            old_qty = m["quantity"] or 0
            new_qty = h["quantity"] or 0
            total_qty = old_qty + new_qty
            # Weighted-average cost basis per share
            old_cost = m["costBasis"] or 0
            new_cost = h["costBasis"] or 0
            total_cost = old_cost + new_cost
            m["quantity"] = total_qty
            m["costBasis"] = total_cost if total_cost > 0 else None
            m["costBasisPerShare"] = round(total_cost / total_qty, 2) if total_cost > 0 and total_qty > 0 else None

    return list(merged.values())


def _fill_prices_from_enrichment(holdings: list) -> list:
    """Fill price/value fields using currentPrice from yfinance enrichment.

    Called after enrich_holdings() for manual entries only, since manual entry
    has no price data — unlike CSV uploads where the brokerage provides prices.
    """
    for h in holdings:
        price = h.get("currentPrice")
        if price and h.get("quantity"):
            h["lastPrice"] = price
            h["currentValue"] = round(price * h["quantity"], 2)
            # Compute gain/loss if cost basis is available
            cost = h.get("costBasis")
            if cost:
                gain = h["currentValue"] - cost
                h["totalGainDollar"] = round(gain, 2)
                h["totalGainPct"] = round(gain / cost * 100, 2) if cost > 0 else None
        else:
            # Unknown ticker or no price available
            h["lastPrice"] = None
            h["currentValue"] = 0

    # Recalculate pctOfAccount across all holdings
    total_value = sum(h.get("currentValue") or 0 for h in holdings)
    for h in holdings:
        val = h.get("currentValue") or 0
        h["pctOfAccount"] = round(val / total_value * 100, 2) if total_value > 0 else 0

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
    "dividendYield": None,
    "fiveYearAvgDividendYield": None,
    "exDividendDate": None,
    "fiftyTwoWeekHigh": None,
    "fiftyTwoWeekLow": None,
    "beta": None,
    "trailingPE": None,
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
            "dividendYield": info.get("dividendYield"),
            "fiveYearAvgDividendYield": info.get("fiveYearAvgDividendYield"),
            "exDividendDate": info.get("exDividendDate"),
            "fiftyTwoWeekHigh": info.get("fiftyTwoWeekHigh"),
            "fiftyTwoWeekLow": info.get("fiftyTwoWeekLow"),
            "beta": info.get("beta"),
            "trailingPE": info.get("trailingPE"),
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
        for future in as_completed(futures, timeout=45):
            sym = futures[future]
            try:
                results[sym] = future.result(timeout=15)
            except Exception:
                results[sym] = dict(_ENRICHMENT_FALLBACK)
    # Fill in any symbols that didn't complete in time
    for sym in symbols:
        if sym not in results:
            results[sym] = dict(_ENRICHMENT_FALLBACK)

    # Apply results to holdings
    for h in holdings:
        h.update(results.get(h["symbol"], _ENRICHMENT_FALLBACK))

    return holdings


def _compute_health_score(holdings, by_sector, concentration, analyst_overview):
    """Compute a 0-100 portfolio health score from four components."""
    # Diversification (0-25): sector count and evenness
    n_sectors = len([s for s in by_sector if s["sector"] != "Unknown"])
    div_score = min(n_sectors / 8 * 25, 25)

    # Concentration (0-25): deduct for concentrated positions
    conc_penalty = len(concentration) * 8
    conc_score = max(25 - conc_penalty, 0)

    # Analyst sentiment (0-25): buy ratio and upside
    total_covered = analyst_overview.get("totalCovered", 0)
    buys = analyst_overview.get("buys", 0)
    buy_ratio = buys / total_covered if total_covered > 0 else 0.5
    weighted_upside = analyst_overview.get("weightedUpside", 0)
    sentiment_score = min(buy_ratio * 15 + min(weighted_upside, 20) / 20 * 10, 25)

    # Cost health (0-25): portfolio gain/loss ratio
    total_val = sum(h.get("currentValue") or 0 for h in holdings)
    total_cost = sum(h.get("costBasis") or 0 for h in holdings if h.get("costBasis"))
    if total_cost > 0:
        gain_ratio = (total_val - total_cost) / total_cost
        cost_score = min(max((gain_ratio + 0.2) / 0.6 * 25, 0), 25)
    else:
        cost_score = 12.5  # no cost data = neutral

    total = round(div_score + conc_score + sentiment_score + cost_score)
    return {
        "total": min(total, 100),
        "diversification": round(div_score),
        "concentration": round(conc_score),
        "sentiment": round(sentiment_score),
        "costHealth": round(cost_score),
    }


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

    # Concentration risk: individual stocks > 15%, ETFs/funds > 30%
    concentration = []
    for h in holdings:
        pct = h.get("pctOfAccount")
        if pct is None and total_value > 0:
            pct = (h.get("currentValue") or 0) / total_value * 100
        is_fund = h.get("isFund") or h.get("sectorWeights")
        threshold = 30 if is_fund else 15
        if pct and pct > threshold:
            concentration.append({
                "symbol": h["symbol"],
                "name": h.get("name", ""),
                "pct": round(pct, 1),
                "currentValue": h.get("currentValue") or 0,
                "isFund": bool(is_fund),
                "threshold": threshold,
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
        # Filter to qualifying picks: sufficient coverage, positive upside
        qualified = []
        for p in picks:
            if (p.get("nAnalysts", 0) >= 5
                    and p.get("upsidePct", 0) > 0
                    and not p.get("lowCoverage", False)):
                # Add composite score: 60% analyst rating + 40% upside
                rating_num = _RATING_SCORES.get(
                    (p.get("recKey") or "").lower(), 3)
                norm_rating = (rating_num - 1) / 4.0
                norm_upside = min(p.get("upsidePct", 0), 100) / 100.0
                p["score"] = round(0.6 * norm_rating + 0.4 * norm_upside, 3)
                qualified.append(p)
        if not qualified:
            return None
        # Default sort by composite score descending
        qualified.sort(key=lambda x: x.get("score", 0), reverse=True)
        return {
            "industryKey": ind_key,
            "industryLabel": INDUSTRY_LABELS.get(ind_key, ind_key),
            "picks": qualified[:3],
            "allPicks": qualified[:6],
        }

    opportunities = []
    fetch_gaps = gap_industries[:_MAX_GAP_FETCHES]
    with ThreadPoolExecutor(max_workers=6) as pool:
        futures = {pool.submit(_fetch_gap_opportunity, k): k for k in fetch_gaps}
        for future in as_completed(futures, timeout=30):
            try:
                result = future.result(timeout=10)
                if result:
                    opportunities.append(result)
            except Exception:
                pass
    opportunities.sort(key=lambda o: o["industryLabel"])

    # Widget 2: Analyst Consensus Overview (inline, no extra API calls)
    analyst_overview = compute_analyst_overview(holdings)

    # Tax-loss harvesting candidates
    tax_loss_candidates = []
    for h in holdings:
        cost = h.get("costBasis")
        val = h.get("currentValue") or 0
        if cost and cost > 0 and val < cost:
            loss = val - cost
            loss_pct = loss / cost * 100
            if loss <= -100 and loss_pct <= -5:
                low52 = h.get("fiftyTwoWeekLow")
                price = h.get("lastPrice") or h.get("currentPrice")
                near_low = (low52 and price and low52 > 0 and
                            (price - low52) / low52 <= 0.10)
                tax_loss_candidates.append({
                    "symbol": h["symbol"],
                    "name": h.get("name", ""),
                    "currentValue": val,
                    "costBasis": cost,
                    "unrealizedLoss": round(loss, 2),
                    "lossPct": round(loss_pct, 1),
                    "estTaxSavings": round(abs(loss) * 0.24, 2),
                    "nearFiftyTwoWeekLow": bool(near_low),
                })
    tax_loss_candidates.sort(key=lambda x: x["unrealizedLoss"])

    # Portfolio Health Score
    health_score = _compute_health_score(holdings, by_sector, concentration, analyst_overview)

    # Dividend Income Projector
    dividend_holdings = []
    total_annual_dividends = 0
    for h in holdings:
        dy = h.get("dividendYield")
        val = h.get("currentValue") or 0
        if dy and dy > 0 and val > 0:
            annual = round(val * dy, 2)
            total_annual_dividends += annual
            dividend_holdings.append({
                "symbol": h["symbol"],
                "name": h.get("name", ""),
                "currentValue": val,
                "dividendYield": round(dy * 100, 2),
                "annualIncome": annual,
                "monthlyIncome": round(annual / 12, 2),
            })
    dividend_holdings.sort(key=lambda x: x["annualIncome"], reverse=True)
    weighted_yield = (total_annual_dividends / total_value * 100) if total_value > 0 else 0

    dividends = {
        "totalAnnual": round(total_annual_dividends, 2),
        "totalMonthly": round(total_annual_dividends / 12, 2),
        "weightedYield": round(weighted_yield, 2),
        "holdings": dividend_holdings,
        "payingCount": len(dividend_holdings),
        "totalCount": len([h for h in holdings if not h.get("isFund")]),
    }

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
            "dividendYield": h.get("dividendYield"),
            "fiftyTwoWeekHigh": h.get("fiftyTwoWeekHigh"),
            "fiftyTwoWeekLow": h.get("fiftyTwoWeekLow"),
            "beta": h.get("beta"),
            "trailingPE": h.get("trailingPE"),
            "targetMeanPrice": h.get("targetMeanPrice"),
            "recommendationKey": h.get("recommendationKey"),
            "nAnalysts": h.get("nAnalysts"),
            "currentPrice": h.get("currentPrice"),
            "lastPrice": h.get("lastPrice"),
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
        "taxLossCandidates": tax_loss_candidates,
        "healthScore": health_score,
        "dividends": dividends,
    }
