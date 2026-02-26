"""Data fetching functions (yfinance) with TTL caching."""

import os
from datetime import datetime

import pandas as pd
import requests
import yfinance as yf

from . import cache

FINNHUB_KEY = os.environ.get("FINNHUB_API_KEY", "").strip()


def fetch_finnhub_recommendations(symbol: str) -> dict | None:
    """Fetch the most recent analyst recommendation trend from Finnhub.

    Returns a dict like {"buy": 12, "hold": 8, "sell": 2, "period": "2024-12"}
    or None on failure / missing API key.  Cached 10 min.
    """
    if not FINNHUB_KEY:
        return None

    cache_key = f"finnhub_rec:{symbol.upper()}"
    cached = cache.get(cache_key)
    if cached is not None:
        return cached

    try:
        resp = requests.get(
            "https://finnhub.io/api/v1/stock/recommendation",
            params={"symbol": symbol.upper(), "token": FINNHUB_KEY},
            timeout=5,
        )
        resp.raise_for_status()
        data = resp.json()
        if not data:
            return None
        latest = data[0]  # most recent period
        result = {
            "buy": latest.get("buy", 0) + latest.get("strongBuy", 0),
            "hold": latest.get("hold", 0),
            "sell": latest.get("sell", 0) + latest.get("strongSell", 0),
            "period": latest.get("period", ""),
        }
        cache.put(cache_key, result, ttl=cache.PEERS_TTL)
        return result
    except Exception:
        return None


def _check_rec_discrepancy(yf_rec_key: str, finnhub_rec: dict | None) -> bool:
    """Return True when yfinance and Finnhub clearly disagree on sentiment."""
    if finnhub_rec is None:
        return False

    total = finnhub_rec["buy"] + finnhub_rec["hold"] + finnhub_rec["sell"]
    if total == 0:
        return False

    buy_pct = finnhub_rec["buy"] / total
    sell_pct = finnhub_rec["sell"] / total

    k = (yf_rec_key or "").lower().replace(" ", "_")

    # yfinance says bullish but less than 40% of Finnhub analysts agree
    if k in ("buy", "strong_buy") and buy_pct < 0.40:
        return True
    # yfinance says bearish but less than 40% of Finnhub analysts agree
    if k in ("sell", "underperform", "strong_sell") and sell_pct < 0.40:
        return True
    # yfinance says hold but Finnhub is clearly directional (>60% one way)
    if k == "hold" and (buy_pct > 0.60 or sell_pct > 0.60):
        return True

    return False


def fetch_data(symbol: str):
    """Return (info dict, quarterly income DataFrame, 1-yr price history).
    Results are cached for 5 minutes."""
    key = f"data:{symbol.upper()}"
    cached = cache.get(key)
    if cached:
        return cached

    ticker = yf.Ticker(symbol)
    info = ticker.info or {}
    quarterly_income = ticker.quarterly_income_stmt
    history = ticker.history(period="1y")
    result = (info, quarterly_income, history)
    cache.put(key, result, ttl=cache.DEFAULT_TTL)
    return result


def fetch_recent_news(symbol: str, n: int = 8) -> list:
    """Return up to n recent news dicts for the ticker. Cached 5 min."""
    key = f"news:{symbol.upper()}"
    cached = cache.get(key)
    if cached is not None:
        return cached

    try:
        raw = yf.Ticker(symbol).news or []
        out = []
        for item in raw[:n]:
            title = item.get("title") or item.get("content", {}).get("title", "")
            publisher = item.get("publisher") or item.get("content", {}).get("provider", {}).get("displayName", "")
            ts = item.get("providerPublishTime") or item.get("content", {}).get("pubDate")
            if ts:
                try:
                    date_str = datetime.fromtimestamp(int(ts)).strftime("%b %d, %Y")
                except Exception:
                    date_str = str(ts)[:10]
            else:
                date_str = ""
            if title:
                out.append({"title": title, "publisher": publisher, "date": date_str})
        cache.put(key, out, ttl=cache.DEFAULT_TTL)
        return out
    except Exception:
        return []


def fetch_industry_peers(symbol: str, info: dict, max_peers: int = 12) -> list:
    """
    Return a list of dicts with key metrics for top companies
    in the same yfinance industry as `symbol`. Cached 10 min.
    """
    key = f"peers:{symbol.upper()}"
    cached = cache.get(key)
    if cached is not None:
        return cached

    industry_key = info.get("industryKey")
    if not industry_key:
        return []

    try:
        industry_obj = yf.Industry(industry_key)
        top_df = industry_obj.top_companies
    except Exception:
        return []

    if top_df is None or (hasattr(top_df, "empty") and top_df.empty):
        return []

    try:
        tickers = list(top_df.index)
    except Exception:
        tickers = []

    sym_upper = symbol.upper()
    if sym_upper not in [t.upper() for t in tickers]:
        tickers = [sym_upper] + tickers
    tickers = tickers[:max_peers]

    peers = []
    for t_sym in tickers:
        try:
            d = yf.Ticker(t_sym).info or {}
            name = d.get("longName") or d.get("shortName")
            if not name:
                continue
            peers.append({
                "symbol":             t_sym,
                "name":               name,
                "marketCap":          d.get("marketCap"),
                "trailingPE":         d.get("trailingPE"),
                "forwardPE":          d.get("forwardPE"),
                "grossMargins":       d.get("grossMargins"),
                "profitMargins":      d.get("profitMargins"),
                "revenueGrowth":      d.get("revenueGrowth"),
                "fiftyTwoWeekChange": d.get("52WeekChange"),
                "is_target":          t_sym.upper() == sym_upper,
            })
        except Exception:
            pass

    cache.put(key, peers, ttl=cache.PEERS_TTL)
    return peers


def fetch_quote(symbol: str) -> dict:
    """Return a quick quote dict for a single ticker. Cached 1 min."""
    key = f"quote:{symbol.upper()}"
    cached = cache.get(key)
    if cached is not None:
        return cached

    t = yf.Ticker(symbol)
    info = t.info or {}
    name = info.get("longName") or info.get("shortName")
    if not name:
        return None

    price = info.get("currentPrice") or info.get("regularMarketPrice")
    prev_close = info.get("previousClose") or info.get("regularMarketPreviousClose")
    change = None
    change_pct = None
    if price and prev_close and prev_close != 0:
        change = round(price - prev_close, 2)
        change_pct = round((change / prev_close) * 100, 2)

    result = {
        "symbol": symbol.upper(),
        "name": name,
        "price": price,
        "previousClose": prev_close,
        "change": change,
        "changePct": change_pct,
        "marketCap": info.get("marketCap"),
    }
    cache.put(key, result, ttl=cache.QUOTE_TTL)
    return result


ALLOWED_INDUSTRIES = {
    # Technology
    "semiconductors", "software-infrastructure", "software-application",
    "internet-content-information", "consumer-electronics",
    # Healthcare
    "drug-manufacturers-general", "biotechnology",
    "medical-devices", "healthcare-plans",
    # Financials
    "banks-diversified", "asset-management", "insurance-diversified",
    # Energy
    "oil-gas-integrated", "oil-gas-e-p", "oil-gas-equipment-services",
    # Utilities
    "utilities-regulated-electric", "utilities-renewable",
    # Industrials
    "aerospace-defense", "railroads", "farm-heavy-construction-machinery",
    # Consumer Cyclical
    "auto-manufacturers", "specialty-retail", "restaurants",
    "home-improvement-retail",
    # Consumer Defensive
    "household-personal-products", "discount-stores",
    # Communication Services
    "telecom-services",
    # Real Estate
    "reit-specialty",
    # Basic Materials
    "specialty-chemicals",
}

INDUSTRY_LABELS = {
    # Technology
    "semiconductors": "Semiconductors",
    "software-infrastructure": "Software - Infrastructure",
    "software-application": "Software - Application",
    "internet-content-information": "Internet Content & Information",
    "consumer-electronics": "Consumer Electronics",
    # Healthcare
    "drug-manufacturers-general": "Drug Manufacturers",
    "biotechnology": "Biotechnology",
    "medical-devices": "Medical Devices",
    "healthcare-plans": "Healthcare Plans",
    # Financials
    "banks-diversified": "Banks - Diversified",
    "asset-management": "Asset Management",
    "insurance-diversified": "Insurance - Diversified",
    # Energy
    "oil-gas-integrated": "Oil & Gas Integrated",
    "oil-gas-e-p": "Oil & Gas E&P",
    "oil-gas-equipment-services": "Oil & Gas Equipment & Services",
    # Utilities
    "utilities-regulated-electric": "Utilities - Regulated Electric",
    "utilities-renewable": "Utilities - Renewable",
    # Industrials
    "aerospace-defense": "Aerospace & Defense",
    "railroads": "Railroads",
    "farm-heavy-construction-machinery": "Farm & Heavy Construction Machinery",
    # Consumer Cyclical
    "auto-manufacturers": "Auto Manufacturers",
    "specialty-retail": "Specialty Retail",
    "restaurants": "Restaurants",
    "home-improvement-retail": "Home Improvement Retail",
    # Consumer Defensive
    "household-personal-products": "Household & Personal Products",
    "discount-stores": "Discount Stores",
    # Communication Services
    "telecom-services": "Telecom Services",
    # Real Estate
    "reit-specialty": "REITs - Specialty",
    # Basic Materials
    "specialty-chemicals": "Specialty Chemicals",
}


def fetch_industry_picks(industry_key: str, max_companies: int = 15) -> list:
    """
    Return a list of dicts with analyst consensus data for top companies
    in the given industry. Cached 10 min.
    """
    if industry_key not in ALLOWED_INDUSTRIES:
        return []

    cache_key = f"industry_picks:{industry_key}"
    cached = cache.get(cache_key)
    if cached is not None:
        return cached

    try:
        industry_obj = yf.Industry(industry_key)
        top_df = industry_obj.top_companies
    except Exception:
        return []

    if top_df is None or (hasattr(top_df, "empty") and top_df.empty):
        return []

    try:
        tickers = list(top_df.index)[:max_companies]
    except Exception:
        return []

    picks = []
    for sym in tickers:
        try:
            ticker_obj = yf.Ticker(sym)
            d = ticker_obj.info or {}
            name = d.get("longName") or d.get("shortName")
            price = d.get("currentPrice") or d.get("regularMarketPrice")

            # --- Improvement 4: prefer analyst_price_targets property ---
            target = None
            target_low = None
            target_high = None
            try:
                apt = ticker_obj.analyst_price_targets
                if apt is not None and isinstance(apt, dict):
                    target = apt.get("mean") or apt.get("current")
                    target_low = apt.get("low")
                    target_high = apt.get("high")
            except Exception:
                pass
            # Fallback to .info fields
            if not target:
                target = d.get("targetMeanPrice")
            if not target_low:
                target_low = d.get("targetLowPrice")
            if not target_high:
                target_high = d.get("targetHighPrice")

            n_analysts = d.get("numberOfAnalystOpinions")

            if not name or not price or not target:
                continue

            # --- Improvement 1: sanity checks ---
            if price <= 0 or target <= 0:
                continue
            if not n_analysts or n_analysts < 1:
                continue

            upside_pct = (target - price) / price * 100

            if upside_pct > 200 or upside_pct < -80:
                continue

            rec_key = d.get("recommendationKey") or "N/A"

            pick = {
                "symbol": sym,
                "name": name,
                "currentPrice": price,
                "targetPrice": target,
                "targetLow": target_low,
                "targetHigh": target_high,
                "upsidePct": round(upside_pct, 1),
                "recKey": rec_key,
                "nAnalysts": n_analysts,
                "marketCap": d.get("marketCap"),
                "lowCoverage": n_analysts < 3,
                "recDiscrepancy": False,
            }

            # --- Improvement 3: Finnhub cross-reference ---
            if FINNHUB_KEY:
                fh = fetch_finnhub_recommendations(sym)
                pick["recDiscrepancy"] = _check_rec_discrepancy(rec_key, fh)

            picks.append(pick)
        except Exception:
            pass

    picks.sort(key=lambda x: x["upsidePct"], reverse=True)
    cache.put(cache_key, picks, ttl=cache.PEERS_TTL)
    return picks


def resolve_ticker(query: str) -> dict:
    """
    Accept a company name or ticker and return a result dict:
      {"symbol": "AAPL", "exact": True}                     - exact match
      {"symbol": None, "candidates": [...], "exact": False}  - ambiguous
    """
    query = query.strip()
    if not query:
        return {"symbol": None, "candidates": [], "exact": False}

    # Fast path: looks like a ticker already
    looks_like_ticker = len(query) <= 6 and " " not in query
    if looks_like_ticker:
        test = yf.Ticker(query.upper())
        if test.info and test.info.get("longName"):
            return {"symbol": query.upper(), "exact": True}

    # Name search
    try:
        results = yf.Search(query, max_results=8, news_count=0)
        quotes = [q for q in results.quotes if q.get("quoteType") == "EQUITY"]
    except Exception:
        quotes = []

    if not quotes:
        # Only fall back if the input looks like a valid ticker
        if looks_like_ticker:
            return {"symbol": query.upper(), "exact": True}
        return {"symbol": None, "candidates": [], "exact": False, "no_results": True}

    # If exact symbol match found in results, auto-select
    upper_query = query.upper()
    for q in quotes:
        if q.get("symbol", "").upper() == upper_query:
            return {"symbol": upper_query, "exact": True}

    # If only one result, auto-select it
    if len(quotes) == 1:
        return {"symbol": quotes[0]["symbol"], "exact": True}

    # Multiple candidates
    candidates = []
    for q in quotes[:5]:
        candidates.append({
            "symbol": q.get("symbol", ""),
            "name": q.get("longname") or q.get("shortname") or "N/A",
            "exchange": q.get("exchange", ""),
        })

    return {"symbol": None, "candidates": candidates, "exact": False}
