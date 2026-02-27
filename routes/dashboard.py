"""Dashboard route — main financial view for a ticker."""

import json

from flask import Blueprint, render_template, jsonify

import pandas as pd

from financials.data import fetch_data, fetch_recent_news, fetch_industry_peers
from financials.ai import generate_ai_commentary, generate_news_summaries
from financials.formatters import fmt_money, fmt_val

dashboard_bp = Blueprint("dashboard", __name__)


def _prepare_dashboard_data(symbol: str) -> dict:
    """Fetch all data needed for the dashboard and return as a plain dict."""
    info, quarterly_income, history = fetch_data(symbol)

    if not info.get("longName"):
        return None

    company_name = info.get("longName", symbol)

    # News + AI commentary
    news = fetch_recent_news(symbol)
    news = generate_news_summaries(news, company_name)
    commentary = generate_ai_commentary(info, quarterly_income, history, news=news)

    # Current price
    current_price = info.get("currentPrice") or info.get("regularMarketPrice")

    # Year change
    yr_change = None
    if not history.empty:
        try:
            yr_change = ((float(history["Close"].iloc[-1]) - float(history["Close"].iloc[0]))
                         / float(history["Close"].iloc[0])) * 100
        except Exception:
            pass

    # KPI data
    kpis = [
        {"label": "MARKET CAP", "value": fmt_money(info.get("marketCap")), "color": "bg-[#1F6AA5]"},
        {"label": "PRICE", "value": fmt_val(current_price, prefix="$"), "color": "bg-[#217346]"},
        {"label": "P/E RATIO", "value": fmt_val(info.get("trailingPE"), suffix="x"), "color": "bg-[#5C3D8F]"},
        {"label": "GROSS MARGIN", "value": fmt_val(
            info.get("grossMargins", 0) * 100 if info.get("grossMargins") else None,
            suffix="%", decimals=1), "color": "bg-[#0070C0]"},
        {"label": "1-YR CHANGE", "value": (
            f"+{yr_change:.1f}%" if yr_change is not None and yr_change >= 0
            else f"{yr_change:.1f}%" if yr_change is not None
            else "N/A"),
         "color": "bg-green-700" if (yr_change is not None and yr_change >= 0) else "bg-red-700"},
    ]

    # Revenue trend (convert to plain list for Jinja)
    revenue_trend = []
    if "Total Revenue" in quarterly_income.index:
        rev = quarterly_income.loc["Total Revenue"].dropna().iloc[:4]
        rev_chrono = list(rev[::-1].items())
        for i, (date, val) in enumerate(rev_chrono):
            date_label = date.strftime("%b %Y") if hasattr(date, "strftime") else str(date)
            qoq = None
            if i > 0:
                prev_val = float(rev_chrono[i - 1][1])
                if prev_val != 0:
                    qoq = (float(val) - prev_val) / abs(prev_val) * 100
            revenue_trend.append({
                "quarter": date_label,
                "revenue": float(val),
                "revenue_fmt": fmt_money(float(val)),
                "qoq": qoq,
            })

    # Analyst targets
    target_low = info.get("targetLowPrice")
    target_mean = info.get("targetMeanPrice")
    target_high = info.get("targetHighPrice")
    rec_key = (info.get("recommendationKey") or "N/A").upper()
    n_analysts = info.get("numberOfAnalystOpinions", "N/A")

    upside_str = "N/A"
    if target_mean and current_price:
        try:
            upside_pct = ((float(target_mean) - float(current_price)) / float(current_price)) * 100
            direction = "upside" if upside_pct >= 0 else "downside"
            upside_str = f"{'+' if upside_pct >= 0 else ''}{upside_pct:.1f}% implied {direction}"
        except Exception:
            pass

    analyst = {
        "low": f"${target_low:.2f}" if target_low else "N/A",
        "mean": f"${target_mean:.2f}" if target_mean else "N/A",
        "high": f"${target_high:.2f}" if target_high else "N/A",
        "rec": rec_key,
        "n_analysts": n_analysts,
        "upside": upside_str,
    }

    # Health indicators
    dte = info.get("debtToEquity")
    roe = info.get("returnOnEquity")
    cr = info.get("currentRatio")
    qr = info.get("quickRatio")
    short_pct = info.get("shortPercentOfFloat")
    short_ratio = info.get("shortRatio")
    ins_pct = info.get("heldPercentInsiders")
    inst_pct = info.get("heldPercentInstitutions")

    def _fv(v, mult=1, suffix="", decimals=2):
        if v is None or (isinstance(v, float) and pd.isna(v)):
            return "N/A"
        return f"{float(v) * mult:.{decimals}f}{suffix}"

    def _safe_float(v):
        if v is None or (isinstance(v, float) and pd.isna(v)):
            return None
        return float(v)

    def _health_color(val, good_fn):
        """Return (bg_class, signal_text) based on threshold evaluation."""
        if val is None:
            return "bg-gray-500", ""
        if good_fn(val):
            return "bg-green-700", "Good"
        return "bg-red-700", "Caution"

    def _health_neutral(val, good_fn, bad_fn):
        """Three-way: good / neutral / caution."""
        if val is None:
            return "bg-gray-500", ""
        if good_fn(val):
            return "bg-green-700", "Good"
        if bad_fn(val):
            return "bg-red-700", "Caution"
        return "bg-yellow-600", "Fair"

    dte_val = _safe_float(dte) * 0.01 if dte is not None else None  # convert to ratio
    roe_val = _safe_float(roe) * 100 if roe is not None else None   # as percentage
    cr_val = _safe_float(cr)
    qr_val = _safe_float(qr)
    sp_val = _safe_float(short_pct) * 100 if short_pct is not None else None  # as percentage

    # D/E: <1x good, 1-2x fair, >2x caution
    dte_color, dte_sig = _health_neutral(
        dte_val, lambda v: v < 1.0, lambda v: v > 2.0)
    # ROE: >15% good, 0-15% fair, <0% caution
    roe_color, roe_sig = _health_neutral(
        roe_val, lambda v: v > 15, lambda v: v < 0)
    # Current ratio: >1.5 good, 1.0-1.5 fair, <1.0 caution
    cr_color, cr_sig = _health_neutral(
        cr_val, lambda v: v >= 1.5, lambda v: v < 1.0)
    # Quick ratio: >1.0 good, 0.5-1.0 fair, <0.5 caution
    qr_color, qr_sig = _health_neutral(
        qr_val, lambda v: v >= 1.0, lambda v: v < 0.5)
    # Short float: <5% good, 5-15% fair, >15% caution
    sp_color, sp_sig = _health_neutral(
        sp_val, lambda v: v < 5, lambda v: v > 15)

    health = {
        "indicators": [
            {"label": "DEBT / EQUITY", "value": _fv(dte, mult=0.01, suffix="x") if dte else "N/A",
             "color": dte_color, "signal": dte_sig},
            {"label": "RETURN ON EQUITY", "value": _fv(roe, mult=100, suffix="%", decimals=1),
             "color": roe_color, "signal": roe_sig},
            {"label": "CURRENT RATIO", "value": _fv(cr, suffix="x"),
             "color": cr_color, "signal": cr_sig},
            {"label": "QUICK RATIO", "value": _fv(qr, suffix="x"),
             "color": qr_color, "signal": qr_sig},
            {"label": "SHORT % FLOAT", "value": _fv(short_pct, mult=100, suffix="%", decimals=1),
             "color": sp_color, "signal": sp_sig},
        ],
        "ownership": [],
    }
    if ins_pct is not None:
        health["ownership"].append(f"Insider ownership: {ins_pct * 100:.1f}%")
    if inst_pct is not None:
        health["ownership"].append(f"Institutional ownership: {inst_pct * 100:.1f}%")
    if short_ratio is not None:
        health["ownership"].append(f"Short ratio (days to cover): {short_ratio:.1f}")

    # Serialize price history for Chart.js
    price_history_json = "[]"
    if not history.empty:
        try:
            price_data = []
            for date, row in history.iterrows():
                price_data.append({
                    "date": date.strftime("%Y-%m-%d"),
                    "close": round(float(row["Close"]), 2),
                })
            price_history_json = json.dumps(price_data)
        except Exception:
            pass

    return {
        "symbol": symbol,
        "company_name": company_name,
        "sector": info.get("sector", ""),
        "exchange": info.get("exchange", ""),
        "kpis": kpis,
        "revenue_trend": revenue_trend,
        "analyst": analyst,
        "commentary": commentary,
        "health": health,
        "news": news,
        "price_history_json": price_history_json,
        "current_price": current_price,
        "fiftyTwoWeekHigh": info.get("fiftyTwoWeekHigh"),
        "fiftyTwoWeekLow": info.get("fiftyTwoWeekLow"),
    }


@dashboard_bp.route("/dashboard/<ticker>")
def dashboard(ticker):
    ticker = ticker.upper()
    try:
        data = _prepare_dashboard_data(ticker)
    except Exception as exc:
        return render_template("error.html",
                               title="Data Fetch Failed",
                               message=f"Could not retrieve data for {ticker}: {exc}"), 500

    if data is None:
        return render_template("error.html",
                               title="Ticker Not Found",
                               message=f"No data found for ticker '{ticker}'. Check the symbol and try again."), 404

    return render_template("dashboard.html", **data)


@dashboard_bp.route("/api/peers/<ticker>")
def peers_api(ticker):
    """Return server-rendered HTML fragment for peer comparison (async loaded)."""
    ticker = ticker.upper()
    try:
        info, _qi, _hist = fetch_data(ticker)  # reuses cache from dashboard load
        peers = fetch_industry_peers(ticker, info)
    except Exception:
        peers = []

    if not peers:
        return "<p class='text-gray-500 italic p-4'>No peer data available for this industry.</p>"

    return render_template("partials/peers_data.html", peers=peers, symbol=ticker, info=info)
