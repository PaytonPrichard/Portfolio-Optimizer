"""Mosaic Score routes — stock intelligence page and API endpoints."""

import os

from flask import Blueprint, render_template, request, jsonify, redirect

from financials.alpha import (
    compute_alpha_score,
    compute_alpha_scores_batch,
    get_db_stats,
    get_stock_of_the_day_symbol,
    _compute_sector_cycles,
)
from financials.alpha_collector import (
    get_collection_status,
    run_in_background,
    run_cron_batch,
)

alpha_bp = Blueprint("alpha", __name__)


# ── Main page ────────────────────────────────────────────────────────

@alpha_bp.route("/score")
def score_page():
    """Render the Mosaic Score page."""
    ticker = request.args.get("ticker", "").strip().upper()
    try:
        sotd_symbol = get_stock_of_the_day_symbol()
    except Exception:
        sotd_symbol = None
    return render_template("alpha.html", ticker=ticker, sotdSymbol=sotd_symbol)


# Keep /alpha as a redirect so old bookmarks/links still work
@alpha_bp.route("/alpha")
def alpha_redirect():
    ticker = request.args.get("ticker", "")
    if ticker:
        return redirect(f"/score?ticker={ticker}", code=301)
    return redirect("/score", code=301)


# ── Score APIs ───────────────────────────────────────────────────────

@alpha_bp.route("/api/score/compute", methods=["POST"])
def score_compute_api():
    """Compute Mosaic Score for a ticker. Returns HTML fragment."""
    data = request.get_json(silent=True) or {}
    symbol = (data.get("symbol") or "").strip().upper()
    if not symbol:
        return '<p class="text-red-500 text-sm italic p-4">Enter a ticker symbol or company name.</p>', 400

    try:
        result = compute_alpha_score(symbol)
        if not result:
            return f'<p class="text-red-500 text-sm italic p-4">Could not analyze {symbol}. Check the ticker and try again.</p>', 404
        return render_template("partials/alpha_result.html", **result)
    except Exception as e:
        return f'<p class="text-red-500 text-sm italic p-4">Error: {e}</p>', 500


@alpha_bp.route("/api/score/summary/<ticker>")
def score_summary_api(ticker):
    """Return a lightweight JSON score summary for a ticker (used by SOTD card)."""
    ticker = ticker.upper().strip()
    try:
        result = compute_alpha_score(ticker)
        if not result:
            return jsonify({"error": "not found"}), 404
        return jsonify({
            "symbol": result["symbol"],
            "companyName": result.get("companyName", result["symbol"]),
            "alphaScore": result["alphaScore"],
            "conviction": result["conviction"],
            "sector": result.get("sector", ""),
            "price": result.get("price"),
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@alpha_bp.route("/api/score/search")
def score_search_api():
    """Autocomplete search — resolves company names to tickers via yfinance."""
    q = request.args.get("q", "").strip()
    if not q or len(q) < 2:
        return jsonify([])

    try:
        import yfinance as yf
        # yfinance search returns a dict with 'quotes' key
        results = yf.Search(q, max_results=6)
        matches = []
        for item in getattr(results, "quotes", []):
            if item.get("quoteType") in ("EQUITY", "ETF"):
                matches.append({
                    "symbol": item.get("symbol", ""),
                    "name": item.get("shortname") or item.get("longname", ""),
                    "exchange": item.get("exchange", ""),
                })
        return jsonify(matches)
    except Exception:
        return jsonify([])


@alpha_bp.route("/api/score/sector-cycles")
def sector_cycles_api():
    """Return current sector cycle analysis as JSON."""
    try:
        cycles = _compute_sector_cycles()
        return jsonify(cycles)
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ── Collection APIs (used by cron/CLI) ───────────────────────────────

@alpha_bp.route("/api/score/collect", methods=["POST"])
def trigger_collection():
    """Trigger a background data collection action."""
    data = request.get_json(silent=True) or {}
    action = data.get("action", "full")
    valid = ["seed", "refresh", "backfill", "returns", "cycles", "hist-cycles", "full"]
    if action not in valid:
        return jsonify({"error": f"Invalid action. Valid: {valid}"}), 400

    started = run_in_background(action)
    if not started:
        return jsonify({"error": "Collection already running."}), 409

    return jsonify({"status": "started", "action": action})


@alpha_bp.route("/api/score/collect/status")
def collection_status():
    """Return current collection status as JSON."""
    status = get_collection_status()
    return jsonify(status)


# ── Portfolio widget ─────────────────────────────────────────────────

@alpha_bp.route("/api/portfolio/widget/mosaic-scores", methods=["POST"])
def mosaic_scores_widget():
    """Portfolio widget: Mosaic Scores for all holdings."""
    try:
        data = request.get_json(silent=True) or {}
        holdings = data.get("holdings", [])
        symbols = [h["symbol"] for h in holdings
                   if not h.get("isFund") and h.get("symbol")]
        if not symbols:
            return '<p class="text-gray-400 text-sm italic">No individual stocks to score.</p>'

        scores = compute_alpha_scores_batch(symbols[:15])  # cap at 15

        scored = []
        for h in holdings:
            sym = h.get("symbol")
            if sym in scores:
                s = scores[sym]
                scored.append({
                    "symbol": sym,
                    "alphaScore": s["alphaScore"],
                    "conviction": s["conviction"],
                    "subScores": s["subScores"],
                    "currentValue": h.get("currentValue", 0),
                    "pctOfAccount": h.get("pctOfAccount", 0),
                })

        scored.sort(key=lambda x: x["alphaScore"], reverse=True)

        total_value = sum(h.get("currentValue", 0) for h in holdings)
        if total_value > 0 and scored:
            weighted = sum(
                s["alphaScore"] * s["currentValue"] / total_value
                for s in scored if s["currentValue"] > 0
            )
        else:
            weighted = 0

        return render_template("partials/portfolio_alpha_scores.html",
                               scored=scored,
                               weightedAlpha=round(weighted),
                               scoredCount=len(scored))
    except Exception as e:
        return f'<p class="text-red-500 text-sm italic">Mosaic scores unavailable: {e}</p>'


# Keep old widget URL working for cached JS
@alpha_bp.route("/api/portfolio/widget/alpha-scores", methods=["POST"])
def alpha_scores_widget_compat():
    return mosaic_scores_widget()


# ── Vercel Cron ──────────────────────────────────────────────────────

@alpha_bp.route("/api/score/cron", methods=["GET"])
def cron_collect():
    """Vercel Cron endpoint — runs a time-boxed batch of data collection."""
    cron_secret = os.environ.get("CRON_SECRET")
    if cron_secret:
        auth = request.headers.get("Authorization", "")
        if auth != f"Bearer {cron_secret}":
            return jsonify({"error": "Unauthorized"}), 401

    try:
        result = run_cron_batch(max_seconds=50)
        stats = get_db_stats()
        return jsonify({
            "status": "ok",
            "result": result,
            "db": {
                "symbols": stats.get("uniqueSymbols", 0),
                "snapshots": stats.get("totalSnapshots", 0),
            },
        })
    except Exception as e:
        return jsonify({"status": "error", "error": str(e)}), 500
