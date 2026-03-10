"""Alpha Score routes — stock intelligence page and API endpoints."""

import os

from flask import Blueprint, render_template, request, jsonify

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


@alpha_bp.route("/alpha")
def alpha_page():
    """Render the Alpha Score page."""
    ticker = request.args.get("ticker", "").strip().upper()
    sotd_symbol = get_stock_of_the_day_symbol()
    return render_template("alpha.html", ticker=ticker, sotdSymbol=sotd_symbol)


@alpha_bp.route("/api/alpha/score", methods=["POST"])
def alpha_score_api():
    """Compute Alpha Score for a ticker. Returns HTML fragment."""
    data = request.get_json(silent=True) or {}
    symbol = (data.get("symbol") or "").strip().upper()
    if not symbol:
        return '<p class="text-red-500 text-sm italic p-4">Enter a ticker symbol.</p>', 400

    try:
        result = compute_alpha_score(symbol)
        if not result:
            return f'<p class="text-red-500 text-sm italic p-4">Could not analyze {symbol}. Check the ticker and try again.</p>', 404
        return render_template("partials/alpha_result.html", **result)
    except Exception as e:
        return f'<p class="text-red-500 text-sm italic p-4">Error: {e}</p>', 500


@alpha_bp.route("/api/alpha/summary/<ticker>")
def alpha_summary_api(ticker):
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


@alpha_bp.route("/api/alpha/sector-cycles")
def sector_cycles_api():
    """Return current sector cycle analysis as JSON."""
    try:
        cycles = _compute_sector_cycles()
        return jsonify(cycles)
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@alpha_bp.route("/api/alpha/collect", methods=["POST"])
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


@alpha_bp.route("/api/alpha/collect/status")
def collection_status():
    """Return current collection status as JSON."""
    status = get_collection_status()
    return jsonify(status)


@alpha_bp.route("/api/portfolio/widget/alpha-scores", methods=["POST"])
def alpha_scores_widget():
    """Portfolio widget: Alpha Scores for all holdings."""
    try:
        data = request.get_json(silent=True) or {}
        holdings = data.get("holdings", [])
        symbols = [h["symbol"] for h in holdings
                   if not h.get("isFund") and h.get("symbol")]
        if not symbols:
            return '<p class="text-gray-400 text-sm italic">No individual stocks to score.</p>'

        scores = compute_alpha_scores_batch(symbols[:15])  # cap at 15

        # Merge with holding data for portfolio context
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
            weighted_alpha = sum(
                s["alphaScore"] * s["currentValue"] / total_value
                for s in scored if s["currentValue"] > 0
            )
        else:
            weighted_alpha = 0

        return render_template("partials/portfolio_alpha_scores.html",
                               scored=scored,
                               weightedAlpha=round(weighted_alpha),
                               scoredCount=len(scored))
    except Exception as e:
        return f'<p class="text-red-500 text-sm italic">Alpha scores unavailable: {e}</p>'


@alpha_bp.route("/api/alpha/cron", methods=["GET"])
def cron_collect():
    """Vercel Cron endpoint — runs a time-boxed batch of data collection.

    Secured by CRON_SECRET env var: Vercel sends it as an Authorization
    header on cron invocations.  Returns 401 if the secret doesn't match.
    """
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
