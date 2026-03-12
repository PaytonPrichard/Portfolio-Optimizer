"""Stock tracker routes — watchlist + alert APIs."""

from flask import Blueprint, render_template, request, jsonify

from financials.data import fetch_quote
from financials.validation import validate_ticker

tracker_bp = Blueprint("tracker", __name__)


@tracker_bp.route("/tracker")
def tracker():
    return render_template("tracker.html")


@tracker_bp.route("/api/quote/<ticker>")
def quote(ticker):
    """Return current quote for a single ticker (cached 1 min)."""
    ticker = validate_ticker(ticker)
    if not ticker:
        return jsonify({"error": "Invalid ticker format."}), 400
    try:
        result = fetch_quote(ticker)
        if result is None:
            return jsonify({"error": f"No data for {ticker}"}), 404
        return jsonify(result)
    except Exception:
        return jsonify({"error": "Failed to fetch quote."}), 500


@tracker_bp.route("/api/quotes", methods=["POST"])
def quotes_batch():
    """Return quotes for multiple tickers (max 20, each cached 1 min)."""
    data = request.get_json(silent=True) or {}
    tickers = data.get("tickers", [])[:20]

    results = []
    for raw in tickers:
        ticker = validate_ticker(raw)
        if not ticker:
            continue
        try:
            result = fetch_quote(ticker)
            if result is None:
                results.append({"symbol": ticker, "error": "Not found"})
            else:
                results.append(result)
        except Exception:
            results.append({"symbol": ticker, "error": "Fetch failed"})

    return jsonify(results)
