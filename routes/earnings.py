"""Earnings calendar routes."""

from flask import Blueprint, render_template, request, jsonify

from financials.data import fetch_earnings_dates
from financials.validation import validate_ticker

earnings_bp = Blueprint("earnings", __name__)

# Popular stocks to show by default on the earnings calendar
_DEFAULT_SYMBOLS = [
    "AAPL", "MSFT", "GOOGL", "AMZN", "NVDA", "META", "TSLA",
    "JPM", "V", "JNJ", "WMT", "PG", "UNH", "HD", "DIS",
    "BA", "NFLX", "CRM", "AMD", "INTC", "COST", "PEP", "KO",
]


@earnings_bp.route("/earnings")
def earnings_page():
    """Render the earnings calendar page."""
    return render_template("earnings.html")


@earnings_bp.route("/api/earnings", methods=["POST"])
def earnings_api():
    """Return earnings dates for given symbols (or defaults)."""
    try:
        data = request.get_json(silent=True) or {}
        symbols = data.get("symbols", [])
        if not symbols or not isinstance(symbols, list):
            symbols = _DEFAULT_SYMBOLS
        else:
            symbols = [validate_ticker(s) for s in symbols if isinstance(s, str)]
            symbols = [s for s in symbols if s]
            if not symbols:
                symbols = _DEFAULT_SYMBOLS

        results = fetch_earnings_dates(symbols[:30])
        return jsonify({"earnings": results})
    except Exception:
        return jsonify({"error": "Could not fetch earnings data"}), 500
