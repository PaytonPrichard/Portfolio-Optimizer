"""Backtest results page — shows factor IC and performance stats from the
historical backtest run (client_id=backtest_v1 in portfolio_recommendations).
"""

import traceback

from flask import Blueprint, render_template, jsonify

from financials.backtest import (
    summary_stats, load_cached_ic,
    BACKTEST_CLIENT_ID, PORTFOLIO_TEMPLATES,
)
from financials.rolling_backtest import load_cached_rolling
from financials.outcomes import get_recs_with_outcomes

backtest_bp = Blueprint("backtest", __name__)


@backtest_bp.route("/backtest")
def backtest_page():
    """Static shell; data loads async via /api/backtest/data."""
    return render_template("backtest.html")


@backtest_bp.route("/api/backtest/data")
def backtest_data():
    """Return summary stats + cached IC for the backtest.

    IC is read from the on-disk cache built by `python -m financials.backtest
    cache-ic`. Computing IC inline would require thousands of yfinance calls
    in a cold Flask process cache, taking minutes to respond. If the cache
    is missing, the UI tells the user to run the CLI.
    """
    try:
        summary = summary_stats()
        cached = load_cached_ic()
        ic_payload = {}
        ic_meta = {"cached": False}
        if cached and cached.get("horizons"):
            for horizon, payload in cached["horizons"].items():
                ic_payload[f"{horizon}d"] = payload
            ic_meta = {"cached": True, "computed_at": cached.get("computed_at")}
        return jsonify({
            "summary": summary,
            "ic": ic_payload,
            "ic_meta": ic_meta,
            "templates": list(PORTFOLIO_TEMPLATES.keys()),
        })
    except Exception:
        traceback.print_exc()
        return jsonify({"error": "backtest summary failed"}), 500


@backtest_bp.route("/api/backtest/rolling")
def backtest_rolling():
    """Rolling-rebalance backtest results. Reads from the on-disk cache
    built by `python -m financials.rolling_backtest run`."""
    data = load_cached_rolling()
    if data is None:
        return jsonify({"cached": False})
    return jsonify({"cached": True, **data})


@backtest_bp.route("/api/backtest/recs")
def backtest_recs_fragment():
    """Rendered HTML of all backtest recs with outcomes (uses the portfolio
    history partial — same shape)."""
    try:
        recs = get_recs_with_outcomes(BACKTEST_CLIENT_ID, limit=1000)
    except Exception:
        traceback.print_exc()
        return '<p class="text-red-500 italic p-4">Could not load backtest recs.</p>', 500
    return render_template("partials/portfolio_history_list.html",
                           recs=recs, client_id=BACKTEST_CLIENT_ID)
