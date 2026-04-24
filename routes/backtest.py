"""Backtest results page — shows factor IC and performance stats from the
historical backtest run (client_id=backtest_v1 in portfolio_recommendations).
"""

import traceback

from flask import Blueprint, render_template, jsonify

from financials.backtest import (
    summary_stats, compute_ic,
    BACKTEST_CLIENT_ID, PORTFOLIO_TEMPLATES,
)
from financials.outcomes import get_recs_with_outcomes

backtest_bp = Blueprint("backtest", __name__)


@backtest_bp.route("/backtest")
def backtest_page():
    """Static shell; data loads async via /api/backtest/data."""
    return render_template("backtest.html")


@backtest_bp.route("/api/backtest/data")
def backtest_data():
    """Return summary stats + IC for the backtest. Separated from recs list
    to keep the initial payload small.
    """
    try:
        summary = summary_stats()
        ic = {
            "30d": compute_ic(30),
            "90d": compute_ic(90),
            "180d": compute_ic(180),
            "365d": compute_ic(365),
        }
        return jsonify({
            "summary": summary,
            "ic": ic,
            "templates": list(PORTFOLIO_TEMPLATES.keys()),
        })
    except Exception:
        traceback.print_exc()
        return jsonify({"error": "backtest summary failed"}), 500


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
