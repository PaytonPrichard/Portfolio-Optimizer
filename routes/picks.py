"""Analyst Picks route — discover investment ideas by industry."""

from flask import Blueprint, render_template

from financials.data import (
    fetch_industry_picks,
    ALLOWED_INDUSTRIES,
    INDUSTRY_LABELS,
    FINNHUB_KEY,
)

picks_bp = Blueprint("picks", __name__)


@picks_bp.route("/picks")
def picks_page():
    """Render the Analyst Picks page shell."""
    industries = [
        {"key": k, "label": INDUSTRY_LABELS.get(k, k)}
        for k in sorted(ALLOWED_INDUSTRIES, key=lambda k: INDUSTRY_LABELS.get(k, k))
    ]
    return render_template("picks.html", industries=industries)


@picks_bp.route("/api/picks/<industry_key>")
def picks_api(industry_key):
    """Return server-rendered HTML fragment with analyst picks for an industry."""
    if industry_key not in ALLOWED_INDUSTRIES:
        return '<p class="text-red-500 italic p-4">Invalid industry selection.</p>', 400

    picks = fetch_industry_picks(industry_key)

    if not picks:
        return '<p class="text-gray-500 italic p-4">No analyst data available for this industry.</p>'

    label = INDUSTRY_LABELS.get(industry_key, industry_key)
    return render_template(
        "partials/picks_results.html",
        picks=picks,
        industry_label=label,
        has_finnhub=bool(FINNHUB_KEY),
    )
