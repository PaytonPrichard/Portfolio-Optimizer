"""Portfolio analysis blueprint — upload brokerage CSV or manual entry, get analysis."""

import traceback
from html import escape

from flask import Blueprint, render_template, request, jsonify

from financials.portfolio import (
    parse_portfolio_csv, enrich_holdings, analyze_portfolio,
    build_holdings_from_manual, _fill_prices_from_enrichment,
)
from financials.outcomes import get_recs_with_outcomes

portfolio_bp = Blueprint("portfolio", __name__)


@portfolio_bp.route("/portfolio")
def portfolio_page():
    """Render the portfolio upload page shell."""
    return render_template("portfolio.html")


@portfolio_bp.route("/portfolio/history")
def portfolio_history_page():
    """Shell page for past recommendations and their outcomes. Data loads
    async via /api/portfolio/history once the JS reads client_id from
    localStorage.
    """
    return render_template("portfolio_history.html")


@portfolio_bp.route("/api/portfolio/history", methods=["GET"])
def portfolio_history_data():
    """Return rendered HTML fragment of recs+outcomes for a given client_id."""
    client_id = (request.args.get("client_id") or "").strip()
    if not client_id:
        return '<p class="text-gray-400 italic p-4">No client id. Try running the optimizer at least once first.</p>'
    try:
        recs = get_recs_with_outcomes(client_id, limit=50)
    except Exception:
        traceback.print_exc()
        return '<p class="text-red-500 italic p-4">Could not load history. Try again later.</p>', 500
    return render_template("partials/portfolio_history_list.html", recs=recs, client_id=client_id)


@portfolio_bp.route("/api/portfolio/analyze", methods=["POST"])
def portfolio_analyze():
    """Accept a brokerage CSV upload, parse, enrich, analyze, return HTML."""
    if "csv" not in request.files:
        return '<p class="text-red-500 italic p-4">No file uploaded. Please select a CSV file.</p>', 400

    file = request.files["csv"]
    if not file.filename:
        return '<p class="text-red-500 italic p-4">No file selected.</p>', 400

    if not file.filename.lower().endswith(".csv"):
        return '<p class="text-red-500 italic p-4">Please upload a .csv file (got: ' + escape(file.filename) + ').</p>', 400

    try:
        holdings = parse_portfolio_csv(file.stream)
    except Exception as e:
        return '<p class="text-red-500 italic p-4">Could not parse CSV file. Make sure this is a positions export from your brokerage.</p>', 400

    if not holdings:
        return '<p class="text-red-500 italic p-4">No valid stock positions found in the CSV. Cash and money market positions are excluded.</p>', 400

    try:
        tax_rate_pct = request.form.get("tax_rate", 24, type=float)
        tax_rate = max(0, min(50, tax_rate_pct)) / 100
        holdings = enrich_holdings(holdings)
        analysis = analyze_portfolio(holdings, tax_rate=tax_rate)
        return render_template("partials/portfolio_results.html", **analysis)
    except Exception:
        traceback.print_exc()
        return '<p class="text-red-500 italic p-4">Something went wrong during analysis. Please check your CSV and try again.</p>', 500


@portfolio_bp.route("/api/portfolio/analyze-manual", methods=["POST"])
def portfolio_analyze_manual():
    """Accept a JSON array of manual holdings, enrich, analyze, return HTML."""
    data = request.get_json(silent=True)
    if not data or not isinstance(data, list):
        return '<p class="text-red-500 italic p-4">Invalid request. Please add at least one holding.</p>', 400

    try:
        holdings = build_holdings_from_manual(data)
    except Exception:
        return '<p class="text-red-500 italic p-4">Could not process holdings. Please check your entries.</p>', 400

    if not holdings:
        return '<p class="text-red-500 italic p-4">No valid holdings found. Enter at least one ticker with shares.</p>', 400

    try:
        tax_rate_pct = (data[0] if data else {}).get("_taxRate", 24) if isinstance(data, list) else 24
        try:
            tax_rate_pct = float(tax_rate_pct)
        except (TypeError, ValueError):
            tax_rate_pct = 24
        tax_rate = max(0, min(50, tax_rate_pct)) / 100
        holdings = enrich_holdings(holdings)
        holdings = _fill_prices_from_enrichment(holdings)
        analysis = analyze_portfolio(holdings, tax_rate=tax_rate)
        return render_template("partials/portfolio_results.html", **analysis)
    except Exception:
        traceback.print_exc()
        return '<p class="text-red-500 italic p-4">Something went wrong during analysis. Please check your entries and try again.</p>', 500
