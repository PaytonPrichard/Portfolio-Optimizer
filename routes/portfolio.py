"""Portfolio analysis blueprint — upload brokerage CSV or manual entry, get analysis."""

import traceback

from flask import Blueprint, render_template, request, jsonify

from financials.portfolio import (
    parse_portfolio_csv, enrich_holdings, analyze_portfolio,
    build_holdings_from_manual, _fill_prices_from_enrichment,
)

portfolio_bp = Blueprint("portfolio", __name__)


@portfolio_bp.route("/portfolio")
def portfolio_page():
    """Render the portfolio upload page shell."""
    return render_template("portfolio.html")


@portfolio_bp.route("/api/portfolio/analyze", methods=["POST"])
def portfolio_analyze():
    """Accept a brokerage CSV upload, parse, enrich, analyze, return HTML."""
    if "csv" not in request.files:
        return '<p class="text-red-500 italic p-4">No file uploaded. Please select a CSV file.</p>', 400

    file = request.files["csv"]
    if not file.filename:
        return '<p class="text-red-500 italic p-4">No file selected.</p>', 400

    if not file.filename.lower().endswith(".csv"):
        return '<p class="text-red-500 italic p-4">Please upload a .csv file (got: ' + file.filename + ').</p>', 400

    try:
        holdings = parse_portfolio_csv(file.stream)
    except Exception as e:
        return '<p class="text-red-500 italic p-4">Could not parse CSV file. Make sure this is a positions export from your brokerage.</p>', 400

    if not holdings:
        return '<p class="text-red-500 italic p-4">No valid stock positions found in the CSV. Cash and money market positions are excluded.</p>', 400

    try:
        holdings = enrich_holdings(holdings)
        analysis = analyze_portfolio(holdings)
        return render_template("partials/portfolio_results.html", **analysis)
    except Exception as e:
        traceback.print_exc()
        return f'<p class="text-red-500 italic p-4">Error during analysis: {e.__class__.__name__}: {e}</p>', 500


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
        holdings = enrich_holdings(holdings)
        holdings = _fill_prices_from_enrichment(holdings)
        analysis = analyze_portfolio(holdings)
        return render_template("partials/portfolio_results.html", **analysis)
    except Exception as e:
        traceback.print_exc()
        return f'<p class="text-red-500 italic p-4">Error during analysis: {e.__class__.__name__}: {e}</p>', 500
