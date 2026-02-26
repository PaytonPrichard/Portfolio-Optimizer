"""Portfolio analysis blueprint — upload Fidelity CSV, get analysis."""

import traceback

from flask import Blueprint, render_template, request

from financials.portfolio import parse_fidelity_csv, enrich_holdings, analyze_portfolio

portfolio_bp = Blueprint("portfolio", __name__)


@portfolio_bp.route("/portfolio")
def portfolio_page():
    """Render the portfolio upload page shell."""
    return render_template("portfolio.html")


@portfolio_bp.route("/api/portfolio/analyze", methods=["POST"])
def portfolio_analyze():
    """Accept a Fidelity CSV upload, parse, enrich, analyze, return HTML."""
    if "csv" not in request.files:
        return '<p class="text-red-500 italic p-4">No file uploaded. Please select a CSV file.</p>', 400

    file = request.files["csv"]
    if not file.filename:
        return '<p class="text-red-500 italic p-4">No file selected.</p>', 400

    if not file.filename.lower().endswith(".csv"):
        return '<p class="text-red-500 italic p-4">Please upload a .csv file (got: ' + file.filename + ').</p>', 400

    try:
        holdings = parse_fidelity_csv(file.stream)
    except Exception as e:
        return '<p class="text-red-500 italic p-4">Could not parse CSV file. Make sure this is a Fidelity positions export.</p>', 400

    if not holdings:
        return '<p class="text-red-500 italic p-4">No valid stock positions found in the CSV. Cash and money market positions are excluded.</p>', 400

    try:
        holdings = enrich_holdings(holdings)
        analysis = analyze_portfolio(holdings)
        return render_template("partials/portfolio_results.html", **analysis)
    except Exception as e:
        traceback.print_exc()
        return f'<p class="text-red-500 italic p-4">Error during analysis: {e.__class__.__name__}: {e}</p>', 500
