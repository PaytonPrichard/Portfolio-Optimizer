"""Home page and search routes."""

from flask import Blueprint, render_template, request, redirect, url_for

from financials.data import resolve_ticker

home_bp = Blueprint("home", __name__)


@home_bp.route("/")
def index():
    return redirect(url_for("portfolio.portfolio_page"))


@home_bp.route("/search")
def search():
    query = request.args.get("q", "").strip()
    if not query:
        return render_template("home.html", query="", candidates=[], no_results=False)

    result = resolve_ticker(query)

    if result["exact"] and result["symbol"]:
        return redirect(url_for("dashboard.dashboard", ticker=result["symbol"]))

    # Ambiguous or no results
    no_results = result.get("no_results", False)
    return render_template(
        "home.html",
        query=query,
        candidates=result.get("candidates", []),
        no_results=no_results,
    )
