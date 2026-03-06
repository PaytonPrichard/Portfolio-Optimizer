"""Portfolio insight widget endpoints — each returns an HTML fragment."""

import json

from flask import Blueprint, render_template, request

from financials.portfolio_widgets import (
    fetch_sector_momentum,
    fetch_holdings_news,
    generate_portfolio_ai_commentary,
    fetch_peer_valuations,
    compute_analyst_overview,
    fetch_ethical_analysis,
    fetch_portfolio_performance,
    compute_correlation_matrix,
)
from financials.portfolio_risk import (
    compute_risk_metrics,
    run_monte_carlo,
    run_stress_tests,
    compute_efficient_frontier,
    compute_fee_analysis,
)
from financials.portfolio_fundamentals import (
    analyze_portfolio_fundamentals,
    compute_factor_exposure,
)

portfolio_widgets_bp = Blueprint("portfolio_widgets", __name__)


@portfolio_widgets_bp.route("/api/portfolio/widget/sector-momentum", methods=["POST"])
def sector_momentum_widget():
    """Return sector momentum heatmap HTML fragment."""
    try:
        data = request.get_json(silent=True) or {}
        portfolio_sectors = data.get("portfolioSectors", {})
        momentum = fetch_sector_momentum(portfolio_sectors)
        return render_template("partials/portfolio_sector_momentum.html",
                               sectors=momentum)
    except Exception as e:
        return f'<p class="text-red-500 text-sm italic">Sector momentum unavailable: {e}</p>'


@portfolio_widgets_bp.route("/api/portfolio/widget/news-digest", methods=["POST"])
def news_digest_widget():
    """Return holdings news digest HTML fragment."""
    try:
        data = request.get_json(silent=True) or {}
        holdings = data.get("holdings", [])
        news = fetch_holdings_news(holdings)
        return render_template("partials/portfolio_news_digest.html",
                               news=news)
    except Exception as e:
        return f'<p class="text-red-500 text-sm italic">News digest unavailable: {e}</p>'


@portfolio_widgets_bp.route("/api/portfolio/widget/ai-commentary", methods=["POST"])
def ai_commentary_widget():
    """Return AI portfolio commentary HTML fragment."""
    try:
        data = request.get_json(silent=True) or {}
        holdings = data.get("holdings", [])
        by_sector = data.get("bySector", [])
        concentration = data.get("concentration", [])
        analyst_overview = data.get("analystOverview", {})
        commentary = generate_portfolio_ai_commentary(
            holdings, by_sector, concentration, analyst_overview)
        return render_template("partials/portfolio_ai_commentary.html",
                               commentary=commentary)
    except Exception as e:
        return f'<p class="text-red-500 text-sm italic">AI commentary unavailable: {e}</p>'


@portfolio_widgets_bp.route("/api/portfolio/widget/peer-valuation", methods=["POST"])
def peer_valuation_widget():
    """Return peer valuation comparison HTML fragment."""
    try:
        data = request.get_json(silent=True) or {}
        holdings = data.get("holdings", [])
        comparisons = fetch_peer_valuations(holdings)
        return render_template("partials/portfolio_peer_valuation.html",
                               comparisons=comparisons)
    except Exception as e:
        return f'<p class="text-red-500 text-sm italic">Peer valuation unavailable: {e}</p>'


@portfolio_widgets_bp.route("/api/portfolio/widget/historical-performance", methods=["POST"])
def historical_performance_widget():
    """Return historical portfolio performance HTML fragment."""
    try:
        data = request.get_json(silent=True) or {}
        holdings = data.get("holdings", [])
        period = data.get("period", "1mo")
        if period not in ("1d", "1mo", "1y"):
            period = "1mo"
        perf = fetch_portfolio_performance(holdings, period)
        return render_template("partials/portfolio_historical_performance.html",
                               **perf)
    except Exception as e:
        return f'<p class="text-red-500 text-sm italic">Historical performance unavailable: {e}</p>'


@portfolio_widgets_bp.route("/api/portfolio/widget/correlation", methods=["POST"])
def correlation_widget():
    """Return correlation matrix heatmap HTML fragment."""
    try:
        data = request.get_json(silent=True) or {}
        holdings = data.get("holdings", [])
        result = compute_correlation_matrix(holdings)
        return render_template("partials/portfolio_correlation.html", **result)
    except Exception as e:
        return f'<p class="text-red-500 text-sm italic">Correlation matrix unavailable: {e}</p>'


@portfolio_widgets_bp.route("/api/portfolio/widget/ethical-investing", methods=["POST"])
def ethical_investing_widget():
    """Return ESG / ethical investing analysis HTML fragment."""
    try:
        data = request.get_json(silent=True) or {}
        holdings = data.get("holdings", [])
        analysis = fetch_ethical_analysis(holdings)
        return render_template("partials/portfolio_ethical_investing.html",
                               **analysis)
    except Exception as e:
        return f'<p class="text-red-500 text-sm italic">ESG analysis unavailable: {e}</p>'


@portfolio_widgets_bp.route("/api/portfolio/widget/risk-dashboard", methods=["POST"])
def risk_dashboard_widget():
    """Return risk metrics dashboard HTML fragment."""
    try:
        data = request.get_json(silent=True) or {}
        holdings = data.get("holdings", [])
        metrics = compute_risk_metrics(holdings)
        return render_template("partials/portfolio_risk_dashboard.html", **metrics)
    except Exception as e:
        return f'<p class="text-red-500 text-sm italic">Risk dashboard unavailable: {e}</p>'


@portfolio_widgets_bp.route("/api/portfolio/widget/monte-carlo", methods=["POST"])
def monte_carlo_widget():
    """Return Monte Carlo simulation HTML fragment."""
    try:
        data = request.get_json(silent=True) or {}
        holdings = data.get("holdings", [])
        years = data.get("years", 10)
        result = run_monte_carlo(holdings, years=years)
        return render_template("partials/portfolio_monte_carlo.html", **result)
    except Exception as e:
        return f'<p class="text-red-500 text-sm italic">Monte Carlo unavailable: {e}</p>'


@portfolio_widgets_bp.route("/api/portfolio/widget/stress-test", methods=["POST"])
def stress_test_widget():
    """Return stress testing HTML fragment."""
    try:
        data = request.get_json(silent=True) or {}
        holdings = data.get("holdings", [])
        scenarios = run_stress_tests(holdings)
        return render_template("partials/portfolio_stress_test.html",
                               scenarios=scenarios)
    except Exception as e:
        return f'<p class="text-red-500 text-sm italic">Stress test unavailable: {e}</p>'


@portfolio_widgets_bp.route("/api/portfolio/widget/optimizer", methods=["POST"])
def optimizer_widget():
    """Return portfolio optimizer / rebalancing HTML fragment."""
    try:
        data = request.get_json(silent=True) or {}
        holdings = data.get("holdings", [])
        result = compute_efficient_frontier(holdings)
        if result is None:
            return '<p class="text-gray-400 text-sm italic">Need at least 2 holdings for portfolio optimization.</p>'
        return render_template("partials/portfolio_optimizer.html", **result)
    except Exception as e:
        return f'<p class="text-red-500 text-sm italic">Optimizer unavailable: {e}</p>'


@portfolio_widgets_bp.route("/api/portfolio/widget/fee-analysis", methods=["POST"])
def fee_analysis_widget():
    """Return fee drag analysis HTML fragment."""
    try:
        data = request.get_json(silent=True) or {}
        holdings = data.get("holdings", [])
        result = compute_fee_analysis(holdings)
        return render_template("partials/portfolio_fee_analysis.html", **result)
    except Exception as e:
        return f'<p class="text-red-500 text-sm italic">Fee analysis unavailable: {e}</p>'


@portfolio_widgets_bp.route("/api/portfolio/widget/factor-exposure", methods=["POST"])
def factor_exposure_widget():
    """Return factor exposure analysis HTML fragment."""
    try:
        data = request.get_json(silent=True) or {}
        holdings = data.get("holdings", [])
        result = compute_factor_exposure(holdings)
        return render_template("partials/portfolio_factor_exposure.html", **result)
    except Exception as e:
        return f'<p class="text-red-500 text-sm italic">Factor exposure unavailable: {e}</p>'


@portfolio_widgets_bp.route("/api/portfolio/widget/fundamentals", methods=["POST"])
def fundamentals_widget():
    """Return fundamental scorecard HTML fragment."""
    try:
        data = request.get_json(silent=True) or {}
        holdings = data.get("holdings", [])
        result = analyze_portfolio_fundamentals(holdings)
        return render_template("partials/portfolio_fundamentals.html", **result)
    except Exception as e:
        return f'<p class="text-red-500 text-sm italic">Fundamental analysis unavailable: {e}</p>'
