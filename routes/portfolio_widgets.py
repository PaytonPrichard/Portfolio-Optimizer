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
from financials.portfolio_optimizer import black_litterman_optimize, ETF_SECTOR_KEY
from financials.recommendations import insert_recommendation
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
    except Exception:
        return '<p class="text-red-500 text-sm italic">Sector momentum temporarily unavailable.</p>'


@portfolio_widgets_bp.route("/api/portfolio/widget/news-digest", methods=["POST"])
def news_digest_widget():
    """Return holdings news digest HTML fragment."""
    try:
        data = request.get_json(silent=True) or {}
        holdings = data.get("holdings", [])
        news = fetch_holdings_news(holdings)
        return render_template("partials/portfolio_news_digest.html",
                               news=news)
    except Exception:
        return '<p class="text-red-500 text-sm italic">News digest temporarily unavailable.</p>'


@portfolio_widgets_bp.route("/api/portfolio/widget/ai-commentary", methods=["POST"])
def ai_commentary_widget():
    """Return AI portfolio commentary HTML fragment."""
    try:
        data = request.get_json(silent=True) or {}
        holdings = data.get("holdings", [])
        by_sector = data.get("bySector", [])
        concentration = data.get("concentration", [])
        analyst_overview = data.get("analystOverview", {})
        holdings_news = data.get("holdingsNews", [])
        commentary = generate_portfolio_ai_commentary(
            holdings, by_sector, concentration, analyst_overview,
            holdings_news=holdings_news)
        return render_template("partials/portfolio_ai_commentary.html",
                               commentary=commentary)
    except Exception:
        return '<p class="text-red-500 text-sm italic">AI commentary temporarily unavailable.</p>'


@portfolio_widgets_bp.route("/api/portfolio/widget/peer-valuation", methods=["POST"])
def peer_valuation_widget():
    """Return peer valuation comparison HTML fragment."""
    try:
        data = request.get_json(silent=True) or {}
        holdings = data.get("holdings", [])
        comparisons = fetch_peer_valuations(holdings)
        return render_template("partials/portfolio_peer_valuation.html",
                               comparisons=comparisons)
    except Exception:
        return '<p class="text-red-500 text-sm italic">Peer valuation temporarily unavailable.</p>'


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
    except Exception:
        return '<p class="text-red-500 text-sm italic">Historical performance temporarily unavailable.</p>'


@portfolio_widgets_bp.route("/api/portfolio/widget/correlation", methods=["POST"])
def correlation_widget():
    """Return correlation matrix heatmap HTML fragment."""
    try:
        data = request.get_json(silent=True) or {}
        holdings = data.get("holdings", [])
        result = compute_correlation_matrix(holdings)
        return render_template("partials/portfolio_correlation.html", **result)
    except Exception:
        return '<p class="text-red-500 text-sm italic">Correlation matrix temporarily unavailable.</p>'


@portfolio_widgets_bp.route("/api/portfolio/widget/ethical-investing", methods=["POST"])
def ethical_investing_widget():
    """Return ESG / ethical investing analysis HTML fragment."""
    try:
        data = request.get_json(silent=True) or {}
        holdings = data.get("holdings", [])
        analysis = fetch_ethical_analysis(holdings)
        return render_template("partials/portfolio_ethical_investing.html",
                               **analysis)
    except Exception:
        return '<p class="text-red-500 text-sm italic">ESG analysis temporarily unavailable.</p>'


@portfolio_widgets_bp.route("/api/portfolio/widget/risk-dashboard", methods=["POST"])
def risk_dashboard_widget():
    """Return risk metrics dashboard HTML fragment."""
    try:
        data = request.get_json(silent=True) or {}
        holdings = data.get("holdings", [])
        metrics = compute_risk_metrics(holdings)
        return render_template("partials/portfolio_risk_dashboard.html", **metrics)
    except Exception:
        return '<p class="text-red-500 text-sm italic">Risk dashboard temporarily unavailable.</p>'


@portfolio_widgets_bp.route("/api/portfolio/widget/monte-carlo", methods=["POST"])
def monte_carlo_widget():
    """Return Monte Carlo simulation HTML fragment."""
    try:
        data = request.get_json(silent=True) or {}
        holdings = data.get("holdings", [])
        years = data.get("years", 10)
        result = run_monte_carlo(holdings, years=years)
        return render_template("partials/portfolio_monte_carlo.html", **result)
    except Exception:
        return '<p class="text-red-500 text-sm italic">Monte Carlo temporarily unavailable.</p>'


@portfolio_widgets_bp.route("/api/portfolio/widget/stress-test", methods=["POST"])
def stress_test_widget():
    """Return stress testing HTML fragment."""
    try:
        data = request.get_json(silent=True) or {}
        holdings = data.get("holdings", [])
        scenarios = run_stress_tests(holdings)
        return render_template("partials/portfolio_stress_test.html",
                               scenarios=scenarios)
    except Exception:
        return '<p class="text-red-500 text-sm italic">Stress test temporarily unavailable.</p>'


MODE_CONSTRAINTS = {
    "diversification": {},  # use optimizer defaults (tight)
    "return_max": {
        "max_sector": 0.50,
        "max_turnover": 0.40,
    },
}


def _compliance_narrative(result):
    """If the rebalance is driven by bringing current back into constraint
    compliance (not by Alpha signals), generate a plain-English explanation.
    """
    report = result.get("diagnostics", {}).get("constraintReport", {})
    forced = report.get("min_turnover_forced", 0) or 0
    if forced <= 0.02:
        return None
    sectors = result.get("diagnostics", {}).get("sectors", {})
    cur_weights = {w["symbol"]: w["weight"] / 100 for w in result["current"]["weights"]}
    cur_sector_totals = {}
    for sym, sec in sectors.items():
        if sec == ETF_SECTOR_KEY:
            continue  # ETF bucket is exempt from sector cap
        cur_sector_totals[sec] = cur_sector_totals.get(sec, 0) + cur_weights.get(sym, 0)
    max_sector_cap = result["diagnostics"]["constraints"]["max_sector"]
    over = [(s, v) for s, v in cur_sector_totals.items() if v > max_sector_cap + 0.005]
    if not over:
        return None
    over.sort(key=lambda x: -x[1])
    sec, val = over[0]
    excess_pp = round((val - max_sector_cap) * 100, 1)
    return (
        f"Your {sec} allocation is {excess_pp} percentage points above the "
        f"{int(max_sector_cap * 100)}% target. Most of this rebalance is "
        f"bringing sector exposure back into range."
    )


def _build_rec_payload(client_id, holdings, result, mode):
    return {
        "client_id": client_id,
        "total_value": result["totalValue"],
        "holdings": holdings,
        "suggested_weights": result["optimal"]["weights"],
        "current_return_pct": result["current"]["return"],
        "current_volatility_pct": result["current"]["volatility"],
        "current_sharpe": result["current"]["sharpe"],
        "expected_return_pct": result["optimal"]["return"],
        "expected_volatility_pct": result["optimal"]["volatility"],
        "expected_sharpe": result["optimal"]["sharpe"],
        "constraint_params": {**result["diagnostics"]["constraints"], "mode": mode},
        "factor_weights": result.get("factorWeights", {}),
        "regime_vix": (result.get("regime") or {}).get("vix"),
        "regime_yield_curve": (result.get("regime") or {}).get("yield_curve_10y_3m"),
        "regime_snapshot": result.get("regime"),
        "attribution": result.get("attribution", {}),
        "confidence_score": result.get("confidenceScore"),
    }


@portfolio_widgets_bp.route("/api/portfolio/widget/optimizer", methods=["POST"])
def optimizer_widget():
    """Portfolio optimizer / rebalancing HTML fragment. Uses Black-Litterman
    with Alpha Score views, applies constraint layer, persists recommendation.
    """
    try:
        data = request.get_json(silent=True) or {}
        holdings = data.get("holdings", [])
        mode = data.get("mode", "diversification")
        if mode not in MODE_CONSTRAINTS:
            mode = "diversification"
        client_id = (data.get("clientId") or "").strip() or "anonymous"

        constraints_override = MODE_CONSTRAINTS[mode]
        result = black_litterman_optimize(holdings, constraints=constraints_override)
        if result is None:
            return '<p class="text-gray-400 text-sm italic">Need at least 2 holdings for portfolio optimization.</p>'

        # Persist the recommendation. Failure here should not break the UI.
        try:
            rec_id = insert_recommendation(_build_rec_payload(client_id, holdings, result, mode))
            result["recId"] = rec_id
        except Exception:
            result["recId"] = None

        result["mode"] = mode
        result["complianceNarrative"] = _compliance_narrative(result)

        return render_template("partials/portfolio_optimizer.html", **result)
    except Exception:
        return '<p class="text-red-500 text-sm italic">Optimizer temporarily unavailable.</p>'


@portfolio_widgets_bp.route("/api/portfolio/widget/fee-analysis", methods=["POST"])
def fee_analysis_widget():
    """Return fee drag analysis HTML fragment."""
    try:
        data = request.get_json(silent=True) or {}
        holdings = data.get("holdings", [])
        growth_pct = data.get("growthRate", 8)
        try:
            growth_pct = max(1, min(20, float(growth_pct)))
        except (TypeError, ValueError):
            growth_pct = 8
        result = compute_fee_analysis(holdings, growth_rate=growth_pct / 100)
        return render_template("partials/portfolio_fee_analysis.html", **result)
    except Exception:
        return '<p class="text-red-500 text-sm italic">Fee analysis temporarily unavailable.</p>'


@portfolio_widgets_bp.route("/api/portfolio/widget/factor-exposure", methods=["POST"])
def factor_exposure_widget():
    """Return factor exposure analysis HTML fragment."""
    try:
        data = request.get_json(silent=True) or {}
        holdings = data.get("holdings", [])
        result = compute_factor_exposure(holdings)
        return render_template("partials/portfolio_factor_exposure.html", **result)
    except Exception:
        return '<p class="text-red-500 text-sm italic">Factor exposure temporarily unavailable.</p>'


@portfolio_widgets_bp.route("/api/portfolio/widget/fundamentals", methods=["POST"])
def fundamentals_widget():
    """Return fundamental scorecard HTML fragment."""
    try:
        data = request.get_json(silent=True) or {}
        holdings = data.get("holdings", [])
        result = analyze_portfolio_fundamentals(holdings)
        return render_template("partials/portfolio_fundamentals.html", **result)
    except Exception:
        return '<p class="text-red-500 text-sm italic">Fundamental analysis temporarily unavailable.</p>'
