"""Portfolio insight widget endpoints — each returns an HTML fragment."""

import json

from flask import Blueprint, render_template, request

from financials.portfolio_widgets import (
    fetch_sector_momentum,
    fetch_holdings_news,
    generate_portfolio_ai_commentary,
    fetch_peer_valuations,
    compute_analyst_overview,
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
