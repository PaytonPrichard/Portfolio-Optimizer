"""Excel download route — in-memory workbook served as file."""

from flask import Blueprint, send_file

from financials.data import fetch_data, fetch_recent_news, fetch_industry_peers
from financials.ai import generate_ai_commentary, generate_news_summaries
from financials.excel import build_full_workbook

download_bp = Blueprint("download", __name__)


@download_bp.route("/download/<ticker>")
def download_excel(ticker):
    ticker = ticker.upper()

    info, quarterly_income, history = fetch_data(ticker)

    if not info.get("longName"):
        return "Ticker not found", 404

    company_name = info.get("longName", ticker)
    news = fetch_recent_news(ticker)
    news = generate_news_summaries(news, company_name)
    commentary = generate_ai_commentary(info, quarterly_income, history, news=news)
    peers = fetch_industry_peers(ticker, info)

    buf = build_full_workbook(ticker, info, quarterly_income, history,
                              commentary, news, peers)

    return send_file(
        buf,
        mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        as_attachment=True,
        download_name=f"{ticker}_financials.xlsx",
    )
