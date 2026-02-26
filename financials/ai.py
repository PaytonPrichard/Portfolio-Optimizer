"""AI commentary generation (Claude Haiku) with rule-based fallback."""

import os
import re

import pandas as pd

from .formatters import fmt_money, fmt_val


def generate_summary(info: dict, quarterly_income: pd.DataFrame, history: pd.DataFrame) -> str:
    """Build a plain-English paragraph summarising financial trends."""
    company = info.get("longName", "This company")
    sentences = []

    if "Total Revenue" in quarterly_income.index:
        rev = quarterly_income.loc["Total Revenue"].dropna()
        if len(rev) >= 2:
            latest, prev = float(rev.iloc[0]), float(rev.iloc[1])
            pct = ((latest - prev) / abs(prev)) * 100
            direction = "grew" if pct >= 0 else "declined"
            sentences.append(
                f"{company} revenue {direction} {abs(pct):.1f}% quarter-over-quarter "
                f"({fmt_money(prev)} -> {fmt_money(latest)})."
            )
        if len(rev) >= 4:
            oldest, latest_4 = float(rev.iloc[3]), float(rev.iloc[0])
            overall = ((latest_4 - oldest) / abs(oldest)) * 100
            trend = "upward" if overall >= 0 else "downward"
            sentences.append(
                f"The 4-quarter revenue trend is {trend} "
                f"({fmt_money(oldest)} -> {fmt_money(latest_4)}, "
                f"{abs(overall):.1f}% {'growth' if overall >= 0 else 'decline'} overall)."
            )

    gm = info.get("grossMargins")
    if gm:
        sentences.append(f"Gross margin stands at {gm * 100:.1f}%.")

    if "Net Income" in quarterly_income.index:
        ni = quarterly_income.loc["Net Income"].dropna()
        if len(ni) >= 1:
            latest_ni = float(ni.iloc[0])
            if latest_ni > 0:
                sentences.append(
                    f"The most recent quarter shows positive net income of {fmt_money(latest_ni)}."
                )
            else:
                sentences.append(
                    f"The most recent quarter shows a net loss of {fmt_money(abs(latest_ni))}, "
                    f"indicating the company is not yet profitable."
                )

    if not history.empty:
        start = float(history["Close"].iloc[0])
        end = float(history["Close"].iloc[-1])
        pct = ((end - start) / start) * 100
        direction = "gained" if pct >= 0 else "lost"
        sentences.append(
            f"Over the past year the stock has {direction} {abs(pct):.1f}% "
            f"(${start:.2f} -> ${end:.2f})."
        )

    pe = info.get("trailingPE")
    if pe and not pd.isna(pe):
        pe = float(pe)
        if pe < 15:
            comment = "suggesting the stock may be undervalued relative to the broader market"
        elif pe < 30:
            comment = "in line with typical market valuations"
        else:
            comment = "reflecting high growth expectations from investors"
        sentences.append(f"The trailing P/E ratio is {pe:.1f}x, {comment}.")

    if not sentences:
        return "Insufficient data available to generate a trend summary."
    return " ".join(sentences)


def generate_ai_commentary(info: dict, quarterly_income: pd.DataFrame,
                           history: pd.DataFrame, news: list = None) -> str:
    """
    Generate analyst commentary using Claude Haiku.
    Falls back to generate_summary() if ANTHROPIC_API_KEY is not set.
    """
    api_key = os.environ.get("ANTHROPIC_API_KEY")
    if not api_key:
        return generate_summary(info, quarterly_income, history)

    try:
        import anthropic
    except ImportError:
        return generate_summary(info, quarterly_income, history)

    news = news or []

    company = info.get("longName", "This company")
    symbol = info.get("symbol", "")
    current_price = info.get("currentPrice") or info.get("regularMarketPrice")
    target_mean = info.get("targetMeanPrice")
    target_low = info.get("targetLowPrice")
    target_high = info.get("targetHighPrice")
    rec = (info.get("recommendationKey") or "N/A").upper()
    n_analysts = info.get("numberOfAnalystOpinions", "N/A")

    rev_lines = []
    if "Total Revenue" in quarterly_income.index:
        rev = quarterly_income.loc["Total Revenue"].dropna().iloc[:4]
        for date, val in rev.items():
            label = date.strftime("%b %Y") if hasattr(date, "strftime") else str(date)
            rev_lines.append(f"  {label}: {fmt_money(float(val))}")

    ni_line = ""
    if "Net Income" in quarterly_income.index:
        ni = quarterly_income.loc["Net Income"].dropna()
        if len(ni) >= 1:
            ni_line = f"Net income (latest quarter): {fmt_money(float(ni.iloc[0]))}"

    price_change_line = ""
    if not history.empty:
        try:
            start = float(history["Close"].iloc[0])
            end = float(history["Close"].iloc[-1])
            pct = ((end - start) / start) * 100
            price_change_line = f"1-year stock change: {'+' if pct >= 0 else ''}{pct:.1f}%"
        except Exception:
            pass

    upside_line = ""
    if target_mean and current_price:
        try:
            upside_pct = ((float(target_mean) - float(current_price)) / float(current_price)) * 100
            upside_line = f"Implied upside to consensus: {'+' if upside_pct >= 0 else ''}{upside_pct:.1f}%"
        except Exception:
            pass

    news_lines = [
        f"  - {n['date']}  {n['publisher']}:  {n['title']}"
        for n in news if n.get("title")
    ]

    data_block = "\n".join(filter(None, [
        f"Company: {company} ({symbol})",
        f"Current price: ${current_price}",
        f"52-week range: ${info.get('fiftyTwoWeekLow', 'N/A')} - ${info.get('fiftyTwoWeekHigh', 'N/A')}",
        price_change_line,
        f"P/E ratio (trailing): {fmt_val(info.get('trailingPE'), suffix='x')}",
        f"Gross margin: {fmt_val(info.get('grossMargins', 0) * 100 if info.get('grossMargins') else None, suffix='%', decimals=1)}",
        f"Net profit margin: {fmt_val(info.get('profitMargins', 0) * 100 if info.get('profitMargins') else None, suffix='%', decimals=1)}",
        ni_line,
        "Quarterly revenue (newest first):",
        *rev_lines,
        f"Analyst consensus target: ${target_mean} (low: ${target_low}, high: ${target_high})",
        f"Analyst recommendation: {rec} ({n_analysts} analysts)",
        upside_line,
        ("Recent news headlines:\n" + "\n".join(news_lines)) if news_lines else "",
    ]))

    prompt = (
        "You are a concise equity research analyst writing a company assessment "
        "for a financial dashboard.\n\n"
        "Based on the data and recent news below, write exactly 5 sentences with NO headers or labels:\n"
        "1. Company health: summarize operational performance (revenue trend, margins, profitability).\n"
        "2. Price outlook: reference the analyst consensus target and implied upside/downside.\n"
        "3. Key financial risk or opportunity evident in the numbers.\n"
        "4. Recent company-specific development: draw on the news headlines.\n"
        "5. Broader industry or macro context relevant to this company right now.\n\n"
        "Rules: be specific and reference actual figures or news items. Keep each sentence under 50 words. "
        "No bullet points, no numbered list, no section labels. Just 5 plain sentences in a paragraph.\n\n"
        f"Financial data and news:\n{data_block}"
    )

    try:
        client = anthropic.Anthropic(api_key=api_key)
        response = client.messages.create(
            model="claude-haiku-4-5-20251001",
            max_tokens=500,
            messages=[{"role": "user", "content": prompt}],
        )
        return response.content[0].text.strip()
    except Exception:
        return generate_summary(info, quarterly_income, history)


def generate_news_summaries(news: list, company_name: str) -> list:
    """Add a 'summary' field to each news item using Claude Haiku."""
    if not news:
        return news

    api_key = os.environ.get("ANTHROPIC_API_KEY")
    if not api_key:
        for item in news:
            item["summary"] = item.get("title", "")
        return news

    try:
        import anthropic
    except ImportError:
        for item in news:
            item["summary"] = item.get("title", "")
        return news

    headlines_block = "\n".join(
        f"{i + 1}. [{item.get('publisher', '')}]  {item['title']}"
        for i, item in enumerate(news)
        if item.get("title")
    )
    n = len(news)
    prompt = (
        f"These are recent news headlines related to {company_name}.\n"
        f"For each headline write ONE sentence (max 30 words) summarising what the article is about.\n\n"
        f"{headlines_block}\n\n"
        f"Return exactly {n} lines numbered 1-{n}. One sentence per line. No extra text."
    )

    try:
        client = anthropic.Anthropic(api_key=api_key)
        resp = client.messages.create(
            model="claude-haiku-4-5-20251001",
            max_tokens=400,
            messages=[{"role": "user", "content": prompt}],
        )
        lines = [l.strip() for l in resp.content[0].text.strip().split("\n") if l.strip()]
        summaries = [re.sub(r"^\d+[\.\)]\s*", "", l) for l in lines]
        for i, item in enumerate(news):
            item["summary"] = summaries[i] if i < len(summaries) else item.get("title", "")
    except Exception:
        for item in news:
            item["summary"] = item.get("title", "")

    return news
