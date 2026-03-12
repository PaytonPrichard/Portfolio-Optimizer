"""Portfolio risk analytics — Sharpe, Sortino, drawdown, VaR, Monte Carlo,
stress testing, efficient frontier optimization, fee analysis."""

import math
import random
from concurrent.futures import ThreadPoolExecutor, as_completed

import yfinance as yf

from . import cache

_RISK_FREE_FALLBACK = 0.045  # fallback if ^IRX fetch fails


def _fetch_risk_free_rate():
    """Fetch current 3-month Treasury bill rate from ^IRX."""
    try:
        t = yf.Ticker("^IRX")
        price = (t.info or {}).get("regularMarketPrice") or (t.info or {}).get("previousClose")
        if price and price > 0:
            return round(price / 100, 4)  # ^IRX is quoted as e.g. 4.35 → 0.0435
    except Exception:
        pass
    return _RISK_FREE_FALLBACK


RISK_FREE_RATE = _fetch_risk_free_rate()
TRADING_DAYS = 252
RISK_TTL = 900  # 15 min cache for risk data


# ── Math helpers (pure Python, no numpy) ──────────────────────────────

def _mean(xs):
    return sum(xs) / len(xs) if xs else 0.0


def _std(xs):
    if len(xs) < 2:
        return 0.0
    m = _mean(xs)
    return math.sqrt(sum((x - m) ** 2 for x in xs) / (len(xs) - 1))


def _percentile(sorted_xs, p):
    """Compute p-th percentile (0-100) of a sorted list."""
    if not sorted_xs:
        return 0.0
    k = (len(sorted_xs) - 1) * p / 100.0
    f = math.floor(k)
    c = math.ceil(k)
    if f == c:
        return sorted_xs[int(k)]
    return sorted_xs[f] * (c - k) + sorted_xs[c] * (k - f)


# ── Daily return fetcher (cached) ────────────────────────────────────

def _fetch_daily_returns(symbol, period="1y"):
    """Fetch daily returns for a symbol. Returns list of floats."""
    cache_key = f"daily_returns:{symbol}:{period}"
    cached = cache.get(cache_key)
    if cached is not None:
        return cached
    try:
        hist = yf.Ticker(symbol).history(period=period)
        if hist is None or hist.empty or len(hist) < 5:
            cache.put(cache_key, [], ttl=RISK_TTL)
            return []
        closes = hist["Close"].dropna()
        returns = []
        for i in range(1, len(closes)):
            prev = float(closes.iloc[i - 1])
            if prev > 0:
                returns.append((float(closes.iloc[i]) - prev) / prev)
        cache.put(cache_key, returns, ttl=RISK_TTL)
        return returns
    except Exception:
        cache.put(cache_key, [], ttl=RISK_TTL)
        return []


def _compute_beta(returns, benchmark_returns):
    """Compute beta of returns vs benchmark."""
    n = min(len(returns), len(benchmark_returns))
    if n < 20:
        return None
    xs = benchmark_returns[:n]
    ys = returns[:n]
    mean_x = _mean(xs)
    mean_y = _mean(ys)
    cov = sum((xs[i] - mean_x) * (ys[i] - mean_y) for i in range(n)) / n
    var_x = sum((xs[i] - mean_x) ** 2 for i in range(n)) / n
    if var_x == 0:
        return None
    return cov / var_x


# ── Risk Metrics ─────────────────────────────────────────────────────

def _empty_risk():
    return {
        "annualReturn": 0, "annualVolatility": 0,
        "sharpeRatio": 0, "sortinoRatio": 0,
        "maxDrawdown": 0, "var95": 0, "var99": 0,
        "beta": None, "totalValue": 0,
        "holdingRisk": [], "riskLevel": "Unknown",
    }


def compute_risk_metrics(holdings):
    """Compute portfolio-level risk metrics from 1-year daily returns.

    Returns dict with: annualReturn, annualVolatility, sharpeRatio,
    sortinoRatio, maxDrawdown, var95, var99, beta, holdingRisk, riskLevel
    """
    valid = [h for h in holdings if (h.get("currentValue") or 0) > 0]
    if not valid:
        return _empty_risk()

    total_value = sum(h["currentValue"] for h in valid)
    if total_value <= 0:
        return _empty_risk()

    symbols = [h["symbol"] for h in valid]
    weights = {h["symbol"]: h["currentValue"] / total_value for h in valid}

    # Fetch daily returns in parallel
    returns_map = {}
    with ThreadPoolExecutor(max_workers=8) as pool:
        futures = {pool.submit(_fetch_daily_returns, sym): sym for sym in symbols}
        for future in as_completed(futures, timeout=45):
            sym = futures[future]
            try:
                returns_map[sym] = future.result(timeout=15)
            except Exception:
                returns_map[sym] = []

    # SPY benchmark
    spy_returns = _fetch_daily_returns("SPY", "1y")

    # Align to shortest return series (must have at least 20 days)
    lengths = [len(returns_map.get(s, [])) for s in symbols if returns_map.get(s)]
    if not lengths:
        return _empty_risk()
    min_len = min(lengths)
    if min_len < 20:
        return _empty_risk()

    # Portfolio daily returns (weighted sum)
    portfolio_returns = []
    for day in range(min_len):
        day_r = 0.0
        for sym in symbols:
            rets = returns_map.get(sym, [])
            if day < len(rets):
                day_r += weights.get(sym, 0) * rets[day]
        portfolio_returns.append(day_r)

    # Annualized return
    cumulative = 1.0
    for r in portfolio_returns:
        cumulative *= (1 + r)
    annual_return = cumulative ** (TRADING_DAYS / len(portfolio_returns)) - 1

    # Volatility
    daily_vol = _std(portfolio_returns)
    annual_vol = daily_vol * math.sqrt(TRADING_DAYS)

    # Sharpe
    sharpe = (annual_return - RISK_FREE_RATE) / annual_vol if annual_vol > 0 else 0

    # Sortino (downside deviation below 0)
    downside = [r for r in portfolio_returns if r < 0]
    if len(downside) > 1:
        downside_dev = _std(downside) * math.sqrt(TRADING_DAYS)
    else:
        downside_dev = annual_vol
    sortino = (annual_return - RISK_FREE_RATE) / downside_dev if downside_dev > 0 else 0

    # Max drawdown
    peak = 0.0
    max_dd = 0.0
    cumul = 1.0
    for r in portfolio_returns:
        cumul *= (1 + r)
        if cumul > peak:
            peak = cumul
        dd = (peak - cumul) / peak if peak > 0 else 0
        if dd > max_dd:
            max_dd = dd

    # Value at Risk (parametric, 1-day)
    mean_daily = _mean(portfolio_returns)
    var95 = (1.645 * daily_vol - mean_daily) * total_value
    var99 = (2.326 * daily_vol - mean_daily) * total_value

    # Portfolio beta vs SPY
    beta = _compute_beta(portfolio_returns, spy_returns[:min_len])

    # Risk level label
    if annual_vol < 10:
        risk_level = "Low"
    elif annual_vol < 18:
        risk_level = "Moderate"
    elif annual_vol < 28:
        risk_level = "High"
    else:
        risk_level = "Very High"

    # Per-holding risk
    holding_risk = []
    for h in valid:
        sym = h["symbol"]
        rets = returns_map.get(sym, [])
        if len(rets) < 20:
            continue
        n = len(rets)
        sym_vol = _std(rets) * math.sqrt(TRADING_DAYS)
        sym_cumul = 1.0
        for r in rets:
            sym_cumul *= (1 + r)
        sym_annual = sym_cumul ** (TRADING_DAYS / n) - 1
        sym_sharpe = (sym_annual - RISK_FREE_RATE) / sym_vol if sym_vol > 0 else 0
        sym_beta = _compute_beta(rets, spy_returns[:n])

        # Per-holding max drawdown
        sym_peak = 0.0
        sym_dd = 0.0
        sym_c = 1.0
        for r in rets:
            sym_c *= (1 + r)
            if sym_c > sym_peak:
                sym_peak = sym_c
            d = (sym_peak - sym_c) / sym_peak if sym_peak > 0 else 0
            if d > sym_dd:
                sym_dd = d

        holding_risk.append({
            "symbol": sym,
            "name": h.get("name", ""),
            "weight": round(weights.get(sym, 0) * 100, 1),
            "annualReturn": round(sym_annual * 100, 1),
            "volatility": round(sym_vol * 100, 1),
            "sharpe": round(sym_sharpe, 2),
            "beta": round(sym_beta, 2) if sym_beta is not None else None,
            "maxDrawdown": round(sym_dd * 100, 1),
            "currentValue": h.get("currentValue", 0),
        })

    holding_risk.sort(key=lambda x: x.get("sharpe", 0), reverse=True)

    return {
        "annualReturn": round(annual_return * 100, 1),
        "annualVolatility": round(annual_vol * 100, 1),
        "sharpeRatio": round(sharpe, 2),
        "sortinoRatio": round(sortino, 2),
        "maxDrawdown": round(max_dd * 100, 1),
        "var95": round(var95, 2),
        "var99": round(var99, 2),
        "beta": round(beta, 2) if beta is not None else None,
        "totalValue": total_value,
        "holdingRisk": holding_risk,
        "riskLevel": risk_level,
    }


# ── Monte Carlo Simulation ───────────────────────────────────────────

def _empty_monte_carlo():
    return {
        "years": 0, "simulations": 0, "startValue": 0,
        "curves": {}, "yearLabels": [],
        "medianFinal": 0, "p10Final": 0, "p90Final": 0,
        "probabilityOfLoss": 0,
        "meanAnnualReturn": 0, "annualVolatility": 0,
    }


def run_monte_carlo(holdings, years=10, simulations=1000):
    """Monte Carlo simulation using portfolio's historical return distribution.

    Returns probability cones at yearly intervals for chart display.
    """
    valid = [h for h in holdings if (h.get("currentValue") or 0) > 0]
    if not valid:
        return _empty_monte_carlo()

    total_value = sum(h["currentValue"] for h in valid)
    if total_value <= 0:
        return _empty_monte_carlo()

    symbols = [h["symbol"] for h in valid]
    weights = {h["symbol"]: h["currentValue"] / total_value for h in valid}

    # Fetch daily returns (hits cache if risk_metrics already ran)
    returns_map = {}
    with ThreadPoolExecutor(max_workers=8) as pool:
        futures = {pool.submit(_fetch_daily_returns, sym): sym for sym in symbols}
        for future in as_completed(futures, timeout=45):
            sym = futures[future]
            try:
                returns_map[sym] = future.result(timeout=15)
            except Exception:
                returns_map[sym] = []

    # Build portfolio daily returns
    lengths = [len(returns_map.get(s, [])) for s in symbols if returns_map.get(s)]
    if not lengths:
        return _empty_monte_carlo()
    min_len = min(lengths)
    if min_len < 20:
        return _empty_monte_carlo()

    portfolio_returns = []
    for day in range(min_len):
        day_r = sum(weights.get(s, 0) * returns_map.get(s, [0])[day]
                    for s in symbols if day < len(returns_map.get(s, [])))
        portfolio_returns.append(day_r)

    mean_daily = _mean(portfolio_returns)
    std_daily = _std(portfolio_returns)
    if std_daily <= 0:
        return _empty_monte_carlo()

    # Simulate
    yearly_snapshots = [[] for _ in range(years + 1)]
    final_values = []

    random.seed(42)
    for _ in range(simulations):
        value = total_value
        yearly_snapshots[0].append(value)
        for y in range(years):
            for _ in range(TRADING_DAYS):
                value *= (1 + random.gauss(mean_daily, std_daily))
                if value < 0:
                    value = 0
            yearly_snapshots[y + 1].append(value)
        final_values.append(value)

    final_values.sort()

    # Percentile curves for chart
    curves = {}
    for p in [10, 25, 50, 75, 90]:
        curve = []
        for y in range(years + 1):
            s = sorted(yearly_snapshots[y])
            curve.append(round(_percentile(s, p), 2))
        curves[f"p{p}"] = curve

    prob_loss = sum(1 for v in final_values if v < total_value) / len(final_values) * 100

    return {
        "years": years,
        "simulations": simulations,
        "startValue": total_value,
        "curves": curves,
        "yearLabels": list(range(years + 1)),
        "medianFinal": round(_percentile(final_values, 50), 2),
        "p10Final": round(_percentile(final_values, 10), 2),
        "p90Final": round(_percentile(final_values, 90), 2),
        "probabilityOfLoss": round(prob_loss, 1),
        "meanAnnualReturn": round(mean_daily * TRADING_DAYS * 100, 1),
        "annualVolatility": round(std_daily * math.sqrt(TRADING_DAYS) * 100, 1),
    }


# ── Stress Testing ───────────────────────────────────────────────────

_STRESS_SCENARIOS = [
    {
        "name": "2008 Financial Crisis",
        "description": "Sep 2008 - Mar 2009",
        "spyReturn": -50.9,
    },
    {
        "name": "2020 COVID Crash",
        "description": "Feb - Mar 2020",
        "spyReturn": -33.9,
    },
    {
        "name": "2022 Rate Hike Selloff",
        "description": "Jan - Oct 2022",
        "spyReturn": -25.4,
    },
    {
        "name": "2018 Q4 Selloff",
        "description": "Oct - Dec 2018",
        "spyReturn": -19.8,
    },
    {
        "name": "Dot-com Crash",
        "description": "Mar 2000 - Oct 2002",
        "spyReturn": -49.1,
    },
]


def run_stress_tests(holdings):
    """Estimate portfolio impact under historical stress scenarios.

    Uses beta-adjusted approach: each holding's loss = SPY loss * beta.
    Fast — uses beta already in enrichment data, no new API calls.
    """
    total_value = sum(h.get("currentValue", 0) for h in holdings)
    if total_value <= 0:
        return []

    results = []
    for scenario in _STRESS_SCENARIOS:
        portfolio_loss_pct = 0.0
        holding_impacts = []

        for h in holdings:
            val = h.get("currentValue", 0)
            if val <= 0:
                continue
            weight = val / total_value
            beta = h.get("beta") or 1.0
            est_loss_pct = max(scenario["spyReturn"] * beta, -100)
            est_loss_dollar = val * est_loss_pct / 100
            portfolio_loss_pct += weight * est_loss_pct

            holding_impacts.append({
                "symbol": h["symbol"],
                "beta": round(beta, 2),
                "estLossPct": round(est_loss_pct, 1),
                "estLossDollar": round(est_loss_dollar, 2),
                "currentValue": val,
            })

        holding_impacts.sort(key=lambda x: x["estLossPct"])

        results.append({
            "name": scenario["name"],
            "description": scenario["description"],
            "spyReturn": scenario["spyReturn"],
            "portfolioReturn": round(portfolio_loss_pct, 1),
            "portfolioLoss": round(total_value * portfolio_loss_pct / 100, 2),
            "holdingImpacts": holding_impacts,
        })

    return results


# ── Efficient Frontier / Portfolio Optimization ──────────────────────

def compute_efficient_frontier(holdings, n_portfolios=1000):
    """Monte Carlo optimization: random weight portfolios to find
    the highest-Sharpe allocation, then generate rebalancing trades.
    """
    valid = [h for h in holdings if (h.get("currentValue") or 0) > 0]
    if len(valid) < 2:
        return None

    total_value = sum(h["currentValue"] for h in valid)
    symbols = [h["symbol"] for h in valid]
    current_weights = [h["currentValue"] / total_value for h in valid]

    # Fetch returns (cached from risk_metrics)
    returns_map = {}
    with ThreadPoolExecutor(max_workers=8) as pool:
        futures = {pool.submit(_fetch_daily_returns, sym): sym for sym in symbols}
        for future in as_completed(futures, timeout=45):
            sym = futures[future]
            try:
                returns_map[sym] = future.result(timeout=15)
            except Exception:
                returns_map[sym] = []

    lengths = [len(returns_map.get(s, [])) for s in symbols if returns_map.get(s)]
    if not lengths:
        return None
    min_len = min(lengths)
    if min_len < 30:
        return None

    # Build return matrix
    return_matrix = [returns_map[sym][:min_len] for sym in symbols]
    n = len(symbols)

    def _portfolio_stats(w):
        port_rets = []
        for day in range(min_len):
            port_rets.append(sum(w[i] * return_matrix[i][day] for i in range(n)))
        mean_r = _mean(port_rets) * TRADING_DAYS
        vol = _std(port_rets) * math.sqrt(TRADING_DAYS)
        sharpe = (mean_r - RISK_FREE_RATE) / vol if vol > 0 else 0
        return mean_r, vol, sharpe

    # Current portfolio stats
    curr_ret, curr_vol, curr_sharpe = _portfolio_stats(current_weights)

    # Random portfolios
    best_sharpe = curr_sharpe
    best_weights = list(current_weights)
    frontier_points = []

    random.seed(42)
    for _ in range(n_portfolios):
        raw = [random.expovariate(1) for _ in range(n)]
        total = sum(raw)
        w = [x / total for x in raw]
        ret, vol, sharpe = _portfolio_stats(w)
        frontier_points.append({
            "ret": round(ret * 100, 1),
            "vol": round(vol * 100, 1),
            "sharpe": round(sharpe, 2),
        })
        if sharpe > best_sharpe:
            best_sharpe = sharpe
            best_weights = w

    opt_ret, opt_vol, opt_sharpe = _portfolio_stats(best_weights)

    # Rebalancing trades
    trades = []
    for i, h in enumerate(valid):
        current_pct = current_weights[i] * 100
        optimal_pct = best_weights[i] * 100
        diff_pct = optimal_pct - current_pct
        diff_dollar = diff_pct / 100 * total_value
        if abs(diff_pct) > 1.0:
            trades.append({
                "symbol": h["symbol"],
                "name": h.get("name", ""),
                "currentPct": round(current_pct, 1),
                "optimalPct": round(optimal_pct, 1),
                "diffPct": round(diff_pct, 1),
                "diffDollar": round(diff_dollar, 2),
                "action": "Increase" if diff_pct > 0 else "Decrease",
            })

    trades.sort(key=lambda x: abs(x["diffPct"]), reverse=True)

    return {
        "currentPortfolio": {
            "return": round(curr_ret * 100, 1),
            "volatility": round(curr_vol * 100, 1),
            "sharpe": round(curr_sharpe, 2),
        },
        "optimalPortfolio": {
            "return": round(opt_ret * 100, 1),
            "volatility": round(opt_vol * 100, 1),
            "sharpe": round(opt_sharpe, 2),
            "weights": [{"symbol": symbols[i], "weight": round(best_weights[i] * 100, 1)}
                        for i in range(n)],
        },
        "trades": trades,
        "frontierPoints": frontier_points,
        "totalValue": total_value,
        "improvementPct": round((opt_sharpe - curr_sharpe) / abs(curr_sharpe) * 100, 1) if curr_sharpe != 0 else 0,
    }


# ── Fee / Expense Ratio Analysis ─────────────────────────────────────

def _fetch_expense_ratio(symbol):
    cache_key = f"expense_ratio:{symbol}"
    cached = cache.get(cache_key)
    if cached is not None:
        return cached
    try:
        info = yf.Ticker(symbol).info or {}
        er = info.get("netExpenseRatio") or info.get("annualReportExpenseRatio")
        if er is not None:
            er = float(er)
        result = {"expenseRatio": er, "fundName": info.get("longName", "")}
        cache.put(cache_key, result, ttl=cache.DEFAULT_TTL)
        return result
    except Exception:
        result = {"expenseRatio": None, "fundName": ""}
        cache.put(cache_key, result, ttl=cache.DEFAULT_TTL)
        return result


def compute_fee_analysis(holdings, growth_rate=0.08):
    """Analyze expense ratio drag for ETFs/funds in portfolio."""
    total_value = sum(h.get("currentValue", 0) for h in holdings)
    if total_value <= 0:
        return {"holdings": [], "totalAnnualFees": 0,
                "blendedExpenseRatio": 0, "projectedDrag": {}, "totalValue": 0,
                "growthRate": round(growth_rate * 100)}

    # Fetch expense ratios for probable funds
    fee_data = {}
    fund_symbols = [h["symbol"] for h in holdings
                    if h.get("isFund") or h.get("sectorWeights")]
    if fund_symbols:
        with ThreadPoolExecutor(max_workers=6) as pool:
            futures = {pool.submit(_fetch_expense_ratio, sym): sym
                       for sym in fund_symbols}
            for future in as_completed(futures, timeout=20):
                sym = futures[future]
                try:
                    fee_data[sym] = future.result(timeout=10)
                except Exception:
                    fee_data[sym] = {"expenseRatio": None, "fundName": ""}

    fee_holdings = []
    total_annual_fees = 0.0
    for h in holdings:
        sym = h["symbol"]
        val = h.get("currentValue", 0)
        fd = fee_data.get(sym, {})
        er = fd.get("expenseRatio")
        if er and er > 0:
            annual_fee = val * er / 100
            total_annual_fees += annual_fee
            fee_holdings.append({
                "symbol": sym,
                "name": h.get("name", "") or fd.get("fundName", ""),
                "currentValue": val,
                "expenseRatio": round(er, 4),
                "annualFee": round(annual_fee, 2),
            })
    fee_holdings.sort(key=lambda x: x["annualFee"], reverse=True)

    # Project fee drag over time
    blended_er = total_annual_fees / total_value if total_value > 0 else 0
    drag = {}
    for years in [10, 20, 30]:
        with_fees = total_value
        without_fees = total_value
        for _ in range(years):
            with_fees *= (1 + growth_rate - blended_er)
            without_fees *= (1 + growth_rate)
        drag[f"{years}yr"] = {
            "withFees": round(with_fees, 2),
            "withoutFees": round(without_fees, 2),
            "feeDrag": round(without_fees - with_fees, 2),
        }

    return {
        "holdings": fee_holdings,
        "totalAnnualFees": round(total_annual_fees, 2),
        "blendedExpenseRatio": round(blended_er * 100, 4),
        "projectedDrag": drag,
        "totalValue": total_value,
        "growthRate": round(growth_rate * 100),
    }
