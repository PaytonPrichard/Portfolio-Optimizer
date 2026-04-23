"""Candidate universe and helpers for new-position recommendations.

Phase 1.5 of the portfolio optimizer. Extends beyond rebalancing existing
holdings to suggest NEW positions via three methods:
  A) Sector gap detection — find under/missing sectors, recommend the
     matching sector ETF.
  B) Marginal Sharpe contribution — for each candidate, compute the change
     in portfolio Sharpe from adding a small slice; rank top N.
  C) Holistic full-universe optimization — re-run B-L on (current UNION
     universe), filter to NEW positions above weight/dollar thresholds.
     Phase 13.3.
"""

from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed

import numpy as np

from .alpha import _get_db
from .portfolio_widgets import SECTOR_ETFS
from .portfolio_risk import _fetch_daily_returns, RISK_FREE_RATE, TRADING_DAYS
from .portfolio_optimizer import (
    _build_return_matrix, _fetch_classifications, _fetch_alpha_scores,
    _fetch_market_caps, _apply_hard_constraints,
    ETF_SECTOR_KEY, GICS_SECTORS, COV_RIDGE, DEFAULT_WINDOW,
    DEFAULT_TAU, DEFAULT_DELTA, DEFAULT_VIEW_TILT,
)


SCREEN_TILT = 0.06          # less skeptical than the optimizer's 0.03
MARGINAL_EPSILON = 0.02     # 2% slice when computing marginal Sharpe
GAP_THRESHOLD = 0.05        # sector below 5% effective is considered a gap
MIN_MARGINAL_DELTA = 0.0    # show all positive contributors; let user filter visually
TOP_N_MARGINAL = 8          # cap displayed marginal candidates
HOLISTIC_MIN_WEIGHT = 0.05  # minimum weight to surface in holistic view
HOLISTIC_MIN_DOLLAR = 500   # minimum dollar amount to surface in holistic view


def _get_data_freshness():
    """Return ISO date of the most recent alpha.db snapshot, or None."""
    try:
        conn = _get_db()
        try:
            row = conn.execute(
                "SELECT MAX(snapshot_date) AS d FROM metric_snapshots"
            ).fetchone()
            return row["d"] if row else None
        finally:
            conn.close()
    except Exception:
        return None

# Curated broad-market ETFs for the candidate universe. Excludes bonds and
# commodities pending cross-asset support (task #17).
BROAD_ETFS = [
    "VOO",   # S&P 500
    "VTI",   # Total US market
    "QQQ",   # NASDAQ-100
    "VXUS",  # International ex-US
    "IWM",   # Russell 2000 small cap
]


def get_candidate_universe():
    """Return the full candidate universe tagged by source.

    Stocks come from alpha.db (whatever has been seeded by alpha_collector).
    ETFs are constants. Returns: list of {symbol, source} dicts where source
    is one of "stock", "broad_etf", "sector_etf".

    Degrades gracefully if alpha.db is empty — returns ETFs only.
    """
    universe = []

    try:
        conn = _get_db()
        try:
            rows = conn.execute(
                "SELECT DISTINCT symbol FROM metric_snapshots ORDER BY symbol"
            ).fetchall()
            for row in rows:
                universe.append({"symbol": row["symbol"], "source": "stock"})
        finally:
            conn.close()
    except Exception:
        pass

    for etf in BROAD_ETFS:
        universe.append({"symbol": etf, "source": "broad_etf"})

    for etf in SECTOR_ETFS:
        universe.append({"symbol": etf, "source": "sector_etf"})

    return universe


def _effective_sector_exposure(weights, sectors):
    """Sector exposure with broad ETFs spread evenly across 11 GICS sectors."""
    exposure = defaultdict(float)
    for w, s in zip(weights, sectors):
        if s == ETF_SECTOR_KEY:
            etf_share = float(w) / len(GICS_SECTORS)
            for sec in GICS_SECTORS:
                exposure[sec] += etf_share
        else:
            exposure[s] += float(w)
    return dict(exposure)


def sector_gap_recommendations(effective_exposure, total_value, threshold=GAP_THRESHOLD):
    """Identify under-exposed sectors and recommend the matching sector ETF.

    For each gap, suggested allocation is sized to bring the sector to the
    threshold (5% by default). Returns list of dicts with both pct and
    dollar amount for the suggested move.
    """
    gaps = []
    for sector in GICS_SECTORS:
        exposure = effective_exposure.get(sector, 0.0)
        if exposure < threshold:
            etf = next((s for s, sec in SECTOR_ETFS.items() if sec == sector), None)
            if etf:
                deficit_pct = (threshold - exposure) * 100
                suggested_dollar = (threshold - exposure) * total_value
                gaps.append({
                    "symbol": etf,
                    "source": "sector_etf",
                    "sector": sector,
                    "currentExposurePct": round(exposure * 100, 1),
                    "deficitPct": round(deficit_pct, 1),
                    "suggestedWeightPct": round((threshold - exposure) * 100, 1),
                    "suggestedDollar": round(suggested_dollar, 0),
                    "reason": f"Fills your {sector} gap (currently {round(exposure * 100, 1)}%)",
                })
    gaps.sort(key=lambda g: -g["deficitPct"])
    return gaps


def _candidate_alpha(symbol, source, alpha_data):
    """Map a candidate to an expected excess return using the screening tilt.
    ETFs sit at risk-free (no view); stocks tilt off Alpha Score around 50.
    """
    if source in ("broad_etf", "sector_etf"):
        return RISK_FREE_RATE
    alpha = (alpha_data.get(symbol, {}) or {}).get("alphaScore", 50)
    return RISK_FREE_RATE + (alpha - 50) / 100.0 * SCREEN_TILT


def marginal_sharpe_screening(
    candidates, current_weights, sigma, mu, current_symbols, total_value,
    top_n=TOP_N_MARGINAL, min_delta=MIN_MARGINAL_DELTA,
):
    """Screen candidates for marginal Sharpe contribution.

    For each candidate not in the current portfolio, fetch its returns,
    compute variance and covariance with current holdings, derive an
    expected excess return from Alpha Score, then compute the change in
    portfolio Sharpe from adding a small (epsilon) slice. Filter and rank.
    """
    # Re-fetch current holdings' returns to get the actual aligned matrix
    # (sigma alone doesn't carry the underlying series). Cache hit makes it
    # cheap.
    current_returns_lists = []
    for s in current_symbols:
        rets = _fetch_daily_returns(s, DEFAULT_WINDOW)
        current_returns_lists.append(rets)
    if not current_returns_lists or any(not r for r in current_returns_lists):
        return []
    min_len = min(len(r) for r in current_returns_lists)
    current_matrix = np.array([r[-min_len:] for r in current_returns_lists]).T  # T x N

    # Pre-fetch Alpha for stock candidates only.
    stock_symbols = [c["symbol"] for c in candidates if c["source"] == "stock"]
    alpha_data = _fetch_alpha_scores(stock_symbols) if stock_symbols else {}

    def _process(cand):
        sym = cand["symbol"]
        rets = _fetch_daily_returns(sym, DEFAULT_WINDOW)
        if len(rets) < min_len:
            return None
        cand_rets = np.array(rets[-min_len:])
        # Augmented covariance: [candidate, holding_1, holding_2, ...]
        augmented = np.column_stack([cand_rets, current_matrix])
        full_cov = np.cov(augmented, rowvar=False, ddof=1) * TRADING_DAYS
        candidate_var = float(full_cov[0, 0])
        candidate_cov = full_cov[0, 1:]

        candidate_mu = _candidate_alpha(sym, cand["source"], alpha_data)
        delta = marginal_sharpe_contribution(
            current_weights, sigma, mu,
            candidate_var, candidate_cov, candidate_mu, RISK_FREE_RATE,
            epsilon=MARGINAL_EPSILON,
        )
        # Quick correlation snapshot vs portfolio (avg correlation w current holdings)
        cur_vol = float(np.sqrt(np.diag(full_cov[1:, 1:])).mean()) if full_cov.shape[0] > 1 else 0
        cand_vol = float(np.sqrt(candidate_var))
        avg_cov = float(candidate_cov.mean()) if len(candidate_cov) else 0
        avg_corr = avg_cov / (cand_vol * cur_vol) if (cand_vol > 0 and cur_vol > 0) else 0
        suggested_dollar = MARGINAL_EPSILON * total_value
        return {
            "symbol": sym,
            "source": cand["source"],
            "sharpeDelta": round(float(delta), 4),
            "alphaScore": (alpha_data.get(sym, {}) or {}).get("alphaScore") if cand["source"] == "stock" else None,
            "suggestedWeightPct": round(MARGINAL_EPSILON * 100, 1),
            "suggestedDollar": round(suggested_dollar, 0),
            "avgCorrelation": round(max(-1.0, min(1.0, avg_corr)), 2),
            "annualizedVolPct": round(cand_vol * 100, 1),
        }

    results = []
    with ThreadPoolExecutor(max_workers=8) as pool:
        futures = {pool.submit(_process, c): c for c in candidates}
        for future in as_completed(futures, timeout=120):
            try:
                r = future.result(timeout=15)
                if r and r["sharpeDelta"] >= min_delta:
                    results.append(r)
            except Exception:
                pass

    results.sort(key=lambda r: -r["sharpeDelta"])
    return results[:top_n]


def holistic_optimization(
    holdings, min_weight=HOLISTIC_MIN_WEIGHT, min_dollar=HOLISTIC_MIN_DOLLAR,
    tau=DEFAULT_TAU, delta=DEFAULT_DELTA, tilt=DEFAULT_VIEW_TILT,
):
    """Method C: re-run B-L over (current UNION universe). Surface NEW
    positions above the weight/dollar threshold.

    Applies position bounds (2-25%) and sector cap (40%) but no turnover
    cap or vol cap, since "rebuild from scratch" doesn't have a meaningful
    turnover baseline. Returns sorted list of new-position suggestions.
    """
    valid = [h for h in holdings if (h.get("currentValue") or 0) > 0 and h.get("symbol")]
    if len(valid) < 2:
        return []

    total_value = float(sum(h["currentValue"] for h in valid))
    held_symbols = {h["symbol"] for h in valid}
    universe = get_candidate_universe()
    universe_lookup = {c["symbol"]: c["source"] for c in universe}

    candidate_symbols = [c["symbol"] for c in universe if c["symbol"] not in held_symbols]
    all_symbols = [h["symbol"] for h in valid] + candidate_symbols

    return_matrix, usable, _ = _build_return_matrix(all_symbols, DEFAULT_WINDOW)
    if return_matrix is None:
        return []

    n = len(usable)
    sigma = np.cov(return_matrix, rowvar=False, ddof=1) * TRADING_DAYS
    sigma = sigma + np.eye(n) * COV_RIDGE

    # Market-cap prior over the combined universe.
    caps_map = _fetch_market_caps(usable)
    cap_list = [caps_map.get(s) for s in usable]
    if any(c is None or c <= 0 for c in cap_list):
        w_mkt = np.ones(n) / n
    else:
        caps_array = np.array(cap_list, dtype=float)
        w_mkt = caps_array / caps_array.sum()

    pi = delta * sigma @ w_mkt

    # Alpha views over the combined universe. ETFs neutral.
    alpha_data = _fetch_alpha_scores(usable)
    classifications_map = _fetch_classifications(usable)
    is_etf_set = {s for s, (_, is_etf) in classifications_map.items() if is_etf}
    for s in is_etf_set:
        alpha_data[s] = {"alphaScore": 50}
    alphas = [(alpha_data.get(s, {}) or {}).get("alphaScore", 50.0) for s in usable]
    view_q = np.array([(a - 50) / 100.0 * tilt for a in alphas])

    p_mat = np.eye(n)
    omega = np.diag(tau * np.diag(sigma))
    try:
        tau_sigma_inv = np.linalg.inv(tau * sigma)
        omega_inv = np.linalg.inv(omega)
        posterior_precision = tau_sigma_inv + p_mat.T @ omega_inv @ p_mat
        mu = np.linalg.solve(
            posterior_precision,
            tau_sigma_inv @ pi + p_mat.T @ omega_inv @ view_q,
        )
        w_opt = np.linalg.solve(delta * sigma, mu)
    except np.linalg.LinAlgError:
        return []

    # Long-only projection then constraint layer (position + sector caps).
    w_opt = np.clip(w_opt, 0.0, None)
    if w_opt.sum() <= 0:
        return []
    w_opt = w_opt / w_opt.sum()

    sector_list = [classifications_map.get(s, ("Unknown", False))[0] for s in usable]
    w_opt = _apply_hard_constraints(w_opt, sector_list, min_w=0.02, max_w=0.25, max_sector=0.40)

    # Filter to NEW positions above threshold.
    suggestions = []
    for i, sym in enumerate(usable):
        if sym in held_symbols:
            continue
        weight = float(w_opt[i])
        dollar = weight * total_value
        if weight >= min_weight and dollar >= min_dollar:
            suggestions.append({
                "symbol": sym,
                "source": universe_lookup.get(sym, "stock"),
                "alphaScore": (alpha_data.get(sym, {}) or {}).get("alphaScore"),
                "suggestedWeightPct": round(weight * 100, 1),
                "suggestedDollar": round(dollar, 0),
                "expectedReturnPct": round(float(mu[i]) * 100, 2),
                "annualizedVolPct": round(float(np.sqrt(sigma[i, i])) * 100, 1),
                "sector": sector_list[i] if sector_list[i] not in (ETF_SECTOR_KEY, "Unknown") else None,
            })

    suggestions.sort(key=lambda s: -s["suggestedWeightPct"])
    return suggestions


def compute_suggestions(holdings, top_n_marginal=TOP_N_MARGINAL, gap_threshold=GAP_THRESHOLD):
    """Top-level entry: returns gaps, marginal, holistic (placeholder).

    Detects rate-limit silently via empty return histories on the user's own
    holdings — if all current holdings come back empty, the data source is
    almost certainly throttled rather than the user genuinely holding 0
    history tickers. Surfaces this so the UI can show an honest banner.
    """
    base = {
        "gaps": [],
        "marginal": [],
        "holistic": [],
        "candidatesScreened": 0,
        "currentSymbols": [],
        "totalValue": 0,
        "currentSectorBreakdown": {},
        "dataFreshness": _get_data_freshness(),
        "rateLimited": False,
    }

    valid = [h for h in holdings if (h.get("currentValue") or 0) > 0 and h.get("symbol")]
    if len(valid) < 2:
        return base

    total_value = float(sum(h["currentValue"] for h in valid))
    base["totalValue"] = total_value

    held_symbols = {h["symbol"] for h in valid}
    universe = [c for c in get_candidate_universe() if c["symbol"] not in held_symbols]
    base["candidatesScreened"] = len(universe)

    current_symbols_input = [h["symbol"] for h in valid]
    return_matrix, usable, _ = _build_return_matrix(current_symbols_input, DEFAULT_WINDOW)
    if return_matrix is None:
        # Heuristic rate-limit detection: if a major ticker comes back empty,
        # yfinance is almost certainly throttled.
        probe = _fetch_daily_returns(valid[0]["symbol"], DEFAULT_WINDOW)
        if not probe:
            base["rateLimited"] = True
        return base

    usable_holdings = [h for h in valid if h["symbol"] in usable]
    symbols = [h["symbol"] for h in usable_holdings]
    n = len(symbols)
    usable_total = float(sum(h["currentValue"] for h in usable_holdings))
    current_weights = np.array([h["currentValue"] / usable_total for h in usable_holdings])
    base["currentSymbols"] = symbols

    sigma = np.cov(return_matrix, rowvar=False, ddof=1) * TRADING_DAYS
    sigma = sigma + np.eye(n) * COV_RIDGE

    classifications_current = _fetch_classifications(symbols)
    sector_list = [classifications_current.get(s, ("Unknown", False))[0] for s in symbols]

    effective_sector = _effective_sector_exposure(current_weights, sector_list)
    base["currentSectorBreakdown"] = {
        sec: round(pct * 100, 1) for sec, pct in effective_sector.items() if pct > 0.001
    }

    gaps = sector_gap_recommendations(effective_sector, total_value, gap_threshold)

    alpha_current = _fetch_alpha_scores(symbols)
    is_etf_current = {s for s, (_, is_etf) in classifications_current.items() if is_etf}
    for s in is_etf_current:
        alpha_current[s] = {"alphaScore": 50}
    mu = np.array([
        RISK_FREE_RATE + (alpha_current.get(s, {}).get("alphaScore", 50) - 50) / 100 * SCREEN_TILT
        for s in symbols
    ])

    marginal = marginal_sharpe_screening(
        universe, current_weights, sigma, mu, symbols, total_value,
        top_n=top_n_marginal,
    )

    holistic = holistic_optimization(holdings)

    # High-conviction tagging: any symbol present in 2+ method outputs.
    gap_syms = {g["symbol"] for g in gaps}
    marg_syms = {m["symbol"] for m in marginal}
    holi_syms = {h["symbol"] for h in holistic}
    occurrence_count = {}
    for s in gap_syms | marg_syms | holi_syms:
        occurrence_count[s] = (
            (1 if s in gap_syms else 0)
            + (1 if s in marg_syms else 0)
            + (1 if s in holi_syms else 0)
        )
    high_conv = {s for s, c in occurrence_count.items() if c >= 2}
    for collection in (gaps, marginal, holistic):
        for item in collection:
            item["highConviction"] = item["symbol"] in high_conv

    base["gaps"] = gaps
    base["marginal"] = marginal
    base["holistic"] = holistic
    return base


def marginal_sharpe_contribution(
    current_weights, sigma, mu, candidate_var, candidate_cov,
    candidate_mu, risk_free, epsilon=0.02,
):
    """Change in portfolio Sharpe from adding a small slice of a candidate.

    Method: scale existing weights down by (1 - epsilon), add epsilon to
    candidate, compute new Sharpe vs current Sharpe. The block formula
    Var(scaled + eps*c) = Var(scaled) + 2*eps*Cov(scaled,c) + eps^2*Var(c)
    avoids needing the full augmented covariance matrix.

    Inputs:
      current_weights: 1D NumPy array of current weights (sums to 1)
      sigma: NxN annualized covariance matrix of current holdings
      mu: N annualized expected returns (decimals)
      candidate_var: annualized variance of the candidate (decimal)
      candidate_cov: 1xN vector of candidate's covariance with each holding
      candidate_mu: candidate's annualized expected return (decimal)
      risk_free: risk-free rate (decimal)
      epsilon: weight to add to candidate (default 2%)

    Returns scalar Sharpe delta. Positive = candidate would improve Sharpe.
    """
    cur_ret = float(current_weights @ mu)
    cur_var = float(current_weights @ sigma @ current_weights)
    cur_vol = max(0.0, cur_var) ** 0.5
    cur_sharpe = (cur_ret - risk_free) / cur_vol if cur_vol > 0 else 0.0

    scaled = current_weights * (1 - epsilon)
    new_ret = float(scaled @ mu) + epsilon * float(candidate_mu)
    new_var = (
        float(scaled @ sigma @ scaled)
        + 2 * epsilon * float(scaled @ candidate_cov)
        + epsilon ** 2 * float(candidate_var)
    )
    new_vol = max(0.0, new_var) ** 0.5
    new_sharpe = (new_ret - risk_free) / new_vol if new_vol > 0 else 0.0

    return new_sharpe - cur_sharpe
