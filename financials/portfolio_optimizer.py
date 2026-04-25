"""Black-Litterman portfolio optimizer.

Blends a market-cap equilibrium prior with Alpha Score views to produce a
posterior expected-return vector, then solves for optimal weights. Replaces
the naive Monte Carlo / historical-mean approach in
portfolio_risk.compute_efficient_frontier.

References:
  Black & Litterman (1992). Global Portfolio Optimization.
  He & Litterman (1999). The Intuition Behind Black-Litterman Model Portfolios.

Calibration is intentionally conservative. See portfolio-recommendations-plan.md.
"""

import math
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed

import numpy as np
import yfinance as yf

from . import cache
from .portfolio_risk import _fetch_daily_returns, RISK_FREE_RATE, TRADING_DAYS
from .alpha import compute_alpha_score, _get_factor_weights

DEFAULT_TAU = 0.05
DEFAULT_DELTA = 2.5
DEFAULT_VIEW_TILT = 0.03
DEFAULT_WINDOW = "3y"
COV_RIDGE = 1e-6
# One-way transaction cost applied to turnover (10 bps). Trades whose
# expected-return edge doesn't exceed 2x TC (round-trip) are dropped.
DEFAULT_TC_BPS = 10.0
MKT_CAP_TTL = 3600
SECTOR_TTL = 86400

DEFAULT_CONSTRAINTS = {
    "min_weight": 0.02,    # below this, position is zeroed (not forced up)
    "max_weight": 0.25,    # single-position cap
    "max_sector": 0.40,    # GICS sector cap
    "max_turnover": 0.30,  # L1 distance cap vs current weights
    "max_vol_ratio": 1.10, # optimal vol may not exceed current vol by more than 10%
}


def _fetch_market_cap(symbol):
    cache_key = f"mkt_cap:{symbol}"
    cached = cache.get(cache_key)
    if cached is not None:
        return cached
    try:
        info = yf.Ticker(symbol).info or {}
        raw = info.get("marketCap")
        cap = float(raw) if raw else None
    except Exception:
        cap = None
    cache.put(cache_key, cap, ttl=MKT_CAP_TTL)
    return cap


def _fetch_market_caps(symbols):
    caps = {}
    with ThreadPoolExecutor(max_workers=8) as pool:
        futures = {pool.submit(_fetch_market_cap, s): s for s in symbols}
        for future in as_completed(futures, timeout=30):
            sym = futures[future]
            try:
                caps[sym] = future.result(timeout=10)
            except Exception:
                caps[sym] = None
    return caps


ETF_SECTOR_KEY = "__ETF__"  # internal marker for ETF positions

GICS_SECTORS = [
    "Communication Services", "Consumer Cyclical", "Consumer Defensive",
    "Energy", "Financial Services", "Healthcare", "Industrials",
    "Real Estate", "Technology", "Utilities", "Basic Materials",
]


# Sector-specific ETF detection: match yfinance category substrings to GICS.
# Order doesn't matter — we just need any one to match. Lowercase compare.
_SECTOR_ETF_CATEGORY_MAP = {
    "technology": "Technology",
    "communication": "Communication Services",
    "financial": "Financial Services",
    "health": "Healthcare",
    "energy": "Energy",
    "industrial": "Industrials",
    "utilit": "Utilities",
    "consumer cyclical": "Consumer Cyclical",
    "consumer defensive": "Consumer Defensive",
    "real estate": "Real Estate",
    "basic material": "Basic Materials",
    "natural resource": "Basic Materials",
    "precious metal": "Basic Materials",
}


def _classify_etf_sector(category):
    """Match yfinance ETF category to a GICS sector name. Returns the GICS
    sector if the category targets a single sector (e.g. XLK -> Technology),
    or None for broad-market ETFs (Large Blend, Foreign Large Blend, etc.).
    """
    if not category:
        return None
    cat_lower = category.lower()
    for needle, gics in _SECTOR_ETF_CATEGORY_MAP.items():
        if needle in cat_lower:
            return gics
    return None


def _fetch_classification(symbol):
    """Return (sector, is_etf_like). Broad-market ETFs get sector=ETF_SECTOR_KEY
    (exempt from cap, modeled as spread across all sectors). Sector-specific
    ETFs get their actual GICS sector — they're treated like a stock in that
    sector for cap and diversification purposes, but Alpha is still pinned at
    neutral 50 since they have no factor sub-scores.
    """
    cache_key = f"classification:{symbol}"
    cached = cache.get(cache_key)
    if cached is not None:
        return cached
    sector, is_etf = "Unknown", False
    try:
        info = yf.Ticker(symbol).info or {}
        qt = (info.get("quoteType") or "").upper()
        if qt in ("ETF", "MUTUALFUND"):
            is_etf = True
            sector_match = _classify_etf_sector(info.get("category"))
            sector = sector_match if sector_match else ETF_SECTOR_KEY
        else:
            sector = info.get("sector") or "Unknown"
    except Exception:
        pass
    result = (sector, is_etf)
    cache.put(cache_key, result, ttl=SECTOR_TTL)
    return result


def _fetch_classifications(symbols):
    """Batch wrapper. Returns dict symbol -> (sector, is_etf)."""
    out = {}
    with ThreadPoolExecutor(max_workers=8) as pool:
        futures = {pool.submit(_fetch_classification, s): s for s in symbols}
        for future in as_completed(futures, timeout=30):
            sym = futures[future]
            try:
                out[sym] = future.result(timeout=10)
            except Exception:
                out[sym] = ("Unknown", False)
    return out


def ledoit_wolf_shrinkage(returns_matrix):
    """Ledoit-Wolf (2004) shrinkage toward a scaled-identity target.

    Returns (sigma_daily, shrinkage_intensity). `returns_matrix` is T x N.
    Shrinkage intensity is in [0, 1] where 1 = full shrinkage to target
    (homogeneous variance, zero correlation), 0 = sample covariance.

    The scaled-identity target is the simplest Ledoit-Wolf variant and
    works well for equity returns at typical N (10-40 holdings) and
    T (500-750 days).

    Math (Ledoit & Wolf 2003/2004):
      S = sample cov (T-divisor, not T-1)
      F = (trace(S) / N) * I                  [shrinkage target]
      d^2 = ||S - F||^2_F                      [squared distance to target]
      b^2 = (1/T^2) * sum_t ||x_t x_t' - S||^2_F   [MLE noise]
      b^2 capped at d^2; delta = b^2 / d^2 in [0, 1]
      S_shrunk = delta * F + (1 - delta) * S
    """
    T, N = returns_matrix.shape
    if T < 2 or N < 1:
        return np.eye(N), 1.0

    X = returns_matrix - returns_matrix.mean(axis=0, keepdims=True)
    S = (X.T @ X) / T  # MLE cov, matches Ledoit-Wolf paper

    # Target: scaled identity.
    mu = np.trace(S) / N
    F = mu * np.eye(N)

    # d^2 = ||S - F||^2_F
    d2 = float(np.sum((S - F) ** 2))
    if d2 <= 0:
        return S, 0.0

    # b^2 vectorized: (1/T^2) * sum_t [(x_t'x_t)^2 - 2 x_t' S x_t + ||S||^2_F]
    norms_x = (X * X).sum(axis=1)                  # T-vector
    qforms = np.einsum("ij,jk,ik->i", X, S, X)     # T-vector
    frob_S_sq = float((S * S).sum())
    b2 = float(((norms_x ** 2).sum() - 2 * qforms.sum() + T * frob_S_sq) / (T ** 2))
    b2 = max(0.0, min(b2, d2))  # theoretical bound

    delta = b2 / d2
    delta = max(0.0, min(1.0, delta))
    sigma = delta * F + (1 - delta) * S
    return sigma, delta


def _cap_max(w, max_w):
    # Water-fill: cap above-max positions, redistribute excess proportionally
    # across unconstrained positions until stable.
    w = np.array(w, dtype=float)
    for _ in range(30):
        over = w > max_w + 1e-9
        if not over.any():
            break
        excess = float((w[over] - max_w).sum())
        w[over] = max_w
        free = ~over
        free_sum = float(w[free].sum())
        if free_sum <= 0:
            break
        w[free] = w[free] * (free_sum + excess) / free_sum
    return w


def _floor_min(w, min_w):
    return np.where((w > 0) & (w < min_w), 0.0, w)


def _cap_sector(w, sectors, max_sector):
    # Effective sector cap: single-stock weight in a sector PLUS that sector's
    # share of the broad-market ETF allocation must be <= max_sector. This
    # stops the optimizer from concentrating Tech in single stocks when ETFs
    # already provide significant Tech exposure. ETFs themselves aren't moved
    # by this function (they're rebalanced elsewhere in the pipeline) — we
    # cap by reducing the single-stock contribution to over-cap sectors.
    w = np.array(w, dtype=float)
    etf_mask = np.array([s == ETF_SECTOR_KEY for s in sectors])
    etf_total = float(w[etf_mask].sum())
    etf_per_sector = etf_total / len(GICS_SECTORS)
    effective_max = max(0.0, max_sector - etf_per_sector)

    unique = set(s for s in sectors if s != ETF_SECTOR_KEY)
    for _ in range(30):
        changed = False
        for sec in unique:
            mask = np.array([s == sec for s in sectors])
            single_stock_sec_total = float(w[mask].sum())
            if single_stock_sec_total > effective_max + 1e-9:
                excess = single_stock_sec_total - effective_max
                w[mask] = w[mask] * (effective_max / single_stock_sec_total)
                # Redistribute excess to OTHER non-ETF positions only, so ETF
                # total stays fixed and effective_max doesn't shift mid-loop.
                non_etf_other = (~mask) & (~etf_mask)
                other_sum = float(w[non_etf_other].sum())
                if other_sum > 0:
                    w[non_etf_other] = w[non_etf_other] * (other_sum + excess) / other_sum
                elif etf_total > 0:
                    # Fallback: no other single-stock capacity. Push to ETFs.
                    w[etf_mask] = w[etf_mask] * (etf_total + excess) / etf_total
                changed = True
        if not changed:
            break
    return w


def _apply_hard_constraints(w, sectors, min_w, max_w, max_sector):
    # Iteratively apply position and sector caps until both satisfied.
    w = np.array(w, dtype=float)
    s = w.sum()
    if s > 0:
        w = w / s
    for _ in range(20):
        w_prev = w.copy()
        w = _cap_max(w, max_w)
        w = _cap_sector(w, sectors, max_sector)
        s = w.sum()
        if s > 0:
            w = w / s
        if np.allclose(w, w_prev, atol=1e-6):
            break
    w = _floor_min(w, min_w)
    s = w.sum()
    if s > 0:
        w = w / s
    return w


def _blend_with_turnover_cap(w_opt, w_current, w_current_feasible, max_turnover):
    # Blend w_opt toward w_current_feasible (both are feasible, so any blend
    # is feasible too). Measure turnover against original w_current. Binary
    # search for largest alpha under the cap. Returns (w, alpha, min_achievable).
    min_achievable = float(np.sum(np.abs(w_current_feasible - w_current)))
    if min_achievable >= max_turnover - 1e-9:
        # Bringing current back into compliance already exceeds turnover cap.
        # Hard constraints win; accept the feasible projection of current.
        return w_current_feasible, 0.0, min_achievable
    lo, hi = 0.0, 1.0
    for _ in range(40):
        mid = (lo + hi) / 2
        w = mid * w_opt + (1 - mid) * w_current_feasible
        if float(np.sum(np.abs(w - w_current))) <= max_turnover:
            lo = mid
        else:
            hi = mid
    w = lo * w_opt + (1 - lo) * w_current_feasible
    return w, lo, min_achievable


def _cap_vol(w_opt, w_current, sigma, max_vol):
    """Find a blend of w_opt and w_current with vol <= max_vol if possible.
    Returns (w_blended, alpha, honored). When both endpoints exceed the cap,
    falls back to the variance-minimizing blend (closed form on the quadratic
    along the linear path) and reports honored=False.
    """
    vol_opt = float(math.sqrt(max(0.0, w_opt @ sigma @ w_opt)))
    if vol_opt <= max_vol:
        return w_opt, 1.0, True

    var_cur = float(w_current @ sigma @ w_current)
    vol_cur = math.sqrt(max(0.0, var_cur))

    if vol_cur <= max_vol:
        # Standard case: monotone reduction toward w_current. Binary search.
        lo, hi = 0.0, 1.0
        for _ in range(40):
            mid = (lo + hi) / 2
            w = mid * w_opt + (1 - mid) * w_current
            v = float(math.sqrt(max(0.0, w @ sigma @ w)))
            if v <= max_vol:
                lo = mid
            else:
                hi = mid
        return lo * w_opt + (1 - lo) * w_current, lo, True

    # Both endpoints exceed cap. Variance along blend is quadratic in alpha;
    # closed-form minimum: alpha = (var_cur - cov) / (var_opt + var_cur - 2*cov).
    var_opt = float(w_opt @ sigma @ w_opt)
    cov = float(w_opt @ sigma @ w_current)
    denom = var_opt + var_cur - 2 * cov
    if denom <= 1e-12:
        return w_current, 0.0, False
    alpha_star = max(0.0, min(1.0, (var_cur - cov) / denom))
    return alpha_star * w_opt + (1 - alpha_star) * w_current, alpha_star, False


def _apply_constraints(w_opt, w_current, sectors, sigma, constraints):
    """Project w_opt into the feasible region. Returns (w_final, report).

    Pipeline: project both w_opt and w_current through the hard-constraint set
    (position bounds, sector caps), then blend between those two feasible
    points with a turnover cap measured against original w_current. A final
    vol cap can blend further toward the feasible current if needed. Since
    both blend endpoints satisfy hard constraints, any blend is feasible.
    """
    report = {
        "applied": [],
        "turnover_alpha": 1.0,
        "vol_alpha": 1.0,
        "min_turnover_forced": 0.0,
    }

    min_w = constraints["min_weight"]
    max_w = constraints["max_weight"]
    max_sector = constraints["max_sector"]

    w_opt_feasible = _apply_hard_constraints(w_opt, sectors, min_w, max_w, max_sector)
    w_cur_feasible = _apply_hard_constraints(w_current, sectors, min_w, max_w, max_sector)
    report["applied"].extend(["floor_min", "cap_max", "cap_sector"])

    w, t_alpha, min_forced = _blend_with_turnover_cap(
        w_opt_feasible, w_current, w_cur_feasible, constraints["max_turnover"],
    )
    report["turnover_alpha"] = round(t_alpha, 3)
    report["min_turnover_forced"] = round(min_forced, 3)
    if min_forced > constraints["max_turnover"]:
        report["applied"].append("turnover_forced_above_cap")
    elif t_alpha < 1.0:
        report["applied"].append("cap_turnover")

    cur_vol = float(math.sqrt(max(0.0, w_current @ sigma @ w_current)))
    max_vol = cur_vol * constraints["max_vol_ratio"]
    w, v_alpha, vol_honored = _cap_vol(w, w_cur_feasible, sigma, max_vol)
    report["vol_alpha"] = round(v_alpha, 3)
    report["vol_cap_honored"] = vol_honored
    if v_alpha < 1.0:
        report["applied"].append("cap_vol")
    if not vol_honored:
        report["applied"].append("vol_cap_unenforceable")

    return w, report


def _fetch_alpha_scores(symbols):
    scores = {}
    with ThreadPoolExecutor(max_workers=6) as pool:
        futures = {pool.submit(compute_alpha_score, s): s for s in symbols}
        for future in as_completed(futures, timeout=90):
            sym = futures[future]
            try:
                result = future.result(timeout=30)
                if result and "alphaScore" in result:
                    scores[sym] = {
                        "alphaScore": result["alphaScore"],
                        "subScores": result.get("subScores", {}),
                    }
            except Exception:
                pass
    return scores


def _confidence_score(views_detail):
    """Compute a 0-100 signal strength score for the recommendation.

    Despite the field name (kept for DB schema stability), this measures
    SIGNAL STRENGTH — how strong are the factor views going in — not
    confidence in the rec or impact of acting on it. The UI labels this
    "Signal Strength" and shows Impact separately.

    Simple v1 heuristic: base 50, up to +40 for view strength, +10 for
    data completeness. Strong views in this setup cap around 0.5% excess
    (skeptical tilt=0.03 means 0-100 Alpha range spans +/-1.5%).
    """
    if not views_detail:
        return 0, "Low"
    strengths = [abs(v.get("viewExcess") or 0.0) for v in views_detail]
    mean_strength = sum(strengths) / len(strengths)
    with_alpha = sum(1 for v in views_detail if v.get("alphaScore") is not None)
    completeness = with_alpha / len(views_detail) if views_detail else 0
    view_signal = min(1.0, mean_strength / 0.5)
    score = int(round(50 + 40 * view_signal + 10 * completeness))
    label = "High" if score >= 75 else ("Moderate" if score >= 55 else "Low")
    return score, label


def _impact_score(current, optimal, total_value, trades):
    """How much will acting on this rec actually change the portfolio?

    Composite of three normalized signals: Sharpe delta, diversification
    delta, and dollar turnover as a fraction of portfolio. A "High signal
    strength, Low impact" rec is the constrained-but-conviction case
    (signal wants more, constraints won't let us); "Low signal, High
    impact" is the rare case where we're forced to do a lot for compliance.
    """
    sharpe_delta = abs(optimal.get("sharpe", 0) - current.get("sharpe", 0))
    div_delta = abs(optimal.get("diversification", 0) - current.get("diversification", 0))
    turnover_dollar = sum(abs(t.get("diffDollar", 0) or 0) for t in trades)
    turnover_pct = turnover_dollar / total_value if total_value > 0 else 0.0

    sharpe_signal = min(1.0, sharpe_delta / 0.10)
    div_signal = min(1.0, div_delta / 15.0)
    turnover_signal = min(1.0, turnover_pct / 0.30)

    composite = (sharpe_signal + div_signal + turnover_signal) / 3.0
    score = int(round(composite * 100))
    label = "High" if score >= 60 else ("Moderate" if score >= 30 else "Low")
    return score, label


def _diversification_score(weights, sectors):
    """Effective-sector-count diversification, normalized to 100.

    Models broad-market ETFs as spread evenly across the 11 GICS sectors
    (approximation; sector-specific ETF handling is task #11). Individual
    stocks contribute to their own sector. Score normalized against fully
    diversified (equal weight across all 11 sectors = 100).

    100% one stock -> ~9. 100% one broad ETF -> 100. 50/50 mix -> ~28.
    """
    sector_totals = defaultdict(float)
    for w, s in zip(weights, sectors):
        if s == ETF_SECTOR_KEY:
            etf_share = float(w) / len(GICS_SECTORS)
            for sec in GICS_SECTORS:
                sector_totals[sec] += etf_share
        else:
            sector_totals[s] += float(w)
    if not sector_totals:
        return 0
    hhi = sum(v ** 2 for v in sector_totals.values())
    if hhi <= 0:
        return 0
    effective = 1.0 / hhi
    return int(round(effective / len(GICS_SECTORS) * 100))


def _capture_regime():
    """Capture market regime indicators at rec time. Used for outcome
    attribution later (Phase 4 weight learner conditions on regime)."""
    def _px(ticker):
        try:
            info = yf.Ticker(ticker).info or {}
            val = info.get("regularMarketPrice") or info.get("previousClose")
            return float(val) if val else None
        except Exception:
            return None

    vix = _px("^VIX")
    t10 = _px("^TNX")
    t3m = _px("^IRX")
    spread = None
    if t10 is not None and t3m is not None:
        spread = round(t10 - t3m, 3)
    return {"vix": vix, "yield_curve_10y_3m": spread}


def _compute_attribution(views_detail, factor_weights, tilt, neutral=50.0):
    """Decompose each view excess into per-factor contributions.

    alpha.py composites via weighted AVERAGE (divides by total_weight), so
    we use the same normalization here: w_norm = w / Σw. Then:
      contribution_pct_f_i = w_norm_f * (subScore_f_i - neutral) * tilt
    which sums exactly to view_i.
    """
    total_weight = sum(float(w) for w in factor_weights.values()) or 1.0
    norm_weights = {k: float(v) / total_weight for k, v in factor_weights.items()}

    attribution = {}
    for v in views_detail:
        sym = v["symbol"]
        sub_scores = v.get("subScores") or {}
        if not sub_scores:
            continue

        factors = []
        for name, weight in norm_weights.items():
            score = sub_scores.get(name)
            if score is None:
                continue
            contribution = weight * (float(score) - neutral) * tilt
            factors.append({
                "name": name,
                "subScore": int(score),
                "weight": round(weight, 4),
                "contribution": round(contribution, 4),
            })
        factors.sort(key=lambda f: abs(f["contribution"]), reverse=True)

        top_drivers = [f["name"] for f in factors if f["contribution"] > 0][:3]
        top_detractors = [f["name"] for f in factors if f["contribution"] < 0][:3]

        attribution[sym] = {
            "alphaScore": v.get("alphaScore"),
            "viewExcess": v.get("viewExcess"),
            "factors": factors,
            "topDrivers": top_drivers,
            "topDetractors": top_detractors,
        }
    return attribution


def _alpha_views(alpha_scores, tilt=DEFAULT_VIEW_TILT, neutral=50.0):
    # Map Alpha to excess return relative to a fixed neutral score (50).
    # A portfolio of all high-Alpha stocks still produces positive tilts
    # off the market prior; conservative lean is in the tilt constant itself.
    return [(a - neutral) / 100.0 * tilt for a in alpha_scores]


MIN_HISTORY_DAYS = 252  # 1 trading year — covariance below this is too noisy


def _build_return_matrix(symbols, window=DEFAULT_WINDOW):
    """Fetch aligned daily returns. Returns (matrix, usable, dropped) where
    dropped is a list of (symbol, days_available) tuples for holdings cut
    from the optimization for insufficient history.
    """
    returns_map = {}
    with ThreadPoolExecutor(max_workers=8) as pool:
        futures = {pool.submit(_fetch_daily_returns, s, window): s for s in symbols}
        for future in as_completed(futures, timeout=45):
            sym = futures[future]
            try:
                returns_map[sym] = future.result(timeout=15)
            except Exception:
                returns_map[sym] = []

    usable = [s for s in symbols if len(returns_map.get(s, [])) >= MIN_HISTORY_DAYS]
    dropped = [(s, len(returns_map.get(s, []))) for s in symbols if s not in usable]
    if len(usable) < 2:
        return None, usable, dropped
    min_len = min(len(returns_map[s]) for s in usable)
    matrix = np.array([returns_map[s][-min_len:] for s in usable]).T
    return matrix, usable, dropped


def black_litterman_optimize(holdings, tau=DEFAULT_TAU, delta=DEFAULT_DELTA,
                              tilt=DEFAULT_VIEW_TILT, window=DEFAULT_WINDOW,
                              constraints=None,
                              as_of_date=None,
                              alpha_scores_override=None,
                              regime_override=None,
                              historical_returns_matrix=None,
                              historical_usable=None,
                              pin_broad_etfs=False):
    """Run Black-Litterman optimization on a list of holdings.

    holdings: list of dicts with at minimum 'symbol' and 'currentValue'.
    Returns None on insufficient data. Otherwise returns a dict with
    current/optimal stats, trades, per-symbol view detail, and diagnostics.

    Backtest hooks (all optional, unused in the production path):
      as_of_date: datetime — rec date for a historical run.
      alpha_scores_override: {symbol: {alphaScore, subScores}} — skip yfinance
        fetch of current scores; use pre-computed historical reconstruction.
      regime_override: {vix, yield_curve_10y_3m} — skip live regime capture.
      historical_returns_matrix, historical_usable: pre-built return matrix and
        usable-symbol list ending at as_of_date. Lets the backtest fetch once
        and reuse across multiple holdings-templates for the same date.
    """
    valid = [h for h in holdings if (h.get("currentValue") or 0) > 0 and h.get("symbol")]
    if len(valid) < 2:
        return None

    all_symbols = [h["symbol"] for h in valid]
    if historical_returns_matrix is not None and historical_usable is not None:
        # Backtest path — caller supplied the pre-built matrix.
        return_matrix = historical_returns_matrix
        usable = historical_usable
        dropped_history = [
            (s, 0) for s in all_symbols if s not in usable
        ]
        if return_matrix is None or len(usable) < 2:
            return None
    else:
        return_matrix, usable, dropped_history = _build_return_matrix(all_symbols, window)
        if return_matrix is None:
            return None

    usable_holdings = [h for h in valid if h["symbol"] in usable]
    symbols = [h["symbol"] for h in usable_holdings]
    n = len(symbols)
    usable_total = sum(h["currentValue"] for h in usable_holdings)
    current_weights = np.array([h["currentValue"] / usable_total for h in usable_holdings])

    # Covariance via Ledoit-Wolf shrinkage toward a scaled-identity target.
    # Reduces noise in off-diagonal estimates; sample cov is notoriously noisy
    # at typical N/T ratios. Tiny ridge kept for numerical insurance against
    # degenerate returns (e.g., ETFs with near-identical daily closes).
    sigma_daily, shrinkage_intensity = ledoit_wolf_shrinkage(return_matrix)
    sigma = sigma_daily * TRADING_DAYS + np.eye(n) * COV_RIDGE

    # Market-cap equilibrium prior. If some caps are missing, substitute the
    # median of resolved caps rather than silently falling back to equal-weight
    # for the whole portfolio. If fewer than half resolve, the median is a bad
    # anchor and equal-weight is the honest fallback.
    caps_map = _fetch_market_caps(symbols)
    resolved = [c for c in (caps_map.get(s) for s in symbols) if c and c > 0]
    caps_resolved_ratio = len(resolved) / n if n else 0
    if caps_resolved_ratio < 0.5 or not resolved:
        w_mkt = np.ones(n) / n
        caps_fallback = "equal_weight"
    else:
        median_cap = float(np.median(resolved))
        cap_list = [
            float(caps_map.get(s)) if (caps_map.get(s) and caps_map.get(s) > 0)
            else median_cap
            for s in symbols
        ]
        caps_array = np.array(cap_list, dtype=float)
        w_mkt = caps_array / caps_array.sum()
        caps_fallback = "median_substitution" if caps_resolved_ratio < 1.0 else "all_resolved"

    pi = delta * sigma @ w_mkt

    if alpha_scores_override is not None:
        alpha_data = {s: alpha_scores_override[s] for s in symbols if s in alpha_scores_override}
    else:
        alpha_data = _fetch_alpha_scores(symbols)
    # ETFs get Alpha=50 (neutral) — sit at market-cap prior with no view, so
    # the optimizer doesn't actively trim broad-market index funds into single
    # stocks. classifications_map populated below; precompute is_etf set here.
    classifications_map = _fetch_classifications(symbols)
    is_etf_set = {s for s, (_, is_etf) in classifications_map.items() if is_etf}
    is_broad_etf_set = {
        s for s, (sec, is_etf) in classifications_map.items()
        if is_etf and sec == ETF_SECTOR_KEY
    }
    for s in is_etf_set:
        alpha_data[s] = {"alphaScore": 50, "subScores": {}}
    alphas = [alpha_data.get(s, {}).get("alphaScore", 50.0) for s in symbols]
    view_q = np.array(_alpha_views(alphas, tilt=tilt))

    p_mat = np.eye(n)
    # He-Litterman convention: view uncertainty proportional to tau * own variance.
    omega = np.diag(tau * np.diag(sigma))

    try:
        tau_sigma_inv = np.linalg.inv(tau * sigma)
        omega_inv = np.linalg.inv(omega)
        posterior_precision = tau_sigma_inv + p_mat.T @ omega_inv @ p_mat
        mu = np.linalg.solve(
            posterior_precision,
            tau_sigma_inv @ pi + p_mat.T @ omega_inv @ view_q,
        )
        w_opt_raw = np.linalg.solve(delta * sigma, mu)
    except np.linalg.LinAlgError:
        return None

    # Long-only projection. Not the QP-optimal long-only solution; a simple
    # truncate-and-renormalize is sufficient for v1.
    w_opt = np.clip(w_opt_raw, 0.0, None)
    total_w = w_opt.sum()
    if total_w <= 0:
        w_opt = current_weights.copy()
    else:
        w_opt = w_opt / total_w

    # Constraint layer: position bounds, sector caps, turnover cap, vol cap.
    # Reuse classifications_map fetched earlier for ETF detection.
    effective_constraints = {**DEFAULT_CONSTRAINTS, **(constraints or {})}
    sector_list = [classifications_map.get(s, ("Unknown", False))[0] for s in symbols]
    w_opt, constraint_report = _apply_constraints(
        w_opt, current_weights, sector_list, sigma, effective_constraints,
    )

    # Index-Core mode: pin broad-market ETF weights to current, redistribute
    # the remainder across non-ETF positions only. Lets users hold a passive
    # core (VOO/VTI/QQQ etc.) untouched while the optimizer works on the
    # satellite. Non-ETF positions get scaled proportionally to their
    # post-constraint weights, then we re-cap any that exceed max_weight.
    pinned_etfs_count = 0
    if pin_broad_etfs and is_broad_etf_set:
        etf_idx = [i for i, s in enumerate(symbols) if s in is_broad_etf_set]
        non_etf_idx = [i for i in range(n) if i not in etf_idx]
        if etf_idx and non_etf_idx:
            pinned_total = float(sum(current_weights[i] for i in etf_idx))
            free_budget = max(0.0, 1.0 - pinned_total)
            non_etf_sum = float(sum(w_opt[i] for i in non_etf_idx))
            if non_etf_sum > 0 and free_budget > 0:
                scale = free_budget / non_etf_sum
                for i in etf_idx:
                    w_opt[i] = float(current_weights[i])
                for i in non_etf_idx:
                    w_opt[i] = float(w_opt[i]) * scale
                # Re-cap any non-ETF that breached max_weight after scaling.
                # Only redistribute among other non-ETF (ETFs stay pinned).
                max_w = effective_constraints["max_weight"]
                for _ in range(20):
                    over = [i for i in non_etf_idx if w_opt[i] > max_w + 1e-9]
                    if not over:
                        break
                    excess = sum(w_opt[i] - max_w for i in over)
                    for i in over:
                        w_opt[i] = max_w
                    free_others = [i for i in non_etf_idx if i not in over]
                    free_others_sum = sum(w_opt[i] for i in free_others)
                    if free_others_sum <= 0:
                        break
                    for i in free_others:
                        w_opt[i] *= (free_others_sum + excess) / free_others_sum
                pinned_etfs_count = len(etf_idx)
                constraint_report["applied"].append("pin_broad_etfs")

    def _stats(w):
        exp_ret = float(w @ mu)
        vol = float(math.sqrt(max(0.0, w @ sigma @ w)))
        sharpe = (exp_ret - RISK_FREE_RATE) / vol if vol > 0 else 0.0
        return exp_ret, vol, sharpe

    cur_ret, cur_vol, cur_sharpe = _stats(current_weights)
    opt_ret, opt_vol, opt_sharpe = _stats(w_opt)

    # Transaction cost gate. For each trade, expected annual benefit is
    # approximately Δw × (mu_i - rf); round-trip cost is 2 × |Δw| × tc. We drop
    # trades where the posterior return edge over cash is smaller than the
    # round-trip cost (i.e., |mu_i - rf| < 2 × tc). Weak-conviction positions
    # may still hold their current weight for diversification — we just won't
    # recommend paying TC to add to them.
    tc_decimal = DEFAULT_TC_BPS / 10000.0
    tc_edge_hurdle = 2 * tc_decimal
    tc_filtered = 0

    trades = []
    sub_threshold_trades = []  # everything the optimizer moved but we didn't surface as a trade
    SUB_THRESHOLD_FLOOR_PCT = 0.1  # don't bother collecting near-zero drifts
    for i, h in enumerate(usable_holdings):
        curr_pct = float(current_weights[i]) * 100
        opt_pct = float(w_opt[i]) * 100
        diff_pct = opt_pct - curr_pct
        diff_dollar = diff_pct / 100 * usable_total
        row = {
            "symbol": h["symbol"],
            "name": h.get("name", ""),
            "currentPct": round(curr_pct, 1),
            "optimalPct": round(opt_pct, 1),
            "diffPct": round(diff_pct, 2),
            "diffDollar": round(diff_dollar, 2),
            "action": "Increase" if diff_pct > 0 else ("Decrease" if diff_pct < 0 else "Hold"),
            "isEtf": h["symbol"] in is_etf_set,
            "isBroadEtf": h["symbol"] in is_broad_etf_set,
            "etfSector": (
                classifications_map[h["symbol"]][0]
                if h["symbol"] in is_etf_set and h["symbol"] not in is_broad_etf_set
                else None
            ),
        }

        mu_edge = abs(float(mu[i]) - RISK_FREE_RATE)
        passes_tc = mu_edge >= tc_edge_hurdle
        if not passes_tc and abs(diff_pct) < 5.0:
            # TC gate: weak view + small move. Keep in sub-threshold list so
            # the UI can surface it on "see all changes", but not in headline trades.
            tc_filtered += 1
            if abs(diff_pct) >= SUB_THRESHOLD_FLOOR_PCT:
                row["filteredReason"] = "tc"
                sub_threshold_trades.append(row)
            continue
        # Headline threshold: 2% AND $100 minimum. Sub-threshold moves are
        # below the noise floor and not worth executing given fees / friction.
        if abs(diff_pct) >= 2.0 and abs(diff_dollar) >= 100:
            trades.append(row)
        elif abs(diff_pct) >= SUB_THRESHOLD_FLOOR_PCT:
            row["filteredReason"] = "threshold"
            sub_threshold_trades.append(row)
    trades.sort(key=lambda x: abs(x["diffPct"]), reverse=True)
    sub_threshold_trades.sort(key=lambda x: abs(x["diffPct"]), reverse=True)

    views_detail = []
    for i, sym in enumerate(symbols):
        a = alpha_data.get(sym, {})
        views_detail.append({
            "symbol": sym,
            "alphaScore": a.get("alphaScore"),
            "subScores": a.get("subScores", {}),
            "viewExcess": round(float(view_q[i]) * 100, 2),
            "impliedReturn": round(float(pi[i]) * 100, 2),
            "posteriorReturn": round(float(mu[i]) * 100, 2),
        })

    try:
        factor_weights_snapshot = _get_factor_weights()
    except Exception:
        factor_weights_snapshot = {}
    attribution = _compute_attribution(views_detail, factor_weights_snapshot, tilt)

    confidence_score, confidence_label = _confidence_score(views_detail)
    current_diversification = _diversification_score(current_weights, sector_list)
    optimal_diversification = _diversification_score(w_opt, sector_list)
    regime = regime_override if regime_override is not None else _capture_regime()

    current_block = {
        "return": round(cur_ret * 100, 1),
        "volatility": round(cur_vol * 100, 1),
        "sharpe": round(cur_sharpe, 2),
        "diversification": current_diversification,
        "weights": [{"symbol": symbols[i], "weight": round(float(current_weights[i]) * 100, 1)} for i in range(n)],
    }
    optimal_block = {
        "return": round(opt_ret * 100, 1),
        "volatility": round(opt_vol * 100, 1),
        "sharpe": round(opt_sharpe, 2),
        "diversification": optimal_diversification,
        "weights": [{"symbol": symbols[i], "weight": round(float(w_opt[i]) * 100, 1)} for i in range(n)],
    }
    impact_score, impact_label = _impact_score(current_block, optimal_block, usable_total, trades)

    return {
        "current": current_block,
        "optimal": optimal_block,
        "confidenceScore": confidence_score,
        "confidenceLabel": confidence_label,
        "impactScore": impact_score,
        "impactLabel": impact_label,
        "regime": regime,
        "trades": trades,
        "subThresholdTrades": sub_threshold_trades,
        "views": views_detail,
        "attribution": attribution,
        "factorWeights": factor_weights_snapshot,
        "diagnostics": {
            "tau": tau,
            "delta": delta,
            "tilt": tilt,
            "window": window,
            "usableSymbols": symbols,
            "dataDays": int(return_matrix.shape[0]),
            "constraints": effective_constraints,
            "constraintReport": constraint_report,
            "sectors": dict(zip(symbols, sector_list)),
            "droppedShortHistory": [
                {"symbol": s, "daysAvailable": d} for s, d in dropped_history
            ],
            "minHistoryDays": MIN_HISTORY_DAYS,
            "shrinkageIntensity": round(float(shrinkage_intensity), 3),
            "mktCapResolvedRatio": round(caps_resolved_ratio, 2),
            "mktCapFallback": caps_fallback,
            "transactionCostBps": DEFAULT_TC_BPS,
            "tcFilteredTrades": tc_filtered,
            "pinnedEtfsCount": pinned_etfs_count,
        },
        "totalValue": usable_total,
        "improvementPct": round((opt_sharpe - cur_sharpe) / abs(cur_sharpe) * 100, 1) if cur_sharpe != 0 else 0,
    }
