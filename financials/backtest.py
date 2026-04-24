"""Backtest harness for the portfolio optimizer.

Generates synthetic rec-outcome pairs by running the optimizer on a series
of historical dates, using the Alpha-Lite reconstruction (price-based
factors only; non-price factors neutral at 50). Writes recs to the existing
portfolio_recommendations table with client_id="backtest_v1" so the
existing outcome observer and history view work unchanged.

Usage:
    python -m financials.backtest run \
      --start 2020-01-01 --end 2024-06-30 --cadence monthly \
      --templates broad_index,tech_heavy,diversified
    python -m financials.backtest observe   # run outcome observer on all backtest recs
    python -m financials.backtest summary   # IC + hit-rate stats

Notes on fidelity:
- Alpha scores reconstructed from price data only. Non-price factors held
  at a neutral 50. Documented in every rec via `notes` field.
- Market caps and sector classifications are CURRENT, not historical. An
  acceptable approximation for large-cap US equities over 2020-2024. Do
  not trust the backtest for names that changed industry or had major
  cap swings inside the window.
- Holdings templates are held static per-rec (no cumulative rebalancing
  between rec dates). Each rec is an independent "what would the optimizer
  have said on this date" snapshot.
"""

import argparse
import logging
import math
import sys
import time
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
from typing import Optional

import numpy as np

from . import cache
from .alpha_historical import (
    BacktestContext, prefetch_backtest_context, reconstruct_alpha_lite,
    _daily_closes_up_to, _value_at,
    RECONSTRUCTED_FACTORS, NON_RECONSTRUCTED_FACTORS,
)
from .portfolio_optimizer import (
    black_litterman_optimize, _fetch_classifications, MIN_HISTORY_DAYS,
    DEFAULT_WINDOW, TRADING_DAYS,
)
from .portfolio_risk import RISK_FREE_RATE
from .recommendations import insert_recommendation
from .outcome_observer import compute_outcomes_for_rec, HORIZONS_DAYS
from .outcomes import insert_outcome, get_outcomes_for_rec

BACKTEST_CLIENT_ID = "backtest_v1"
DEFAULT_TOTAL_VALUE = 100_000.0
WINDOW_YEARS = 3

log = logging.getLogger("backtest")


# ── Portfolio templates ──────────────────────────────────────────────
# Each template = list of (symbol, weight) pairs that sum to 1.0. Holdings
# are fabricated by multiplying weights by DEFAULT_TOTAL_VALUE.
#
# Chosen to stress-test the optimizer across different starting conditions:
# - broad_index: US passive allocation. Tests "does optimizer leave well-
#   diversified portfolios alone?"
# - tech_heavy: concentrated single-sector. Tests diversification nudge.
# - diversified_equity: ~10 names across sectors, equal weight. Tests
#   signal-driven rebalancing without extreme starting skew.

PORTFOLIO_TEMPLATES = {
    "broad_index": [
        ("VOO", 0.40), ("QQQ", 0.30), ("IWM", 0.30),
    ],
    "tech_heavy": [
        ("AAPL", 0.20), ("MSFT", 0.20), ("GOOGL", 0.20),
        ("AMZN", 0.20), ("NVDA", 0.20),
    ],
    "diversified_equity": [
        ("AAPL", 0.10), ("JPM", 0.10), ("JNJ", 0.10), ("PG", 0.10),
        ("XOM", 0.10), ("NEE", 0.10), ("HD", 0.10), ("BA", 0.10),
        ("VZ", 0.10), ("CAT", 0.10),
    ],
}


def _rec_dates(start, end, cadence="monthly"):
    """Generate rec dates between start and end at the given cadence."""
    out = []
    current = start
    while current <= end:
        out.append(current)
        if cadence == "monthly":
            # Add ~30 days, aligned to the first of next month for cleanness.
            year = current.year
            month = current.month + 1
            if month > 12:
                year += 1
                month = 1
            current = current.replace(year=year, month=month, day=1)
        elif cadence == "quarterly":
            month = current.month + 3
            year = current.year
            while month > 12:
                month -= 12
                year += 1
            current = current.replace(year=year, month=month, day=1)
        else:
            raise ValueError(f"unknown cadence: {cadence}")
    return out


def _template_holdings(template_name, total_value=DEFAULT_TOTAL_VALUE):
    """Build a holdings list for a template. Returns [{symbol, currentValue}]."""
    if template_name not in PORTFOLIO_TEMPLATES:
        raise ValueError(f"unknown template: {template_name}")
    return [
        {"symbol": sym, "currentValue": w * total_value}
        for sym, w in PORTFOLIO_TEMPLATES[template_name]
    ]


def _build_historical_return_matrix(symbols, as_of_date, ctx, window_years=WINDOW_YEARS):
    """Assemble the annualized-ready daily returns matrix for `symbols`
    ending at `as_of_date`. Uses cached full-history closes from ctx.

    Returns (matrix T×N, usable list of symbols, dropped list of (symbol, days)).
    """
    start_cutoff = as_of_date - timedelta(days=window_years * 365 + 7)
    returns_map = {}
    for sym in symbols:
        series = ctx.closes.get(sym, [])
        closes_in_window = [
            (d, c) for d, c in series
            if start_cutoff <= d <= as_of_date
        ]
        if len(closes_in_window) < 2:
            returns_map[sym] = []
            continue
        prices = [c for _d, c in closes_in_window]
        rets = []
        for i in range(1, len(prices)):
            prev = prices[i - 1]
            if prev > 0:
                rets.append((prices[i] - prev) / prev)
        returns_map[sym] = rets

    usable = [s for s in symbols if len(returns_map.get(s, [])) >= MIN_HISTORY_DAYS]
    dropped = [(s, len(returns_map.get(s, []))) for s in symbols if s not in usable]
    if len(usable) < 2:
        return None, usable, dropped
    min_len = min(len(returns_map[s]) for s in usable)
    matrix = np.array([returns_map[s][-min_len:] for s in usable]).T
    return matrix, usable, dropped


def _build_historical_regime(ctx, as_of_date):
    """Historical VIX + 10y-3m spread, formatted like _capture_regime output."""
    vix = _value_at(ctx.macro_history["vix"], as_of_date) if ctx.macro_history else None
    y10 = _value_at(ctx.macro_history["yield10y"], as_of_date) if ctx.macro_history else None
    y3m = _value_at(ctx.macro_history["yield3m"], as_of_date) if ctx.macro_history else None
    spread = round(y10 - y3m, 3) if (y10 is not None and y3m is not None) else None
    return {"vix": vix, "yield_curve_10y_3m": spread}


def _universe_for_backtest():
    """Full set of symbols we need price data for across all templates.
    This keeps the yfinance fetch batch to one pass per symbol, reused for
    every (template × date) combination.
    """
    universe = set()
    for template in PORTFOLIO_TEMPLATES.values():
        for sym, _w in template:
            universe.add(sym)
    return sorted(universe)


def run_backtest(start, end, cadence="monthly", template_names=None,
                 dry_run=False, verbose=False):
    """Generate backtest recs for each (date, template) pair in the window.

    Returns stats dict. Recs are written to portfolio_recommendations with
    client_id=BACKTEST_CLIENT_ID and historical created_at.
    """
    if template_names is None:
        template_names = list(PORTFOLIO_TEMPLATES.keys())

    stats = {
        "dates": 0, "templates": len(template_names),
        "recs_generated": 0, "recs_skipped": 0, "recs_failed": 0,
    }

    dates = _rec_dates(start, end, cadence)
    stats["dates"] = len(dates)
    log.info(f"Backtest: {len(dates)} dates × {len(template_names)} templates = "
             f"{len(dates) * len(template_names)} recs to generate")

    universe = _universe_for_backtest()
    log.info(f"Prefetching historical data for {len(universe)} symbols…")
    ctx = prefetch_backtest_context(universe)
    log.info(f"Context loaded: {len(ctx.closes)} closes, {len(ctx.earnings_raw)} earnings")

    # Fetch classifications once for the whole universe (roughly time-invariant).
    classifications = _fetch_classifications(universe)

    for as_of_date in dates:
        for template in template_names:
            try:
                holdings = _template_holdings(template)
                symbols = [h["symbol"] for h in holdings]

                return_matrix, usable, dropped = _build_historical_return_matrix(
                    symbols, as_of_date, ctx,
                )
                if return_matrix is None or len(usable) < 2:
                    stats["recs_skipped"] += 1
                    if verbose:
                        log.info(f"  [{as_of_date.date()} {template}] skipped: insufficient history")
                    continue

                # Reconstruct Alpha-Lite scores for usable symbols.
                alpha_scores = {}
                for sym in usable:
                    sector_hint = classifications.get(sym, ("Unknown", False))[0]
                    alpha_scores[sym] = reconstruct_alpha_lite(sym, as_of_date, ctx, sector_hint)

                regime = _build_historical_regime(ctx, as_of_date)

                # Only send USABLE holdings to the optimizer (those with enough
                # history) — the optimizer would otherwise drop them itself and
                # complain if the filtered list is <2.
                usable_holdings = [h for h in holdings if h["symbol"] in usable]
                result = black_litterman_optimize(
                    usable_holdings,
                    as_of_date=as_of_date,
                    alpha_scores_override=alpha_scores,
                    regime_override=regime,
                    historical_returns_matrix=return_matrix,
                    historical_usable=usable,
                )
                if result is None:
                    stats["recs_failed"] += 1
                    continue

                payload = {
                    "rec_id": str(uuid.uuid4()),
                    "client_id": BACKTEST_CLIENT_ID,
                    "created_at": as_of_date.isoformat(),
                    "total_value": result["totalValue"],
                    "holdings": usable_holdings,
                    "suggested_weights": result["optimal"]["weights"],
                    "current_return_pct": result["current"]["return"],
                    "current_volatility_pct": result["current"]["volatility"],
                    "current_sharpe": result["current"]["sharpe"],
                    "expected_return_pct": result["optimal"]["return"],
                    "expected_volatility_pct": result["optimal"]["volatility"],
                    "expected_sharpe": result["optimal"]["sharpe"],
                    "constraint_params": {
                        **result["diagnostics"]["constraints"],
                        "mode": "backtest",
                        "template": template,
                        "alpha_lite": True,
                    },
                    "factor_weights": result.get("factorWeights", {}),
                    "regime_vix": regime.get("vix"),
                    "regime_yield_curve": regime.get("yield_curve_10y_3m"),
                    "regime_snapshot": regime,
                    "attribution": result.get("attribution", {}),
                    "confidence_score": result.get("confidenceScore"),
                }
                if not dry_run:
                    insert_recommendation(payload)
                stats["recs_generated"] += 1
                if verbose:
                    log.info(f"  [{as_of_date.date()} {template}] ok, "
                             f"Δ-Sharpe {result['current']['sharpe']} → {result['optimal']['sharpe']}")
            except Exception as e:
                stats["recs_failed"] += 1
                log.exception(f"  [{as_of_date.date()} {template}] failed: {e}")

    return stats


# ── Outcome computation for backtest recs ────────────────────────────

def observe_backtest():
    """Run the outcome observer over every backtest rec. Because backtest
    recs carry historical `created_at` timestamps that are already older
    than HORIZONS_DAYS, all horizons compute on the first pass.
    """
    from .recommendations import iter_all_recommendations
    stats = {"recs_scanned": 0, "outcomes_computed": 0, "errors": 0}
    for rec in iter_all_recommendations(min_age_days=min(HORIZONS_DAYS)):
        if rec.get("client_id") != BACKTEST_CLIENT_ID:
            continue
        stats["recs_scanned"] += 1
        existing = {o["horizon_days"] for o in get_outcomes_for_rec(rec["rec_id"])}
        for horizon in HORIZONS_DAYS:
            if horizon in existing:
                continue
            try:
                outcome = compute_outcomes_for_rec(rec, horizon)
                if outcome is None:
                    continue
                insert_outcome(outcome)
                stats["outcomes_computed"] += 1
            except Exception:
                stats["errors"] += 1
    return stats


# ── Analysis ─────────────────────────────────────────────────────────

def _spearman_corr(xs, ys):
    """Spearman rank correlation. Pure Python, no scipy dep."""
    n = len(xs)
    if n < 3:
        return None

    def _ranks(arr):
        indexed = sorted(range(n), key=lambda i: arr[i])
        ranks = [0.0] * n
        i = 0
        while i < n:
            j = i
            while j + 1 < n and arr[indexed[j + 1]] == arr[indexed[i]]:
                j += 1
            # Average rank for ties.
            avg = (i + j) / 2.0 + 1
            for k in range(i, j + 1):
                ranks[indexed[k]] = avg
            i = j + 1
        return ranks

    rx, ry = _ranks(xs), _ranks(ys)
    mx = sum(rx) / n
    my = sum(ry) / n
    num = sum((rx[i] - mx) * (ry[i] - my) for i in range(n))
    den_x = math.sqrt(sum((rx[i] - mx) ** 2 for i in range(n)))
    den_y = math.sqrt(sum((ry[i] - my) ** 2 for i in range(n)))
    if den_x == 0 or den_y == 0:
        return None
    return num / (den_x * den_y)


def compute_ic(horizon_days=90):
    """Information coefficient per reconstructed factor.

    For each (rec, symbol), pair the factor's subScore at rec time with the
    symbol's forward return over `horizon_days`. Compute Spearman rank
    correlation across all (symbol, rec-date) pairs. Positive IC = the
    factor predicts. Non-reconstructed factors are held at 50 and thus
    give IC ≈ 0; we don't report them.
    """
    from .recommendations import iter_all_recommendations
    from .outcome_observer import _fetch_period_data

    # Collect one data row per (rec, symbol) pair that has a suggested weight.
    rows = []  # each: {symbol, rec_dt, forward_return, subScores}
    for rec in iter_all_recommendations(min_age_days=horizon_days):
        if rec.get("client_id") != BACKTEST_CLIENT_ID:
            continue
        rec_dt = datetime.fromisoformat(rec["created_at"])
        end_dt = rec_dt + timedelta(days=horizon_days)
        attribution = rec.get("attribution") or {}
        for s in (rec.get("suggested_weights") or []):
            sym = s.get("symbol")
            if not sym or sym not in attribution:
                continue
            payload = _fetch_period_data(sym, rec_dt, end_dt)
            if payload["period_return"] is None:
                continue
            sub = {
                f["name"]: f["subScore"]
                for f in (attribution[sym].get("factors") or [])
                if "subScore" in f and "name" in f
            }
            if not sub:
                continue
            rows.append({
                "symbol": sym, "rec_dt": rec_dt,
                "forward_return": payload["period_return"],
                "subScores": sub,
            })

    ic_by_factor = {}
    n_obs = len(rows)
    for factor in RECONSTRUCTED_FACTORS:
        scores, rets = [], []
        for r in rows:
            score = r["subScores"].get(factor)
            if score is None:
                continue
            scores.append(score)
            rets.append(r["forward_return"])
        if len(scores) < 5:
            ic_by_factor[factor] = {"ic": None, "n": len(scores)}
            continue
        ic = _spearman_corr(scores, rets)
        ic_by_factor[factor] = {"ic": ic, "n": len(scores)}
    return {"ic": ic_by_factor, "n_observations": n_obs, "horizon_days": horizon_days}


def summary_stats():
    """Summary metrics across all backtest outcomes. Returns dict used by
    both the CLI and the /backtest page."""
    from .outcomes import get_recs_with_outcomes
    recs = get_recs_with_outcomes(BACKTEST_CLIENT_ID, limit=10_000)
    out = {"n_recs": len(recs), "by_horizon": {}, "by_template": {}}
    if not recs:
        return out

    for horizon in HORIZONS_DAYS:
        bucket = {
            "n": 0, "mean_realized": 0.0, "mean_counterfactual": 0.0,
            "mean_spy": 0.0, "beats_counterfactual": 0, "beats_spy": 0,
        }
        for rec in recs:
            for o in rec.get("outcomes") or []:
                if o["horizon_days"] != horizon:
                    continue
                if o.get("realized_return_pct") is None:
                    continue
                bucket["n"] += 1
                bucket["mean_realized"] += o["realized_return_pct"]
                if o.get("counterfactual_return_pct") is not None:
                    bucket["mean_counterfactual"] += o["counterfactual_return_pct"]
                    if o["realized_return_pct"] > o["counterfactual_return_pct"]:
                        bucket["beats_counterfactual"] += 1
                if o.get("benchmark_spy_return_pct") is not None:
                    bucket["mean_spy"] += o["benchmark_spy_return_pct"]
                    if o["realized_return_pct"] > o["benchmark_spy_return_pct"]:
                        bucket["beats_spy"] += 1
        if bucket["n"]:
            bucket["mean_realized"] = round(bucket["mean_realized"] / bucket["n"], 2)
            bucket["mean_counterfactual"] = round(bucket["mean_counterfactual"] / bucket["n"], 2)
            bucket["mean_spy"] = round(bucket["mean_spy"] / bucket["n"], 2)
            bucket["hit_rate_vs_counterfactual"] = round(
                bucket["beats_counterfactual"] / bucket["n"] * 100, 1)
            bucket["hit_rate_vs_spy"] = round(
                bucket["beats_spy"] / bucket["n"] * 100, 1)
        out["by_horizon"][horizon] = bucket

    # By template (uses constraint_params.template if present).
    for rec in recs:
        tpl = ((rec.get("constraint_params") or {}).get("template")) or "unknown"
        tpl_bucket = out["by_template"].setdefault(tpl, {"n_recs": 0})
        tpl_bucket["n_recs"] += 1

    return out


# ── CLI ──────────────────────────────────────────────────────────────

def _main():
    parser = argparse.ArgumentParser(prog="backtest")
    sub = parser.add_subparsers(dest="cmd", required=True)

    runp = sub.add_parser("run", help="Generate backtest recs over a window")
    runp.add_argument("--start", default="2020-01-01", help="ISO date")
    runp.add_argument("--end", default="2024-06-30", help="ISO date")
    runp.add_argument("--cadence", choices=("monthly", "quarterly"), default="monthly")
    runp.add_argument("--templates", default=None,
                      help="Comma-separated template names. Default: all.")
    runp.add_argument("--dry-run", action="store_true")
    runp.add_argument("-v", "--verbose", action="store_true")

    sub.add_parser("observe", help="Compute outcomes for all backtest recs")
    sub.add_parser("summary", help="Print summary stats and factor IC")

    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO if not getattr(args, "verbose", False) else logging.DEBUG,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    logging.getLogger("yfinance").setLevel(logging.CRITICAL)

    if args.cmd == "run":
        start = datetime.fromisoformat(args.start)
        end = datetime.fromisoformat(args.end)
        templates = args.templates.split(",") if args.templates else None
        stats = run_backtest(start, end, args.cadence, templates,
                             dry_run=args.dry_run, verbose=args.verbose)
    elif args.cmd == "observe":
        stats = observe_backtest()
    elif args.cmd == "summary":
        stats = {"summary": summary_stats(), "ic_90d": compute_ic(90)}
    else:
        parser.print_help()
        sys.exit(1)

    import json
    print(json.dumps(stats, indent=2, default=str))


if __name__ == "__main__":
    _main()
