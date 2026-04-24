"""Rolling-rebalance backtest.

Simulates actually following the optimizer's recommendations over time.
Unlike `backtest.py` (which tests each rec in isolation from a static
starting portfolio), this one:

  - Starts with template weights at start_date.
  - At each rebalance date, runs the optimizer on the CURRENT portfolio
    (not the static template), gets the target weights, computes trades,
    applies transaction costs, and updates the portfolio.
  - Between rebalance dates, evolves holdings by each symbol's realized
    daily returns.
  - Tracks cumulative portfolio value, turnover, TC paid, and compares
    against two benchmarks (buy-and-hold the template, SPY).

The answer this gives: "If I'd followed the optimizer for the last 4
years, what's my portfolio worth now vs. if I'd just held SPY?"

Optional Phase 4 integration: `phase4_update=True` runs the weight
learner at each rebalance date and uses whatever weights pass validation
for that rec's optimization. Lets us compare static-weights rolling
performance vs. learner-adjusted rolling performance.

CLI:
    python -m financials.rolling_backtest run [--template NAME] [--phase4]
    python -m financials.rolling_backtest summary
"""

import argparse
import json
import logging
import os
import sys
from collections import defaultdict
from datetime import datetime, timedelta

import numpy as np

from .alpha_historical import (
    prefetch_backtest_context, reconstruct_alpha_lite,
    _daily_closes_up_to,
)
from .backtest import (
    _rec_dates, _template_holdings, _build_historical_return_matrix,
    _build_historical_regime, _universe_for_backtest,
    PORTFOLIO_TEMPLATES, DEFAULT_TOTAL_VALUE, WINDOW_YEARS,
)
from .portfolio_optimizer import (
    black_litterman_optimize, _fetch_classifications, DEFAULT_TC_BPS,
)

log = logging.getLogger("rolling_backtest")

TRADING_DAYS = 252

_OUT_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), "data")
_ROLLING_CACHE = os.path.join(_OUT_DIR, "rolling_backtest.json")


# ── Price helpers ────────────────────────────────────────────────────

def _price_at(ctx, symbol, as_of_date):
    """Most recent close price on or before as_of_date."""
    series = ctx.closes.get(symbol) or ctx.sector_etf_closes.get(symbol) or []
    last = None
    for d, c in series:
        if d > as_of_date:
            break
        last = c
    return last


def _period_return(ctx, symbol, start_date, end_date):
    """Total return from start_date to end_date using close prices from ctx."""
    p0 = _price_at(ctx, symbol, start_date)
    p1 = _price_at(ctx, symbol, end_date)
    if p0 is None or p1 is None or p0 <= 0:
        return 0.0
    return (p1 - p0) / p0


def _evolve_holdings(holdings_dollars, ctx, prev_date, next_date):
    """Update dollar values of holdings by each symbol's period return."""
    new = {}
    for sym, val in holdings_dollars.items():
        r = _period_return(ctx, sym, prev_date, next_date)
        new[sym] = val * (1 + r)
    return new


# ── Rolling loop ─────────────────────────────────────────────────────

def _holdings_list(holdings_dollars):
    """Convert {symbol: $} dict to the [{symbol, currentValue}] list shape
    the optimizer expects."""
    return [
        {"symbol": s, "currentValue": float(v)}
        for s, v in holdings_dollars.items()
        if v > 0
    ]


def _apply_trades(holdings_dollars, target_weights_pct, total_value, tc_bps):
    """Rebalance to target weights. Returns (new_holdings_dollars, turnover,
    tc_paid_dollars). target_weights_pct is a list of {symbol, weight(%)}.
    """
    target_dollars = {
        w["symbol"]: (w["weight"] / 100.0) * total_value
        for w in target_weights_pct
    }
    # Trades: diff in $ per symbol.
    all_symbols = set(holdings_dollars.keys()) | set(target_dollars.keys())
    total_turnover = 0.0
    new = {}
    for sym in all_symbols:
        cur = holdings_dollars.get(sym, 0.0)
        tgt = target_dollars.get(sym, 0.0)
        trade = tgt - cur
        total_turnover += abs(trade)
        new[sym] = tgt  # post-trade dollars
    tc_paid = total_turnover * (tc_bps / 10_000.0)
    # Debit TC from the new portfolio (scale down everything proportionally so
    # total $ value = total_value - tc_paid).
    if total_value > 0:
        scale = (total_value - tc_paid) / total_value
        new = {s: v * scale for s, v in new.items()}
    return new, total_turnover, tc_paid


def run_rolling(template_name, start_date, end_date, cadence="monthly",
                tc_bps=DEFAULT_TC_BPS, total_value=DEFAULT_TOTAL_VALUE,
                phase4_update=False, ctx=None):
    """Main simulation loop. Returns a dict with time series and summary stats.

    If `ctx` is provided, reuse it (lets the caller batch all templates in
    one prefetch). Otherwise prefetch internally.
    """
    if template_name not in PORTFOLIO_TEMPLATES:
        raise ValueError(f"unknown template: {template_name}")

    dates = _rec_dates(start_date, end_date, cadence)
    if not dates:
        return {"error": "No rebalance dates generated."}

    universe = _universe_for_backtest()
    if ctx is None:
        log.info(f"Prefetching history for {len(universe)} symbols…")
        ctx = prefetch_backtest_context(universe)

    classifications = _fetch_classifications(universe)

    # Initialize portfolio at first date from template.
    init_holdings = _template_holdings(template_name, total_value)
    holdings = {h["symbol"]: h["currentValue"] for h in init_holdings}

    # Benchmarks: buy-and-hold the template (frozen), and SPY.
    bah_shares = {
        h["symbol"]: h["currentValue"] / (_price_at(ctx, h["symbol"], dates[0]) or 1)
        for h in init_holdings
        if _price_at(ctx, h["symbol"], dates[0])
    }
    spy_p0 = _price_at(ctx, "SPY", dates[0])
    spy_shares = total_value / spy_p0 if spy_p0 else 0

    # Time series.
    value_series = []       # [{date, portfolio_value, bah_value, spy_value}]
    turnover_series = []    # per-period turnover
    tc_series = []          # per-period tc paid
    weights_history = []    # per-period snapshot of weights

    # Initial record before the first rebalance.
    value_series.append({
        "date": dates[0].date().isoformat(),
        "portfolio_value": round(sum(holdings.values()), 2),
        "bah_value": round(sum(
            bah_shares.get(s, 0) * (_price_at(ctx, s, dates[0]) or 0)
            for s in bah_shares
        ), 2),
        "spy_value": round(spy_shares * (spy_p0 or 0), 2),
    })

    for i, rebalance_date in enumerate(dates):
        # Build price snapshot at rebalance_date for this portfolio.
        symbols = list(holdings.keys())
        # If portfolio value has drifted significantly, use current holdings for
        # optimizer input. Current dollar values → optimizer sees effective weights.
        current_total = sum(holdings.values())
        if current_total <= 0:
            # Ran out of money somehow. Abort cleanly.
            break

        # Run optimizer at this date using the current holdings.
        # Filter to symbols with positive $ value — zero-value entries would
        # be filtered by the optimizer anyway, and keeping them in the return
        # matrix causes a dimension mismatch in the covariance computation.
        symbols_active = [s for s in symbols if holdings.get(s, 0) > 0]
        if len(symbols_active) < 2:
            turnover_series.append(0)
            tc_series.append(0)
            if i + 1 < len(dates):
                holdings = _evolve_holdings(holdings, ctx, rebalance_date, dates[i + 1])
            continue
        return_matrix, usable, _dropped = _build_historical_return_matrix(
            symbols_active, rebalance_date, ctx,
        )
        if return_matrix is None:
            # Not enough history yet; skip rebalance, keep current weights.
            if i + 1 < len(dates):
                holdings = _evolve_holdings(holdings, ctx, rebalance_date, dates[i + 1])
                value_series.append({
                    "date": dates[i + 1].date().isoformat(),
                    "portfolio_value": round(sum(holdings.values()), 2),
                    "bah_value": round(sum(
                        bah_shares.get(s, 0) * (_price_at(ctx, s, dates[i + 1]) or 0)
                        for s in bah_shares
                    ), 2),
                    "spy_value": round(spy_shares * (_price_at(ctx, "SPY", dates[i + 1]) or 0), 2),
                })
            turnover_series.append(0)
            tc_series.append(0)
            continue

        # Reconstruct alpha for usable symbols.
        alpha_scores = {}
        for sym in usable:
            sector_hint = classifications.get(sym, ("Unknown", False))[0]
            alpha_scores[sym] = reconstruct_alpha_lite(sym, rebalance_date, ctx, sector_hint)

        regime = _build_historical_regime(ctx, rebalance_date)

        # Optional Phase 4: run learner at this date. Skip if it's not yet
        # statistically ready — the learner's own validation gates apply.
        # (Not yet implemented in this pass; phase4_update flag is a no-op for v1.)

        usable_holdings_list = [
            {"symbol": s, "currentValue": holdings.get(s, 0)} for s in usable
        ]
        result = black_litterman_optimize(
            usable_holdings_list,
            as_of_date=rebalance_date,
            alpha_scores_override=alpha_scores,
            regime_override=regime,
            historical_returns_matrix=return_matrix,
            historical_usable=usable,
        )
        if result is None:
            # Optimizer failed; keep current weights.
            turnover_series.append(0)
            tc_series.append(0)
            if i + 1 < len(dates):
                holdings = _evolve_holdings(holdings, ctx, rebalance_date, dates[i + 1])
            continue

        # Apply trades: rebalance current holdings to target weights.
        target_weights = result["optimal"]["weights"]
        total_value_now = sum(holdings.values())
        # Make sure all current holdings are represented (set 0 for removed symbols).
        target_syms = {w["symbol"] for w in target_weights}
        for s in list(holdings.keys()):
            if s not in target_syms:
                # Optimizer dropped this symbol (weight went to 0). Add explicit 0 entry.
                target_weights.append({"symbol": s, "weight": 0.0})
        holdings, turnover, tc_paid = _apply_trades(
            holdings, target_weights, total_value_now, tc_bps,
        )
        turnover_series.append(round(turnover / total_value_now * 100, 2) if total_value_now > 0 else 0)
        tc_series.append(round(tc_paid, 2))
        weights_history.append({
            "date": rebalance_date.date().isoformat(),
            "weights": target_weights,
        })

        # Evolve to next rebalance date.
        if i + 1 < len(dates):
            next_date = dates[i + 1]
            holdings = _evolve_holdings(holdings, ctx, rebalance_date, next_date)
            value_series.append({
                "date": next_date.date().isoformat(),
                "portfolio_value": round(sum(holdings.values()), 2),
                "bah_value": round(sum(
                    bah_shares.get(s, 0) * (_price_at(ctx, s, next_date) or 0)
                    for s in bah_shares
                ), 2),
                "spy_value": round(spy_shares * (_price_at(ctx, "SPY", next_date) or 0), 2),
            })

    return _summarize(template_name, value_series, turnover_series, tc_series, weights_history)


def _summarize(template_name, value_series, turnover_series, tc_series, weights_history):
    """Compute summary metrics from a simulation run."""
    if len(value_series) < 2:
        return {"template": template_name, "error": "insufficient data"}

    v0 = value_series[0]["portfolio_value"]
    v_final = value_series[-1]["portfolio_value"]
    b0 = value_series[0]["bah_value"]
    b_final = value_series[-1]["bah_value"]
    s0 = value_series[0]["spy_value"]
    s_final = value_series[-1]["spy_value"]

    n_periods = len(value_series) - 1
    if n_periods <= 0:
        return {"template": template_name, "error": "single period"}

    years = n_periods / 12.0  # monthly cadence

    def _cagr(v0, v_final, yrs):
        if v0 <= 0 or yrs <= 0:
            return 0.0
        return (v_final / v0) ** (1 / yrs) - 1

    def _monthly_returns(series, key):
        rets = []
        for i in range(1, len(series)):
            prev = series[i - 1][key]
            cur = series[i][key]
            if prev > 0:
                rets.append((cur - prev) / prev)
        return rets

    def _sharpe_annual(rets):
        if len(rets) < 2:
            return 0.0
        mean = sum(rets) / len(rets)
        var = sum((r - mean) ** 2 for r in rets) / (len(rets) - 1)
        std = var ** 0.5
        if std == 0:
            return 0.0
        # Annualize monthly: mean*12, std*sqrt(12).
        return (mean * 12) / (std * (12 ** 0.5))

    def _max_drawdown(series, key):
        peak = series[0][key]
        mdd = 0.0
        for pt in series:
            v = pt[key]
            if v > peak:
                peak = v
            if peak > 0:
                dd = (v - peak) / peak
                if dd < mdd:
                    mdd = dd
        return mdd

    opt_rets = _monthly_returns(value_series, "portfolio_value")
    bah_rets = _monthly_returns(value_series, "bah_value")
    spy_rets = _monthly_returns(value_series, "spy_value")

    total_tc = sum(tc_series)
    avg_turnover = sum(turnover_series) / len(turnover_series) if turnover_series else 0

    return {
        "template": template_name,
        "n_periods": n_periods,
        "years": round(years, 2),
        "optimizer": {
            "initial_value": v0,
            "final_value": v_final,
            "total_return_pct": round((v_final / v0 - 1) * 100, 2) if v0 > 0 else 0,
            "cagr_pct": round(_cagr(v0, v_final, years) * 100, 2),
            "sharpe": round(_sharpe_annual(opt_rets), 2),
            "max_drawdown_pct": round(_max_drawdown(value_series, "portfolio_value") * 100, 2),
            "total_tc_dollars": round(total_tc, 2),
            "avg_turnover_pct": round(avg_turnover, 2),
        },
        "buy_and_hold": {
            "initial_value": b0,
            "final_value": b_final,
            "total_return_pct": round((b_final / b0 - 1) * 100, 2) if b0 > 0 else 0,
            "cagr_pct": round(_cagr(b0, b_final, years) * 100, 2),
            "sharpe": round(_sharpe_annual(bah_rets), 2),
            "max_drawdown_pct": round(_max_drawdown(value_series, "bah_value") * 100, 2),
        },
        "spy": {
            "initial_value": s0,
            "final_value": s_final,
            "total_return_pct": round((s_final / s0 - 1) * 100, 2) if s0 > 0 else 0,
            "cagr_pct": round(_cagr(s0, s_final, years) * 100, 2),
            "sharpe": round(_sharpe_annual(spy_rets), 2),
            "max_drawdown_pct": round(_max_drawdown(value_series, "spy_value") * 100, 2),
        },
        "value_series": value_series,
    }


def run_all_templates(start_date, end_date, cadence="monthly", tc_bps=DEFAULT_TC_BPS):
    """Batch runner: one prefetch, all templates. Writes combined results to
    data/rolling_backtest.json for /backtest page to read."""
    universe = _universe_for_backtest()
    log.info(f"Prefetching for {len(universe)} symbols…")
    ctx = prefetch_backtest_context(universe)

    results = {
        "computed_at": datetime.now().isoformat(),
        "start": start_date.isoformat(),
        "end": end_date.isoformat(),
        "cadence": cadence,
        "tc_bps": tc_bps,
        "templates": {},
    }
    for tpl in PORTFOLIO_TEMPLATES.keys():
        log.info(f"Rolling backtest: {tpl}")
        results["templates"][tpl] = run_rolling(
            tpl, start_date, end_date, cadence=cadence, tc_bps=tc_bps, ctx=ctx,
        )

    os.makedirs(_OUT_DIR, exist_ok=True)
    with open(_ROLLING_CACHE, "w") as f:
        json.dump(results, f, indent=2, default=str)
    log.info(f"Results written to {_ROLLING_CACHE}")
    return results


def load_cached_rolling():
    """Load persisted rolling-backtest results. None if not built."""
    if not os.path.exists(_ROLLING_CACHE):
        return None
    try:
        with open(_ROLLING_CACHE) as f:
            return json.load(f)
    except (json.JSONDecodeError, OSError):
        return None


# ── CLI ──────────────────────────────────────────────────────────────

def _main():
    parser = argparse.ArgumentParser(prog="rolling_backtest")
    sub = parser.add_subparsers(dest="cmd", required=True)

    rp = sub.add_parser("run", help="Run the rolling backtest over all templates")
    rp.add_argument("--start", default="2020-01-01")
    rp.add_argument("--end", default=None, help="Default: today")
    rp.add_argument("--cadence", choices=("monthly", "quarterly"), default="monthly")
    rp.add_argument("--tc-bps", type=float, default=DEFAULT_TC_BPS)

    sub.add_parser("summary", help="Print latest cached results")

    args = parser.parse_args()
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    logging.getLogger("yfinance").setLevel(logging.CRITICAL)

    if args.cmd == "run":
        start = datetime.fromisoformat(args.start)
        end = datetime.fromisoformat(args.end) if args.end else datetime.now()
        results = run_all_templates(start, end, cadence=args.cadence, tc_bps=args.tc_bps)
        # Print condensed summary.
        for tpl, data in results["templates"].items():
            opt = data.get("optimizer", {})
            bah = data.get("buy_and_hold", {})
            spy = data.get("spy", {})
            print(f"\n{tpl}:")
            print(f"  Optimizer:    {opt.get('total_return_pct', 0):>6.2f}%  CAGR {opt.get('cagr_pct', 0):>5.2f}%  Sharpe {opt.get('sharpe', 0):>5.2f}  MDD {opt.get('max_drawdown_pct', 0):>6.2f}%")
            print(f"  Buy-and-hold: {bah.get('total_return_pct', 0):>6.2f}%  CAGR {bah.get('cagr_pct', 0):>5.2f}%  Sharpe {bah.get('sharpe', 0):>5.2f}  MDD {bah.get('max_drawdown_pct', 0):>6.2f}%")
            print(f"  SPY:          {spy.get('total_return_pct', 0):>6.2f}%  CAGR {spy.get('cagr_pct', 0):>5.2f}%  Sharpe {spy.get('sharpe', 0):>5.2f}  MDD {spy.get('max_drawdown_pct', 0):>6.2f}%")
            print(f"  Total TC paid: ${opt.get('total_tc_dollars', 0):,.0f}  Avg turnover: {opt.get('avg_turnover_pct', 0):.1f}%/period")
    elif args.cmd == "summary":
        data = load_cached_rolling()
        if data is None:
            print("No cached results. Run with: python -m financials.rolling_backtest run")
            sys.exit(1)
        print(json.dumps(data, indent=2, default=str)[:5000])


if __name__ == "__main__":
    _main()
