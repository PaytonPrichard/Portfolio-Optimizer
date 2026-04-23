"""Outcome observer: measures realized performance of past recommendations.

Phase 2.2 of the recommendation feedback loop. For each rec that has horizons
elapsed but not yet measured, fetches historical period returns for every
ticker involved, computes:

  - realized_return_pct: hypothetical return if user fully executed the rec
  - counterfactual_return_pct: what the do-nothing portfolio (current
    holdings held throughout) returned
  - benchmark_spy_return_pct: SPY total return over the same period
  - benchmark_equalweight_return_pct: equal-weight of rec-time holdings
  - factor_attribution: per-factor realized P&L contribution via the linear
    Brinson-style formula (see docs in compute_outcomes_for_rec)

Robust to yfinance gaps: missing tickers are recorded in the notes field
rather than aborting the whole computation.

CLI:
    python -m financials.outcome_observer run            # process all due
    python -m financials.outcome_observer run --client X # only one client
    python -m financials.outcome_observer run --dry-run  # don't write
"""

import argparse
import sys
from collections import defaultdict
from datetime import datetime, timedelta

import yfinance as yf

from . import cache
from .outcomes import insert_outcome, list_due_horizons, HORIZONS_DAYS
from .recommendations import iter_all_recommendations

PERIOD_RETURN_TTL = 86400  # 24h cache — historical returns don't change


def _fetch_period_return(symbol, start_date, end_date):
    """Total return for symbol from start_date to end_date as a decimal
    (0.05 = 5%). Returns None if data unavailable.

    Uses adjusted close (yfinance default), so dividends are accounted for.
    """
    cache_key = f"period_return:{symbol}:{start_date.date()}:{end_date.date()}"
    cached = cache.get(cache_key)
    if cached is not None:
        return cached if cached != "MISS" else None
    try:
        # Buffer the window so weekends/holidays don't lose us start/end days.
        hist = yf.Ticker(symbol).history(
            start=start_date - timedelta(days=5),
            end=end_date + timedelta(days=5),
        )
        if hist is None or hist.empty:
            cache.put(cache_key, "MISS", ttl=PERIOD_RETURN_TTL)
            return None
        closes = hist["Close"].dropna()
        if closes.empty:
            cache.put(cache_key, "MISS", ttl=PERIOD_RETURN_TTL)
            return None
        # Strip timezone for naive date comparison.
        try:
            closes.index = closes.index.tz_localize(None)
        except (TypeError, AttributeError):
            pass

        # First close at or after start_date.
        starts = closes[closes.index >= start_date]
        if starts.empty:
            cache.put(cache_key, "MISS", ttl=PERIOD_RETURN_TTL)
            return None
        start_price = float(starts.iloc[0])
        # Last close at or before end_date.
        ends = closes[closes.index <= end_date]
        if ends.empty:
            cache.put(cache_key, "MISS", ttl=PERIOD_RETURN_TTL)
            return None
        end_price = float(ends.iloc[-1])
        if start_price <= 0:
            cache.put(cache_key, "MISS", ttl=PERIOD_RETURN_TTL)
            return None
        ret = (end_price - start_price) / start_price
        cache.put(cache_key, ret, ttl=PERIOD_RETURN_TTL)
        return ret
    except Exception:
        cache.put(cache_key, "MISS", ttl=PERIOD_RETURN_TTL)
        return None


def _suggested_weight_pct(s):
    """Pull the optimal-weight percent from a suggested_weights entry,
    handling the two shapes the optimizer emits: trade rows have
    `optimalPct`, while the full weights list has `weight`."""
    if "optimalPct" in s:
        return float(s["optimalPct"])
    if "weight" in s:
        return float(s["weight"])
    return 0.0


def _current_weight_decimal(h, total_value):
    if total_value <= 0:
        return 0.0
    cv = h.get("currentValue") or 0
    return float(cv) / float(total_value)


def compute_outcomes_for_rec(rec, horizon_days):
    """Compute the outcome dict for one (rec, horizon) pair.

    Returns dict suitable for insert_outcome(). None if rec is malformed
    or no usable price data could be fetched.
    """
    if not rec.get("created_at"):
        return None
    rec_dt = datetime.fromisoformat(rec["created_at"])
    end_dt = rec_dt + timedelta(days=horizon_days)
    if end_dt > datetime.now():
        return None  # not elapsed yet

    holdings = rec.get("holdings") or []
    suggested = rec.get("suggested_weights") or []
    attribution = rec.get("attribution") or {}
    total_value = float(rec.get("total_value") or 0)
    if total_value <= 0:
        return None

    # Universe of symbols we need prices for.
    symbols = set()
    for h in holdings:
        if h.get("symbol"):
            symbols.add(h["symbol"])
    for s in suggested:
        if s.get("symbol"):
            symbols.add(s["symbol"])
    symbols.add("SPY")

    # Fetch period returns once per symbol.
    returns = {}
    for sym in symbols:
        r = _fetch_period_return(sym, rec_dt, end_dt)
        if r is not None:
            returns[sym] = r

    # Realized return: hypothetically executed rec.
    realized_total = 0.0
    realized_components = []  # (symbol, weight_decimal, period_return)
    for s in suggested:
        sym = s.get("symbol")
        if not sym:
            continue
        weight = _suggested_weight_pct(s) / 100.0
        ret = returns.get(sym)
        if ret is None:
            continue
        realized_total += weight * ret
        realized_components.append((sym, weight, ret))

    # Counterfactual: held current weights throughout.
    counterfactual_total = 0.0
    for h in holdings:
        sym = h.get("symbol")
        if not sym:
            continue
        ret = returns.get(sym)
        if ret is None:
            continue
        counterfactual_total += _current_weight_decimal(h, total_value) * ret

    # SPY benchmark.
    spy_return = returns.get("SPY")

    # Equal-weight benchmark of rec-time holdings.
    held_with_data = [h["symbol"] for h in holdings if h.get("symbol") in returns]
    if held_with_data:
        ew = sum(returns[s] for s in held_with_data) / len(held_with_data)
    else:
        ew = None

    # Per-factor realized attribution (linear Brinson-style).
    # For each suggested position with rec-time factor breakdown:
    #   share = factor_contrib_pct / view_excess_pct  (fraction of view from this factor)
    #   factor_realized += share * realized_return * weight
    # Aggregated across positions per factor.
    factor_realized = defaultdict(float)
    for sym, weight, ret in realized_components:
        sym_attr = attribution.get(sym) or {}
        factors = sym_attr.get("factors") or []
        view_excess = sym_attr.get("viewExcess")
        if not factors or not view_excess:
            continue
        for f in factors:
            name = f.get("name")
            contribution = f.get("contribution")
            if name is None or contribution is None:
                continue
            share = float(contribution) / float(view_excess)
            factor_realized[name] += share * ret * weight

    # Notes about gaps.
    missing = [s for s in symbols if s not in returns and s != "SPY"]
    notes = None
    if missing:
        head = ", ".join(missing[:5])
        more = f" (+{len(missing) - 5} more)" if len(missing) > 5 else ""
        notes = f"No price data for: {head}{more}"

    return {
        "rec_id": rec["rec_id"],
        "horizon_days": horizon_days,
        "measured_at": datetime.now().isoformat(),
        "realized_return_pct": round(realized_total * 100, 2),
        "realized_volatility_pct": None,  # daily-vol calc deferred to a follow-up
        "counterfactual_return_pct": round(counterfactual_total * 100, 2),
        "benchmark_spy_return_pct": round(spy_return * 100, 2) if spy_return is not None else None,
        "benchmark_equalweight_return_pct": round(ew * 100, 2) if ew is not None else None,
        "factor_attribution": {k: round(v * 100, 4) for k, v in factor_realized.items()},
        "notes": notes,
    }


def run_outcome_observer(client_id=None, limit=None, dry_run=False):
    """Find every (rec, horizon) pair that's elapsed but unmeasured, compute,
    and insert. Returns a stats dict.
    """
    stats = {
        "recs_scanned": 0,
        "outcomes_computed": 0,
        "outcomes_skipped": 0,
        "errors": 0,
    }

    processed = 0
    for rec in iter_all_recommendations(min_age_days=min(HORIZONS_DAYS)):
        if client_id and rec.get("client_id") != client_id:
            continue
        if limit and processed >= limit:
            break
        processed += 1
        stats["recs_scanned"] += 1

        try:
            due = list_due_horizons(rec["rec_id"], rec["created_at"])
        except Exception:
            stats["errors"] += 1
            continue

        for horizon in due:
            try:
                outcome = compute_outcomes_for_rec(rec, horizon)
                if outcome is None:
                    stats["outcomes_skipped"] += 1
                    continue
                if not dry_run:
                    insert_outcome(outcome)
                stats["outcomes_computed"] += 1
            except Exception:
                stats["errors"] += 1

    return stats


# ── CLI ──────────────────────────────────────────────────────────────

def _main():
    parser = argparse.ArgumentParser(prog="outcome_observer")
    sub = parser.add_subparsers(dest="cmd", required=True)
    runp = sub.add_parser("run", help="Compute outcomes for due (rec, horizon) pairs")
    runp.add_argument("--client", default=None, help="Limit to a single client_id")
    runp.add_argument("--limit", type=int, default=None, help="Max recs to scan")
    runp.add_argument("--dry-run", action="store_true", help="Compute but don't insert")
    args = parser.parse_args()

    if args.cmd == "run":
        stats = run_outcome_observer(
            client_id=args.client, limit=args.limit, dry_run=args.dry_run,
        )
        for k, v in stats.items():
            print(f"  {k}: {v}")
    else:
        parser.print_help()
        sys.exit(1)


if __name__ == "__main__":
    _main()
