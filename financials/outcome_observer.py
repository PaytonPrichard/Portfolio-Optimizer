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
import math
import sys
import time
from collections import defaultdict
from datetime import datetime, timedelta

import yfinance as yf

TRADING_DAYS_PER_YEAR = 252
MIN_DAYS_FOR_VOL = 20  # below this, annualized vol is too noisy to publish

from . import cache
from .outcomes import (
    insert_outcome, list_due_horizons, HORIZONS_DAYS, get_outcomes_with_gaps,
)
from .recommendations import iter_all_recommendations, get_recommendation

PERIOD_RETURN_TTL = 86400  # 24h cache — historical returns don't change
RETRY_DELAY_SECONDS = 0.5  # short pause before single retry on transient failure


def _is_rate_limit_error(exc):
    """yfinance throws YFRateLimitError when throttled. Match by string so we
    don't depend on the specific exception class being importable.
    """
    s = str(exc).lower()
    return ("rate limit" in s) or ("too many requests" in s)


def _fetch_period_data(symbol, start_date, end_date):
    """Fetch both the period total return AND the daily returns series in one
    shot. Returns dict {period_return, daily_returns} where period_return is
    a decimal (0.05 = 5%) and daily_returns is a list of daily pct changes
    within the [start, end] window. Either field may be None / empty list.

    Uses adjusted close (yfinance default), so dividends are accounted for.
    Retries once on transient failure (empty result or non-rate-limit error).
    On rate-limit specifically, returns an empty payload WITHOUT caching MISS
    so the heal CLI can pick it up later.
    """
    cache_key = f"period_data:{symbol}:{start_date.date()}:{end_date.date()}"
    cached = cache.get(cache_key)
    if cached is not None:
        return {"period_return": None, "daily_returns": []} if cached == "MISS" else cached

    def _attempt():
        hist = yf.Ticker(symbol).history(
            start=start_date - timedelta(days=5),
            end=end_date + timedelta(days=5),
        )
        if hist is None or hist.empty:
            return None
        closes = hist["Close"].dropna()
        if closes.empty:
            return None
        try:
            closes.index = closes.index.tz_localize(None)
        except (TypeError, AttributeError):
            pass
        in_window = closes[(closes.index >= start_date) & (closes.index <= end_date)]
        if in_window.empty or len(in_window) < 2:
            return None
        start_price = float(in_window.iloc[0])
        end_price = float(in_window.iloc[-1])
        if start_price <= 0:
            return None
        period_return = (end_price - start_price) / start_price
        daily = in_window.pct_change().dropna()
        daily_returns = [float(x) for x in daily.tolist()]
        return {"period_return": period_return, "daily_returns": daily_returns}

    for attempt_num in range(2):
        try:
            payload = _attempt()
            if payload is not None:
                cache.put(cache_key, payload, ttl=PERIOD_RETURN_TTL)
                return payload
            if attempt_num == 0:
                time.sleep(RETRY_DELAY_SECONDS)
                continue
        except Exception as e:
            if _is_rate_limit_error(e):
                return {"period_return": None, "daily_returns": []}
            if attempt_num == 0:
                time.sleep(RETRY_DELAY_SECONDS)
                continue
    cache.put(cache_key, "MISS", ttl=PERIOD_RETURN_TTL)
    return {"period_return": None, "daily_returns": []}


def _fetch_period_return(symbol, start_date, end_date):
    """Back-compat shim; delegates to _fetch_period_data."""
    return _fetch_period_data(symbol, start_date, end_date)["period_return"]


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


def _portfolio_realized_volatility(weight_daily_pairs):
    """Annualized realized vol of a portfolio defined by (weight, daily_returns)
    pairs. Weights are renormalized over symbols with data; series are aligned
    to the shortest available length (drops the prefix of longer series).
    Returns None if fewer than 2 symbols or under MIN_DAYS_FOR_VOL observations.
    """
    usable = [(w, d) for w, d in weight_daily_pairs if w > 0 and d]
    if len(usable) < 2:
        return None
    total_w = sum(w for w, _ in usable)
    if total_w <= 0:
        return None
    min_len = min(len(d) for _, d in usable)
    if min_len < MIN_DAYS_FOR_VOL:
        return None

    port_daily = []
    for i in range(min_len):
        r = 0.0
        for w, d in usable:
            # Align on the tail — the last `min_len` entries — so recent data wins
            # if yfinance returns slightly different windows across symbols.
            r += (w / total_w) * d[-min_len + i]
        port_daily.append(r)

    n = len(port_daily)
    mean = sum(port_daily) / n
    var = sum((x - mean) ** 2 for x in port_daily) / (n - 1)
    return math.sqrt(var) * math.sqrt(TRADING_DAYS_PER_YEAR) * 100


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

    # Fetch period return + daily series once per symbol.
    returns = {}
    daily_by_symbol = {}
    for sym in symbols:
        payload = _fetch_period_data(sym, rec_dt, end_dt)
        if payload["period_return"] is not None:
            returns[sym] = payload["period_return"]
        if payload["daily_returns"]:
            daily_by_symbol[sym] = payload["daily_returns"]

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

    # Realized volatility of the rec portfolio (suggested weights) over the
    # horizon. Apples-to-apples with expected_volatility_pct stored at rec time.
    rec_vol_pairs = [
        (_suggested_weight_pct(s) / 100.0, daily_by_symbol.get(s.get("symbol") or ""))
        for s in suggested
    ]
    realized_vol = _portfolio_realized_volatility(rec_vol_pairs)

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
        "realized_volatility_pct": round(realized_vol, 2) if realized_vol is not None else None,
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


def heal_stale_outcomes(client_id=None, limit=None, dry_run=False):
    """Re-run outcome computation for outcomes whose notes mention missing
    price data. Useful after yfinance throttling clears or a previously-bad
    ticker resolves (e.g., a recently-IPO'd stock now has 30+ days of data).

    INSERT OR REPLACE in the outcomes table makes this idempotent.
    """
    stats = {
        "outcomes_scanned": 0,
        "outcomes_healed": 0,
        "outcomes_unchanged": 0,
        "errors": 0,
    }

    stale = get_outcomes_with_gaps(client_id=client_id, limit=limit)
    for old in stale:
        stats["outcomes_scanned"] += 1
        rec = get_recommendation(old["rec_id"])
        if not rec:
            stats["errors"] += 1
            continue
        try:
            new_outcome = compute_outcomes_for_rec(rec, old["horizon_days"])
            if new_outcome is None:
                stats["errors"] += 1
                continue
            old_notes = old.get("prev_notes") or ""
            new_notes = new_outcome.get("notes") or ""
            if old_notes != new_notes:
                stats["outcomes_healed"] += 1
            else:
                stats["outcomes_unchanged"] += 1
            if not dry_run:
                insert_outcome(new_outcome)
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

    healp = sub.add_parser("heal", help="Re-process outcomes with recorded yfinance gaps")
    healp.add_argument("--client", default=None, help="Limit to a single client_id")
    healp.add_argument("--limit", type=int, default=None, help="Max outcomes to retry")
    healp.add_argument("--dry-run", action="store_true", help="Compute but don't insert")

    args = parser.parse_args()

    if args.cmd == "run":
        stats = run_outcome_observer(
            client_id=args.client, limit=args.limit, dry_run=args.dry_run,
        )
    elif args.cmd == "heal":
        stats = heal_stale_outcomes(
            client_id=args.client, limit=args.limit, dry_run=args.dry_run,
        )
    else:
        parser.print_help()
        sys.exit(1)
    for k, v in stats.items():
        print(f"  {k}: {v}")


if __name__ == "__main__":
    _main()
