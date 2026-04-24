"""Phase 4: factor weight learner.

Closes the recommendation feedback loop. Takes all recs that have
outcomes (backtest or production), measures each factor's information
coefficient (IC) against realized forward returns at every horizon,
and proposes updated `factor_weights` via regularized shrinkage.

Safety rails:
- Train/test split by date (80/20). IC measured on train, composite
  validated on test.
- Only applied if test-set composite IC of the proposed weights beats
  the current weights' composite IC. Otherwise we log the proposal and
  exit without writing.
- Shrinkage λ defaults to 0.3 (conservative). One update shifts weights
  30% of the way toward the IC target; 70% stays with current.
- Reconstructed factors only. Factors held at neutral 50 in backtest
  have no variance and get no IC signal — their weights are left
  untouched.
- Previous weights saved to `data/factor_weights_history.json` so
  `--rollback` restores them.
- Floor of 0.005 on any factor that was non-zero — we never let a single
  update zero out a factor entirely.

CLI:
    python -m financials.weight_learner learn --dry-run
    python -m financials.weight_learner learn --apply
    python -m financials.weight_learner rollback
    python -m financials.weight_learner status
"""

import argparse
import json
import logging
import math
import os
import sys
from collections import defaultdict
from datetime import datetime, timedelta

from .alpha import _get_db, _get_factor_weights
from .alpha_historical import RECONSTRUCTED_FACTORS
from .backtest import _spearman_corr
from .recommendations import iter_all_recommendations
from .outcomes import get_outcomes_for_rec
from .outcome_observer import _fetch_period_data, HORIZONS_DAYS

log = logging.getLogger("weight_learner")

DEFAULT_SHRINKAGE = 0.3       # how fast we move toward IC target (0 = no move)
DEFAULT_TRAIN_FRACTION = 0.8  # chronological split
MIN_FACTOR_FLOOR = 0.005      # floor on any factor that was non-zero
MIN_SAMPLE_SIZE = 30          # per plan: gate per factor

_HIST_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), "data")
_HIST_PATH = os.path.join(_HIST_DIR, "factor_weights_history.json")


# ── Data gathering ──────────────────────────────────────────────────

def _gather_observations(horizon_days, min_rec_age_days=30):
    """Pull (rec_date, symbol, subScores, forward_return) rows for every
    mature rec. Used for both IC computation and composite validation.

    A rec is "mature" for a given horizon if (now - rec_date) >= horizon.
    """
    obs = []
    for rec in iter_all_recommendations(min_age_days=min_rec_age_days):
        if not rec.get("created_at"):
            continue
        rec_dt = datetime.fromisoformat(rec["created_at"])
        end_dt = rec_dt + timedelta(days=horizon_days)
        if end_dt > datetime.now():
            continue
        attribution = rec.get("attribution") or {}
        if not attribution:
            continue
        for sym, sym_attr in attribution.items():
            factors = sym_attr.get("factors") or []
            sub = {f["name"]: f["subScore"] for f in factors
                   if "subScore" in f and "name" in f}
            if not sub:
                continue
            payload = _fetch_period_data(sym, rec_dt, end_dt)
            if payload["period_return"] is None:
                continue
            obs.append({
                "rec_dt": rec_dt,
                "symbol": sym,
                "subScores": sub,
                "forward_return": payload["period_return"],
            })
    obs.sort(key=lambda r: r["rec_dt"])
    return obs


def _split_train_test(obs, train_fraction=DEFAULT_TRAIN_FRACTION):
    """Chronological split. Newest 20% becomes test set."""
    n = len(obs)
    if n < 10:
        return obs, []
    cutoff = int(n * train_fraction)
    return obs[:cutoff], obs[cutoff:]


# ── IC computation ──────────────────────────────────────────────────

def _ic_per_factor(observations, factors=RECONSTRUCTED_FACTORS):
    """Spearman correlation per factor between subScore and forward_return
    across the supplied observations. Returns dict {factor: {ic, n}}.
    """
    out = {}
    for factor in factors:
        scores, rets = [], []
        for r in observations:
            s = r["subScores"].get(factor)
            if s is None:
                continue
            scores.append(s)
            rets.append(r["forward_return"])
        if len(scores) < MIN_SAMPLE_SIZE:
            out[factor] = {"ic": None, "n": len(scores)}
            continue
        out[factor] = {"ic": _spearman_corr(scores, rets), "n": len(scores)}
    return out


def _composite_ic(observations, weights):
    """IC of the weighted-composite Alpha Score (using `weights`) vs
    forward returns, across the observations. Used for test-set validation.
    """
    total_weight = sum(weights.values()) or 1.0
    composite = []
    rets = []
    for r in observations:
        wsum = 0.0
        used = 0.0
        for f, w in weights.items():
            s = r["subScores"].get(f)
            if s is None:
                continue
            wsum += w * s
            used += w
        if used <= 0:
            continue
        composite.append(wsum / used)
        rets.append(r["forward_return"])
    if len(composite) < MIN_SAMPLE_SIZE:
        return None
    return _spearman_corr(composite, rets)


# ── Weight proposal ─────────────────────────────────────────────────

def _propose_weights(current, train_obs, horizons=HORIZONS_DAYS,
                     shrinkage=DEFAULT_SHRINKAGE):
    """Compute proposed weights using IC-averaged-across-horizons shrinkage.

    Algorithm:
      1. For each reconstructed factor, compute IC at each horizon.
      2. Average IC across horizons (equal weight).
      3. target[f] = max(0, avg_IC[f]).
      4. Scale targets to match the current SUM of reconstructed-factor
         weights, so we don't inadvertently rescale the whole factor set.
      5. Blend: new[f] = λ × target[f] + (1-λ) × current[f].
      6. Floor non-zero factors at MIN_FACTOR_FLOOR.
      7. Leave non-reconstructed factors untouched.

    Returns (proposed_weights, diagnostics_dict).
    """
    # IC per factor per horizon, collected from observations gathered at
    # multiple horizon ages. We need per-horizon observation sets because
    # forward returns depend on horizon.
    ic_per_horizon = {}
    for h in horizons:
        obs_h = [o for o in train_obs if o.get("horizon") == h]
        ic_per_horizon[h] = _ic_per_factor(obs_h)

    # Average IC across horizons per factor.
    avg_ic = {}
    for f in RECONSTRUCTED_FACTORS:
        values = []
        for h in horizons:
            v = ic_per_horizon[h].get(f, {}).get("ic")
            if v is not None:
                values.append(v)
        avg_ic[f] = sum(values) / len(values) if values else None

    current_reconstructed_sum = sum(
        current.get(f, 0) for f in RECONSTRUCTED_FACTORS
    )

    raw_targets = {f: max(0.0, avg_ic[f]) if avg_ic[f] is not None else None
                   for f in RECONSTRUCTED_FACTORS}
    raw_sum = sum(v for v in raw_targets.values() if v is not None)

    # Scale targets so they sum to the current reconstructed total — preserves
    # the balance between reconstructed and non-reconstructed factor groups.
    scaled_targets = {}
    for f in RECONSTRUCTED_FACTORS:
        if raw_targets[f] is None or raw_sum <= 0:
            scaled_targets[f] = 0.0
        else:
            scaled_targets[f] = raw_targets[f] / raw_sum * current_reconstructed_sum

    # Blend.
    proposed = dict(current)  # start from current; non-reconstructed unchanged
    for f in RECONSTRUCTED_FACTORS:
        cur = current.get(f, 0.0)
        target = scaled_targets[f]
        new = shrinkage * target + (1 - shrinkage) * cur
        if cur > 0 and new < MIN_FACTOR_FLOOR:
            new = MIN_FACTOR_FLOOR
        proposed[f] = round(new, 4)

    diagnostics = {
        "avg_ic_per_factor": {f: round(v, 4) if v is not None else None
                              for f, v in avg_ic.items()},
        "ic_per_horizon": {str(h): ic_per_horizon[h] for h in horizons},
        "raw_targets": {f: round(v, 4) if v is not None else None
                        for f, v in raw_targets.items()},
        "scaled_targets": {f: round(v, 4) for f, v in scaled_targets.items()},
        "shrinkage": shrinkage,
        "current_reconstructed_sum": round(current_reconstructed_sum, 4),
    }
    return proposed, diagnostics


# ── DB update ───────────────────────────────────────────────────────

def _write_weights(weights):
    """Overwrite factor_weights table with the supplied dict. Only updates
    factors that exist in `weights`; leaves others alone."""
    now = datetime.now().isoformat()
    conn = _get_db()
    try:
        for name, w in weights.items():
            conn.execute(
                "UPDATE factor_weights SET weight = ?, last_updated = ? WHERE factor_name = ?",
                (float(w), now, name),
            )
        conn.commit()
    finally:
        conn.close()


def _save_history_entry(previous_weights, proposed_weights, diagnostics):
    """Append to data/factor_weights_history.json for audit + rollback."""
    os.makedirs(_HIST_DIR, exist_ok=True)
    entry = {
        "applied_at": datetime.now().isoformat(),
        "previous": previous_weights,
        "proposed": proposed_weights,
        "diagnostics": diagnostics,
    }
    history = []
    if os.path.exists(_HIST_PATH):
        try:
            with open(_HIST_PATH) as f:
                history = json.load(f)
        except (json.JSONDecodeError, OSError):
            history = []
    history.append(entry)
    with open(_HIST_PATH, "w") as f:
        json.dump(history, f, indent=2, default=str)


def _last_history_entry():
    if not os.path.exists(_HIST_PATH):
        return None
    try:
        with open(_HIST_PATH) as f:
            history = json.load(f)
    except (json.JSONDecodeError, OSError):
        return None
    return history[-1] if history else None


# ── Top-level learn + rollback ───────────────────────────────────────

def learn_weights(shrinkage=DEFAULT_SHRINKAGE,
                  train_fraction=DEFAULT_TRAIN_FRACTION,
                  horizons=HORIZONS_DAYS,
                  apply=False):
    """Compute proposed weights from observed IC, validate on held-out test
    set, optionally apply.

    Returns a diagnostics dict with keys:
      - current_weights, proposed_weights
      - train_ic_per_factor, avg_ic_per_factor
      - test_composite_ic_current, test_composite_ic_proposed
      - validation_passed (bool)
      - applied (bool)
      - n_train, n_test
    """
    current = _get_factor_weights()

    # Gather observations once per horizon. Tag each obs with its horizon so
    # we can filter later without re-fetching prices.
    all_obs = []
    for h in horizons:
        horizon_obs = _gather_observations(h)
        for o in horizon_obs:
            o["horizon"] = h
        all_obs.extend(horizon_obs)

    if not all_obs:
        return {"error": "No observations with outcomes. Run backtest + observe first."}

    # Split chronologically on rec_dt (not on horizon — we want oldest recs
    # in train so the test set reflects more-recent behavior).
    all_obs.sort(key=lambda r: r["rec_dt"])
    train_obs, test_obs = _split_train_test(all_obs, train_fraction)

    proposed, diag = _propose_weights(current, train_obs, horizons, shrinkage)

    # Test-set validation: does the proposed weighted composite beat the
    # current weighted composite on held-out data? Measure on every horizon
    # individually and average (equal-weight horizons, same convention as
    # when proposing).
    def _composite_ic_avg(obs):
        by_h = defaultdict(list)
        for o in obs:
            by_h[o["horizon"]].append(o)
        ics = []
        for h in horizons:
            bucket = by_h.get(h) or []
            cur_ic = _composite_ic(bucket, current)
            if cur_ic is not None:
                ics.append(("current", h, cur_ic, _composite_ic(bucket, proposed)))
        return ics

    validation_rows = _composite_ic_avg(test_obs)
    current_ics = [r[2] for r in validation_rows if r[2] is not None]
    proposed_ics = [r[3] for r in validation_rows if r[3] is not None]
    current_avg = sum(current_ics) / len(current_ics) if current_ics else None
    proposed_avg = sum(proposed_ics) / len(proposed_ics) if proposed_ics else None
    validation_passed = (
        current_avg is not None and proposed_avg is not None
        and proposed_avg > current_avg
    )

    applied = False
    if apply and validation_passed:
        _write_weights(proposed)
        _save_history_entry(current, proposed, diag)
        applied = True
    elif apply and not validation_passed:
        log.warning(
            "Validation failed: proposed composite IC (%s) did not beat "
            "current composite IC (%s) on test set. Skipping apply.",
            proposed_avg, current_avg,
        )

    return {
        "current_weights": current,
        "proposed_weights": proposed,
        "diagnostics": diag,
        "test_composite_ic_current": round(current_avg, 4) if current_avg is not None else None,
        "test_composite_ic_proposed": round(proposed_avg, 4) if proposed_avg is not None else None,
        "test_ic_by_horizon": [
            {"horizon_days": r[1],
             "current": round(r[2], 4) if r[2] is not None else None,
             "proposed": round(r[3], 4) if r[3] is not None else None}
            for r in validation_rows
        ],
        "validation_passed": validation_passed,
        "applied": applied,
        "n_train": len(train_obs),
        "n_test": len(test_obs),
    }


def rollback_last():
    """Restore the weights saved by the most recent apply."""
    entry = _last_history_entry()
    if entry is None:
        return {"error": "No history entries to roll back."}
    _write_weights(entry["previous"])
    return {
        "rolled_back_to": entry["previous"],
        "applied_at_original": entry["applied_at"],
    }


def status():
    """Current weights + last-applied history entry."""
    return {
        "current_weights": _get_factor_weights(),
        "last_history_entry": _last_history_entry(),
    }


# ── CLI ──────────────────────────────────────────────────────────────

def _main():
    parser = argparse.ArgumentParser(prog="weight_learner")
    sub = parser.add_subparsers(dest="cmd", required=True)

    lp = sub.add_parser("learn", help="Propose new weights; optionally apply")
    lp.add_argument("--dry-run", action="store_true", help="Show proposal; do not write")
    lp.add_argument("--apply", action="store_true", help="Write proposal if validation passes")
    lp.add_argument("--shrinkage", type=float, default=DEFAULT_SHRINKAGE)
    lp.add_argument("--train-fraction", type=float, default=DEFAULT_TRAIN_FRACTION)

    sub.add_parser("rollback", help="Restore weights from the last apply")
    sub.add_parser("status", help="Show current weights + last apply log")

    args = parser.parse_args()
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    logging.getLogger("yfinance").setLevel(logging.CRITICAL)

    if args.cmd == "learn":
        apply = bool(getattr(args, "apply", False))
        if getattr(args, "dry_run", False):
            apply = False
        result = learn_weights(
            shrinkage=args.shrinkage,
            train_fraction=args.train_fraction,
            apply=apply,
        )
    elif args.cmd == "rollback":
        result = rollback_last()
    elif args.cmd == "status":
        result = status()
    else:
        parser.print_help()
        sys.exit(1)

    print(json.dumps(result, indent=2, default=str))


if __name__ == "__main__":
    _main()
