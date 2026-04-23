"""Outcome observations for portfolio recommendations.

Phase 2 of the recommendation feedback loop. Stores the realized performance
of every rec at standard horizons (30/90/180/365 days) along with two
benchmark comparisons (SPY, equal-weight of rec-time holdings) and a
counterfactual (do-nothing baseline). Per-factor attribution decomposes
realized P&L by signal so the Phase 4 weight learner has data to act on.

This module is the data layer only. The observer job that fills these rows
lives in Phase 2.2 (`outcome_observer.py`); the history view that reads them
lives in Phase 2.3 (route + template).
"""

import json
import os
import sqlite3
import threading
from datetime import datetime

_PROJECT_ROOT = os.path.dirname(os.path.dirname(__file__))
_DEFAULT_DB_DIR = os.path.join(_PROJECT_ROOT, "data")
# Mirror recommendations.py: read-only filesystem fallback to /tmp.
if os.access(_DEFAULT_DB_DIR, os.W_OK) or not os.path.exists(_DEFAULT_DB_DIR):
    DB_PATH = os.path.join(_DEFAULT_DB_DIR, "alpha.db")
else:
    DB_PATH = os.path.join("/tmp", "alpha.db")

HORIZONS_DAYS = (30, 90, 180, 365)
_db_lock = threading.Lock()


def _get_db():
    conn = sqlite3.connect(DB_PATH, timeout=10)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def init_db():
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    conn = _get_db()
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS recommendation_outcomes (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            rec_id TEXT NOT NULL,
            horizon_days INTEGER NOT NULL,
            measured_at TEXT NOT NULL,

            realized_return_pct REAL,
            realized_volatility_pct REAL,

            counterfactual_return_pct REAL,
            benchmark_spy_return_pct REAL,
            benchmark_equalweight_return_pct REAL,

            factor_attribution_json TEXT,
            notes TEXT,

            UNIQUE(rec_id, horizon_days)
        );

        CREATE INDEX IF NOT EXISTS idx_outcomes_rec ON recommendation_outcomes(rec_id);
        CREATE INDEX IF NOT EXISTS idx_outcomes_measured ON recommendation_outcomes(measured_at);
        CREATE INDEX IF NOT EXISTS idx_outcomes_horizon ON recommendation_outcomes(horizon_days);
    """)
    conn.commit()
    conn.close()


try:
    init_db()
except Exception:
    pass


def _jdump(v):
    return None if v is None else json.dumps(v, default=str)


def insert_outcome(outcome):
    """Insert (or replace) an outcome row.

    Required keys: rec_id, horizon_days. All other fields optional. The
    UNIQUE(rec_id, horizon_days) constraint means a re-run of the observer
    for the same rec+horizon will REPLACE the existing row (not error) —
    enables idempotent re-measurement when prices update.
    """
    rec_id = outcome["rec_id"]
    horizon = int(outcome["horizon_days"])
    measured_at = outcome.get("measured_at") or datetime.now().isoformat()

    with _db_lock:
        conn = _get_db()
        try:
            conn.execute("""
                INSERT OR REPLACE INTO recommendation_outcomes (
                    rec_id, horizon_days, measured_at,
                    realized_return_pct, realized_volatility_pct,
                    counterfactual_return_pct,
                    benchmark_spy_return_pct, benchmark_equalweight_return_pct,
                    factor_attribution_json, notes
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                rec_id, horizon, measured_at,
                outcome.get("realized_return_pct"),
                outcome.get("realized_volatility_pct"),
                outcome.get("counterfactual_return_pct"),
                outcome.get("benchmark_spy_return_pct"),
                outcome.get("benchmark_equalweight_return_pct"),
                _jdump(outcome.get("factor_attribution")),
                outcome.get("notes"),
            ))
            conn.commit()
        finally:
            conn.close()


def _row_to_outcome(row):
    if row is None:
        return None
    d = dict(row)
    raw = d.pop("factor_attribution_json", None)
    if raw:
        try:
            d["factor_attribution"] = json.loads(raw)
        except (json.JSONDecodeError, TypeError):
            d["factor_attribution"] = None
    else:
        d["factor_attribution"] = None
    return d


def get_outcomes_for_rec(rec_id):
    """Return all outcome rows for one rec, ordered by horizon ascending."""
    conn = _get_db()
    try:
        rows = conn.execute(
            "SELECT * FROM recommendation_outcomes "
            "WHERE rec_id = ? ORDER BY horizon_days ASC",
            (rec_id,),
        ).fetchall()
        return [_row_to_outcome(r) for r in rows]
    finally:
        conn.close()


_REC_JSON_COLS = (
    "holdings_json", "suggested_weights_json", "constraint_params_json",
    "factor_weights_json", "regime_snapshot_json", "attribution_json",
)


def _row_to_rec(row):
    """Convert a portfolio_recommendations row dict, hydrating JSON columns."""
    d = dict(row)
    for key in _REC_JSON_COLS:
        raw = d.pop(key, None)
        clean = key[:-5]
        if raw:
            try:
                d[clean] = json.loads(raw)
            except (json.JSONDecodeError, TypeError):
                d[clean] = None
        else:
            d[clean] = None
    return d


def get_recs_with_outcomes(client_id, limit=50):
    """Return recs + their outcomes for a client. Most-recent first.

    Returns list of dicts, each shaped:
      {rec_id, created_at, ... rec fields ..., outcomes: [horizon dicts]}
    """
    conn = _get_db()
    try:
        rec_rows = conn.execute("""
            SELECT * FROM portfolio_recommendations
            WHERE client_id = ?
            ORDER BY created_at DESC
            LIMIT ?
        """, (client_id, int(limit))).fetchall()
        if not rec_rows:
            return []

        rec_ids = [r["rec_id"] for r in rec_rows]
        placeholders = ",".join("?" * len(rec_ids))
        outcome_rows = conn.execute(
            f"SELECT * FROM recommendation_outcomes WHERE rec_id IN ({placeholders}) "
            f"ORDER BY rec_id, horizon_days ASC",
            rec_ids,
        ).fetchall()

        outcomes_by_rec = {}
        for o in outcome_rows:
            outcomes_by_rec.setdefault(o["rec_id"], []).append(_row_to_outcome(o))

        result = []
        for r in rec_rows:
            rec = _row_to_rec(r)
            rec["outcomes"] = outcomes_by_rec.get(rec["rec_id"], [])
            result.append(rec)
        return result
    finally:
        conn.close()


def list_due_horizons(rec_id, rec_created_at_iso, now=None):
    """For a given rec, return the list of horizons that have come due (i.e.
    enough days have elapsed since the rec) AND don't yet have an outcome row.
    Used by the observer to decide what to compute.
    """
    from datetime import datetime as _dt
    now = now or _dt.now()
    if isinstance(rec_created_at_iso, str):
        rec_created = _dt.fromisoformat(rec_created_at_iso)
    else:
        rec_created = rec_created_at_iso
    elapsed_days = (now - rec_created).days

    existing = {o["horizon_days"] for o in get_outcomes_for_rec(rec_id)}
    return [h for h in HORIZONS_DAYS if elapsed_days >= h and h not in existing]
