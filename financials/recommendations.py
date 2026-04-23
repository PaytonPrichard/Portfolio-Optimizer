"""Portfolio recommendation store.

Captures every recommendation with its full input vector (holdings, factor
weights at time of rec, regime tags) and per-position attribution. Backs
the learning loop: we read these rows later to measure what signals worked.

Shares alpha.db with the Alpha Score engine so outcome observation (Phase 2)
and weight learning (Phase 4) can join across tables.
"""

import json
import os
import sqlite3
import threading
import uuid
from datetime import datetime

_PROJECT_ROOT = os.path.dirname(os.path.dirname(__file__))
_DEFAULT_DB_DIR = os.path.join(_PROJECT_ROOT, "data")
# Fallback for read-only filesystems (Vercel, etc).
if os.access(_DEFAULT_DB_DIR, os.W_OK) or not os.path.exists(_DEFAULT_DB_DIR):
    DB_PATH = os.path.join(_DEFAULT_DB_DIR, "alpha.db")
else:
    DB_PATH = os.path.join("/tmp", "alpha.db")

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
        CREATE TABLE IF NOT EXISTS portfolio_recommendations (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            rec_id TEXT NOT NULL UNIQUE,
            client_id TEXT NOT NULL,
            created_at TEXT NOT NULL,

            total_value REAL NOT NULL,
            holdings_json TEXT NOT NULL,
            current_return_pct REAL,
            current_volatility_pct REAL,
            current_sharpe REAL,

            suggested_weights_json TEXT NOT NULL,
            expected_return_pct REAL,
            expected_volatility_pct REAL,
            expected_sharpe REAL,

            constraint_params_json TEXT,
            factor_weights_json TEXT,

            regime_vix REAL,
            regime_yield_curve REAL,
            regime_snapshot_json TEXT,

            attribution_json TEXT,
            confidence_score INTEGER
        );

        CREATE INDEX IF NOT EXISTS idx_recs_client ON portfolio_recommendations(client_id);
        CREATE INDEX IF NOT EXISTS idx_recs_created ON portfolio_recommendations(created_at);
    """)
    conn.commit()
    conn.close()


try:
    init_db()
except Exception:
    pass


def _jdump(v):
    return None if v is None else json.dumps(v, default=str)


def insert_recommendation(rec):
    """Insert a recommendation row. Returns the rec_id (UUID string).

    Required keys: client_id, total_value, holdings, suggested_weights.
    Optional: current_return_pct, current_volatility_pct, current_sharpe,
    expected_return_pct, expected_volatility_pct, expected_sharpe,
    constraint_params (dict), factor_weights (dict), regime_vix,
    regime_yield_curve, regime_snapshot (dict), attribution (dict:
    symbol -> {factor: contribution}), confidence_score (0-100).
    """
    rec_id = rec.get("rec_id") or str(uuid.uuid4())
    now = datetime.now().isoformat()

    with _db_lock:
        conn = _get_db()
        try:
            conn.execute("""
                INSERT INTO portfolio_recommendations (
                    rec_id, client_id, created_at,
                    total_value, holdings_json,
                    current_return_pct, current_volatility_pct, current_sharpe,
                    suggested_weights_json, expected_return_pct,
                    expected_volatility_pct, expected_sharpe,
                    constraint_params_json, factor_weights_json,
                    regime_vix, regime_yield_curve, regime_snapshot_json,
                    attribution_json, confidence_score
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                rec_id, rec["client_id"], now,
                rec["total_value"], json.dumps(rec["holdings"], default=str),
                rec.get("current_return_pct"),
                rec.get("current_volatility_pct"),
                rec.get("current_sharpe"),
                json.dumps(rec["suggested_weights"], default=str),
                rec.get("expected_return_pct"),
                rec.get("expected_volatility_pct"),
                rec.get("expected_sharpe"),
                _jdump(rec.get("constraint_params")),
                _jdump(rec.get("factor_weights")),
                rec.get("regime_vix"),
                rec.get("regime_yield_curve"),
                _jdump(rec.get("regime_snapshot")),
                _jdump(rec.get("attribution")),
                rec.get("confidence_score"),
            ))
            conn.commit()
        finally:
            conn.close()
    return rec_id


_JSON_COLS = (
    "holdings_json", "suggested_weights_json", "constraint_params_json",
    "factor_weights_json", "regime_snapshot_json", "attribution_json",
)


def _row_to_dict(row):
    if row is None:
        return None
    d = dict(row)
    for key in _JSON_COLS:
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


def get_recommendation(rec_id):
    """Fetch one recommendation by rec_id. Returns dict or None."""
    conn = _get_db()
    try:
        row = conn.execute(
            "SELECT * FROM portfolio_recommendations WHERE rec_id = ?",
            (rec_id,),
        ).fetchone()
        return _row_to_dict(row)
    finally:
        conn.close()


def list_recommendations(client_id, limit=50):
    """List recent recommendations for a client. Most recent first."""
    conn = _get_db()
    try:
        rows = conn.execute("""
            SELECT * FROM portfolio_recommendations
            WHERE client_id = ?
            ORDER BY created_at DESC
            LIMIT ?
        """, (client_id, int(limit))).fetchall()
        return [_row_to_dict(r) for r in rows]
    finally:
        conn.close()
