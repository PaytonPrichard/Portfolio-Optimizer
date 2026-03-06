"""Background data collector for the Alpha intelligence database.

Provides CLI and programmatic interface for:
  - Seeding the DB with a universe of ~200 major stocks
  - Refreshing snapshots for all tracked symbols
  - Backfilling historical price data at quarterly intervals
  - Computing forward returns for historical snapshots
  - Persisting sector cycle data

Usage (CLI):
    python -m financials.alpha_collector seed       # Seed universe with current snapshots
    python -m financials.alpha_collector refresh     # Re-snapshot all tracked symbols
    python -m financials.alpha_collector backfill    # Historical price snapshots (quarterly, 5yr)
    python -m financials.alpha_collector returns     # Compute forward returns
    python -m financials.alpha_collector cycles      # Store sector cycles
    python -m financials.alpha_collector full        # Run complete pipeline
"""

import sys
import time
import threading
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed

import yfinance as yf

from .alpha import (
    collect_snapshot,
    _get_db,
    _db_lock,
    _safe_float,
    _compute_sector_cycles,
    backfill_forward_returns,
)
from .portfolio_widgets import SECTOR_ETFS

# ── Seed universe ─────────────────────────────────────────────────────
# ~200 major stocks across all 11 GICS sectors for broad coverage.

SEED_UNIVERSE = [
    # Technology
    "AAPL", "MSFT", "GOOGL", "AMZN", "META", "NVDA", "AVGO",
    "ORCL", "ADBE", "CRM", "AMD", "INTC", "QCOM", "TXN", "AMAT",
    "NOW", "IBM", "MU", "LRCX", "SNPS", "CDNS", "KLAC", "MRVL",
    "ADI", "PANW", "CRWD", "FTNT", "WDAY", "DDOG", "ZS",
    "PLTR", "NET", "SHOP", "SQ", "DELL", "CSCO",
    # Healthcare
    "UNH", "JNJ", "LLY", "PFE", "ABBV", "MRK", "TMO", "ABT",
    "DHR", "BMY", "AMGN", "GILD", "ISRG", "VRTX", "MDT", "SYK",
    "CVS", "CI", "ELV", "ZTS", "REGN", "MRNA", "DXCM", "BSX", "HCA",
    # Financials
    "JPM", "BAC", "WFC", "GS", "MS", "C", "BLK", "SCHW",
    "AXP", "SPGI", "ICE", "CME", "AON", "MMC", "PGR", "TRV",
    "USB", "PNC", "COF", "BK", "MET", "AIG", "PRU", "ALL", "AFL",
    # Consumer Discretionary
    "TSLA", "HD", "MCD", "NKE", "SBUX", "LOW", "TJX", "BKNG",
    "MAR", "CMG", "ORLY", "AZO", "ROST", "DHI", "LEN", "GM",
    "F", "YUM", "DG", "DLTR",
    # Consumer Staples
    "PG", "KO", "PEP", "COST", "WMT", "PM", "MO", "CL",
    "MDLZ", "GIS", "KHC", "STZ", "KDP", "HSY",
    # Industrials
    "CAT", "DE", "UNP", "HON", "UPS", "RTX", "BA", "LMT",
    "GE", "MMM", "EMR", "ETN", "ITW", "WM", "RSG", "FDX",
    "CSX", "NSC", "TT", "CARR",
    # Communication Services
    "DIS", "NFLX", "CMCSA", "T", "VZ", "TMUS", "CHTR",
    "EA", "TTWO",
    # Energy
    "XOM", "CVX", "COP", "EOG", "SLB", "MPC", "PSX", "VLO",
    "OXY", "HES", "DVN", "WMB", "KMI", "OKE",
    # Utilities
    "NEE", "DUK", "SO", "D", "AEP", "SRE", "EXC", "XEL",
    "ED", "WEC",
    # Real Estate
    "PLD", "AMT", "EQIX", "CCI", "SPG", "PSA", "O", "WELL",
    "DLR", "AVB",
    # Materials
    "LIN", "APD", "SHW", "ECL", "DD", "NEM", "FCX", "NUE",
    "VMC", "MLM",
]


# ── Collection status (for web UI) ───────────────────────────────────

_status = {
    "running": False,
    "action": None,
    "progress": "",
    "last_run": None,
    "last_result": "",
}
_status_lock = threading.Lock()


def get_collection_status():
    with _status_lock:
        return dict(_status)


def _update_status(**kwargs):
    with _status_lock:
        _status.update(kwargs)


# ── Helpers ───────────────────────────────────────────────────────────

def get_all_tracked_symbols():
    """Get all unique symbols already in the database."""
    with _db_lock:
        conn = _get_db()
        try:
            rows = conn.execute(
                "SELECT DISTINCT symbol FROM metric_snapshots ORDER BY symbol"
            ).fetchall()
            return [r["symbol"] for r in rows]
        finally:
            conn.close()


def _log(msg):
    """Print and update status progress."""
    print(msg)
    _update_status(progress=msg)


# ── Seed universe ─────────────────────────────────────────────────────

def seed_universe(batch_size=8, delay=1.5):
    """Seed the database with current snapshots for all universe stocks.
    Returns count of successfully seeded symbols.
    """
    _log(f"Seeding {len(SEED_UNIVERSE)} stocks...")
    done = 0
    errors = 0

    for i in range(0, len(SEED_UNIVERSE), batch_size):
        batch = SEED_UNIVERSE[i:i + batch_size]

        def _snap(sym):
            try:
                return collect_snapshot(sym)
            except Exception:
                return None

        with ThreadPoolExecutor(max_workers=batch_size) as pool:
            futures = {pool.submit(_snap, s): s for s in batch}
            for future in as_completed(futures, timeout=60):
                sym = futures[future]
                try:
                    result = future.result(timeout=20)
                    if result:
                        done += 1
                    else:
                        errors += 1
                except Exception:
                    errors += 1

        _log(f"  Seed: {done + errors}/{len(SEED_UNIVERSE)} ({done} ok, {errors} err)")
        if i + batch_size < len(SEED_UNIVERSE):
            time.sleep(delay)

    _log(f"Seeding complete: {done} stocks added, {errors} errors.")
    return done


# ── Refresh all tracked symbols ───────────────────────────────────────

def refresh_all(batch_size=8, delay=1.5):
    """Re-snapshot every symbol already in the database.
    Returns count of successfully refreshed symbols.
    """
    symbols = get_all_tracked_symbols()
    if not symbols:
        _log("No symbols in database. Run 'seed' first.")
        return 0

    _log(f"Refreshing {len(symbols)} tracked symbols...")
    done = 0
    errors = 0

    for i in range(0, len(symbols), batch_size):
        batch = symbols[i:i + batch_size]

        def _snap(sym):
            try:
                return collect_snapshot(sym)
            except Exception:
                return None

        with ThreadPoolExecutor(max_workers=batch_size) as pool:
            futures = {pool.submit(_snap, s): s for s in batch}
            for future in as_completed(futures, timeout=60):
                try:
                    result = future.result(timeout=20)
                    if result:
                        done += 1
                    else:
                        errors += 1
                except Exception:
                    errors += 1

        _log(f"  Refresh: {done + errors}/{len(symbols)} ({done} ok)")
        if i + batch_size < len(symbols):
            time.sleep(delay)

    _log(f"Refresh complete: {done} updated, {errors} errors.")
    return done


# ── Historical price backfill ─────────────────────────────────────────

def backfill_historical_prices(years=5, samples_per_year=4, batch_size=5, delay=2):
    """Create historical price-only snapshots from yfinance daily data.

    For each tracked symbol, fetches daily price history and creates
    snapshot records at quarterly intervals going back `years` years.
    This enables forward-return computation for historical dates.

    Returns total number of snapshots inserted.
    """
    symbols = get_all_tracked_symbols()
    if not symbols:
        _log("No symbols in database. Run 'seed' first.")
        return 0

    today = datetime.now()

    # Generate target dates: 15th of Mar, Jun, Sep, Dec for past N years
    target_dates = []
    for y in range(1, years + 1):
        for month in [3, 6, 9, 12]:
            target = datetime(today.year - y, month, 15)
            if target < today - timedelta(days=90):
                target_dates.append(target)
    target_dates.sort()

    _log(f"Backfilling {len(target_dates)} dates for {len(symbols)} symbols...")

    total_inserted = 0
    processed = 0

    for i in range(0, len(symbols), batch_size):
        batch = symbols[i:i + batch_size]

        def _process(symbol):
            try:
                ticker = yf.Ticker(symbol)
                hist = ticker.history(period=f"{years + 1}y")
                if hist is None or hist.empty or len(hist) < 20:
                    return 0

                closes = hist["Close"].dropna()
                # Build date -> price lookup
                price_map = {}
                for dt, price in closes.items():
                    price_map[dt.strftime("%Y-%m-%d")] = float(price)

                if not price_map:
                    return 0

                # Get sector/industry from most recent full snapshot
                with _db_lock:
                    conn = _get_db()
                    try:
                        row = conn.execute(
                            "SELECT sector, industry FROM metric_snapshots "
                            "WHERE symbol = ? AND sector IS NOT NULL "
                            "ORDER BY snapshot_date DESC LIMIT 1",
                            (symbol,)
                        ).fetchone()
                        sector = row["sector"] if row else "Unknown"
                        industry = row["industry"] if row else "Unknown"
                    finally:
                        conn.close()

                count = 0
                with _db_lock:
                    conn = _get_db()
                    try:
                        for target_date in target_dates:
                            # Find closest available trading day within 7 days
                            best_date = None
                            best_price = None
                            for offset in range(0, 8):
                                for delta in [offset, -offset]:
                                    check = (target_date + timedelta(days=delta)).strftime("%Y-%m-%d")
                                    if check in price_map:
                                        best_date = check
                                        best_price = price_map[check]
                                        break
                                if best_date:
                                    break

                            if not best_date:
                                continue

                            # INSERT OR IGNORE: don't overwrite existing full snapshots
                            cur = conn.execute(
                                "INSERT OR IGNORE INTO metric_snapshots "
                                "(symbol, snapshot_date, sector, industry, price) "
                                "VALUES (?, ?, ?, ?, ?)",
                                (symbol, best_date, sector, industry, round(best_price, 2)),
                            )
                            count += cur.rowcount
                        conn.commit()
                    finally:
                        conn.close()
                return count
            except Exception:
                return 0

        with ThreadPoolExecutor(max_workers=batch_size) as pool:
            futures = {pool.submit(_process, sym): sym for sym in batch}
            for future in as_completed(futures, timeout=90):
                try:
                    n = future.result(timeout=30)
                    total_inserted += n
                except Exception:
                    pass

        processed += len(batch)
        _log(f"  Backfill: {processed}/{len(symbols)} symbols, {total_inserted} snapshots added")
        if i + batch_size < len(symbols):
            time.sleep(delay)

    _log(f"Historical backfill complete: {total_inserted} snapshots inserted.")
    return total_inserted


# ── Improved forward-return backfill ──────────────────────────────────

def backfill_returns_grouped():
    """Fill forward returns for historical snapshots, grouped by symbol
    to avoid redundant API calls. Returns count of updated rows.
    """
    with _db_lock:
        conn = _get_db()
        try:
            # Get symbols that have snapshots needing forward return fill
            symbols = conn.execute("""
                SELECT DISTINCT symbol FROM metric_snapshots
                WHERE fwd_return_1y IS NULL AND price IS NOT NULL
                AND snapshot_date <= date('now', '-90 days')
            """).fetchall()
            symbols = [r["symbol"] for r in symbols]
        finally:
            conn.close()

    if not symbols:
        _log("No snapshots need forward-return backfill.")
        return 0

    _log(f"Backfilling forward returns for {len(symbols)} symbols...")
    updated = 0

    for sym in symbols:
        try:
            # Fetch full price history once for this symbol
            ticker = yf.Ticker(sym)
            hist = ticker.history(period="max")
            if hist is None or hist.empty:
                continue

            closes = hist["Close"].dropna()
            price_map = {}
            sorted_dates = []
            for dt, price in closes.items():
                d = dt.strftime("%Y-%m-%d")
                price_map[d] = float(price)
                sorted_dates.append(d)
            sorted_dates.sort()

            if len(sorted_dates) < 63:
                continue

            # Get all snapshots for this symbol that need updating
            with _db_lock:
                conn = _get_db()
                try:
                    rows = conn.execute("""
                        SELECT id, snapshot_date, price FROM metric_snapshots
                        WHERE symbol = ? AND fwd_return_1y IS NULL
                        AND price IS NOT NULL
                        AND snapshot_date <= date('now', '-90 days')
                        ORDER BY snapshot_date
                    """, (sym,)).fetchall()
                finally:
                    conn.close()

            if not rows:
                continue

            # Compute forward returns for each snapshot
            batch_updates = []
            for row in rows:
                snap_date = row["snapshot_date"]
                snap_price = row["price"]
                if not snap_price or snap_price <= 0:
                    continue

                updates = {}
                for label, days in [
                    ("fwd_return_3m", 63),
                    ("fwd_return_6m", 126),
                    ("fwd_return_1y", 252),
                    ("fwd_return_3y", 756),
                    ("fwd_return_5y", 1260),
                ]:
                    target_date = (
                        datetime.strptime(snap_date, "%Y-%m-%d") + timedelta(days=days)
                    ).strftime("%Y-%m-%d")

                    # Find closest date on or after target
                    fwd_price = None
                    for d in sorted_dates:
                        if d >= target_date:
                            fwd_price = price_map[d]
                            break

                    if fwd_price is not None:
                        updates[label] = round(
                            (fwd_price - snap_price) / snap_price * 100, 2
                        )

                if updates:
                    batch_updates.append((updates, row["id"]))

            # Write all updates for this symbol in one lock
            if batch_updates:
                with _db_lock:
                    conn = _get_db()
                    try:
                        for updates, row_id in batch_updates:
                            set_clause = ", ".join(f"{k} = ?" for k in updates)
                            conn.execute(
                                f"UPDATE metric_snapshots SET {set_clause} WHERE id = ?",
                                list(updates.values()) + [row_id],
                            )
                            updated += 1
                        conn.commit()
                    finally:
                        conn.close()

        except Exception:
            continue

    _log(f"Forward returns complete: {updated} snapshots updated.")
    return updated


# ── Sector cycle persistence ──────────────────────────────────────────

def persist_sector_cycles():
    """Compute and store current sector cycle data to the database.
    Returns count of sectors stored.
    """
    _log("Computing and storing sector cycles...")
    cycles = _compute_sector_cycles()
    if not cycles:
        _log("No sector cycle data available.")
        return 0

    today = datetime.now().strftime("%Y-%m-%d")

    with _db_lock:
        conn = _get_db()
        try:
            for sector, data in cycles.items():
                conn.execute(
                    "INSERT OR REPLACE INTO sector_cycles "
                    "(sector, cycle_date, etf_symbol, price, return_1m, "
                    "return_3m, return_6m, return_1y, relative_to_spy_1y, cycle_phase) "
                    "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    (
                        sector, today,
                        data.get("etf"),
                        data.get("price"),
                        data.get("return_1m"),
                        data.get("return_3m"),
                        data.get("return_6m"),
                        data.get("return_1y"),
                        data.get("relative_to_spy"),
                        data.get("phase"),
                    ),
                )
            conn.commit()
        finally:
            conn.close()

    _log(f"Sector cycles stored: {len(cycles)} sectors.")
    return len(cycles)


# ── Historical sector cycle backfill ──────────────────────────────────

def _classify_phase(r_1y, r_6m):
    """Determine cycle phase from 6m and 1y returns (same logic as alpha.py)."""
    if r_1y is None or r_6m is None:
        return "unknown"
    if r_1y > 15 and r_6m > 5:
        return "expansion"
    elif r_1y > 0 and r_6m <= 0:
        return "peak"
    elif r_1y < -5 and r_6m < 0:
        return "contraction"
    elif r_1y < 0 and r_6m > 0:
        return "recovery"
    return "neutral"


def backfill_historical_cycles(years=10):
    """Replay sector cycle detection at quarterly intervals going back `years`.

    For each sector ETF, fetches long-term price history and computes
    the cycle phase at each quarterly sample date by looking at the
    returns as of that date. Also computes relative-to-SPY at each point.

    Returns total number of cycle records inserted.
    """
    _log(f"Backfilling historical sector cycles ({years} years)...")

    today = datetime.now()

    # Generate target dates: 15th of Mar, Jun, Sep, Dec
    target_dates = []
    for y in range(1, years + 1):
        for month in [3, 6, 9, 12]:
            d = datetime(today.year - y, month, 15)
            if d < today - timedelta(days=30):
                target_dates.append(d)
    target_dates.sort()

    # Fetch long-term history for SPY and all sector ETFs
    all_etfs = list(SECTOR_ETFS.keys()) + ["SPY"]
    etf_prices = {}  # etf_symbol -> {date_str: price}

    def _fetch_etf(sym):
        try:
            hist = yf.Ticker(sym).history(period=f"{years + 2}y")
            if hist is None or hist.empty:
                return sym, {}
            closes = hist["Close"].dropna()
            pm = {}
            for dt, price in closes.items():
                pm[dt.strftime("%Y-%m-%d")] = float(price)
            return sym, pm
        except Exception:
            return sym, {}

    _log(f"  Fetching {len(all_etfs)} ETF histories...")
    with ThreadPoolExecutor(max_workers=6) as pool:
        futures = {pool.submit(_fetch_etf, sym): sym for sym in all_etfs}
        for future in as_completed(futures, timeout=60):
            try:
                sym, pm = future.result(timeout=30)
                if pm:
                    etf_prices[sym] = pm
            except Exception:
                pass

    _log(f"  Got data for {len(etf_prices)} ETFs. Computing cycles at {len(target_dates)} dates...")

    def _find_price(price_map, sorted_dates, target_date, max_offset=7):
        """Find closest trading day price to a target date."""
        for offset in range(0, max_offset + 1):
            for delta in [offset, -offset]:
                check = (target_date + timedelta(days=delta)).strftime("%Y-%m-%d")
                if check in price_map:
                    return price_map[check], check
        return None, None

    def _historical_return(price_map, sorted_dates, as_of_date, lookback_days):
        """Compute return looking back `lookback_days` from `as_of_date`."""
        current_price, _ = _find_price(price_map, sorted_dates, as_of_date)
        past_date = as_of_date - timedelta(days=int(lookback_days * 365 / 252))
        past_price, _ = _find_price(price_map, sorted_dates, past_date)
        if current_price and past_price and past_price > 0:
            return round((current_price - past_price) / past_price * 100, 2)
        return None

    # Pre-sort dates for each ETF
    etf_sorted = {sym: sorted(pm.keys()) for sym, pm in etf_prices.items()}

    total_inserted = 0

    with _db_lock:
        conn = _get_db()
        try:
            for target_date in target_dates:
                # Compute SPY 1Y return at this date for relative comparison
                spy_1y = None
                if "SPY" in etf_prices:
                    spy_1y = _historical_return(
                        etf_prices["SPY"], etf_sorted["SPY"], target_date, 252
                    )

                for etf_sym, sector_name in SECTOR_ETFS.items():
                    if etf_sym not in etf_prices:
                        continue

                    pm = etf_prices[etf_sym]
                    sd = etf_sorted[etf_sym]

                    price, actual_date = _find_price(pm, sd, target_date)
                    if not price or not actual_date:
                        continue

                    r_1m = _historical_return(pm, sd, target_date, 21)
                    r_3m = _historical_return(pm, sd, target_date, 63)
                    r_6m = _historical_return(pm, sd, target_date, 126)
                    r_1y = _historical_return(pm, sd, target_date, 252)

                    phase = _classify_phase(r_1y, r_6m)

                    rel_spy = None
                    if r_1y is not None and spy_1y is not None:
                        rel_spy = round(r_1y - spy_1y, 2)

                    cur = conn.execute(
                        "INSERT OR IGNORE INTO sector_cycles "
                        "(sector, cycle_date, etf_symbol, price, return_1m, "
                        "return_3m, return_6m, return_1y, relative_to_spy_1y, cycle_phase) "
                        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                        (
                            sector_name, actual_date, etf_sym,
                            round(price, 2), r_1m, r_3m, r_6m, r_1y,
                            rel_spy, phase,
                        ),
                    )
                    total_inserted += cur.rowcount

            conn.commit()
        finally:
            conn.close()

    _log(f"Historical sector cycles complete: {total_inserted} records inserted.")
    return total_inserted


# ── Full pipeline ─────────────────────────────────────────────────────

def run_full_pipeline():
    """Run the complete data collection pipeline."""
    start = time.time()
    _log("=== Starting full collection pipeline ===")

    _log("Step 1/6: Seeding universe...")
    seeded = seed_universe()

    _log("Step 2/6: Refreshing all tracked symbols...")
    refreshed = refresh_all()

    _log("Step 3/6: Backfilling historical prices (5 years quarterly)...")
    backfilled = backfill_historical_prices(years=5, samples_per_year=4)

    _log("Step 4/6: Computing forward returns...")
    returns = backfill_returns_grouped()

    _log("Step 5/6: Storing current sector cycles...")
    cycles = persist_sector_cycles()

    _log("Step 6/6: Backfilling historical sector cycles (10 years)...")
    hist_cycles = backfill_historical_cycles(years=10)

    elapsed = time.time() - start
    summary = (
        f"Pipeline complete in {elapsed/60:.1f} min: "
        f"{seeded} seeded, {refreshed} refreshed, "
        f"{backfilled} historical prices, {returns} fwd returns, "
        f"{cycles} current cycles, {hist_cycles} historical cycles"
    )
    _log(f"=== {summary} ===")
    return summary


# ── Background runner (for web UI trigger) ────────────────────────────

_bg_thread = None


def run_in_background(action="full"):
    """Start a collection action in a background thread.
    Returns True if started, False if already running.
    """
    global _bg_thread
    if _status["running"]:
        return False

    actions = {
        "seed": seed_universe,
        "refresh": refresh_all,
        "backfill": lambda: backfill_historical_prices(years=5),
        "returns": backfill_returns_grouped,
        "cycles": persist_sector_cycles,
        "hist-cycles": lambda: backfill_historical_cycles(years=10),
        "full": run_full_pipeline,
    }

    fn = actions.get(action)
    if not fn:
        return False

    def _run():
        _update_status(running=True, action=action, progress=f"Starting: {action}")
        try:
            result = fn()
            _update_status(
                running=False,
                last_run=datetime.now().isoformat(),
                last_result=f"Completed: {action} ({result})",
                progress="",
            )
        except Exception as e:
            _update_status(
                running=False,
                last_run=datetime.now().isoformat(),
                last_result=f"Error in {action}: {e}",
                progress="",
            )

    _bg_thread = threading.Thread(target=_run, daemon=True)
    _bg_thread.start()
    return True


# ── CLI ───────────────────────────────────────────────────────────────

def main():
    commands = {
        "seed": ("Seed database with ~200 major stocks", seed_universe),
        "refresh": ("Re-snapshot all tracked symbols", refresh_all),
        "backfill": ("Backfill historical prices (5yr quarterly)", lambda: backfill_historical_prices(years=5)),
        "returns": ("Compute forward returns for old snapshots", backfill_returns_grouped),
        "cycles": ("Store current sector cycle data", persist_sector_cycles),
        "hist-cycles": ("Backfill historical sector cycles (10yr)", lambda: backfill_historical_cycles(years=10)),
        "full": ("Run complete pipeline (all steps)", run_full_pipeline),
    }

    if len(sys.argv) < 2 or sys.argv[1] not in commands:
        print("Alpha Intelligence Database Collector")
        print("=" * 45)
        print(f"\nUsage: python -m financials.alpha_collector <command>\n")
        print("Commands:")
        for cmd, (desc, _) in commands.items():
            print(f"  {cmd:12s}  {desc}")
        print()

        # Show current DB stats
        from .alpha import get_db_stats
        stats = get_db_stats()
        print(f"Current DB: {stats['totalSnapshots']} snapshots, "
              f"{stats['uniqueSymbols']} symbols")
        if stats["oldestSnapshot"]:
            print(f"Date range: {stats['oldestSnapshot']} to {stats['newestSnapshot']}")
        sys.exit(1)

    cmd = sys.argv[1]
    desc, fn = commands[cmd]
    print(f"\n{desc}...\n")

    start = time.time()
    result = fn()
    elapsed = time.time() - start
    print(f"\nDone in {elapsed/60:.1f} minutes. Result: {result}")


if __name__ == "__main__":
    main()
