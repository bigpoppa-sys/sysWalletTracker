#!/usr/bin/env python3
"""Verify sentry timestamps against exact headers without changing live node state."""

from __future__ import annotations

import argparse
import concurrent.futures
import csv
import datetime as dt
import fcntl
import json
import os
from pathlib import Path
import re
import sqlite3
import sys
import uuid

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import syscoin_tracker as tracker

TIME_FIELDS = (("collateral_height", "collateral_time"), ("registered_height", "registered_time"))
CACHE_TABLE = "tracker_verified_block_times"
RETRY_TABLE = "tracker_block_time_retries"


def positive_integer(value):
    if isinstance(value, bool) or not isinstance(value, (int, str)):
        return None
    try:
        number = int(value)
    except ValueError:
        return None
    return number if number > 0 else None


def collect_heights(conn: sqlite3.Connection, history_csvs: list[Path]) -> tuple[set[int], set[int]]:
    heights = set()
    campaign = set()

    def include(collateral, registered, stored_time):
        heights.update(height for value in (collateral, registered) if (height := positive_integer(value)) is not None)
        timestamp = positive_integer(stored_time)
        collateral_height = positive_integer(collateral)
        # Untrusted dates affect lookup priority only, never the verified result.
        if collateral_height and timestamp and tracker.SN_COMP_START_TS <= timestamp <= tracker.SN_COMP_END_TS:
            campaign.add(collateral_height)

    for row in conn.execute("SELECT collateral_height, registered_height, collateral_time FROM network_masternodes"):
        include(*row)
    for path in history_csvs:
        with path.open(newline="", encoding="utf-8") as source:
            reader = csv.DictReader(source)
            if not {field for field, _ in TIME_FIELDS}.issubset(reader.fieldnames or []):
                raise ValueError(f"Missing height columns in {path}")
            for row in reader:
                include(row.get("collateral_height"), row.get("registered_height"), row.get("collateral_time"))
    return heights, campaign


def checked_header(height: int, header: dict, source: str, expected_hash: str | None = None) -> dict:
    if not isinstance(header, dict) or positive_integer(header.get("height")) != height:
        raise ValueError("Header height does not match requested height")
    block_time = positive_integer(header.get("time"))
    block_hash = header.get("hash")
    if block_time is None or not isinstance(block_hash, str) or not re.fullmatch(r"[0-9a-fA-F]{64}", block_hash):
        raise ValueError("Header lacks a valid timestamp or block hash")
    if expected_hash is not None and block_hash.lower() != expected_hash.lower():
        raise ValueError("Header hash does not match active-chain block hash")
    if header.get("confirmations") is not None and positive_integer(header["confirmations"]) is None:
        raise ValueError("Header is not confirmed on the active chain")
    return {
        "height": height,
        "block_time": block_time,
        "block_hash": block_hash.lower(),
        "source": source,
        "verified_at": tracker.now_iso(),
    }


def read_seed_rows(path: Path | None) -> list[dict]:
    if path is None:
        return []
    with path.open(newline="", encoding="utf-8") as source:
        rows = list(csv.DictReader(source))
    for row in rows:
        txid = row.get("source_txid", "")
        vout = row.get("source_vout", "")
        if not re.fullmatch(r"[0-9a-fA-F]{64}", txid) or not vout.isdigit():
            raise ValueError(f"Invalid collateral outpoint in {path}")
        if tracker.normalize_outpoint(row.get("outpoint", "")) != f"{txid}:{int(vout)}":
            raise ValueError(f"Inconsistent collateral outpoint in {path}")
    return rows


def seed_missing_history(conn: sqlite3.Connection, rows: list[dict]) -> list[dict]:
    seeded = []
    for row in rows:
        outpoint = tracker.normalize_outpoint(row["outpoint"])
        cursor = conn.execute(
            """INSERT INTO network_masternodes(
                outpoint, source_txid, source_vout, pro_tx_hash, status,
                collateral_address, collateral_height, registered_height,
                first_seen_at, last_seen_at
            ) VALUES(?, ?, ?, ?, 'HISTORICAL', ?, ?, ?, ?, ?)
            ON CONFLICT(outpoint) DO NOTHING""",
            (
                outpoint, row["source_txid"], int(row["source_vout"]), row.get("pro_tx_hash") or None,
                row.get("collateral_address") or "",
                positive_integer(row.get("collateral_height")), positive_integer(row.get("registered_height")),
                row.get("first_seen_at") or tracker.now_iso(), row.get("last_seen_at") or tracker.now_iso(),
            ),
        )
        if cursor.rowcount:
            seeded.append({"outpoint": outpoint, "status": "HISTORICAL", "snapshot_status": row.get("status")})
    return seeded


def fetch_headers(heights: list[int], blockbook, rpc=None) -> tuple[list[dict], list[dict]]:
    def fetch_one(height):
        try:
            if rpc is not None:
                block_hash = rpc.call("getblockhash", [height])
                header = rpc.call("getblockheader", [block_hash])
                result = checked_header(height, header, "core", expected_hash=block_hash)
            else:
                result = checked_header(height, blockbook.block(height), "blockbook")
            return result, None
        except Exception as exc:
            return None, {"height": height, "error": f"{type(exc).__name__}: {exc}"}

    verified, failed = [], []
    if heights:
        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
            for result, error in executor.map(fetch_one, heights):
                if result is not None:
                    verified.append(result)
                else:
                    failed.append(error)
    return verified, failed


def ready_core_rpc(args) -> tuple[tracker.SyscoinRpcClient | None, str | None]:
    if args.provider == "blockbook":
        return None, None
    try:
        user, password = os.getenv("SYS_RPC_USER"), os.getenv("SYS_RPC_PASSWORD")
        if not user or not password:
            user, password = args.rpc_cookie.read_text().strip().split(":", 1)
        rpc = tracker.SyscoinRpcClient(args.rpc_url, user, password, timeout=10)
        info = rpc.call("getblockchaininfo")
        if info.get("chain") != "main" or info.get("initialblockdownload") is not False:
            raise RuntimeError("Core is not a ready mainnet node")
        if int(info.get("blocks", -1)) < int(info.get("headers", 0)):
            raise RuntimeError("Core has not caught up to its headers")
        return rpc, None
    except Exception as exc:
        if args.provider == "core":
            raise RuntimeError("A ready mainnet Core RPC is required") from exc
        return None, f"Core unavailable/not ready ({type(exc).__name__}); using Blockbook"


def verify_sentry_times(
    conn: sqlite3.Connection,
    *,
    history_csvs: list[Path],
    audit_dir: Path,
    max_heights: int = 100,
    seed_csv: Path | None = None,
    quarantine_unverified: bool = False,
    blockbook=None,
    rpc=None,
    select_rpc=None,
) -> dict:
    if not 0 <= max_heights <= 100:
        raise ValueError("max_heights must be between 0 and 100")
    seed_rows = read_seed_rows(seed_csv)
    heights, campaign = collect_heights(conn, history_csvs + ([seed_csv] if seed_csv is not None else []))
    conn.execute(f"""
        CREATE TABLE IF NOT EXISTS {CACHE_TABLE} (
            height INTEGER PRIMARY KEY CHECK(height > 0),
            block_time INTEGER NOT NULL CHECK(block_time > 0),
            block_hash TEXT NOT NULL,
            source TEXT NOT NULL,
            verified_at TEXT NOT NULL
        )
    """)
    conn.execute(f"""
        CREATE TABLE IF NOT EXISTS {RETRY_TABLE} (
            height INTEGER PRIMARY KEY,
            last_attempt_at TEXT NOT NULL,
            error TEXT NOT NULL
        )
    """)
    cached = {row[0] for row in conn.execute(f"SELECT height FROM {CACHE_TABLE}")}
    retries = dict(conn.execute(f"SELECT height, last_attempt_at FROM {RETRY_TABLE}"))
    # Try new heights first, then rotate failures oldest-first so unavailable blocks cannot starve the rest.
    pending = sorted(heights - cached, key=lambda height: (
        height in retries, retries.get(height, ""), height not in campaign, height,
    ))
    selected = pending[:max_heights]
    source_note = None
    if selected and select_rpc is not None:
        rpc, source_note = select_rpc()
    if selected and rpc is None and blockbook is None:
        blockbook = tracker.BlockbookClient(
            os.getenv("SYS_BLOCKBOOK_URL", tracker.DEFAULT_BLOCKBOOK_URL), timeout=15, retries=2
        )
    verified, failed = fetch_headers(selected, blockbook, rpc)
    audit_dir.mkdir(parents=True, exist_ok=True)
    stamp = dt.datetime.now(dt.timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
    audit_path = audit_dir / f"sentry-times-{stamp}-{uuid.uuid4().hex[:8]}.json"
    changes = []
    unverified_fields = []
    conn.execute("BEGIN IMMEDIATE")
    try:
        seeded = seed_missing_history(conn, seed_rows)
        for failure in failed:
            conn.execute(
                f"""INSERT INTO {RETRY_TABLE}(height, last_attempt_at, error) VALUES(?, ?, ?)
                    ON CONFLICT(height) DO UPDATE SET last_attempt_at=excluded.last_attempt_at, error=excluded.error""",
                (failure["height"], tracker.now_iso(), failure["error"]),
            )
        for header in verified:
            conn.execute(
                f"""INSERT INTO {CACHE_TABLE}(height, block_time, block_hash, source, verified_at)
                    VALUES(:height, :block_time, :block_hash, :source, :verified_at)
                    ON CONFLICT(height) DO NOTHING""",
                header,
            )
            conn.execute(f"DELETE FROM {RETRY_TABLE} WHERE height = ?", (header["height"],))
        for height_field, time_field in TIME_FIELDS:
            rows = conn.execute(f"""
                SELECT n.outpoint, n.{height_field}, n.{time_field},
                       v.block_time, v.block_hash, v.source
                FROM network_masternodes AS n
                LEFT JOIN {CACHE_TABLE} AS v ON v.height = n.{height_field}
            """).fetchall()
            for outpoint, height, previous_time, block_time, block_hash, source in rows:
                if block_time is None:
                    if quarantine_unverified and previous_time is not None:
                        conn.execute(
                            f"UPDATE network_masternodes SET {time_field} = NULL WHERE outpoint = ? AND {height_field} IS ?",
                            (outpoint, height),
                        )
                        changes.append({
                            "outpoint": outpoint, "field": time_field, "height": height,
                            "previous_time": previous_time, "verified_time": None,
                            "block_hash": None, "source": None,
                            "action": "quarantine_unverified",
                        })
                    unverified_fields.append({
                        "outpoint": outpoint, "field": time_field,
                        "height": height, "stored_time": None if quarantine_unverified else previous_time,
                    })
                elif previous_time != block_time:
                    conn.execute(
                        f"UPDATE network_masternodes SET {time_field} = ? WHERE outpoint = ? AND {height_field} = ?",
                        (block_time, outpoint, height),
                    )
                    changes.append({
                        "outpoint": outpoint, "field": time_field, "height": height,
                        "previous_time": previous_time, "verified_time": block_time,
                        "block_hash": block_hash, "source": source,
                    })
        cached = {row[0] for row in conn.execute(f"SELECT height FROM {CACHE_TABLE}")}
        # Re-read heights under the write lock: another worker may have added nodes during lookup.
        current_heights, _campaign = collect_heights(conn, [])
        heights.update(current_heights)
        remaining = sorted(heights - cached)
        report = {
            "generated_at": tracker.now_iso(),
            "transaction_status": "prepared",
            "source_note": source_note,
            "seed_csv": str(seed_csv) if seed_csv is not None else None,
            "rows_seeded": len(seeded),
            "seeded_rows": seeded,
            "heights_total": len(heights),
            "heights_attempted": len(selected),
            "heights_verified": len(heights & cached),
            "heights_remaining": len(remaining),
            "failed_heights": failed,
            "fields_changed": len(changes),
            "quarantine_unverified": quarantine_unverified,
            "fields_quarantined": sum(change.get("action") == "quarantine_unverified" for change in changes),
            "rows_changed": len({change["outpoint"] for change in changes}),
            "unverified_fields_count": len(unverified_fields),
            "complete": not remaining and not unverified_fields,
            "audit_path": str(audit_path),
            "changes": changes,
            "unverified_fields": unverified_fields,
        }
        # Preserve the before/after evidence before committing any timestamp changes.
        tracker.atomic_write_json(audit_path, report)
        conn.commit()
    except BaseException:
        conn.rollback()
        raise
    report["transaction_status"] = "committed"
    tracker.atomic_write_json(audit_path, report)
    return report


def main(argv: list[str] | None = None) -> int:
    data = Path(os.getenv("SYS_TRACKER_DATA_DIR", "/srv/syswallettracker/data"))
    core = Path(os.getenv("SYSCOIN_DATA_DIR", "/srv/syswallettracker/core"))
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", type=Path, default=Path(os.getenv("SYS_TRACKER_DB", str(data / "syscoin_tracker.sqlite"))))
    parser.add_argument("--history-csv", type=Path, action="append", help="Read historical heights only; repeat for multiple snapshots")
    parser.add_argument("--seed-csv", type=Path, help="Read-only historical snapshot; insert only missing outpoints, never its chain timestamps or live status")
    parser.add_argument("--quarantine-unverified", action="store_true", help="One-time recovery: audit then clear uncached timestamps; use --max-heights 0 for offline quarantine")
    parser.add_argument("--audit-dir", type=Path, help="Defaults to sentry-time-audits beside the database")
    parser.add_argument("--max-heights", type=int, default=os.getenv("SYS_SENTRY_TIME_MAX_HEIGHTS", "100"))
    parser.add_argument("--provider", choices=("auto", "core", "blockbook"), default="auto")
    parser.add_argument("--rpc-url", default=os.getenv("SYS_RPC_URL", "http://127.0.0.1:8370/"))
    parser.add_argument("--rpc-cookie", type=Path, default=core / ".cookie")
    args = parser.parse_args(argv)
    if not 0 <= args.max_heights <= 100:
        parser.error("--max-heights must be between 0 and 100")
    db = args.db.resolve()
    if not db.is_file():
        parser.error(f"Existing tracker database required: {db}")
    history = args.history_csv
    if history is None:
        bundled_history = ROOT / "network_masternodes.csv"
        history = [bundled_history] if bundled_history.exists() else []
    with (db.parent / ".sentry-time-verifier.lock").open("a") as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            print(json.dumps({"skipped": "verification already running"}))
            return 0
        conn = sqlite3.connect(db.as_uri() + "?mode=rw", uri=True, timeout=30, isolation_level=None)
        try:
            report = verify_sentry_times(
                conn, history_csvs=history,
                audit_dir=args.audit_dir or db.parent / "sentry-time-audits",
                max_heights=args.max_heights, seed_csv=args.seed_csv,
                quarantine_unverified=args.quarantine_unverified,
                select_rpc=lambda: ready_core_rpc(args),
            )
        finally:
            conn.close()
    print(json.dumps({key: value for key, value in report.items() if key not in {"changes", "unverified_fields", "seeded_rows"}}))
    return 1 if report["failed_heights"] else 0


if __name__ == "__main__":
    raise SystemExit(main())
