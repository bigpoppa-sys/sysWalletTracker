#!/usr/bin/env python3
"""Recover missing historical outpoints without overwriting a live snapshot."""

import argparse
import csv
import json
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import syscoin_tracker as tracker


def restore(store, path):
    inserted = 0
    first_seen_restored = 0
    stamp = tracker.now_iso()
    store.conn.execute("BEGIN IMMEDIATE")
    try:
        with path.open(newline="") as source:
            for row in csv.DictReader(source):
                existing = store.conn.execute(
                    "SELECT first_seen_at FROM network_masternodes WHERE outpoint = ?", (row["outpoint"],)
                ).fetchone()
                if existing:
                    previous = row.get("first_seen_at")
                    if previous and previous < existing["first_seen_at"]:
                        tracker.dt.datetime.fromisoformat(previous)
                        store.conn.execute(
                            "UPDATE network_masternodes SET first_seen_at = ? WHERE outpoint = ?",
                            (previous, row["outpoint"]),
                        )
                        first_seen_restored += 1
                    continue
                for key in ("source_vout", "collateral_height", "registered_height", "last_paid_time", "last_paid_block", "taken_down_time", "taken_down_height"):
                    row[key] = tracker.int_or_none(row.get(key))
                # Missing from the freshly fetched live list: preserve the historical
                # row, but never revive an old ENABLED flag or an unverified date.
                row.update(status="REMOVED", removed_at=row.get("removed_at") or stamp,
                           collateral_time=None, registered_time=None)
                store.save_network_masternode(row)
                inserted += 1
        store.conn.commit()
    except BaseException:
        store.conn.rollback()
        raise
    return {"inserted_history": inserted, "first_seen_restored": first_seen_restored}


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", type=Path, required=True)
    parser.add_argument("--csv", type=Path, required=True)
    args = parser.parse_args()
    if not args.db.is_file():
        parser.error("An existing live tracker database is required")
    store = tracker.Store(args.db)
    try:
        print(json.dumps(restore(store, args.csv)))
    finally:
        store.conn.close()
