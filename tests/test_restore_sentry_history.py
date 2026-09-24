import csv
from pathlib import Path
import tempfile
import unittest

import syscoin_tracker as tracker
from scripts.restore_sentry_history import restore


class HistoryRestoreTests(unittest.TestCase):
    def test_live_state_is_preserved_and_missing_history_is_not_revived(self):
        with tempfile.TemporaryDirectory() as directory:
            store = tracker.Store(Path(directory) / "tracker.sqlite")
            row = {
                "outpoint": "live:0", "source_txid": "live", "source_vout": 0,
                "status": "POSE_BANNED", "service": "current", "collateral_address": "address",
                "first_seen_at": "2026-09-24T00:00:00+00:00", "collateral_time": 100,
            }
            store.save_network_masternode(row)
            path = Path(directory) / "history.csv"
            historical = {
                **row, "status": "ENABLED", "service": "old", "collateral_time": 999,
                "first_seen_at": "2026-05-30T00:00:00+00:00",
            }
            with path.open("w", newline="") as target:
                writer = csv.DictWriter(target, fieldnames=historical.keys())
                writer.writeheader()
                writer.writerows([historical, {**historical, "outpoint": "gone:0", "source_txid": "gone"}])
            result = restore(store, path)
            self.assertEqual(result, {"inserted_history": 1, "first_seen_restored": 1})
            live = store.conn.execute("SELECT * FROM network_masternodes WHERE outpoint='live:0'").fetchone()
            self.assertEqual((live["status"], live["service"], live["collateral_time"]), ("POSE_BANNED", "current", 100))
            absent = store.conn.execute("SELECT * FROM network_masternodes WHERE outpoint='gone:0'").fetchone()
            self.assertEqual(absent["status"], "REMOVED")
            self.assertIsNone(absent["collateral_time"])
            self.assertIsNone(absent["registered_time"])
            self.assertTrue(absent["removed_at"])
            self.assertEqual(restore(store, path), {"inserted_history": 0, "first_seen_restored": 0})
            store.conn.close()
