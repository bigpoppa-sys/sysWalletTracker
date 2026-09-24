import argparse
import contextlib
import csv
import io
import json
import os
from pathlib import Path
import tempfile
import threading
import time
import unittest
from unittest import mock

import syscoin_tracker as tracker
from scripts import verify_sentry_times as verifier


class ResolverTests(unittest.TestCase):
    def setUp(self):
        self.context = contextlib.ExitStack()
        self.addCleanup(self.context.close)
        self.tip = self.context.enter_context(mock.patch.object(
            tracker, "blockbook_tip_height_time", return_value=(1000, tracker.SN_COMP_END_TS)
        ))
        self.context.enter_context(mock.patch.object(
            tracker, "estimate_block_height_from_tip",
            side_effect=lambda timestamp, *_: 100 if timestamp == tracker.SN_COMP_START_TS else 200,
        ))
        self.context.enter_context(mock.patch.object(tracker, "DEFAULT_SYSNODE_TIME_WINDOW_MARGIN_BLOCKS", 0))
        self.context.enter_context(mock.patch.dict(os.environ, {"SYS_SYSNODE_TIME_WINDOW_MARGIN_BLOCKS": "0"}))
        self.blocks = self.context.enter_context(mock.patch.object(tracker, "block_times_from_blockbook", return_value={}))
        self.txs = self.context.enter_context(mock.patch.object(tracker, "tx_times_from_blockbook", return_value={}))
        self.context.enter_context(mock.patch.object(
            tracker, "estimate_block_time_from_tip", side_effect=AssertionError("guessed timestamp")
        ))

    def row(self, height):
        return {
            "source_txid": f"tx-{height}", "collateral_height": height,
            "registered_height": height, "collateral_time": None, "registered_time": None,
        }

    def test_zero_limit_never_looks_up_or_estimates_campaign_dates(self):
        rows = [self.row(150)]
        result = tracker.resolve_missing_network_masternode_times_from_blockbook(None, rows, max_lookups=0)
        self.assertEqual(result, {"needed": 1, "looked_up": 0, "filled": 0, "estimated": 0})
        self.assertIsNone(rows[0]["collateral_time"])
        self.tip.assert_not_called()
        self.blocks.assert_not_called()
        self.txs.assert_not_called()

    def test_campaign_priority_is_capped_after_selection(self):
        rows = [self.row(height) for height in (150, 151, 152, 999)]
        self.blocks.side_effect = lambda _client, heights: {height: 1700000000 + height for height in heights}
        result = tracker.resolve_missing_network_masternode_times_from_blockbook(None, rows, max_lookups=2)
        self.assertEqual(self.blocks.call_args.args[1], {151, 152})
        self.assertEqual(result["looked_up"], 2)
        self.assertEqual(result["filled"], 4)
        self.assertIsNone(rows[0]["collateral_time"])
        self.assertIsNone(rows[-1]["collateral_time"])

    def test_campaign_only_scope_obeys_cap(self):
        rows = [self.row(height) for height in (150, 151, 152, 999)]
        result = tracker.resolve_missing_network_masternode_times_from_blockbook(None, rows, max_lookups=1, scope="sn_comp")
        self.assertEqual(self.blocks.call_args.args[1], {152})
        self.assertEqual(result["estimated"], 0)

    def test_failed_lookups_remain_null_and_retry(self):
        rows = [self.row(150)]
        first = tracker.resolve_missing_network_masternode_times_from_blockbook(None, rows, max_lookups=1)
        self.assertEqual(first["filled"], 0)
        self.assertEqual(rows, [self.row(150)])
        self.blocks.return_value = {150: 1700000150}
        second = tracker.resolve_missing_network_masternode_times_from_blockbook(None, rows, max_lookups=1)
        self.assertEqual(second["filled"], 2)
        self.assertEqual(second["estimated"], 0)
        self.assertEqual(self.blocks.call_count, 2)

    def test_exact_transaction_fallback_only_fills_collateral(self):
        rows = [self.row(150)]
        self.txs.return_value = {"tx-150": 1700000150}
        result = tracker.resolve_missing_network_masternode_times_from_blockbook(None, rows, max_lookups=1)
        self.assertEqual(result["filled"], 1)
        self.assertEqual(rows[0]["collateral_time"], 1700000150)
        self.assertIsNone(rows[0]["registered_time"])


class VerifierTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.store = tracker.Store(self.root / "tracker.sqlite")
        self.addCleanup(self.store.conn.close)
        self.client = mock.Mock(spec=tracker.BlockbookClient)
        self.client.block.side_effect = self.header
        self.network_guard = mock.patch("urllib.request.urlopen", side_effect=AssertionError("real network call"))
        self.network_guard.start()
        self.addCleanup(self.network_guard.stop)

    def header(self, height):
        return {"height": height, "time": 1700000000 + height, "hash": f"{height:064x}", "confirmations": 10}

    def add_node(self, index=1, collateral_height=10, registered_height=20, **overrides):
        row = {
            "outpoint": f"{index:064x}:0", "source_txid": f"{index:064x}", "source_vout": 0,
            "pro_tx_hash": f"{index + 100:064x}", "status": "ENABLED", "service": "live-service:8369",
            "collateral_address": f"address-{index}", "collateral_height": collateral_height,
            "registered_height": registered_height, "collateral_time": 123, "registered_time": None,
            "first_seen_at": "2026-05-15T00:00:00+00:00", "last_seen_at": "2026-09-25T00:00:00+00:00",
            "removed_at": "", "taken_down_txid": "", "taken_down_time": None,
        }
        row.update(overrides)
        self.store.save_network_masternode(row)
        return row

    def node(self, outpoint):
        return dict(self.store.conn.execute("SELECT * FROM network_masternodes WHERE outpoint = ?", (outpoint,)).fetchone())

    def run_verifier(self, **kwargs):
        return verifier.verify_sentry_times(
            self.store.conn, history_csvs=kwargs.pop("history_csvs", []),
            audit_dir=self.root / "audits", blockbook=self.client, **kwargs,
        )

    def write_csv(self, rows):
        path = self.root / "original-september.csv"
        with path.open("w", newline="") as output:
            writer = csv.DictWriter(output, fieldnames=tracker.NETWORK_MASTERNODE_HEADERS, extrasaction="ignore")
            writer.writeheader()
            writer.writerows(rows)
        return path

    def test_exact_headers_correct_filled_and_null_times_preserving_all_other_fields(self):
        first = self.add_node()
        second = self.add_node(2, status="REMOVED", removed_at="2026-09-01T00:00:00+00:00", registered_time=456)
        before = {row["outpoint"]: self.node(row["outpoint"]) for row in (first, second)}
        result = self.run_verifier()
        self.assertEqual(result["heights_attempted"], 2)
        self.assertEqual(result["fields_changed"], 4)
        self.assertTrue(result["complete"])
        self.assertCountEqual([call.args[0] for call in self.client.block.call_args_list], [10, 20])
        for outpoint, original in before.items():
            updated = self.node(outpoint)
            for key in original:
                if key not in {"collateral_time", "registered_time"}:
                    self.assertEqual(updated[key], original[key], key)
            self.assertEqual(updated["collateral_time"], 1700000010)
            self.assertEqual(updated["registered_time"], 1700000020)
        audit = json.loads(Path(result["audit_path"]).read_text())
        self.assertEqual(audit, result)
        self.assertEqual(audit["transaction_status"], "committed")
        self.assertEqual({change["previous_time"] for change in audit["changes"]}, {123, 456, None})
        cached = self.store.conn.execute(f"SELECT * FROM {verifier.CACHE_TABLE}").fetchall()
        self.assertEqual(len(cached), 2)
        self.assertEqual(cached[0]["source"], "blockbook")

    def test_failed_headers_keep_unknown_values_and_are_retried(self):
        row = self.add_node(collateral_height=10, registered_height=10)
        before = self.node(row["outpoint"])
        self.client.block.side_effect = TimeoutError("unavailable")
        first = self.run_verifier(max_heights=1)
        self.assertEqual(self.node(row["outpoint"]), before)
        self.assertEqual(first["heights_verified"], 0)
        self.assertEqual(first["unverified_fields_count"], 2)
        self.assertEqual(self.store.conn.execute(f"SELECT COUNT(*) FROM {verifier.RETRY_TABLE}").fetchone()[0], 1)
        self.client.block.side_effect = self.header
        second = self.run_verifier(max_heights=1)
        self.assertTrue(second["complete"])
        self.assertEqual(self.store.conn.execute(f"SELECT COUNT(*) FROM {verifier.RETRY_TABLE}").fetchone()[0], 0)

    def test_campaign_priority_and_failure_rotation_prevent_starvation(self):
        self.add_node(1, 10, 10)
        self.add_node(2, 20, 20)
        self.add_node(3, 900, 900, collateral_time=tracker.SN_COMP_START_TS + 1)
        self.client.block.side_effect = TimeoutError("first attempt fails")
        self.run_verifier(max_heights=1)
        self.assertEqual(self.client.block.call_args.args, (900,))
        self.client.block.side_effect = self.header
        self.run_verifier(max_heights=1)
        self.assertEqual(self.client.block.call_args.args, (10,))
        self.run_verifier(max_heights=1)
        self.assertEqual(self.client.block.call_args.args, (20,))
        result = self.run_verifier(max_heights=1)
        self.assertEqual(self.client.block.call_args.args, (900,))
        self.assertTrue(result["complete"])

    def test_success_cache_repairs_later_values_without_network(self):
        row = self.add_node()
        self.run_verifier()
        self.store.conn.execute("UPDATE network_masternodes SET collateral_time = 999")
        self.client.block.reset_mock()
        selector = mock.Mock(side_effect=AssertionError("network readiness lookup"))
        report = self.run_verifier(max_heights=0, select_rpc=selector)
        self.assertEqual(report["heights_attempted"], 0)
        self.assertEqual(self.node(row["outpoint"])["collateral_time"], 1700000010)
        selector.assert_not_called()
        self.client.block.assert_not_called()

    def test_seed_csv_inserts_only_missing_history_and_never_trusts_its_dates(self):
        existing = self.add_node()
        original = self.node(existing["outpoint"])
        missing = {
            **existing, "outpoint": f"{99:064x}:0", "source_txid": f"{99:064x}",
            "status": "ENABLED", "collateral_height": 30, "registered_height": 40,
            "collateral_time": 666, "registered_time": 777,
        }
        history = self.write_csv([{**existing, "status": "REMOVED", "service": "stale-service"}, missing])
        original_csv = history.read_bytes()
        first = self.run_verifier(seed_csv=history, max_heights=0)
        self.assertEqual(first["rows_seeded"], 1)
        self.assertEqual(self.node(existing["outpoint"]), original)
        seeded = self.node(missing["outpoint"])
        self.assertEqual(seeded["status"], "HISTORICAL")
        self.assertIsNone(seeded["collateral_time"])
        self.assertIsNone(seeded["registered_time"])
        second = self.run_verifier(seed_csv=history)
        self.assertEqual(second["rows_seeded"], 0)
        self.assertEqual(self.node(missing["outpoint"])["collateral_time"], 1700000030)
        self.assertEqual(self.node(existing["outpoint"])["status"], "ENABLED")
        self.assertEqual(self.node(existing["outpoint"])["service"], "live-service:8369")
        self.assertEqual(history.read_bytes(), original_csv)

    def test_history_height_scan_does_not_insert_rows(self):
        row = self.add_node()
        history = self.write_csv([{**row, "collateral_height": 30, "registered_height": 40}])
        result = self.run_verifier(history_csvs=[history])
        self.assertEqual(result["heights_verified"], 4)
        self.assertEqual(result["rows_seeded"], 0)
        self.assertEqual(self.store.conn.execute("SELECT COUNT(*) FROM network_masternodes").fetchone()[0], 1)

    def test_audit_write_failure_rolls_back_timestamp_and_cache_changes(self):
        row = self.add_node()
        before = self.node(row["outpoint"])
        with mock.patch.object(tracker, "atomic_write_json", side_effect=OSError("audit disk unavailable")):
            with self.assertRaises(OSError):
                self.run_verifier()
        self.assertEqual(self.node(row["outpoint"]), before)
        self.assertEqual(self.store.conn.execute(f"SELECT COUNT(*) FROM {verifier.CACHE_TABLE}").fetchone()[0], 0)

    def test_invalid_headers_are_not_cached(self):
        self.add_node(collateral_height=10, registered_height=10)
        bad_headers = [
            {**self.header(10), "height": 11}, {**self.header(10), "time": None},
            {**self.header(10), "hash": "not-a-hash"}, {**self.header(10), "confirmations": -1},
        ]
        for header in bad_headers:
            with self.subTest(header=header):
                self.client.block.side_effect = None
                self.client.block.return_value = header
                report = self.run_verifier()
                self.assertEqual(report["heights_verified"], 0)
                self.assertEqual(len(report["failed_heights"]), 1)

    def test_core_provider_uses_headers_and_checks_active_chain_hash(self):
        self.add_node(collateral_height=10, registered_height=10)
        rpc = mock.Mock()
        rpc.call.side_effect = lambda method, params: self.header(10)["hash"] if method == "getblockhash" else self.header(10)
        result = self.run_verifier(rpc=rpc)
        self.assertTrue(result["complete"])
        self.client.block.assert_not_called()
        self.assertEqual(result["changes"][0]["source"], "core")
        with self.assertRaises(ValueError):
            verifier.checked_header(10, self.header(10), "core", expected_hash="f" * 64)

    def test_blockbook_uses_at_most_two_concurrent_workers(self):
        active = peak = 0
        lock = threading.Lock()

        def fetch(height):
            nonlocal active, peak
            with lock:
                active += 1
                peak = max(peak, active)
            try:
                time.sleep(0.01)
                return self.header(height)
            finally:
                with lock:
                    active -= 1

        self.client.block.side_effect = fetch
        verified, failed = verifier.fetch_headers(list(range(1, 7)), self.client)
        self.assertEqual(len(verified), 6)
        self.assertEqual(failed, [])
        self.assertEqual(peak, 2)

    def test_core_readiness_falls_back_while_syncing(self):
        args = argparse.Namespace(provider="auto", rpc_cookie=self.root / ".cookie", rpc_url="http://localhost:8370/")
        rpc = mock.Mock()
        rpc.call.return_value = {"chain": "main", "initialblockdownload": True, "blocks": 10, "headers": 100}
        with mock.patch.dict(os.environ, {"SYS_RPC_USER": "user", "SYS_RPC_PASSWORD": "password"}), mock.patch.object(tracker, "SyscoinRpcClient", return_value=rpc):
            selected, note = verifier.ready_core_rpc(args)
            self.assertIsNone(selected)
            self.assertIn("using Blockbook", note)
            rpc.call.return_value.update(initialblockdownload=False, blocks=100)
            self.assertIs(verifier.ready_core_rpc(args)[0], rpc)

    def test_existing_times_are_not_reused_for_different_heights_or_registration(self):
        row = self.add_node(registered_time=456)
        changed = {**row, "collateral_height": 99, "pro_tx_hash": "changed", "collateral_time": None, "registered_time": None}
        tracker.fill_network_masternode_times_from_store(self.store, [changed])
        self.assertIsNone(changed["collateral_time"])
        self.assertIsNone(changed["registered_time"])

    def test_cli_supports_worker_flags_and_environment_limit(self):
        self.add_node()
        with mock.patch.dict(os.environ, {"SYS_SENTRY_TIME_MAX_HEIGHTS": "0"}), mock.patch.object(verifier, "ROOT", self.root), contextlib.redirect_stdout(io.StringIO()) as output:
            result = verifier.main(["--db", str(self.store.path), "--audit-dir", str(self.root / "cli-audits")])
        summary = json.loads(output.getvalue())
        self.assertEqual(result, 0)
        self.assertEqual(summary["heights_attempted"], 0)
        self.assertTrue(Path(summary["audit_path"]).is_file())
        self.assertNotIn("changes", summary)

    def test_cli_reports_failed_lookups_with_nonzero_exit_code(self):
        self.add_node(collateral_height=10, registered_height=10)
        self.client.block.side_effect = TimeoutError("unavailable")
        with mock.patch.object(verifier, "ROOT", self.root), mock.patch.object(tracker, "BlockbookClient", return_value=self.client), contextlib.redirect_stdout(io.StringIO()) as output:
            result = verifier.main([
                "--db", str(self.store.path), "--max-heights", "100",
                "--audit-dir", str(self.root / "cli-audits"), "--provider", "blockbook",
            ])
        self.assertEqual(result, 1)
        self.assertEqual(len(json.loads(output.getvalue())["failed_heights"]), 1)

    def test_limits_outside_zero_to_one_hundred_are_rejected(self):
        for limit in (-1, 101):
            with self.subTest(limit=limit), self.assertRaises(ValueError):
                self.run_verifier(max_heights=limit)

    def test_offline_quarantine_audits_only_uncached_values_and_preserves_live_state(self):
        cached = self.add_node(1, 10, 10)
        self.run_verifier()
        unknown = self.add_node(2, 30, 40, collateral_time=123, registered_time=456,
                                status="REMOVED", removed_at="2026-09-01T00:00:00+00:00")
        missing_height = self.add_node(3, None, None, collateral_time=789)
        before = {row["outpoint"]: self.node(row["outpoint"]) for row in (cached, unknown, missing_height)}
        self.client.block.reset_mock()
        selector = mock.Mock(side_effect=AssertionError("network readiness lookup"))
        result = self.run_verifier(max_heights=0, quarantine_unverified=True, select_rpc=selector)
        self.assertEqual(result["fields_quarantined"], 3)
        self.assertEqual(result["fields_changed"], 3)
        self.assertFalse(result["complete"])
        self.assertEqual(self.node(cached["outpoint"]), before[cached["outpoint"]])
        for row in (unknown, missing_height):
            after = self.node(row["outpoint"])
            self.assertIsNone(after["collateral_time"])
            self.assertIsNone(after["registered_time"])
            for key, value in before[row["outpoint"]].items():
                if key not in {"collateral_time", "registered_time"}:
                    self.assertEqual(after[key], value, key)
        audit = json.loads(Path(result["audit_path"]).read_text())
        self.assertEqual({change["previous_time"] for change in audit["changes"]}, {123, 456, 789})
        self.assertTrue(all(change["action"] == "quarantine_unverified" for change in audit["changes"]))
        self.assertTrue(all(field["stored_time"] is None for field in audit["unverified_fields"]))
        selector.assert_not_called()
        self.client.block.assert_not_called()
        repeated = self.run_verifier(max_heights=0, quarantine_unverified=True)
        self.assertEqual(repeated["fields_quarantined"], 0)

    def test_quarantine_rolls_back_if_audit_cannot_be_written(self):
        row = self.add_node(registered_time=456)
        before = self.node(row["outpoint"])
        with mock.patch.object(tracker, "atomic_write_json", side_effect=OSError("audit unavailable")):
            with self.assertRaises(OSError):
                self.run_verifier(max_heights=0, quarantine_unverified=True)
        self.assertEqual(self.node(row["outpoint"]), before)

    def test_cli_quarantine_does_not_contact_core_or_blockbook(self):
        row = self.add_node(registered_time=456)
        with mock.patch.object(verifier, "ROOT", self.root), mock.patch.object(verifier, "ready_core_rpc", side_effect=AssertionError("Core lookup")), contextlib.redirect_stdout(io.StringIO()) as output:
            result = verifier.main([
                "--db", str(self.store.path), "--max-heights", "0",
                "--audit-dir", str(self.root / "cli-audits"),
                "--provider", "core", "--quarantine-unverified",
            ])
        self.assertEqual(result, 0)
        self.assertEqual(json.loads(output.getvalue())["fields_quarantined"], 2)
        self.assertIsNone(self.node(row["outpoint"])["collateral_time"])
        self.assertIsNone(self.node(row["outpoint"])["registered_time"])


if __name__ == "__main__":
    unittest.main()
