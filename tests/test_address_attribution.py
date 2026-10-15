from pathlib import Path
import contextlib
import io
import json
import tempfile
import unittest
from unittest.mock import patch

import syscoin_tracker as tracker


class AddressAttributionTests(unittest.TestCase):
    def setUp(self):
        self.store = tracker.Store(Path(":memory:"))
        self.addCleanup(self.store.conn.close)
        self.enterContext(patch.object(tracker, "load_exchange_tags", return_value={"exchange": "MEXC"}))
        self.enterContext(patch.object(tracker, "load_wallet_labels", return_value={
            "private": {"name": "Recorded name", "label": "Private Wallet"},
        }))
        self.enterContext(patch.object(tracker, "load_network_masternodes_csv", return_value=0))
        self.enterContext(patch.object(tracker, "load_exchange_hot_wallets", return_value=[
            {"address": "exchange", "label": "MEXC", "note": "Recorded address"},
        ]))
        self.enterContext(patch.object(tracker, "load_exchange_cold_wallets", return_value=[]))
        self.enterContext(patch.object(tracker, "load_exchange_routes", return_value={"customer": "MEXC"}))
        self.enterContext(patch.object(tracker.urllib.request, "urlopen", side_effect=AssertionError("Unexpected network")))
        self.store.set_meta("top_wallet_index", {
            "last_height": 100, "safe_height": 100, "chain_height": 112,
            "history_start_height": 0, "history_complete": True,
            "synced_at": "2026-10-15T00:00:00+00:00",
        })
        self.store.set_meta("last_summary", {"address": "exchange", "balance_sats": 999 * 100_000_000})
        self.store.set_meta("exchange_hot_wallet_balances", {
            "exchange": {"balance_sats": 888 * 100_000_000},
        })
        for address, amount in (("exchange", 60), ("customer", 7), ("private", 5), ("other", 3)):
            self.store.conn.execute(
                "INSERT INTO top_wallet_balances VALUES (?, ?, 1, 100, 1700000000, 'saved')",
                (address, amount * 100_000_000),
            )
        for a, b in (("exchange", "customer"), ("customer", "other"), ("other", "private")):
            tracker.save_top_wallet_cluster_edge(self.store, a, b, 100, 1700000000)

    def seed_node(self, address, n, removed=""):
        self.store.conn.execute(
            """INSERT INTO network_masternodes
               (outpoint, source_txid, source_vout, collateral_address, status,
                collateral_height, first_seen_at, last_seen_at, removed_at)
               VALUES (?, ?, 0, ?, 'ENABLED', 50, 'saved', 'saved', ?)""",
            (f"node-{n}:0", f"node-{n}", address, removed),
        )

    def seed_output(self, address, amount, depth, path, txid):
        self.store.conn.execute(
            """INSERT INTO tracked_outputs
               (source_txid, source_vout, depth, address, value_sats, attributed_sats,
                block_height, block_time, spent, path, created_at, updated_at)
               VALUES (?, 0, ?, ?, ?, ?, 100, 1700000000, 1, ?, 'saved', 'saved')""",
            (txid, depth, address, amount * 100_000_000, amount * 100_000_000, path),
        )

    def test_legacy_graph_cannot_propagate_exchange_or_private_identity(self):
        snapshot = tracker.top_wallets_snapshot(self.store)
        self.assertNotIn("estimated_clusters", snapshot)
        rows = {row["address"]: row for row in snapshot["wallets"]}
        self.assertEqual(rows["exchange"]["balance_sats"], 60 * 100_000_000)
        self.assertEqual(rows["private"]["balance_sats"], 5 * 100_000_000)
        for address in ("customer", "other"):
            self.assertEqual(rows[address]["label"], "Unknown")
        self.assertTrue(all(row["address_count"] == 1 for row in rows.values()))

    def test_legacy_cluster_export_does_not_publish_ownership_totals(self):
        snapshot = tracker.top_wallet_cluster_snapshot(self.store)
        self.assertFalse(snapshot["available"])
        self.assertEqual(snapshot["wallets"], [])
        for key in ("known_exchange_sats", "private_unknown_sats", "clusters", "estimated_private_holders"):
            self.assertNotIn(key, snapshot["totals"])
        self.assertEqual(self.store.conn.execute("SELECT COUNT(*) FROM top_wallet_cluster_edges").fetchone()[0], 3)

    def test_group_progress_does_not_change_exact_address_status_or_claim_holders(self):
        self.seed_node("customer", 1)
        self.seed_node("other", 2)
        page = tracker.top_wallets_html(self.store, refresh_seconds=0)
        for phrase in ("Estimated Holders", "Estimated Address Clusters", "Net Worth", "Sentry Operator", "Known Exchange SYS"):
            self.assertNotIn(phrase, page)
        self.assertIn("Grouped Wallets", page)
        self.assertIn("Links: not indexed", page)
        exact_section = page.split("<h2>Largest Addresses</h2>", 1)[1].split("</section>", 1)[0]
        self.assertNotIn("provisional", exact_section)
        self.assertNotIn('class="rebuilding-notice"', exact_section)
        self.assertIn("Largest Addresses", page)
        self.assertIn("Sentry Collateral Addresses", page)

    def test_sentry_counts_are_per_collateral_address_not_funding_group(self):
        self.seed_node("customer", 1)
        self.seed_node("customer", 2)
        self.seed_node("other", 3)
        self.seed_node("other", 4, removed="2026-10-14T00:00:00+00:00")
        rows = {row["address"]: row for row in tracker.sentry_collateral_address_rows(self.store)}
        self.assertEqual(rows["customer"]["nodes"], 2)
        self.assertEqual(rows["other"]["nodes"], 1)
        self.assertEqual(rows["customer"]["balance_sats"], 7 * 100_000_000)
        self.assertNotIn("exchange", rows)

    def test_complete_index_is_the_same_address_balance_on_both_pages(self):
        result = tracker.address_balance_for_display(self.store, "exchange")
        self.assertEqual(result["balance_sats"], 60 * 100_000_000)
        self.assertEqual(result["source"], "address_index")
        page = tracker.dashboard_html(self.store, refresh_seconds=0)
        self.assertNotIn("888", page)
        self.assertNotIn("999", page.split("<body>", 1)[1])
        self.assertIn("Address balance:", page)
        self.assertNotIn("Final balance:", page)

    def test_missing_address_in_complete_index_is_zero_not_old_cache(self):
        self.store.conn.execute("DELETE FROM top_wallet_balances WHERE address = 'exchange'")
        result = tracker.address_balance_for_display(self.store, "exchange")
        self.assertEqual(result["balance_sats"], 0)
        self.assertEqual(result["source"], "address_index")

    def test_incomplete_index_uses_named_cache_source_not_partial_balance(self):
        self.store.set_meta("top_wallet_index", {"last_height": 20, "safe_height": 100})
        result = tracker.address_balance_for_display(self.store, "exchange")
        self.assertEqual(result["balance_sats"], 888 * 100_000_000)
        self.assertEqual(result["source"], "blockbook_cache")
        self.assertIsNone(tracker.address_balance_for_display(self.store, "other"))

    def test_route_and_downstream_activity_do_not_label_customer_as_exchange(self):
        self.seed_output("customer", 1000, 1, "origin:0", "origin")
        self.seed_output("exchange", 1, 2, "origin:0 > next:0", "next")
        page = tracker.dashboard_html(self.store, refresh_seconds=0)
        row = next(row for row in page.split("<tr>") if "title='customer'" in row)
        self.assertNotIn("MEXC", row.split("</tr>", 1)[0])
        self.assertNotIn("Ex-like", page)
        self.assertNotIn("From Binance:", page)

    def test_direct_flow_is_counted_by_recipient_not_shared_exchange_name(self):
        self.seed_output("exchange", 2, 1, "direct:0", "direct")
        self.seed_output("another-exchange-address", 8, 1, "other:0", "other")
        with patch.object(tracker, "load_exchange_tags", return_value={
            "exchange": "MEXC", "another-exchange-address": "MEXC",
        }):
            page = tracker.dashboard_html(self.store, refresh_seconds=0)
        card = page.split("<article class='wallet-card'>", 1)[1].split("</article>", 1)[0]
        self.assertIn("Direct receipts (tracked):", card)
        self.assertIn("title='2 SYS'", card)
        self.assertNotIn("title='10 SYS'", card)

    def test_collateral_funding_no_longer_creates_ownership_edge(self):
        self.store.conn.execute("DELETE FROM top_wallet_cluster_edges")
        self.store.conn.execute(
            "INSERT INTO top_wallet_cluster_utxos VALUES ('funding', 0, 'exchange', ?, 99, 1700000000)",
            (tracker.SENTRY_COLLATERAL_SATS,),
        )
        tracker.process_top_wallet_cluster_block(self.store, {
            "height": 100, "time": 1700000000,
            "tx": [{"txid": "collateral", "vin": [{"txid": "funding", "vout": 0}],
                    "vout": [{"n": 0, "value": 100000, "scriptPubKey": {"address": "customer"}}]}],
        }, {("collateral", 0): "customer"})
        self.assertEqual(self.store.conn.execute("SELECT COUNT(*) FROM top_wallet_cluster_edges").fetchone()[0], 0)
        self.assertEqual(self.store.conn.execute("SELECT COUNT(*) FROM top_wallet_cluster_utxos").fetchone()[0], 1)

    def test_sentry_destination_label_does_not_use_onward_exchange_route(self):
        self.seed_node("other", 1, removed="2026-10-14T00:00:00+00:00")
        self.store.conn.execute("UPDATE network_masternodes SET moved_to_address='customer'")
        page = tracker.masternodes_html(self.store, refresh_seconds=0)
        self.assertIn("customer", page)
        self.assertNotIn("MEXC", page)

    def test_empty_index_cannot_override_a_real_cached_balance_with_zero(self):
        self.store.conn.execute("DELETE FROM top_wallet_balances")
        balance = tracker.address_balance_for_display(self.store, "exchange")
        self.assertEqual(balance["balance_sats"], 888 * 100_000_000)
        self.assertEqual(balance["source"], "blockbook_cache")

    def test_candidate_cards_do_not_appear_even_with_cached_balances(self):
        with patch.object(tracker, "load_exchange_hot_wallets", return_value=[
            {"address": "customer", "label": "Guessed Exchange", "note": "candidate"},
        ]):
            page = tracker.dashboard_html(self.store, refresh_seconds=0)
        self.assertNotIn("Guessed Exchange", page)
        self.assertNotIn("candidate", page)

    def test_cluster_cli_exports_diagnostics_without_removed_field_error(self):
        stats = {key: 0 for key in (
            "blocks", "txs", "outputs", "spends", "missing_spends", "groups",
            "sentry_groups", "edges", "sentry_edges", "sentry_collateral_outpoints",
            "last_height", "chain_height",
        )}
        with (
            tempfile.TemporaryDirectory() as tmp,
            patch.object(tracker, "Store", return_value=self.store),
            patch.object(tracker, "build_rpc_client", return_value=object()),
            patch.object(tracker, "sync_top_wallet_cluster_index", return_value=stats),
            contextlib.redirect_stdout(io.StringIO()) as output,
        ):
            path = Path(tmp) / "clusters.json"
            result = tracker.main(["sync-top-wallet-clusters", "--json", str(path)])
            self.assertEqual(result, 0)
            self.assertFalse(json.loads(path.read_text())["available"])
            self.assertIn("ownership_attribution=disabled", output.getvalue())

    def test_legacy_missing_address_uses_cache_not_assumed_zero(self):
        self.store.set_meta("top_wallet_index", {"last_height": 100, "safe_height": 100})
        self.store.conn.execute("DELETE FROM top_wallet_balances WHERE address='exchange'")
        balance = tracker.address_balance_for_display(self.store, "exchange")
        self.assertEqual(balance["balance_sats"], 888 * 100_000_000)
        self.assertEqual(balance["source"], "blockbook_cache")

    def test_partial_history_never_replaces_cache_or_claims_current_rankings(self):
        self.store.set_meta("top_wallet_index", {
            "last_height": 100, "safe_height": 100, "history_start_height": 50,
            "history_complete": False,
        })
        self.assertEqual(tracker.address_balance_for_display(self.store, "exchange")["source"], "blockbook_cache")
        self.assertTrue(tracker.top_wallets_snapshot(self.store)["index"]["rebuilding"])


class AddressIndexCoverageTests(unittest.TestCase):
    def setUp(self):
        self.store = tracker.Store(Path(":memory:"))
        self.addCleanup(self.store.conn.close)

    class Rpc:
        def call(self, method):
            return 112

        def batch_call(self, calls):
            if calls[0][0] == "getblockhash":
                return [str(params[0]) for _, params in calls]
            return [{"height": int(params[0]), "previousblockhash": str(int(params[0]) - 1), "tx": []}
                    for _, params in calls]

    def test_sync_keeps_confirmation_target_and_partial_history_marker(self):
        tracker.sync_top_wallet_index(self.store, self.Rpc(), start_height=100, confirmations=12)
        progress = tracker.top_wallet_progress(self.store)
        self.assertEqual(progress["safe_height"], 100)
        self.assertEqual(progress["confirmations"], 12)
        self.assertEqual(progress["history_start_height"], 100)
        self.assertFalse(progress["history_complete"])
        self.assertTrue(tracker.snapshot_index_status(progress)["complete"])
        tracker.sync_top_wallet_index(self.store, self.Rpc(), confirmations=12)
        self.assertFalse(tracker.top_wallet_progress(self.store)["history_complete"])

    def test_genesis_scan_retains_coverage_across_batches(self):
        tracker.sync_top_wallet_index(self.store, self.Rpc(), max_blocks=1, confirmations=12)
        tracker.sync_top_wallet_index(self.store, self.Rpc(), max_blocks=1, confirmations=12)
        progress = tracker.top_wallet_progress(self.store)
        self.assertTrue(progress["history_complete"])
        self.assertEqual(progress["history_start_height"], 0)
        self.assertEqual(progress["last_height"], 1)

    def test_jumping_over_blocks_marks_existing_index_as_partial(self):
        tracker.sync_top_wallet_index(self.store, self.Rpc(), max_blocks=1, confirmations=12)
        with self.assertRaisesRegex(ValueError, "skip"):
            tracker.sync_top_wallet_index(self.store, self.Rpc(), start_height=100, confirmations=12)

    def test_reset_retains_requested_start_height(self):
        tracker.reset_top_wallet_index(self.store, start_height=100)
        progress = tracker.top_wallet_progress(self.store)
        self.assertEqual(progress["history_start_height"], 100)
        self.assertFalse(progress["history_complete"])


class LabelFileTests(unittest.TestCase):
    def test_only_explicitly_identified_labels_are_loaded(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "tags.csv"
            path.write_text("address,label,status\nknown,MEXC,identified\ncandidate,MEXC,unverified\nlegacy,Gate,\n")
            self.assertEqual(tracker.load_exchange_tags(path), {"known": "MEXC"})

    def test_custom_names_also_require_identified_status(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "labels.csv"
            path.write_text("address,name,label,status\nknown,Alice,Private Wallet,identified\nguess,Bob,Private Wallet,unverified\n")
            self.assertEqual(set(tracker.load_wallet_labels(path)), {"known"})

    def test_repository_candidate_labels_are_not_published_as_known(self):
        labels = tracker.load_exchange_tags()
        self.assertIn("sys1qeyt05mxwlud4q66u8u2jtyq4markks4ru5ldk8", labels)
        self.assertNotIn("Ex(Unknown)", labels.values())
        self.assertNotIn("sys1q3ec7z8up0phw83t0rund94h0d577lfls9eel38", labels)
        self.assertNotIn("sys1q796zhdepu0f0guary2qvuhdyczxfvk2mw3k2ym", labels)


if __name__ == "__main__":
    unittest.main()
