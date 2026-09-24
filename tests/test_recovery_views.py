from pathlib import Path
import unittest
from unittest.mock import patch

import syscoin_tracker as tracker


class RecoveryViewTests(unittest.TestCase):
    def setUp(self):
        self.store = tracker.Store(Path(":memory:"))
        self.addCleanup(self.store.conn.close)
        self.enterContext(patch.object(tracker, "load_exchange_tags", return_value={}))
        self.enterContext(patch.object(tracker, "load_wallet_labels", return_value={}))
        self.enterContext(patch.object(tracker, "load_network_masternodes_csv", return_value=0))
        self.enterContext(patch.object(
            tracker.urllib.request, "urlopen", side_effect=AssertionError("Unexpected network call"),
        ))
        self.mock_emissions = self.enterContext(patch.object(
            tracker, "mock_emissions_snapshot", side_effect=AssertionError("Production requested fabricated data"),
        ))

    def progress(self, key, last=1988, chain=2000, **extra):
        self.store.set_meta(key, {
            "last_height": last, "chain_height": chain,
            "synced_at": "2026-09-25T00:00:00+00:00", **extra,
        })

    def seed_wallets(self, *, edges=True):
        self.store.conn.executemany(
            """INSERT INTO top_wallet_balances
               (address, balance_sats, utxo_count, last_seen_height, last_seen_time, updated_at)
               VALUES (?, ?, 1, 100, 1700000000, '2026-09-25T00:00:00+00:00')""",
            [("indexed-address-a", 200_000_000), ("indexed-address-b", 100_000_000)],
        )
        if edges:
            self.store.conn.execute(
                """INSERT INTO top_wallet_cluster_edges
                   (address_a, address_b, tx_count, first_seen_height, last_seen_height, updated_at)
                   VALUES ('indexed-address-a', 'indexed-address-b', 1, 100, 100, 'saved')""",
            )

    def seed_emissions(self, *, utxo=True, nevm=True):
        if utxo:
            self.store.conn.execute(
                """INSERT INTO emission_blocks (
                    height, block_hash, block_time, coinbase_txid, positive_outputs,
                    miner_sats, sentry_sats, governance_sats, fee_sats, payout_sats,
                    issued_sats, raw_outputs_json, created_at, updated_at
                ) VALUES (0, 'saved-utxo-block', 1700000000, 'saved-coinbase', 1,
                          100000000, 0, 0, 0, 100000000, 100000000, '[]', 'saved', 'saved')""",
            )
        if nevm:
            self.store.conn.execute(
                """INSERT INTO nevm_emission_blocks (
                    height, block_hash, block_time, static_reward_wei, miner_total_wei,
                    net_issued_wei, raw_summary_json, created_at, updated_at
                ) VALUES (0, 'saved-nevm-block', 1700000000, '1000', '1000', '1000', '{}', 'saved', 'saved')""",
            )

    def assert_banner(self, page, *, rankings=False):
        self.assertIn('<aside class="rebuilding-notice" role="status">', page)
        self.assertIn("Historical index rebuilding", page)
        self.assertIn("Figures are incomplete while historical blocks are indexed.", page)
        self.assertLess(page.index('<aside class="rebuilding-notice"'), page.index('<section class="section-panel metrics">'))
        if rankings:
            self.assertIn("Rankings are provisional, not current.", page)

    def test_empty_emissions_never_fabricate_data(self):
        snapshot = tracker.emissions_snapshot(self.store)
        self.assertFalse(snapshot["mock"])
        self.assertTrue(snapshot["index"]["rebuilding"])
        self.assertFalse(snapshot["index"]["complete"])
        self.assertFalse(snapshot["index"]["nevm_complete"])
        self.assertEqual(snapshot["totals"]["blocks"], 0)
        self.assertEqual(snapshot["totals"]["nevm"]["blocks"], 0)
        for key, value in snapshot["totals"].items():
            if key.endswith(("_sats", "_wei")):
                self.assertEqual(value, 0, key)
        for key, value in snapshot["totals"]["nevm"].items():
            if key.endswith("_wei"):
                self.assertEqual(value, 0, key)
        self.assertEqual(snapshot["latest_blocks"], [])
        self.assertEqual(snapshot["latest_nevm_blocks"], [])
        self.assertEqual(snapshot["periods"], {"weekly": [], "monthly": [], "yearly": []})
        page = tracker.emissions_html(self.store, refresh_seconds=0)
        self.assert_banner(page)
        self.assertIn("UTXO: not indexed", page)
        self.assertIn("NEVM: not indexed", page)
        self.assertIn("<b>Not indexed</b>", page)
        self.assertNotIn("Preview data", page)
        self.mock_emissions.assert_not_called()

    def test_empty_wallets_are_explicitly_rebuilding(self):
        snapshot = tracker.top_wallets_snapshot(self.store)
        for result in (snapshot, snapshot["estimated_clusters"]):
            self.assertEqual(result["wallets"], [])
            self.assertFalse(result["index"]["complete"])
            self.assertTrue(result["index"]["rebuilding"])
            self.assertEqual(result["totals"]["balance_sats"], 0)
        self.assert_banner(tracker.top_wallets_html(self.store, refresh_seconds=0), rankings=True)

    def test_wallet_completion_uses_safe_height_then_confirmations_then_legacy_tip(self):
        self.seed_wallets()
        cases = (
            ({"safe_height": 1988, "confirmations": 0}, 1988, True),
            ({"confirmations": 12}, 1988, True),
            ({"safe_height": 1989, "confirmations": 12}, 1989, False),
            ({}, 2000, False),
            ({"confirmations": "12"}, 1988, True),
        )
        for metadata, target, complete in cases:
            with self.subTest(metadata=metadata):
                for key in ("top_wallet_index", "top_wallet_cluster_index"):
                    self.progress(key, **metadata)
                snapshot = tracker.top_wallets_snapshot(self.store)
                for result in (snapshot, snapshot["estimated_clusters"]):
                    self.assertEqual(result["index"]["safe_height"], target)
                    self.assertEqual(result["index"]["complete"], complete)
                    self.assertEqual(result["index"]["rebuilding"], not complete)
                page = tracker.top_wallets_html(self.store, refresh_seconds=0)
                if complete:
                    self.assertNotIn('<aside class="rebuilding-notice"', page)
                    self.assertIn("Forensic cluster index complete to safe height", page)
                    self.assertIn("<span>Blocks Remaining</span><b>0</b>", page)
                else:
                    self.assert_banner(page, rankings=True)

    def test_incomplete_wallets_keep_only_real_rows_and_show_heights(self):
        self.seed_wallets()
        for key in ("top_wallet_index", "top_wallet_cluster_index"):
            self.progress(key, last=100, confirmations=12)
        snapshot = tracker.top_wallets_snapshot(self.store)
        self.assertEqual(snapshot["totals"]["balance_sats"], 300_000_000)
        self.assertEqual([row["balance_sats"] for row in snapshot["wallets"]], [200_000_000, 100_000_000])
        self.assertEqual(len(snapshot["estimated_clusters"]["wallets"]), 1)
        page = tracker.top_wallets_html(self.store, refresh_seconds=0)
        self.assert_banner(page, rankings=True)
        self.assertIn("Addresses: indexed height 100 / safe height 1,988 (chain height 2,000)", page)
        self.assertIn("<span>Blocks Remaining</span><b>1,888</b>", page)
        self.assertIn("Rebuilding: rankings are provisional", page)
        self.assertNotIn("Address balances are exact", page)

    def test_cluster_rankings_stay_provisional_if_address_index_is_behind(self):
        self.seed_wallets()
        self.progress("top_wallet_index", last=100, confirmations=12)
        self.progress("top_wallet_cluster_index", confirmations=12)
        cluster = tracker.top_wallet_cluster_snapshot(self.store)
        self.assertTrue(cluster["index"]["complete"])
        self.assertTrue(cluster["index"]["rebuilding"])
        page = tracker.top_wallets_html(self.store, refresh_seconds=0)
        self.assert_banner(page, rankings=True)
        self.assertNotIn("Forensic cluster index complete", page)

    def test_cluster_without_edges_uses_same_completion_semantics(self):
        self.seed_wallets(edges=False)
        for key in ("top_wallet_index", "top_wallet_cluster_index"):
            self.progress(key, confirmations=12)
        cluster = tracker.top_wallet_cluster_snapshot(self.store)
        self.assertTrue(cluster["index"]["complete"])
        self.assertFalse(cluster["index"]["rebuilding"])
        self.assertEqual(cluster["wallets"], [])

    def test_partial_emissions_keep_real_totals_and_explain_missing_history(self):
        self.seed_emissions(nevm=False)
        self.progress("emission_index", last=100, confirmations=12)
        self.progress("nevm_emission_index", last=-1, confirmations=12)
        snapshot = tracker.emissions_snapshot(self.store)
        self.assertFalse(snapshot["mock"])
        self.assertEqual(snapshot["totals"]["blocks"], 1)
        self.assertEqual(snapshot["totals"]["issued_sats"], 100_000_000)
        self.assertEqual(snapshot["totals"]["nevm"]["blocks"], 0)
        self.assertEqual(snapshot["totals"]["nevm"]["net_issued_wei"], 0)
        self.assertEqual(len(snapshot["latest_blocks"]), 1)
        self.assertEqual(snapshot["latest_nevm_blocks"], [])
        self.assertEqual(snapshot["periods"]["monthly"][0]["blocks"], 1)
        page = tracker.emissions_html(self.store, refresh_seconds=0)
        self.assert_banner(page)
        self.assertIn("UTXO: indexed height 100 / safe height 1,988", page)
        self.assertIn("NEVM: not indexed / safe height 1,988", page)
        self.assertIn("1,888 remaining", page)
        self.mock_emissions.assert_not_called()

    def test_nevm_only_emissions_do_not_invent_utxo_data(self):
        self.seed_emissions(utxo=False)
        snapshot = tracker.emissions_snapshot(self.store)
        self.assertEqual(snapshot["totals"]["blocks"], 0)
        self.assertEqual(snapshot["totals"]["issued_sats"], 0)
        self.assertEqual(snapshot["totals"]["net_issued_wei"], 1000)
        self.assertEqual(snapshot["latest_blocks"], [])
        self.assertEqual(len(snapshot["latest_nevm_blocks"]), 1)
        self.assert_banner(tracker.emissions_html(self.store, refresh_seconds=0))

    def test_emissions_caught_up_to_confirmed_targets_hide_rebuilding_notice(self):
        self.seed_emissions()
        for metadata in ({"safe_height": 1988}, {"confirmations": 12}):
            with self.subTest(metadata=metadata):
                for key in ("emission_index", "nevm_emission_index"):
                    self.progress(key, **metadata)
                snapshot = tracker.emissions_snapshot(self.store)
                self.assertTrue(snapshot["index"]["complete"])
                self.assertTrue(snapshot["index"]["nevm_complete"])
                self.assertFalse(snapshot["index"]["rebuilding"])
                self.assertEqual(snapshot["index"]["nevm_safe_height"], 1988)
                page = tracker.emissions_html(self.store, refresh_seconds=0)
                self.assertNotIn('<aside class="rebuilding-notice"', page)
                self.assertIn("Emission indexes complete to safe heights", page)
                self.assertEqual(page.count("0 remaining"), 2)

    def test_either_lagging_emission_index_keeps_notice(self):
        self.seed_emissions()
        for utxo_last, nevm_last in ((1987, 1988), (1988, 1987)):
            with self.subTest(utxo=utxo_last, nevm=nevm_last):
                self.progress("emission_index", last=utxo_last, confirmations=12)
                self.progress("nevm_emission_index", last=nevm_last, confirmations=12)
                snapshot = tracker.emissions_snapshot(self.store)
                self.assertTrue(snapshot["index"]["rebuilding"])
                self.assertEqual(snapshot["index"]["complete"], utxo_last == 1988)
                self.assertEqual(snapshot["index"]["nevm_complete"], nevm_last == 1988)
                self.assert_banner(tracker.emissions_html(self.store, refresh_seconds=0))

    def test_empty_tables_still_warn_even_if_metadata_claims_caught_up(self):
        for key in ("top_wallet_index", "top_wallet_cluster_index", "emission_index", "nevm_emission_index"):
            self.progress(key, confirmations=12)
        self.assert_banner(tracker.top_wallets_html(self.store, refresh_seconds=0), rankings=True)
        self.assert_banner(tracker.emissions_html(self.store, refresh_seconds=0))
        self.mock_emissions.assert_not_called()

    def test_height_zero_is_indexed_but_negative_or_unknown_targets_are_not_complete(self):
        self.seed_emissions()
        for key in ("emission_index", "nevm_emission_index"):
            self.progress(key, last=0, chain=12, confirmations=12)
        page = tracker.emissions_html(self.store, refresh_seconds=0)
        self.assertNotIn("Not indexed", page)
        self.assertNotIn('<aside class="rebuilding-notice"', page)
        for metadata in ({"last_height": -1, "chain_height": 0}, {"last_height": 100}, {"last_height": -1, "safe_height": -1}):
            with self.subTest(metadata=metadata):
                self.store.set_meta("emission_index", metadata)
                self.assertFalse(tracker.emissions_snapshot(self.store)["index"]["complete"])


if __name__ == "__main__":
    unittest.main()
