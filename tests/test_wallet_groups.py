from pathlib import Path
import unittest
from unittest.mock import patch

import syscoin_tracker as tracker


class WalletGroupsTests(unittest.TestCase):
    def setUp(self):
        self.store = tracker.Store(Path(":memory:"))
        self.addCleanup(self.store.conn.close)
        self.enterContext(patch.object(tracker, "load_exchange_tags", return_value={
            "mexc-hot": "MEXC", "mexc-cold": "MEXC", "gate-hot": "Gate",
        }))
        self.enterContext(patch.object(tracker, "load_wallet_labels", return_value={
            "private": {"name": "Recorded holder", "label": "Private Wallet"},
        }))
        self.enterContext(patch.object(tracker, "load_exchange_routes", return_value={
            "route-only": "MEXC",
        }))
        self.enterContext(patch.object(tracker, "load_network_masternodes_csv", return_value=0))
        self.enterContext(patch.object(
            tracker.urllib.request, "urlopen", side_effect=AssertionError("Unexpected network"),
        ))
        for key in ("top_wallet_index", "top_wallet_cluster_index"):
            self.store.set_meta(key, {
                "last_height": 100, "safe_height": 100, "chain_height": 112,
                "confirmations": 12, "history_start_height": 0, "history_complete": True,
                "synced_at": "2026-10-15T00:00:00+00:00",
            })
        self.node_number = 0

    def add_balances(self, **amounts):
        for address, amount in amounts.items():
            self.store.conn.execute(
                """INSERT INTO top_wallet_balances
                   (address, balance_sats, utxo_count, last_seen_height, last_seen_time, updated_at)
                   VALUES (?, ?, 1, 100, 1700000000, 'saved')""",
                (address, tracker.sys_to_sats(str(amount))),
            )

    def add_edges(self, *edges):
        for left, right in edges:
            tracker.save_top_wallet_cluster_edge(self.store, left, right, 100, 1700000000)

    def add_node(self, address, *, removed="", status="ENABLED"):
        self.node_number += 1
        txid = f"node-{self.node_number}"
        self.store.conn.execute(
            """INSERT INTO network_masternodes
               (outpoint, source_txid, source_vout, collateral_address, status,
                collateral_height, first_seen_at, last_seen_at, removed_at)
               VALUES (?, ?, 0, ?, ?, 50, 'saved', 'saved', ?)""",
            (f"{txid}:0", txid, address, status, removed),
        )

    def snapshot(self, limit=100):
        snapshot = tracker.estimated_wallet_groups_snapshot(self.store, limit=limit)
        self.assertTrue({"available", "index", "balance_index", "groups", "totals"} <= snapshot.keys())
        return snapshot

    def group_for(self, snapshot, address):
        matches = [
            group for group in snapshot["groups"]
            if address in {member["address"] for member in group["members"]}
        ]
        self.assertEqual(len(matches), 1, f"Expected exactly one group for {address}")
        return matches[0]

    def member_addresses(self, group):
        return {member["address"] for member in group["members"]}

    def update_index(self, key, **changes):
        progress = self.store.get_meta(key)
        progress.update(changes)
        self.store.set_meta(key, progress)

    def test_exchange_customer_boundary_keeps_identity_and_balances_separate(self):
        self.add_balances(**{"mexc-hot": 60, "customer": 7, "peer": 3})
        self.add_edges(("mexc-hot", "customer"), ("customer", "peer"))

        snapshot = self.snapshot()
        exchange = self.group_for(snapshot, "mexc-hot")
        customer = self.group_for(snapshot, "customer")
        self.assertEqual(self.member_addresses(exchange), {"mexc-hot"})
        self.assertEqual(exchange["balance_sats"], 60 * tracker.SATOSHI)
        self.assertEqual(exchange["name"], "MEXC")
        self.assertEqual(exchange["label"], "Exchange")
        self.assertFalse(exchange["is_estimate"])
        self.assertEqual(self.member_addresses(customer), {"customer", "peer"})
        self.assertEqual(customer["balance_sats"], 10 * tracker.SATOSHI)
        self.assertEqual(customer["label"], "Exchange-linked")
        self.assertNotEqual(customer["label"], "Exchange")
        self.assertTrue(customer["is_estimate"])
        self.assertNotEqual(customer["name"], "MEXC")
        self.assertEqual(customer["exchange_links"], ["MEXC"])
        self.assertTrue(all(member["label"] == "Unknown" for member in customer["members"]))

    def test_customers_do_not_merge_through_an_exchange_vertex(self):
        self.add_balances(**{"mexc-hot": 60, "alice": 7, "bob": 3})
        self.add_edges(("alice", "mexc-hot"), ("mexc-hot", "bob"))

        snapshot = self.snapshot()
        alice = self.group_for(snapshot, "alice")
        bob = self.group_for(snapshot, "bob")
        self.assertNotEqual(alice["id"], bob["id"])
        for group, address in ((alice, "alice"), (bob, "bob")):
            self.assertEqual(self.member_addresses(group), {address})
            self.assertEqual(group["label"], "Exchange-linked")
            self.assertNotEqual(group["label"], "Exchange")
            self.assertEqual(group["exchange_links"], ["MEXC"])

    def test_exchange_vertex_without_a_balance_still_blocks_customer_union(self):
        self.add_balances(alice=7, bob=3)
        self.add_edges(("alice", "mexc-hot"), ("mexc-hot", "bob"))

        snapshot = self.snapshot()
        self.assertEqual(self.member_addresses(self.group_for(snapshot, "alice")), {"alice"})
        self.assertEqual(self.member_addresses(self.group_for(snapshot, "bob")), {"bob"})

    def test_exchange_subtotal_combines_only_explicit_addresses_with_the_same_tag(self):
        self.add_balances(**{"mexc-hot": 11, "mexc-cold": 13, "gate-hot": 17, "route-only": 19})
        self.add_edges(("mexc-hot", "gate-hot"), ("mexc-hot", "route-only"))

        snapshot = self.snapshot()
        mexc = self.group_for(snapshot, "mexc-hot")
        self.assertEqual(self.member_addresses(mexc), {"mexc-hot", "mexc-cold"})
        self.assertEqual(mexc["balance_sats"], 24 * tracker.SATOSHI)
        self.assertEqual(mexc["address_count"], 2)
        self.assertFalse(mexc["is_estimate"])
        self.assertEqual(self.member_addresses(self.group_for(snapshot, "gate-hot")), {"gate-hot"})
        route = self.group_for(snapshot, "route-only")
        self.assertEqual(self.member_addresses(route), {"route-only"})
        self.assertEqual(route["label"], "Exchange-linked")
        self.assertNotEqual(route["label"], "Exchange")
        self.assertEqual(route["balance_sats"], 19 * tracker.SATOSHI)

    def test_custom_private_label_is_protected_and_never_propagates(self):
        self.add_balances(private=100000, alice=7, bob=3)
        self.add_edges(("alice", "private"), ("private", "bob"), ("private", "mexc-hot"))
        self.add_node("private")

        snapshot = self.snapshot()
        private = self.group_for(snapshot, "private")
        self.assertEqual(self.member_addresses(private), {"private"})
        self.assertEqual(private["name"], "Recorded holder")
        self.assertEqual(private["label"], "Private Wallet")
        self.assertFalse(private["is_estimate"])
        self.assertEqual(private["node_count"], 1)
        for address in ("alice", "bob"):
            group = self.group_for(snapshot, address)
            self.assertEqual(self.member_addresses(group), {address})
            self.assertEqual(group["label"], "Unknown")
            self.assertNotEqual(group["name"], "Recorded holder")
            self.assertEqual(group["node_count"], 0)
            self.assertEqual(group["exchange_links"], [])

    def test_unfunded_private_vertex_cannot_join_other_addresses(self):
        self.add_balances(alice=7, bob=3)
        self.add_edges(("alice", "private"), ("private", "bob"))

        snapshot = self.snapshot()
        self.assertEqual(self.member_addresses(self.group_for(snapshot, "alice")), {"alice"})
        self.assertEqual(self.member_addresses(self.group_for(snapshot, "bob")), {"bob"})

    def test_members_partition_balances_and_cycles_do_not_duplicate_totals(self):
        amounts = {"alpha": 100, "beta": 50, "gamma": 25, "isolated": 10, "mexc-hot": 7}
        self.add_balances(**amounts)
        self.add_edges(("alpha", "beta"), ("beta", "gamma"), ("gamma", "alpha"))
        self.add_edges(("alpha", "beta"))

        snapshot = self.snapshot()
        seen = []
        for group in snapshot["groups"]:
            with self.subTest(group=group["id"]):
                self.assertTrue({
                    "id", "name", "label", "is_estimate", "address", "address_count",
                    "linked_address_count", "balance_sats", "balance_sys", "node_count",
                    "exchange_links", "members",
                } <= group.keys())
                self.assertEqual(group["address_count"], len(group["members"]))
                self.assertGreaterEqual(group["linked_address_count"], group["address_count"])
                self.assertEqual(group["balance_sats"], sum(member["balance_sats"] for member in group["members"]))
                self.assertEqual(group["balance_sys"], tracker.sats_to_sys_string(group["balance_sats"]))
                for member in group["members"]:
                    self.assertTrue({"address", "balance_sats", "balance_sys", "name", "label", "node_count"} <= member.keys())
                    self.assertEqual(member["balance_sats"], amounts[member["address"]] * tracker.SATOSHI)
                    self.assertEqual(member["balance_sys"], tracker.sats_to_sys_string(member["balance_sats"]))
                    seen.append(member["address"])
        self.assertCountEqual(seen, list(amounts))
        self.assertEqual(sum(group["balance_sats"] for group in snapshot["groups"]), 192 * tracker.SATOSHI)

    def test_node_role_uses_current_collateral_without_adding_it_to_balances(self):
        self.add_balances(current=200007, funder=5, former=100000)
        self.add_edges(("current", "funder"))
        self.add_node("current")
        self.add_node("current", status="POSE_BANNED")
        self.add_node("current", removed="2026-10-14T00:00:00+00:00")
        self.add_node("former", removed="2026-10-14T00:00:00+00:00")

        snapshot = self.snapshot()
        group = self.group_for(snapshot, "current")
        self.assertEqual(group["label"], "Node-linked")
        self.assertEqual(group["node_count"], 2)
        self.assertTrue(group["is_estimate"])
        self.assertEqual(group["balance_sats"], 200012 * tracker.SATOSHI)
        members = {member["address"]: member for member in group["members"]}
        self.assertEqual(members["current"]["node_count"], 2)
        self.assertEqual(members["funder"]["node_count"], 0)
        former = self.group_for(snapshot, "former")
        self.assertEqual(former["label"], "Unknown")
        self.assertEqual(former["node_count"], 0)

    def test_multiple_exchange_links_are_deduplicated_observations_not_ownership(self):
        self.add_balances(**{"alice": 7, "bob": 3, "mexc-hot": 20, "mexc-cold": 30, "gate-hot": 40})
        self.add_edges(
            ("alice", "bob"), ("alice", "mexc-hot"),
            ("bob", "mexc-cold"), ("alice", "gate-hot"),
        )

        snapshot = self.snapshot()
        group = self.group_for(snapshot, "alice")
        self.assertCountEqual(group["exchange_links"], ["MEXC", "Gate"])
        self.assertEqual(self.member_addresses(group), {"alice", "bob"})
        self.assertEqual(group["label"], "Exchange-linked")
        self.assertNotEqual(group["label"], "Exchange")
        self.assertEqual(group["balance_sats"], 10 * tracker.SATOSHI)
        self.assertEqual(self.group_for(snapshot, "mexc-hot")["balance_sats"], 50 * tracker.SATOSHI)
        self.assertEqual(self.group_for(snapshot, "gate-hot")["balance_sats"], 40 * tracker.SATOSHI)

    def test_limit_is_applied_after_complete_group_aggregation(self):
        self.add_balances(outsider=50, **{f"member-{number}": 10 for number in range(6)})
        self.add_edges(*(("member-0", f"member-{number}") for number in range(1, 6)))

        snapshot = self.snapshot(limit=1)
        self.assertEqual(len(snapshot["groups"]), 1)
        group = snapshot["groups"][0]
        self.assertEqual(group["balance_sats"], 60 * tracker.SATOSHI)
        self.assertEqual(group["address_count"], 6)
        self.assertEqual(self.member_addresses(group), {f"member-{number}" for number in range(6)})

    def test_complete_indexes_use_confirmation_target_not_chain_tip(self):
        self.add_balances(alice=7, bob=3)
        self.add_edges(("alice", "bob"))

        snapshot = self.snapshot()
        self.assertTrue(snapshot["available"])
        self.assertFalse(snapshot["index"]["rebuilding"])
        self.assertFalse(snapshot["balance_index"]["rebuilding"])

    def test_incomplete_link_index_marks_groups_rebuilding_only(self):
        self.add_balances(alice=7, bob=3)
        self.add_edges(("alice", "bob"))
        self.update_index("top_wallet_cluster_index", last_height=20)

        snapshot = self.snapshot()
        self.assertTrue(snapshot["index"]["rebuilding"])
        self.assertFalse(snapshot["balance_index"]["rebuilding"])
        self.assertFalse(tracker.top_wallets_snapshot(self.store)["index"]["rebuilding"])

    def test_incomplete_balance_index_marks_groups_rebuilding(self):
        self.add_balances(alice=7, bob=3)
        self.add_edges(("alice", "bob"))
        self.update_index("top_wallet_index", last_height=20)

        snapshot = self.snapshot()
        self.assertTrue(snapshot["index"]["rebuilding"])
        self.assertTrue(snapshot["balance_index"]["rebuilding"])

    def test_partial_balance_history_marks_groups_rebuilding_even_at_target(self):
        self.add_balances(alice=7, bob=3)
        self.add_edges(("alice", "bob"))
        self.update_index("top_wallet_index", history_start_height=50, history_complete=False)

        snapshot = self.snapshot()
        self.assertTrue(snapshot["index"]["rebuilding"])
        self.assertTrue(snapshot["balance_index"]["rebuilding"])

    def test_locally_complete_links_behind_newer_balance_target_are_provisional(self):
        self.add_balances(alice=7, bob=3)
        self.add_edges(("alice", "bob"))
        self.update_index("top_wallet_index", last_height=200, safe_height=200, chain_height=212)
        self.assertTrue(tracker.snapshot_index_status(
            self.store.get_meta("top_wallet_cluster_index"),
        )["complete"])

        snapshot = self.snapshot()
        self.assertTrue(snapshot["index"]["rebuilding"])
        self.assertFalse(snapshot["balance_index"]["rebuilding"])
        exact_index = tracker.top_wallets_snapshot(self.store)["index"]
        self.assertFalse(exact_index["rebuilding"])
        self.assertEqual(exact_index["last_height"], 200)
        self.assertEqual(exact_index["safe_height"], 200)

    def test_locally_complete_balances_behind_newer_link_target_are_provisional(self):
        self.add_balances(alice=7, bob=3)
        self.add_edges(("alice", "bob"))
        self.update_index("top_wallet_cluster_index", last_height=200, safe_height=200, chain_height=212)
        self.assertTrue(tracker.snapshot_index_status(
            self.store.get_meta("top_wallet_index"),
        )["complete"])

        snapshot = self.snapshot()
        self.assertTrue(snapshot["index"]["rebuilding"])
        self.assertTrue(snapshot["balance_index"]["rebuilding"])
        exact_index = tracker.top_wallets_snapshot(self.store)["index"]
        self.assertFalse(exact_index["rebuilding"])
        self.assertEqual(exact_index["last_height"], 100)
        self.assertEqual(exact_index["safe_height"], 100)
        self.assertEqual(self.store.get_meta("top_wallet_index")["safe_height"], 100)

    def test_partial_link_history_marks_only_groups_rebuilding_even_at_target(self):
        self.add_balances(alice=7, bob=3)
        self.add_edges(("alice", "bob"))
        self.update_index("top_wallet_cluster_index", history_start_height=50, history_complete=False)
        self.assertTrue(tracker.snapshot_index_status(
            self.store.get_meta("top_wallet_cluster_index"),
        )["complete"])

        snapshot = self.snapshot()
        self.assertTrue(snapshot["index"]["rebuilding"])
        self.assertFalse(snapshot["balance_index"]["rebuilding"])
        self.assertFalse(tracker.top_wallets_snapshot(self.store)["index"]["rebuilding"])

    def test_snapshot_preserves_historical_graph_balances_and_disabled_legacy_export(self):
        self.add_balances(**{"alice": 7, "bob": 3, "mexc-hot": 60, "private": 5})
        self.add_edges(("mexc-hot", "alice"), ("alice", "bob"), ("bob", "private"))
        edges_before = [tuple(row) for row in self.store.conn.execute(
            "SELECT * FROM top_wallet_cluster_edges ORDER BY address_a, address_b",
        )]
        balances_before = [tuple(row) for row in self.store.conn.execute(
            "SELECT * FROM top_wallet_balances ORDER BY address",
        )]

        self.snapshot()

        self.assertEqual(edges_before, [tuple(row) for row in self.store.conn.execute(
            "SELECT * FROM top_wallet_cluster_edges ORDER BY address_a, address_b",
        )])
        self.assertEqual(balances_before, [tuple(row) for row in self.store.conn.execute(
            "SELECT * FROM top_wallet_balances ORDER BY address",
        )])
        legacy = tracker.top_wallet_cluster_snapshot(self.store)
        self.assertFalse(legacy["available"])
        self.assertEqual(legacy["wallets"], [])
        self.assertNotIn("known_exchange_sats", legacy["totals"])


if __name__ == "__main__":
    unittest.main()
