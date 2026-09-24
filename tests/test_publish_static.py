import contextlib
import copy
import io
import json
from pathlib import Path
import tempfile
import unittest
from unittest import mock

import syscoin_tracker as tracker


class PublishStaticTests(unittest.TestCase):
    def setUp(self):
        self.context = contextlib.ExitStack()
        self.addCleanup(self.context.close)
        self.root = Path(self.context.enter_context(tempfile.TemporaryDirectory()))
        self.context.enter_context(contextlib.chdir(self.root))
        self.store = tracker.Store(self.root / "tracker.sqlite")
        self.addCleanup(self.store.conn.close)
        self.output = self.root / "public"
        self.output.mkdir()
        self.csv_path = self.root / "network_masternodes.csv"
        self.csv_path.write_text("preserved snapshot\n", encoding="utf-8")
        self.wallet_meta = {"synced_at": "2026-05-15T03:24:48+00:00", "txs": 42}
        self.node_meta = {"synced_at": "2026-09-04T08:56:43+00:00", "current": 1, "chain_height": 100}
        self.store.set_meta("last_summary", self.wallet_meta)
        self.store.set_meta("last_masternode_sync", self.node_meta)
        self.store.set_meta("verified_competition_draw", {"winner": "historical-winner"})
        self.store.conn.execute(
            """INSERT INTO network_masternodes(
                outpoint, source_txid, source_vout, status, collateral_address,
                first_seen_at, last_seen_at
            ) VALUES('saved:0', 'saved', 0, 'ENABLED', 'saved-address', ?, ?)""",
            (self.node_meta["synced_at"], self.node_meta["synced_at"]),
        )
        self.client = mock.Mock(spec=tracker.BlockbookClient)
        self.rpc = mock.Mock(spec=tracker.SyscoinRpcClient)
        for target in (
            "urllib.request.urlopen",
            "syscoin_tracker.BlockbookClient.get_json",
            "syscoin_tracker.SyscoinRpcClient.call",
            "syscoin_tracker.SyscoinRpcClient.batch_call",
            "syscoin_tracker.EvmRpcClient.call",
            "syscoin_tracker.EvmRpcClient.batch_call",
            "syscoin_tracker.fetch_sysnode_masternode_list",
        ):
            self.context.enter_context(mock.patch(target, side_effect=AssertionError("unexpected network call")))
        self.sync = self.context.enter_context(mock.patch.object(
            tracker, "sync_static_snapshot", side_effect=AssertionError("unexpected sync")
        ))
        self.miners = self.context.enter_context(mock.patch.object(
            tracker, "miners_snapshot", side_effect=AssertionError("unexpected miners fetch")
        ))
        self.snapshot = {
            "type": "miners",
            "generated_at": "2026-05-29T22:46:17+00:00",
            "status": {"network_hashrate": "12 EH/s", "utxo_height": 100},
            "totals": {"pools": 1, "latest_utxo_height": 100},
            "pools": [{"name": "Saved Pool", "blocks_won": 123}],
            "addresses": [],
            "address_groups": [],
            "recent_blocks": [],
        }

    def publish(self, **options):
        return tracker.publish_static_snapshot(
            self.store, self.client, self.rpc,
            output_dir=self.output,
            address=tracker.DEFAULT_ADDRESS,
            page_size=1000,
            max_pages=None,
            from_height=None,
            since_time=None,
            since_label=None,
            refresh_seconds=0,
            csv_path=self.csv_path,
            next_hop_limit=8,
            node_spend_limit=12,
            **options,
        )

    def test_skip_sync_renders_cached_data_and_preserves_history(self):
        cache = self.output / tracker.MINERS_JSON
        cache_bytes = json.dumps(self.snapshot).encode("utf-8")
        cache.write_bytes(cache_bytes)
        history = {
            tracker.SN_COMP_HTML: b"verified competition page",
            "sn-comp-status.json": b'{"entries": 12}',
            "sn-comp-draw.json": b'{"winner": "historical-winner"}',
        }
        for name, data in history.items():
            (self.output / name).write_bytes(data)
        before = list(self.store.conn.iterdump())

        with mock.patch.object(tracker, "sn_comp_html", side_effect=AssertionError("competition was rebuilt")):
            stats = self.publish(skip_sync=True)

        self.sync.assert_not_called()
        self.miners.assert_not_called()
        self.assertEqual(self.client.mock_calls, [])
        self.assertEqual(self.rpc.mock_calls, [])
        self.assertEqual(list(self.store.conn.iterdump()), before)
        self.assertEqual(self.csv_path.read_text(), "preserved snapshot\n")
        self.assertEqual(cache.read_bytes(), cache_bytes)
        for name, data in history.items():
            self.assertEqual((self.output / name).read_bytes(), data)
        for name in ("index.html", "sentrynode.html", "top-wallets.html", "emissions.html", "miners.html"):
            self.assertIn("<!doctype html>", (self.output / name).read_text())
        miners_page = (self.output / tracker.MINERS_HTML).read_text()
        self.assertIn("Saved Pool", miners_page)
        self.assertIn("12 EH/s", miners_page)
        self.assertNotIn("Miner data unavailable", miners_page)
        status = json.loads((self.output / "status.json").read_text())
        self.assertTrue(stats["sync_skipped"])
        self.assertEqual(status["synced_at"], self.wallet_meta["synced_at"])
        self.assertEqual(status["wallet"], self.wallet_meta)
        self.assertEqual(status["masternodes"], self.node_meta)
        self.assertNotEqual(status["generated_at"], status["synced_at"])

    def test_missing_cache_renders_unavailable_without_inventing_freshness(self):
        self.store.conn.execute("DELETE FROM metadata")
        stats = self.publish(skip_sync=True)
        self.assertIsNone(stats["synced_at"])
        self.assertIsNone(stats["masternodes"])
        self.assertEqual(stats["wallet"], {})
        page = (self.output / tracker.MINERS_HTML).read_text()
        self.assertIn("Miner data unavailable: no valid saved snapshot.", page)
        self.assertNotIn("<span>Latest UTXO Block</span>", page)
        self.assertNotIn("Status API unavailable:", page)
        self.assertFalse((self.output / tracker.MINERS_JSON).exists())
        self.assertTrue((self.output / tracker.SN_COMP_HTML).exists())

    def test_recovery_republishes_competition_with_verification_notice(self):
        self.store.set_meta("sentry_time_verification", {"complete": False})
        page = self.output / tracker.SN_COMP_HTML
        page.write_text("old recovery snapshot")
        draw = self.output / "sn-comp-draw.json"
        draw.write_text('{"winner": "historical-winner"}')
        self.publish(skip_sync=True)
        self.assertIn("Competition totals are provisional.", page.read_text())
        self.assertEqual(json.loads(draw.read_text()), {"winner": "historical-winner"})

    def test_invalid_cache_is_unavailable_and_not_overwritten(self):
        bad_status = {**self.snapshot, "status": []}
        bad_rows = {**self.snapshot, "pools": [None]}
        bad_number = {**self.snapshot, "totals": {"pools": "not-a-number"}}
        bad_timestamp = {**self.snapshot, "generated_at": "not-a-date"}
        missing_rows = {key: value for key, value in self.snapshot.items() if key != "pools"}
        for value in (None, [], {}, bad_status, bad_rows, bad_number, bad_timestamp, missing_rows):
            with self.subTest(value=value):
                cache = self.output / tracker.MINERS_JSON
                content = json.dumps(value)
                cache.write_text(content, encoding="utf-8")
                self.publish(skip_sync=True)
                self.assertIn("Miner data unavailable", (self.output / tracker.MINERS_HTML).read_text())
                self.assertEqual(cache.read_text(), content)
        for content in (b"{unfinished", b"\xff"):
            with self.subTest(content=content):
                cache.write_bytes(content)
                self.publish(skip_sync=True)
                self.assertIn("Miner data unavailable", (self.output / tracker.MINERS_HTML).read_text())
                self.assertEqual(cache.read_bytes(), content)

    def test_render_does_not_replace_a_newer_worker_cache(self):
        cache = self.output / tracker.MINERS_JSON
        cache.write_text(json.dumps(self.snapshot))
        updated = {**self.snapshot, "generated_at": "2026-09-25T00:00:00+00:00"}
        render = tracker.saved_miners_html

        def worker_publishes_after_read(*args, **kwargs):
            page = render(*args, **kwargs)
            cache.write_text(json.dumps(updated))
            return page

        with mock.patch.object(tracker, "saved_miners_html", side_effect=worker_publishes_after_read):
            self.publish(skip_sync=True)
        self.assertEqual(json.loads(cache.read_text()), updated)

    def test_default_still_syncs_and_refreshes_miners(self):
        expected = {
            "synced_at": "2026-09-25T00:00:00+00:00",
            "wallet": {"seen": 2, "inserted": 1},
            "next_hop": {"found_spends": 0},
            "node_spends": {"found_spends": 0},
            "masternodes": {"current": 1},
        }
        self.sync.side_effect = None
        self.sync.return_value = expected
        self.miners.side_effect = None
        self.miners.return_value = copy.deepcopy(self.snapshot)
        (self.output / tracker.SN_COMP_HTML).write_text("old generated competition page")

        self.assertEqual(self.publish(), expected)

        self.sync.assert_called_once()
        self.miners.assert_called_once_with(self.store)
        self.assertEqual(json.loads((self.output / tracker.MINERS_JSON).read_text()), self.snapshot)
        self.assertIn("<!doctype html>", (self.output / tracker.SN_COMP_HTML).read_text())
        self.assertNotIn("sync_skipped", json.loads((self.output / "status.json").read_text()))

    def test_cli_skip_sync_does_not_resolve_date_or_build_rpc(self):
        with (
            mock.patch.object(tracker, "Store", return_value=self.store),
            mock.patch.object(tracker, "block_height_at_or_after", side_effect=AssertionError("date lookup")),
            mock.patch.object(tracker, "build_rpc_client", side_effect=AssertionError("RPC setup")),
            contextlib.redirect_stdout(io.StringIO()) as output,
        ):
            result = tracker.main([
                "--db", str(self.store.path),
                "publish-static", "--skip-sync",
                "--output-dir", str(self.output),
                "--csv", str(self.csv_path),
                "--since-date", "2026-05-01T00:00:00+00:00",
            ])
        self.assertEqual(result, 0)
        self.assertIn("wallet_seen=-", output.getvalue())
        self.assertTrue(json.loads((self.output / "status.json").read_text())["sync_skipped"])

    def test_parser_keeps_sync_as_the_default(self):
        args = tracker.build_parser().parse_args(["publish-static", "--output-dir", str(self.output)])
        self.assertFalse(args.skip_sync)


if __name__ == "__main__":
    unittest.main()
