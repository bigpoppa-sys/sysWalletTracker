import contextlib
import copy
import io
import json
from pathlib import Path
import tempfile
import unittest
from unittest import mock

import syscoin_tracker as tracker


ADDRESS = "watched-address"
SOURCE_TXID = "source-tx"
SOURCE_VOUT = 3
FROM_HEIGHT = 2221358


def transaction(txid, *, spending=False):
    return {
        "txid": txid,
        "blockHeight": FROM_HEIGHT,
        "blockTime": 1776169800,
        "confirmations": 2,
        "value": "99",
        "valueIn": "100",
        "fees": "1",
        "vin": [{
            "txid": SOURCE_TXID if spending else "unrelated-source",
            "vout": SOURCE_VOUT,
            "addresses": [ADDRESS],
            "value": "100",
        }],
        "vout": [{"n": 0, "addresses": ["destination"], "value": "99"}],
    }


def response(page, txids, *, total=-1, capacity=2):
    return {
        "page": page,
        "totalPages": total,
        "itemsOnPage": capacity,
        "txs": 49359,
        "balance": "500",
        "totalReceived": "800",
        "totalSent": "300",
        "blockHeight": FROM_HEIGHT,
        "transactions": [transaction(txid) for txid in txids],
    }


class FakeBlockbook:
    def __init__(self, pages):
        self.pages = pages
        self.calls = []

    def address(self, address, **params):
        self.calls.append((address, params))
        index = len(self.calls) - 1
        if index >= len(self.pages):
            raise AssertionError(f"Unexpected extra page request: {params}")
        return copy.deepcopy(self.pages[index])


class BlockbookPaginationTests(unittest.TestCase):
    def setUp(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.store = tracker.Store(Path(directory.name) / "tracker.sqlite")
        self.addCleanup(self.store.conn.close)
        network = mock.patch(
            "urllib.request.urlopen", side_effect=AssertionError("unexpected network call")
        )
        network.start()
        self.addCleanup(network.stop)

    def call(self, operation, client, *, page_size=2, max_pages=None, quiet=True):
        options = {
            "page_size": page_size,
            "max_pages": max_pages,
            "from_height": FROM_HEIGHT,
        }
        if operation == "sync":
            return tracker.sync_address(
                self.store, client, ADDRESS, watched={ADDRESS}, quiet=quiet, **options
            )
        return tracker.find_spending_tx(client, ADDRESS, SOURCE_TXID, SOURCE_VOUT, **options)

    def assert_requests(self, client, count, page_size=2):
        self.assertEqual(client.calls, [
            (ADDRESS, {
                "page": page,
                "page_size": page_size,
                "details": "txs",
                "from_height": FROM_HEIGHT,
            })
            for page in range(1, count + 1)
        ])

    def assert_traversal(self, pages, count, seen, **options):
        for operation in ("sync", "find"):
            with self.subTest(operation=operation):
                client = FakeBlockbook(pages)
                result = self.call(operation, client, **options)
                self.assert_requests(client, count, options.get("page_size", 2))
                if operation == "sync":
                    self.assertEqual(result["seen"], seen)
                    self.assertEqual(result["outbound"], seen)
                else:
                    self.assertIsNone(result)

    def test_unknown_total_multiple_full_pages_then_short(self):
        self.assert_traversal([
            response(1, ["a", "b"]), response(2, ["c", "d"]), response(3, ["e"]),
        ], 3, 5)
        self.assertEqual(self.store.get_meta("last_summary")["txs"], 49359)

    def test_unknown_total_full_page_then_empty(self):
        self.assert_traversal([response(1, ["a", "b"]), response(2, [])], 2, 2)

    def test_unknown_total_with_reported_live_page_size(self):
        self.assert_traversal([
            response(1, [f"tx-{i}" for i in range(1000)], capacity=1000),
            response(2, [f"tx-{i}" for i in range(1000, 2000)], capacity=1000),
            response(3, ["last-tx"], capacity=1000),
        ], 3, 2001, page_size=1000)
        self.assertEqual(self.store.conn.execute("SELECT COUNT(*) FROM transactions").fetchone()[0], 2001)

    def test_unknown_total_short_first_page(self):
        self.assert_traversal([response(1, ["a"])], 1, 1)

    def test_unknown_total_empty_first_page(self):
        self.assert_traversal([response(1, [])], 1, 0)

    def test_unknown_total_missing_transactions_is_empty(self):
        page = response(1, [])
        del page["transactions"]
        self.assert_traversal([page], 1, 0)

    def test_unknown_total_uses_server_page_capacity(self):
        self.assert_traversal([
            response(1, ["a", "b"]), response(2, ["c"]),
        ], 2, 3, page_size=1000)

    def test_unknown_total_falls_back_to_requested_page_size(self):
        for capacity in (None, 0, -1, "missing"):
            with self.subTest(capacity=capacity):
                pages = [response(1, ["a", "b"]), response(2, ["c"])]
                for page in pages:
                    if capacity == "missing":
                        del page["itemsOnPage"]
                    else:
                        page["itemsOnPage"] = capacity
                self.assert_traversal(pages, 2, 3)

    def test_unknown_total_without_response_page_number(self):
        pages = [response(1, ["a", "b"]), response(2, ["c"])]
        for page in pages:
            del page["page"]
        self.assert_traversal(pages, 2, 3)

    def test_zero_or_one_total_pages_fetches_first_page_only(self):
        for total in (0, 1, None, "missing"):
            for txids in ([], ["a", "b"]):
                with self.subTest(total=total, txids=txids):
                    page = response(1, txids, total=total)
                    if total == "missing":
                        del page["totalPages"]
                    self.assert_traversal([page], 1, len(txids))

    def test_known_total_does_not_stop_on_short_or_empty_intermediate_page(self):
        for middle in ([], ["c"]):
            with self.subTest(middle=middle):
                self.assert_traversal([
                    response(1, ["a", "b"], total=3),
                    response(2, middle, total=3),
                    response(3, ["d", "e"], total=3),
                ], 3, 4 + len(middle))

    def test_max_pages_bounds_known_and_unknown_totals(self):
        for total in (-1, 5):
            for limit in (1, 2):
                with self.subTest(total=total, limit=limit):
                    pages = [response(p, [f"{p}-a", f"{p}-b"], total=total)
                             for p in range(1, limit + 1)]
                    self.assert_traversal(pages, limit, limit * 2, max_pages=limit)

    def test_zero_max_pages_does_not_request_or_change_cached_data(self):
        self.store.set_meta("last_summary", {"synced_at": "saved", "txs": 42})
        before = list(self.store.conn.iterdump())
        self.assert_traversal([], 0, 0, max_pages=0)
        self.assertEqual(list(self.store.conn.iterdump()), before)

    def test_known_total_still_bounds_a_larger_max_pages(self):
        self.assert_traversal([response(1, ["a", "b"], total=1)], 1, 2, max_pages=5)

    def test_total_can_become_known_after_unknown_page(self):
        self.assert_traversal([
            response(1, ["a", "b"]), response(2, ["c", "d"], total=2),
        ], 2, 4)

    def test_repeated_pages_raise_instead_of_silently_finishing(self):
        for total in (-1, 5):
            for operation in ("sync", "find"):
                with self.subTest(total=total, operation=operation):
                    client = FakeBlockbook([
                        response(1, ["a", "b"], total=total),
                        response(2, ["b", "a"], total=total),
                    ])
                    with self.assertRaisesRegex(RuntimeError, "repeated.*page 2"):
                        self.call(operation, client)
                    self.assert_requests(client, 2)

    def test_nonconsecutive_page_cycle_raises(self):
        for operation in ("sync", "find"):
            with self.subTest(operation=operation):
                client = FakeBlockbook([
                    response(1, ["a", "b"]), response(2, ["c", "d"]),
                    response(3, ["a", "b"]),
                ])
                with self.assertRaisesRegex(RuntimeError, "repeated.*page 3"):
                    self.call(operation, client)
                self.assert_requests(client, 3)

    def test_partial_page_overlap_is_not_a_repeated_page(self):
        self.assert_traversal([
            response(1, ["a", "b"]), response(2, ["b", "c"]), response(3, []),
        ], 3, 4)
        self.assertEqual(self.store.conn.execute("SELECT COUNT(*) FROM transactions").fetchone()[0], 3)

    def test_response_page_mismatch_raises_before_processing(self):
        for total in (-1, 3):
            for operation in ("sync", "find"):
                with self.subTest(total=total, operation=operation):
                    wrong = response(1, ["must-not-be-saved"], total=total)
                    wrong["transactions"][0] = transaction("must-not-be-saved", spending=True)
                    client = FakeBlockbook([response(1, ["a", "b"], total=total), wrong])
                    with self.assertRaisesRegex(RuntimeError, "requested page 2.*returned page 1"):
                        self.call(operation, client)
                    self.assert_requests(client, 2)
        self.assertIsNone(self.store.conn.execute(
            "SELECT 1 FROM transactions WHERE txid = 'must-not-be-saved'"
        ).fetchone())

    def test_empty_mismatched_page_also_raises(self):
        for operation in ("sync", "find"):
            with self.subTest(operation=operation):
                client = FakeBlockbook([response(1, ["a", "b"]), response(1, [])])
                with self.assertRaisesRegex(RuntimeError, "requested page 2.*returned page 1"):
                    self.call(operation, client)
                self.assert_requests(client, 2)

    def test_find_spend_on_last_short_page_and_do_not_fetch_more(self):
        pages = [response(1, [SOURCE_TXID, "a"]), response(2, ["b", "c"]), response(3, ["spend"])]
        pages[0]["transactions"][0] = transaction(SOURCE_TXID, spending=True)
        pages[1]["transactions"][0] = transaction("b", spending=True)
        pages[1]["transactions"][0]["vin"][0]["vout"] = SOURCE_VOUT + 1
        spend = transaction("spend", spending=True)
        pages[2]["transactions"][0] = spend
        client = FakeBlockbook(pages)
        self.assertEqual(self.call("find", client), spend)
        self.assert_requests(client, 3)

    def test_find_spend_on_first_full_page_returns_immediately(self):
        page = response(1, ["spend", "a"])
        spend = transaction("spend", spending=True)
        page["transactions"][0] = spend
        client = FakeBlockbook([page])
        self.assertEqual(self.call("find", client), spend)
        self.assert_requests(client, 1)

    def test_sync_preserves_cache_and_saves_all_movements_and_outputs(self):
        cached = transaction("cached-only")
        existing = transaction("a")
        existing["confirmations"] = 1
        self.store.save_tx(cached)
        self.store.save_tx(existing)
        self.store.set_meta("verified_competition_draw", {"winner": "saved-winner"})
        cached_row = dict(self.store.conn.execute(
            "SELECT * FROM transactions WHERE txid = 'cached-only'"
        ).fetchone())
        first_seen = self.store.conn.execute(
            "SELECT first_seen_at FROM transactions WHERE txid = 'a'"
        ).fetchone()[0]
        client = FakeBlockbook([
            response(1, ["a", "b"]), response(2, ["c", "d"]), response(3, ["e"]),
        ])
        self.assertEqual(self.call("sync", client), {"seen": 5, "inserted": 4, "outbound": 5})
        self.assertEqual(dict(self.store.conn.execute(
            "SELECT * FROM transactions WHERE txid = 'cached-only'"
        ).fetchone()), cached_row)
        refreshed = self.store.conn.execute("SELECT * FROM transactions WHERE txid = 'a'").fetchone()
        self.assertEqual(refreshed["first_seen_at"], first_seen)
        self.assertEqual(json.loads(refreshed["raw_json"])["confirmations"], 2)
        for table, count in (("transactions", 6), ("movements", 5), ("tracked_outputs", 5)):
            self.assertEqual(self.store.conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0], count)
        self.assertEqual(self.store.get_meta("verified_competition_draw"), {"winner": "saved-winner"})
        summary = self.store.get_meta("last_summary")
        self.assertEqual(summary["address"], ADDRESS)
        self.assertEqual(summary["balance_sats"], 500)
        self.assertEqual(summary["total_received_sats"], 800)
        self.assertEqual(summary["total_sent_sats"], 300)
        self.assertEqual(summary["block_height"], FROM_HEIGHT)

    def test_failed_pagination_keeps_previous_summary_and_successful_pages(self):
        saved_summary = {"synced_at": "saved", "txs": 42}
        self.store.set_meta("last_summary", saved_summary)
        client = FakeBlockbook([response(1, ["a", "b"]), response(2, ["a", "b"])])
        with self.assertRaises(RuntimeError):
            self.call("sync", client)
        self.assertEqual(self.store.get_meta("last_summary"), saved_summary)
        self.assertEqual(self.store.conn.execute("SELECT COUNT(*) FROM transactions").fetchone()[0], 2)

    def test_unknown_total_progress_does_not_show_negative_page_count(self):
        client = FakeBlockbook([response(1, ["a", "b"]), response(2, ["c"])])
        with contextlib.redirect_stderr(io.StringIO()) as output:
            self.call("sync", client, quiet=False)
        self.assertIn("Fetched page 2/", output.getvalue())
        self.assertNotIn("/-1", output.getvalue())


if __name__ == "__main__":
    unittest.main()
