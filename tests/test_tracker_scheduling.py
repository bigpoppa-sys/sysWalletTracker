from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
import fcntl
import json
from pathlib import Path
import sqlite3
import tempfile
import threading
import unittest
from unittest import mock
import urllib.error

from scripts import tracker_worker as worker


class Clock:
    def __init__(self):
        self.elapsed = 0
        self.sleeps = []

    def monotonic(self):
        return self.elapsed

    def time(self):
        return 1_800_000_000 + self.elapsed

    def sleep(self, seconds):
        self.sleeps.append(seconds)
        self.elapsed += seconds


class SchedulingTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.data = Path(self.temp.name) / "data"
        self.data.mkdir()
        self.public = Path(self.temp.name) / "public"
        self.db = self.data / "tracker.sqlite"
        self.db.write_bytes(b"database fixture")
        for name, value in (
            ("DATA", self.data), ("PUBLIC", self.public), ("DB", self.db),
            ("LOCK_WAIT_SECONDS", 2), ("LOCK_POLL_SECONDS", 0.005),
        ):
            self.enterContext(mock.patch.object(worker, name, value))
        self.enterContext(mock.patch.object(
            worker.tracker.urllib.request, "urlopen", side_effect=AssertionError("unexpected network"),
        ))
        self.output = self.enterContext(mock.patch("builtins.print"))
        self.disk = self.enterContext(mock.patch.object(worker, "check_disk"))
        self.stores = []
        self.store_factory = self.enterContext(mock.patch.object(
            worker.tracker, "Store", side_effect=self.make_store,
        ))
        self.real_run_job = worker.run_job
        self.job = self.enterContext(mock.patch.object(worker, "run_job", return_value={"blocks": 500}))

    def make_store(self, path):
        self.assertEqual(path, self.db)
        store = mock.Mock()
        self.stores.append(store)
        return store

    def status(self, job):
        return json.loads((self.data / f"job-{job}.json").read_text())

    def seed_success(self, job):
        previous = {
            "job": job, "started_at": "old start", "finished_at": "old finish",
            "ok": True, "result": {"last_height": 123},
        }
        worker.tracker.atomic_write_json(self.data / f"job-{job}.json", previous)
        return previous

    @contextmanager
    def held_lock(self, name):
        with (self.data / name).open("a") as lock:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            try:
                yield
            finally:
                fcntl.flock(lock, fcntl.LOCK_UN)

    def assert_locked(self, name):
        with (self.data / name).open("a") as lock:
            with self.assertRaises(BlockingIOError):
                fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)

    def use_clock(self, clock):
        for name in ("monotonic", "time", "sleep"):
            self.enterContext(mock.patch.object(worker.time, name, getattr(clock, name)))

    def test_cross_job_queue_precedes_store_and_preserves_success_until_running(self):
        previous = self.seed_success("clusters")
        top_running = threading.Event()
        release_top = threading.Event()
        clusters_queued = threading.Event()
        clusters_running = threading.Event()
        write_status = worker.write_job_status

        def observe(status, **kwargs):
            write_status(status, **kwargs)
            if status["job"] == "clusters" and status["state"] == "queued":
                clusters_queued.set()

        def run(job, store):
            self.assert_locked(".heavy.lock")
            self.assert_locked(f".{job}.lock")
            current = self.status(job)
            self.assertEqual(current["state"], "running")
            self.assertIsNone(current["ok"])
            if job == "top":
                top_running.set()
                if not release_top.wait(3):
                    raise RuntimeError("test did not release top")
            else:
                clusters_running.set()
            return {"blocks": 500}

        self.job.side_effect = run
        self.enterContext(mock.patch.object(worker, "write_job_status", side_effect=observe))
        with ThreadPoolExecutor(max_workers=2) as pool:
            top = pool.submit(worker.main, ["top"])
            try:
                self.assertTrue(top_running.wait(1))
                clusters = pool.submit(worker.main, ["clusters"])
                self.assertTrue(clusters_queued.wait(1))
                self.assertFalse(clusters_running.is_set())
                self.assertEqual(self.store_factory.call_count, 1)
                queued = self.status("clusters")
                self.assertEqual(queued["waiting_for"], ".heavy.lock")
                self.assertIsNone(queued["ok"])
                self.assertNotIn("result", queued)
                self.assertEqual(queued["last_success"], previous)
            finally:
                release_top.set()
            self.assertEqual(top.result(timeout=2), 0)
            self.assertEqual(clusters.result(timeout=2), 0)
        self.assertTrue(clusters_running.is_set())
        self.assertEqual(self.store_factory.call_count, 2)
        for store in self.stores:
            store.conn.close.assert_called_once_with()
        for job in ("top", "clusters"):
            self.assertTrue(self.status(job)["ok"])
            self.assertNotIn("last_success", self.status(job)["last_success"])
            with self.held_lock(f".{job}.lock"):
                pass
        with self.held_lock(".heavy.lock"):
            pass

    def test_all_heavy_jobs_timeout_before_store_or_work_without_losing_last_success(self):
        self.assertEqual(worker.HEAVY_JOBS, {
            "top", "clusters", "emissions", "nevm", "publish", "backup", "times", "sentry",
        })
        with self.held_lock(".heavy.lock"), mock.patch.object(worker, "LOCK_WAIT_SECONDS", 0.015), mock.patch.object(worker, "backup_database") as backup:
            for job in sorted(worker.HEAVY_JOBS):
                with self.subTest(job=job):
                    previous = self.seed_success(job)
                    self.assertEqual(worker.main([job]), 1)
                    status = self.status(job)
                    self.assertFalse(status["ok"])
                    self.assertEqual(status["state"], "failed")
                    self.assertIn("Queue timeout", status["error"])
                    self.assertEqual(status["last_success"], previous)
                    self.assertNotIn("started_at", status)
                    with self.held_lock(f".{job}.lock"):
                        pass
            backup.assert_not_called()
        self.store_factory.assert_not_called()
        self.job.assert_not_called()
        self.disk.assert_not_called()

    def test_duplicate_timeout_is_nonzero_and_does_not_overwrite_owner_status(self):
        self.seed_success("top")
        path = self.data / "job-top.json"
        original = path.read_bytes()
        with self.held_lock(".top.lock"), mock.patch.object(worker, "LOCK_WAIT_SECONDS", 0.015):
            self.assertEqual(worker.main(["top"]), 1)
        self.assertEqual(path.read_bytes(), original)
        self.store_factory.assert_not_called()
        events = [json.loads(call.args[0]) for call in self.output.call_args_list]
        self.assertEqual([event["state"] for event in events], ["queued", "failed"])
        self.assertIsNone(events[0]["ok"])
        self.assertFalse(events[-1]["ok"])
        self.assertEqual(worker.main(["top"]), 0)

    def test_waiting_duplicate_eventually_executes_after_owner_releases(self):
        waiting = threading.Event()
        write_status = worker.write_job_status

        def observe(status, **kwargs):
            write_status(status, **kwargs)
            if not kwargs.get("persist", True):
                waiting.set()

        self.enterContext(mock.patch.object(worker, "write_job_status", side_effect=observe))
        with ThreadPoolExecutor(max_workers=1) as pool:
            with self.held_lock(".top.lock"):
                queued = pool.submit(worker.main, ["top"])
                self.assertTrue(waiting.wait(1))
                self.store_factory.assert_not_called()
            self.assertEqual(queued.result(timeout=2), 0)
        self.job.assert_called_once()

    def test_health_bypasses_heavy_lock_cooldown_and_sqlite_even_without_database(self):
        self.db.unlink()
        worker.tracker.atomic_write_json(self.data / ".core-cooldown.json", {"until": worker.time.time() + 60})
        self.job.side_effect = self.real_run_job
        rpc = mock.Mock()
        rpc.call.return_value = {"blocks": 2315551, "initialblockdownload": False}
        with self.held_lock(".heavy.lock"), mock.patch.object(worker, "core_rpc", return_value=rpc) as factory:
            self.assertEqual(worker.main(["health"]), 0)
        factory.assert_called_once_with()
        self.store_factory.assert_not_called()
        self.disk.assert_not_called()
        health = json.loads((self.public / "health.json").read_text())
        self.assertEqual(health["chain"]["blocks"], 2315551)

    def test_index_error_releases_both_locks_closes_store_and_does_not_cool_down_core(self):
        previous = self.seed_success("top")
        self.job.side_effect = RuntimeError("index reorg")
        self.assertEqual(worker.main(["top"]), 1)
        self.assertEqual(self.status("top")["last_success"], previous)
        self.assertEqual(self.status("top")["error"], "index reorg")
        self.stores[0].conn.close.assert_called_once_with()
        self.assertFalse((self.data / ".core-cooldown.json").exists())
        with self.held_lock(".heavy.lock"), self.held_lock(".top.lock"):
            pass
        self.job.side_effect = None
        self.assertEqual(worker.main(["top"]), 0)
        self.assertNotIn("error", self.status("top"))

    def test_store_creation_failure_releases_locks_and_reports_error(self):
        self.store_factory.side_effect = sqlite3.OperationalError("database locked")
        self.assertEqual(worker.main(["emissions"]), 1)
        self.job.assert_not_called()
        self.assertEqual(self.status("emissions")["error"], "database locked")
        with self.held_lock(".heavy.lock"), self.held_lock(".emissions.lock"):
            pass

    def test_backup_runs_under_shared_lock_without_constructing_store(self):
        def backup():
            self.assert_locked(".heavy.lock")
            self.assert_locked(".backup.lock")
            return {"archive": "backup.sqlite.gz"}

        with mock.patch.object(worker, "backup_database", side_effect=backup):
            self.assertEqual(worker.main(["backup"]), 0)
        self.store_factory.assert_not_called()
        self.job.assert_not_called()
        with self.held_lock(".heavy.lock"):
            pass

    def test_rpc_failure_persists_cooldown_under_lock_and_next_core_job_waits_before_store(self):
        clock = Clock()
        self.use_clock(clock)
        self.enterContext(mock.patch.object(worker, "LOCK_WAIT_SECONDS", 600))
        previous = self.seed_success("top")
        rpc = mock.Mock()
        rpc.call.return_value = {"initialblockdownload": False}
        rpc.batch_call.side_effect = TimeoutError("RPC timed out")
        self.job.side_effect = self.real_run_job
        write_json = worker.tracker.atomic_write_json

        def observe_write(path, payload):
            if path.name == ".core-cooldown.json":
                self.assert_locked(".heavy.lock")
            write_json(path, payload)

        def sync(store, client, **kwargs):
            return client.batch_call([("getblock", ["hash", 2])])

        with mock.patch.object(worker, "core_rpc", return_value=rpc), mock.patch.object(worker.tracker, "sync_top_wallet_index", side_effect=sync), mock.patch.object(worker.tracker, "atomic_write_json", side_effect=observe_write):
            self.assertEqual(worker.main(["top"]), 1)
        until = json.loads((self.data / ".core-cooldown.json").read_text())["until"]
        self.assertEqual(until, clock.time() + 60)
        self.assertEqual(self.status("top")["last_success"], previous)
        self.stores[0].set_meta.assert_not_called()
        self.stores[0].conn.close.assert_called_once_with()
        with self.held_lock(".heavy.lock"), self.held_lock(".top.lock"):
            pass

        def next_store(path):
            self.assertGreaterEqual(clock.time(), until)
            self.assert_locked(".heavy.lock")
            return self.make_store(path)

        self.store_factory.side_effect = next_store
        self.job.side_effect = None
        self.assertEqual(worker.main(["clusters"]), 0)
        self.assertEqual(clock.sleeps, [60])
        events = [json.loads(call.args[0]) for call in self.output.call_args_list]
        cooldown = next(event for event in events if event["state"] == "cooldown")
        self.assertEqual(cooldown["job"], "clusters")
        self.assertIsNone(cooldown["ok"])
        self.assertNotIn("cooldown_until", self.status("clusters"))

    def test_cooldown_wait_is_bounded_and_preserves_retry_deadline(self):
        clock = Clock()
        self.use_clock(clock)
        until = clock.time() + 60
        path = self.data / ".core-cooldown.json"
        worker.tracker.atomic_write_json(path, {"until": until})
        self.assertEqual(worker.main(["sentry"]), 1)
        self.assertEqual(clock.sleeps, [2])
        self.assertIn("Core cooldown", self.status("sentry")["error"])
        self.assertEqual(json.loads(path.read_text())["until"], until)
        self.store_factory.assert_not_called()
        self.job.assert_not_called()
        with self.held_lock(".heavy.lock"), self.held_lock(".sentry.lock"):
            pass


class BlockRpc:
    def __init__(self, fail_batch=None):
        self.requests = []
        self.heavy_batches = 0
        self.fail_batch = fail_batch

    def call(self, method, params=None):
        if method == "getblockchaininfo":
            return {"initialblockdownload": False}
        if method == "getblockcount":
            return 10000
        raise AssertionError(method)

    def batch_call(self, calls):
        self.requests.append(calls)
        if calls[0][0] == "getblockhash":
            return [str(params[0]) for _, params in calls]
        assert all(method == "getblock" and params[1] == 2 for method, params in calls)
        self.heavy_batches += 1
        if self.heavy_batches == self.fail_batch:
            raise TimeoutError("full-block timeout")
        return [
            {
                "height": int(params[0]), "hash": params[0],
                "previousblockhash": str(int(params[0]) - 1), "time": 1700000000,
                "tx": [{"txid": "coinbase-" + params[0], "vin": [{"coinbase": "01"}], "vout": []}],
            }
            for _, params in calls
        ]


class HistoricalBoundsTests(unittest.TestCase):
    def setUp(self):
        self.store = worker.tracker.Store(Path(":memory:"))
        self.addCleanup(self.store.conn.close)
        self.enterContext(mock.patch.object(worker.tracker, "top_wallet_sentry_collateral_outpoints", return_value={}))
        self.enterContext(mock.patch.object(
            worker.tracker.urllib.request, "urlopen", side_effect=AssertionError("unexpected network"),
        ))

    def test_all_utxo_indexes_resume_existing_progress_with_500_block_and_5_rpc_bounds(self):
        for job, key in (
            ("top", "top_wallet_index"), ("clusters", "top_wallet_cluster_index"),
            ("emissions", "emission_index"),
        ):
            with self.subTest(job=job):
                self.store.set_meta(key, {"last_height": 10, "last_hash": "10"})
                rpc = BlockRpc()
                with mock.patch.object(worker, "core_rpc", return_value=rpc) as factory:
                    result = worker.run_job(job, self.store)
                factory.assert_called_once_with(timeout=120)
                self.assertEqual(result["blocks"], 500)
                self.assertEqual(result["start_height"], 11)
                self.assertEqual(result["last_height"], 510)
                self.assertEqual(len(rpc.requests), 200)
                self.assertTrue(all(len(calls) == 5 for calls in rpc.requests))
                self.assertEqual(rpc.requests[0][0], ("getblockhash", [11]))
                progress = self.store.get_meta(key)
                self.assertEqual(progress["last_height"], 510)
                self.assertEqual(progress["last_hash"], "510")
                self.assertEqual(progress["confirmations"], 12)
                self.assertEqual(progress["safe_height"], 9988)

    def test_failure_keeps_committed_checkpoint_and_does_not_reset_or_replace_metadata(self):
        for job, key in (
            ("top", "top_wallet_index"), ("clusters", "top_wallet_cluster_index"),
            ("emissions", "emission_index"),
        ):
            with self.subTest(job=job):
                original = {"last_height": 10, "last_hash": "10", "confirmations": 12, "safe_height": 9988}
                self.store.set_meta(key, original)
                with mock.patch.object(worker, "core_rpc", return_value=BlockRpc(fail_batch=1)):
                    with self.assertRaises(worker.HistoricalRpcError):
                        worker.run_job(job, self.store)
                self.assertEqual(self.store.get_meta(key), original)
                with mock.patch.object(worker, "core_rpc", return_value=BlockRpc(fail_batch=2)):
                    with self.assertRaises(worker.HistoricalRpcError):
                        worker.run_job(job, self.store)
                progress = self.store.get_meta(key)
                self.assertEqual(progress["last_height"], 15)
                self.assertEqual(progress["last_hash"], "15")

    def test_nevm_bounds_and_timeout_preserve_existing_progress(self):
        original = {"last_height": 200, "last_hash": "saved-hash", "custom": "preserved"}
        self.store.set_meta("nevm_emission_index", original)
        with mock.patch.dict(worker.os.environ, {"SYS_NEVM_RPC_URL": "http://nevm.invalid"}), mock.patch.object(worker.tracker, "EvmRpcClient") as factory, mock.patch.object(worker.tracker, "sync_nevm_emission_index", return_value={"safe_height": 900}) as sync:
            worker.run_job("nevm", self.store)
        factory.assert_called_once_with("http://nevm.invalid", timeout=120)
        sync.assert_called_once_with(self.store, factory.return_value, max_blocks=500, batch_size=5, confirmations=12)
        self.assertEqual(self.store.get_meta("nevm_emission_index"), {**original, "safe_height": 900, "confirmations": 12})

    def test_rpc_wrapper_marks_transport_and_json_rpc_failures_without_retry(self):
        for error in (TimeoutError("timeout"), urllib.error.URLError("offline"), RuntimeError({"code": -1})):
            for method, args in (("call", ("getblockcount",)), ("batch_call", ([("getblock", ["hash", 2])],))):
                with self.subTest(error=type(error).__name__, method=method):
                    rpc = mock.Mock()
                    getattr(rpc, method).side_effect = error
                    with self.assertRaises(worker.HistoricalRpcError) as raised:
                        getattr(worker.HistoricalCoreRpc(rpc), method)(*args)
                    self.assertIs(raised.exception.__cause__, error)
                    self.assertEqual(getattr(rpc, method).call_count, 1)

    def test_worker_default_and_global_rpc_timeouts_are_unchanged(self):
        with tempfile.TemporaryDirectory() as directory:
            core = Path(directory)
            (core / ".cookie").write_text("user:password")
            with mock.patch.object(worker, "CORE", core):
                self.assertEqual(worker.core_rpc().timeout, 30)
                self.assertEqual(worker.core_rpc(timeout=120).timeout, 120)
        self.assertEqual(worker.tracker.SyscoinRpcClient("http://core.invalid", None, None).timeout, 15)


if __name__ == "__main__":
    unittest.main()
