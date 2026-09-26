#!/usr/bin/env python3
"""Independent, restartable tracker jobs for a persistent host."""

from __future__ import annotations

import argparse
from contextlib import ExitStack, closing, contextmanager
import fcntl
import gzip
import json
import os
from pathlib import Path
import shutil
import sqlite3
import subprocess
import sys
import time

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
os.chdir(ROOT)

import syscoin_tracker as tracker

DATA = Path(os.getenv("SYS_TRACKER_DATA_DIR", "/srv/syswallettracker/data"))
PUBLIC = Path(os.getenv("SYS_TRACKER_PUBLIC_DIR", "/var/www/html/syswallettracker"))
CORE = Path(os.getenv("SYSCOIN_DATA_DIR", "/srv/syswallettracker/core"))
DB = DATA / "syscoin_tracker.sqlite"
CORE_JOBS = {"top", "clusters", "emissions", "times", "sentry"}
HEAVY_JOBS = CORE_JOBS | {"nevm", "publish", "backup"}
LOCK_WAIT_SECONDS = 600
LOCK_POLL_SECONDS = 0.25
CORE_COOLDOWN_SECONDS = 60
HISTORICAL_RPC_TIMEOUT = 120
HISTORICAL_MAX_BLOCKS = 500
HISTORICAL_BATCH_SIZE = 5


def core_rpc(*, timeout: int = 30) -> tracker.SyscoinRpcClient:
    username, password = (CORE / ".cookie").read_text().strip().split(":", 1)
    return tracker.SyscoinRpcClient("http://127.0.0.1:8370/", username, password, timeout=timeout)


class HistoricalRpcError(RuntimeError):
    """Distinguish RPC failures from indexing or SQLite failures."""


class HistoricalCoreRpc:
    def __init__(self, rpc: tracker.SyscoinRpcClient) -> None:
        self.rpc = rpc

    def _request(self, method: str, *args):
        try:
            return getattr(self.rpc, method)(*args)
        except Exception as exc:
            raise HistoricalRpcError(str(exc)) from exc

    def call(self, method: str, params=None):
        return self._request("call", method, params)

    def batch_call(self, calls):
        return self._request("batch_call", calls)


@contextmanager
def wait_for_lock(path: Path, deadline: float, on_wait):
    with path.open("a") as lock:
        waiting = False
        while True:
            if time.monotonic() >= deadline:
                raise TimeoutError(f"Queue timeout waiting for {path.name}")
            try:
                fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
                break
            except BlockingIOError:
                if not waiting:
                    on_wait()
                    waiting = True
                time.sleep(min(LOCK_POLL_SECONDS, max(0, deadline - time.monotonic())))
        try:
            yield
        finally:
            fcntl.flock(lock, fcntl.LOCK_UN)


def write_job_status(status: dict, *, persist: bool = True) -> None:
    if persist:
        tracker.atomic_write_json(DATA / f"job-{status['job']}.json", status)
    print(json.dumps(status), flush=True)


def wait_for_core_cooldown(status: dict, deadline: float) -> None:
    path = DATA / ".core-cooldown.json"
    if not path.exists():
        return
    until = float(json.loads(path.read_text())["until"])
    remaining = until - time.time()
    if remaining <= 0:
        return
    status.update(state="cooldown", waiting_for="core_cooldown", cooldown_until=until)
    write_job_status(status)
    budget = max(0, deadline - time.monotonic())
    time.sleep(min(remaining, budget))
    if remaining >= budget:
        raise TimeoutError("Queue timeout waiting for Core cooldown")
    status.pop("cooldown_until", None)


def check_disk() -> None:
    if shutil.disk_usage(DATA).free < 15 * 1024**3:
        raise RuntimeError("Less than 15 GiB free; pausing tracker writes until storage is expanded")


def backup_database() -> dict:
    if not DB.is_file() or DB.stat().st_size == 0:
        raise RuntimeError("Refusing to back up a missing or empty tracker database")
    wal = DB.with_name(DB.name + "-wal")
    source_bytes = DB.stat().st_size + (wal.stat().st_size if wal.exists() else 0)
    required_free = int(source_bytes * 2.1) + 15 * 1024**3
    if shutil.disk_usage(DATA).free < required_free:
        raise RuntimeError("Not enough free space for a consistent database backup and compression")
    destination = DATA.parent / "backups"
    destination.mkdir(exist_ok=True)
    stamp = time.strftime("%Y%m%dT%H%M%SZ", time.gmtime())
    temporary = destination / f".{stamp}.sqlite"
    archive = destination / f"tracker-{stamp}.sqlite.gz"
    compressed_temp = archive.with_name(f".{archive.name}.tmp")
    try:
        with closing(sqlite3.connect(f"file:{DB}?mode=ro", uri=True)) as source:
            if not source.execute("SELECT 1 FROM metadata LIMIT 1").fetchone():
                raise RuntimeError("Refusing to rotate backups from an uninitialized tracker database")
            with closing(sqlite3.connect(temporary)) as target:
                source.backup(target, pages=512, sleep=0.05)
                target.execute("PRAGMA journal_mode=DELETE")
                if target.execute("PRAGMA quick_check").fetchone()[0] != "ok":
                    raise RuntimeError("Backup integrity check failed")
        if shutil.disk_usage(DATA).free < int(temporary.stat().st_size * 1.01) + 15 * 1024**3:
            raise RuntimeError("Backup compression would consume reserved disk headroom")
        with temporary.open("rb") as source, gzip.open(compressed_temp, "wb", compresslevel=3) as target:
            shutil.copyfileobj(source, target)
        compressed_temp.replace(archive)
    finally:
        temporary.unlink(missing_ok=True)
        compressed_temp.unlink(missing_ok=True)
    for old in sorted(destination.glob("tracker-*.sqlite.gz"))[:-3]:
        old.unlink()
    return {"archive": archive.name, "bytes": archive.stat().st_size}


def run_job(job: str, store: tracker.Store | None) -> dict:
    client = tracker.BlockbookClient(
        os.getenv("SYS_BLOCKBOOK_URL", tracker.DEFAULT_BLOCKBOOK_URL), timeout=15, retries=2
    )
    csv_path = DATA / "network_masternodes.csv"
    if job == "wallet":
        stats = tracker.sync_address(
            store, client, tracker.DEFAULT_ADDRESS, page_size=1000, max_pages=None,
            from_height=2221358, watched={tracker.DEFAULT_ADDRESS}, quiet=True,
        )
        if not store.get_meta("recovery_csv_seeded", False):
            tracker.load_node_outputs_csv(store)
            tracker.load_verified_sentries_csv(store)
            store.set_meta("recovery_csv_seeded", True)
        balances = tracker.refresh_exchange_hot_wallet_balances(store, client)
        return {"wallet": stats, "exchange_wallets": len(balances)}
    if job == "traces":
        watched = {tracker.DEFAULT_ADDRESS}
        since_time, _ = tracker.parse_since_date("2026-04-14 02:30", "UTC")
        follow = tracker.refresh_spent_first_hops(
            store, client, watched, since_time=since_time, limit=8,
            min_sats=tracker.sys_to_sats("100"), page_size=100, max_pages_per_address=1,
        )
        nodes = tracker.refresh_node_spends(
            store, client, watched, limit=12, page_size=100, max_pages_per_address=1,
        )
        return {"next_hop": follow, "node_spends": nodes}
    if job == "sentry":
        if not store.conn.execute("SELECT 1 FROM network_masternodes LIMIT 1").fetchone():
            tracker.load_network_masternodes_csv(store, csv_path)
        try:
            rpc = core_rpc()
            ready = not rpc.call("getblockchaininfo").get("initialblockdownload", True)
        except Exception:
            ready = False
        result = (tracker.sync_network_masternodes(store, rpc, client) if ready else
                  tracker.sync_network_masternodes_from_sysnode(
                      store, client, time_lookup_limit=0, time_lookup_scope="full"
                  ))
        tracker.write_network_masternodes_csv(tracker.network_masternode_rows_from_store(store), csv_path)
        return result
    if job == "miners":
        result = tracker.miners_snapshot(store)
        tracker.atomic_write_json(PUBLIC / tracker.MINERS_JSON, result)
        return {"generated_at": result.get("generated_at")}
    if job == "times":
        info = core_rpc().call("getblockchaininfo")
        if info.get("initialblockdownload"):
            waiting = {"complete": False, "waiting_for_chain": True, "blocks": info["blocks"], "headers": info["headers"]}
            store.set_meta("sentry_time_verification", waiting)
            return waiting
        completed = subprocess.run([
            sys.executable, str(ROOT / "scripts/verify_sentry_times.py"),
            "--db", str(DB), "--max-heights", "100", "--provider", "core",
            "--audit-dir", str(DATA / "sentry-time-audits"),
        ], check=False, text=True, capture_output=True, timeout=900)
        if completed.returncode:
            raise RuntimeError(completed.stderr.strip() or completed.stdout.strip())
        result = json.loads(completed.stdout)
        store.set_meta("sentry_time_verification", result)
        return result
    if job == "publish":
        result = tracker.main([
            "--db", str(DB), "--timezone", "UTC", "publish-static",
            "--output-dir", str(PUBLIC), "--csv", str(csv_path), "--skip-sync",
            "--since-date", "2026-04-14 02:30", "--from-height", "2221358",
        ])
        if result:
            raise RuntimeError(f"Static publication failed with exit code {result}")
        return {"published_at": tracker.now_iso()}
    if job in {"top", "clusters", "emissions"}:
        rpc = HistoricalCoreRpc(core_rpc(timeout=HISTORICAL_RPC_TIMEOUT))
        info = rpc.call("getblockchaininfo")
        if info.get("initialblockdownload"):
            return {"waiting_for_chain": True, "blocks": info["blocks"], "headers": info["headers"]}
        function = {
            "top": tracker.sync_top_wallet_index,
            "clusters": tracker.sync_top_wallet_cluster_index,
            "emissions": tracker.sync_emission_index,
        }[job]
        result = function(
            store, rpc, max_blocks=HISTORICAL_MAX_BLOCKS,
            batch_size=HISTORICAL_BATCH_SIZE, confirmations=12,
        )
        key = {"top": "top_wallet_index", "clusters": "top_wallet_cluster_index", "emissions": "emission_index"}[job]
        progress = store.get_meta(key, {}) or {}
        progress.update(confirmations=12, safe_height=result["safe_height"])
        store.set_meta(key, progress)
        return result
    if job == "nevm":
        url = os.getenv("SYS_NEVM_RPC_URL")
        if not url:
            return {"configured": False}
        result = tracker.sync_nevm_emission_index(
            store, tracker.EvmRpcClient(url, timeout=HISTORICAL_RPC_TIMEOUT),
            max_blocks=HISTORICAL_MAX_BLOCKS, batch_size=HISTORICAL_BATCH_SIZE, confirmations=12,
        )
        progress = store.get_meta("nevm_emission_index", {}) or {}
        progress.update(confirmations=12, safe_height=result["safe_height"])
        store.set_meta("nevm_emission_index", progress)
        return result
    if job == "health":
        jobs = {}
        for path in DATA.glob("job-*.json"):
            jobs[path.stem[4:]] = json.loads(path.read_text())
        result = {
            "checked_at": tracker.now_iso(), "disk_free_bytes": shutil.disk_usage(DATA).free,
            "jobs": jobs,
        }
        try:
            info = core_rpc().call("getblockchaininfo")
            result["chain"] = {key: info.get(key) for key in (
                "blocks", "headers", "initialblockdownload", "verificationprogress", "size_on_disk"
            )}
        except Exception as exc:
            result["chain"] = {"error": str(exc)}
        tracker.atomic_write_json(PUBLIC / "health.json", result)
        return {"checked_at": result["checked_at"]}
    raise ValueError(job)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("job", choices=["wallet", "traces", "sentry", "times", "miners", "publish", "top", "clusters", "emissions", "nevm", "backup", "health"])
    args = parser.parse_args(argv)
    DATA.mkdir(parents=True, exist_ok=True)
    PUBLIC.mkdir(parents=True, exist_ok=True)
    deadline = time.monotonic() + LOCK_WAIT_SECONDS
    status = {
        "queued_at": tracker.now_iso(), "job": args.job, "state": "queued",
        "ok": None, "waiting_for": f".{args.job}.lock",
    }
    owns_status = False
    with ExitStack() as locks:
        try:
            locks.enter_context(wait_for_lock(
                DATA / f".{args.job}.lock", deadline,
                lambda: write_job_status(status, persist=False),
            ))
            owns_status = True
            status_path = DATA / f"job-{args.job}.json"
            previous = json.loads(status_path.read_text()) if status_path.exists() else {}
            if previous.get("ok") is True:
                status["last_success"] = {key: value for key, value in previous.items() if key != "last_success"}
            elif previous.get("last_success") is not None:
                status["last_success"] = previous["last_success"]

            if args.job in HEAVY_JOBS:
                status["waiting_for"] = ".heavy.lock"
                write_job_status(status)
                locks.enter_context(wait_for_lock(
                    DATA / ".heavy.lock", deadline, lambda: None,
                ))
                if args.job in CORE_JOBS:
                    wait_for_core_cooldown(status, deadline)

            status.pop("waiting_for", None)
            status.update(state="running", started_at=tracker.now_iso())
            write_job_status(status)
            store = None
            try:
                if args.job == "health":
                    status["result"] = run_job(args.job, None)
                else:
                    check_disk()
                    if not DB.is_file() or DB.stat().st_size == 0:
                        raise RuntimeError("Tracker database is missing or empty; restore it before running jobs")
                    if args.job == "backup":
                        status["result"] = backup_database()
                    else:
                        store = tracker.Store(DB)
                        status["result"] = run_job(args.job, store)
            finally:
                if store is not None:
                    store.conn.close()
            status.update(ok=True, state="succeeded")
        except Exception as exc:
            status.update(ok=False, state="failed", error=str(exc))
            if isinstance(exc, HistoricalRpcError):
                until = time.time() + CORE_COOLDOWN_SECONDS
                tracker.atomic_write_json(DATA / ".core-cooldown.json", {
                    "until": until, "job": args.job, "error": str(exc),
                })
                status["cooldown_until"] = until
        status["finished_at"] = tracker.now_iso()
        if status["ok"]:
            status["last_success"] = {key: value for key, value in status.items() if key != "last_success"}
        # A duplicate invocation must not overwrite the current owner's status.
        write_job_status(status, persist=owns_status)
        return 0 if status["ok"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
