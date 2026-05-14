from __future__ import annotations

import csv
import os
import sys
import threading
import time
import urllib.parse
from http.server import BaseHTTPRequestHandler
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
os.chdir(ROOT)

from syscoin_tracker import (  # noqa: E402
    DEFAULT_ADDRESS,
    DEFAULT_BLOCKBOOK_URL,
    DEFAULT_TIMEZONE,
    BlockbookClient,
    Store,
    block_height_at_or_after,
    dashboard_html,
    parse_since_date,
    refresh_spent_first_hops,
    sync_address,
    sys_to_sats,
)


DEFAULT_SINCE_DATE = "2026-04-14 12:30"
DEFAULT_FROM_HEIGHT = 2221358
DB_PATH = Path(os.getenv("SYS_TRACKER_DB", "/tmp/syscoin_tracker.sqlite"))
VERIFIED_SENTRIES_PATH = ROOT / "verified_sentries.csv"

_lock = threading.Lock()
_store: Store | None = None
_client: BlockbookClient | None = None
_from_height: int | None = None
_last_sync_at = 0.0


def env_int(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, str(default)))
    except ValueError:
        return default


def get_store() -> Store:
    global _store
    if _store is None:
        DB_PATH.parent.mkdir(parents=True, exist_ok=True)
        _store = Store(DB_PATH)
    return _store


def get_client() -> BlockbookClient:
    global _client
    if _client is None:
        _client = BlockbookClient(os.getenv("SYS_BLOCKBOOK_URL", DEFAULT_BLOCKBOOK_URL))
    return _client


def load_verified_sentries(store: Store) -> None:
    if not VERIFIED_SENTRIES_PATH.exists():
        return
    with VERIFIED_SENTRIES_PATH.open(newline="") as f:
        for row in csv.DictReader(f):
            store.conn.execute(
                """
                INSERT INTO verified_sentries(
                    outpoint, source_txid, source_vout, depth, address,
                    block_height, block_time, path, verified_at
                )
                VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(outpoint) DO UPDATE SET
                    depth=excluded.depth,
                    address=excluded.address,
                    block_height=excluded.block_height,
                    block_time=excluded.block_time,
                    path=excluded.path,
                    verified_at=excluded.verified_at
                """,
                (
                    row["outpoint"],
                    row["source_txid"],
                    int(row["source_vout"]),
                    int(row["depth"]),
                    row["address"],
                    int(row["block_height"]),
                    int(row["block_time"]),
                    row["path"],
                    row["verified_at"],
                ),
            )
    store.conn.commit()


def get_from_height(client: BlockbookClient, since_time: int | None) -> int | None:
    global _from_height
    configured = os.getenv("SYS_TRACKER_FROM_HEIGHT")
    if configured:
        return int(configured)
    if os.getenv("SYS_TRACKER_SINCE_DATE", DEFAULT_SINCE_DATE) == DEFAULT_SINCE_DATE:
        return DEFAULT_FROM_HEIGHT
    if _from_height is None and since_time:
        _from_height = block_height_at_or_after(client, since_time)
    return _from_height


def sync_for_request(force: bool = False) -> tuple[Store, int | None, str | None]:
    global _last_sync_at
    interval = env_int("SYS_TRACKER_SYNC_INTERVAL", 60)
    since_date = os.getenv("SYS_TRACKER_SINCE_DATE", DEFAULT_SINCE_DATE)
    since_time, since_label = parse_since_date(since_date, os.getenv("SYS_TRACKER_TIMEZONE", DEFAULT_TIMEZONE))

    with _lock:
        now = time.monotonic()
        store = get_store()
        if not force and _last_sync_at and now - _last_sync_at < interval:
            return store, since_time, since_label

        client = get_client()
        watched = {os.getenv("SYS_TRACKER_ADDRESS", DEFAULT_ADDRESS)}
        sync_address(
            store,
            client,
            next(iter(watched)),
            page_size=env_int("SYS_TRACKER_PAGE_SIZE", 1000),
            max_pages=None,
            from_height=get_from_height(client, since_time),
            watched=watched,
            quiet=True,
        )
        load_verified_sentries(store)
        refresh_spent_first_hops(
            store,
            client,
            watched,
            since_time=since_time,
            limit=env_int("SYS_TRACKER_NEXT_HOP_LIMIT", 8),
            min_sats=sys_to_sats(os.getenv("SYS_TRACKER_NEXT_HOP_MIN_SYS", "100")),
            page_size=min(env_int("SYS_TRACKER_PAGE_SIZE", 1000), 100),
            max_pages_per_address=1,
        )
        _last_sync_at = now
        return store, since_time, since_label


class handler(BaseHTTPRequestHandler):
    def send_dashboard(self, include_body: bool = True) -> None:
        parsed = urllib.parse.urlparse(self.path)
        if parsed.path not in ("/", "/index.html", "/api/index.py"):
            self.send_error(404)
            return

        force = urllib.parse.parse_qs(parsed.query).get("force", ["0"])[0] == "1"
        try:
            store, since_time, since_label = sync_for_request(force=force)
            body = dashboard_html(
                store,
                since_time=since_time,
                since_label=f"{since_label} (from {os.getenv('SYS_TRACKER_SINCE_DATE', DEFAULT_SINCE_DATE)} {os.getenv('SYS_TRACKER_TIMEZONE', DEFAULT_TIMEZONE)})",
                refresh_seconds=env_int("SYS_TRACKER_SYNC_INTERVAL", 60),
            ).encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Cache-Control", "no-store")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            if include_body:
                self.wfile.write(body)
        except Exception as exc:  # pragma: no cover - deployed error surface
            body = f"Syscoin tracker failed to sync: {exc}".encode("utf-8")
            self.send_response(500)
            self.send_header("Content-Type", "text/plain; charset=utf-8")
            self.send_header("Cache-Control", "no-store")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            if include_body:
                self.wfile.write(body)

    def do_GET(self) -> None:  # noqa: N802
        self.send_dashboard(include_body=True)

    def do_HEAD(self) -> None:  # noqa: N802
        self.send_dashboard(include_body=False)

