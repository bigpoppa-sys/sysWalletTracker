from __future__ import annotations

import csv
import io
import json
import os
import ssl
import sys
import tarfile
import threading
import time
import urllib.parse
import urllib.request
from http.server import BaseHTTPRequestHandler
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
os.chdir(ROOT)

from syscoin_tracker import (  # noqa: E402
    CHART_ASSET_ROUTE,
    DEFAULT_ADDRESS,
    DEFAULT_BLOCKBOOK_URL,
    DEFAULT_TIMEZONE,
    EMISSIONS_JSON,
    EMISSIONS_JSON_PATH,
    EMISSIONS_PATHS,
    MINERS_JSON,
    MINERS_JSON_PATH,
    MINERS_PATHS,
    SN_COMP_HTML,
    SN_COMP_PATHS,
    LEGACY_MASTERNODE_PATHS,
    SENTRY_COLLATERAL_SATS,
    SENTRY_NODE_PATHS,
    TOP_WALLETS_JSON,
    TOP_WALLETS_JSON_PATH,
    TOP_WALLETS_PATHS,
    BlockbookClient,
    Store,
    SyscoinRpcClient,
    block_height_at_or_after,
    chart_asset_bytes,
    dashboard_html,
    emissions_html,
    emissions_snapshot,
    load_network_masternodes_csv,
    load_network_masternodes_csv_rows,
    masternodes_html,
    miners_html,
    miners_snapshot,
    parse_since_date,
    refresh_exchange_hot_wallet_balances,
    refresh_node_spends,
    refresh_spent_first_hops,
    sync_address,
    sync_network_masternodes,
    sys_to_sats,
    sn_comp_html,
    top_wallets_snapshot,
    top_wallets_html,
)


DEFAULT_SINCE_DATE = "2026-04-14 12:30"
DEFAULT_FROM_HEIGHT = 2221358
DEFAULT_NETWORK_MASTERNODES_URL = "http://142.93.241.64/syswallettracker/network_masternodes.csv"
DEFAULT_STATIC_BASE_URL = "https://syscoin.dev/syswallettracker"
DB_PATH = Path(os.getenv("SYS_TRACKER_DB", "/tmp/syscoin_tracker.sqlite"))
VERIFIED_SENTRIES_PATH = ROOT / "verified_sentries.csv"
NODE_OUTPUTS_PATH = ROOT / "node_outputs.csv"
INSTALL_BUNDLE_FILES = (
    "syscoin_tracker.py",
    "DEPLOYMENT.md",
    "README.md",
    "package.json",
    "package-lock.json",
    "vercel.json",
    ".gitignore",
    ".vercelignore",
    "destinations.csv",
    "destinations_since_monitoring.csv",
    "destinations_since_monitoring_all_ranked.csv",
    "exchange_hot_wallets.csv",
    "exchange_cold_wallets.csv",
    "exchange_routes.csv",
    "exchange_tags.csv",
    "wallet_labels.csv",
    "miner_addresses.csv",
    "network_masternodes.csv",
    "node_outputs.csv",
    "verified_sentries.csv",
    "emissions.json",
    "miners.json",
    "api/index.py",
    "static/assets/chart.umd.js",
    "scripts/install_vps_cron.sh",
    "scripts/masternode_cron_sync.sh",
    "scripts/static_snapshot_cron.sh",
    "scripts/update_vps_from_vercel_bundle.sh",
    "scripts/update_vps_from_local.sh",
)

_lock = threading.Lock()
_store: Store | None = None
_client: BlockbookClient | None = None
_rpc_client: SyscoinRpcClient | None = None
_from_height: int | None = None
_last_sync_at = 0.0
_static_page_cache: dict[str, tuple[float, bytes]] = {}


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


def get_rpc_client() -> SyscoinRpcClient | None:
    global _rpc_client
    url = os.getenv("SYS_RPC_URL")
    host = os.getenv("SYS_RPC_HOST")
    port = os.getenv("SYS_RPC_PORT", "8370")
    if not url and host:
        url = f"http://{host}:{port}/"
    if not url:
        return None
    if _rpc_client is None:
        _rpc_client = SyscoinRpcClient(url, os.getenv("SYS_RPC_USER"), os.getenv("SYS_RPC_PASSWORD"))
    return _rpc_client


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
            exists = store.conn.execute(
                """
                SELECT 1
                FROM tracked_outputs
                WHERE source_txid = ? AND source_vout = ? AND depth = ? AND path = ?
                """,
                (row["source_txid"], int(row["source_vout"]), int(row["depth"]), row["path"]),
            ).fetchone()
            if not exists:
                store.save_output(
                    {
                        "source_txid": row["source_txid"],
                        "source_vout": int(row["source_vout"]),
                        "depth": int(row["depth"]),
                        "parent_txid": None,
                        "parent_vout": None,
                        "address": row["address"],
                        "value_sats": SENTRY_COLLATERAL_SATS,
                        "attributed_sats": SENTRY_COLLATERAL_SATS,
                        "block_height": int(row["block_height"]),
                        "block_time": int(row["block_time"]),
                        "spent": None,
                        "spent_txid": None,
                        "spent_height": None,
                        "path": row["path"],
                    }
                )
    store.conn.commit()


def load_node_outputs(store: Store) -> None:
    if not NODE_OUTPUTS_PATH.exists():
        return

    def maybe_int(value: str | None) -> int | None:
        if value is None or value == "":
            return None
        return int(value)

    with NODE_OUTPUTS_PATH.open(newline="") as f:
        for row in csv.DictReader(f):
            store.save_output(
                {
                    "source_txid": row["source_txid"],
                    "source_vout": int(row["source_vout"]),
                    "depth": int(row["depth"]),
                    "parent_txid": row["parent_txid"] or None,
                    "parent_vout": maybe_int(row["parent_vout"]),
                    "address": row["address"],
                    "value_sats": int(row["value_sats"]),
                    "attributed_sats": int(row["attributed_sats"]),
                    "block_height": maybe_int(row["block_height"]),
                    "block_time": maybe_int(row["block_time"]),
                    "spent": maybe_int(row["spent"]),
                    "spent_txid": row["spent_txid"] or None,
                    "spent_height": maybe_int(row["spent_height"]),
                    "path": row["path"],
                }
            )
    store.conn.commit()


def load_remote_network_masternodes(store: Store) -> bool:
    url = os.getenv("SYS_NETWORK_MASTERNODES_URL", DEFAULT_NETWORK_MASTERNODES_URL).strip()
    if not url or url.lower() in {"0", "false", "none", "off"}:
        return False
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "sysWalletTracker/1.0"})
        with urllib.request.urlopen(req, timeout=15) as resp:
            text = resp.read().decode("utf-8")
        load_network_masternodes_csv_rows(store, csv.DictReader(io.StringIO(text)), source=url)
        return True
    except Exception:
        return False


def fetch_static_page(path: str, *, force: bool = False) -> bytes | None:
    base_url = os.getenv("SYS_TRACKER_STATIC_BASE_URL", DEFAULT_STATIC_BASE_URL).strip().rstrip("/")
    if not base_url or base_url.lower() in {"0", "false", "none", "off"}:
        return None
    if path in (*SENTRY_NODE_PATHS, *LEGACY_MASTERNODE_PATHS):
        page_name = "sentrynode.html"
    elif path in TOP_WALLETS_PATHS:
        page_name = "top-wallets.html"
    elif path == TOP_WALLETS_JSON_PATH:
        page_name = TOP_WALLETS_JSON
    elif path in EMISSIONS_PATHS:
        page_name = "emissions.html"
    elif path == EMISSIONS_JSON_PATH:
        page_name = EMISSIONS_JSON
    elif path in MINERS_PATHS:
        page_name = "miners.html"
    elif path == MINERS_JSON_PATH:
        page_name = MINERS_JSON
    elif path in SN_COMP_PATHS:
        page_name = SN_COMP_HTML
    else:
        page_name = "index.html"
    url = f"{base_url}/{page_name}"
    cache_ttl = env_int("SYS_TRACKER_STATIC_CACHE_SECONDS", 30)
    cache_key = f"{base_url}/{page_name}"
    now = time.monotonic()
    if not force and cache_ttl > 0:
        cached = _static_page_cache.get(cache_key)
        if cached and now - cached[0] <= cache_ttl:
            return cached[1]
    if force:
        url += f"?t={int(time.time())}"
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "sysWalletTracker/1.0"})
        context = None
        if url.startswith("https://"):
            candidates = [
                os.getenv("SSL_CERT_FILE"),
                os.getenv("REQUESTS_CA_BUNDLE"),
                "/etc/ssl/cert.pem",
                "/opt/homebrew/etc/ca-certificates/cert.pem",
                "/usr/local/etc/openssl@3/cert.pem",
            ]
            try:
                import certifi  # type: ignore

                candidates.insert(0, certifi.where())
            except Exception:
                pass
            for cafile in candidates:
                if cafile and Path(cafile).exists():
                    context = ssl.create_default_context(cafile=cafile)
                    break
        with urllib.request.urlopen(req, timeout=10, context=context) as resp:
            if getattr(resp, "status", 200) != 200:
                return None
            body = resp.read()
            if path in TOP_WALLETS_PATHS and b"Syscoin Top Wallets" not in body:
                return None
            if path in EMISSIONS_PATHS and b"Syscoin Network Emissions" not in body:
                return None
            if path in MINERS_PATHS and b"Syscoin Miners" not in body:
                return None
            if path in SN_COMP_PATHS and b"Syscoin SN Comp" not in body:
                return None
            if path == TOP_WALLETS_JSON_PATH and not body.lstrip().startswith(b"{"):
                return None
            if path == EMISSIONS_JSON_PATH and not body.lstrip().startswith(b"{"):
                return None
            if path == MINERS_JSON_PATH and not body.lstrip().startswith(b"{"):
                return None
            if cache_ttl > 0:
                _static_page_cache[cache_key] = (now, body)
            return body
    except Exception:
        return None


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
        load_node_outputs(store)
        load_verified_sentries(store)
        if not load_remote_network_masternodes(store):
            load_network_masternodes_csv(store)
        refresh_exchange_hot_wallet_balances(store, client)
        rpc = get_rpc_client()
        if rpc is not None:
            try:
                sync_network_masternodes(store, rpc, client)
            except Exception:
                pass
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
        refresh_node_spends(
            store,
            client,
            watched,
            limit=env_int("SYS_TRACKER_NODE_SPEND_LIMIT", 12),
            page_size=min(env_int("SYS_TRACKER_PAGE_SIZE", 1000), 100),
            max_pages_per_address=1,
        )
        _last_sync_at = now
        return store, since_time, since_label


class handler(BaseHTTPRequestHandler):
    def redirect_to_sentry_node(self) -> None:
        parsed = urllib.parse.urlparse(self.path)
        query = f"?{parsed.query}" if parsed.query else ""
        self.send_response(308)
        self.send_header("Location", f"/sentrynode{query}")
        self.send_header("Cache-Control", "public, max-age=300")
        self.send_header("Content-Length", "0")
        self.end_headers()

    def send_install_bundle(self, include_body: bool = True) -> None:
        buffer = io.BytesIO()
        with tarfile.open(fileobj=buffer, mode="w:gz") as tar:
            for relative_path in INSTALL_BUNDLE_FILES:
                path = ROOT / relative_path
                if path.exists():
                    tar.add(path, arcname=relative_path)

        body = buffer.getvalue()
        self.send_response(200)
        self.send_header("Content-Type", "application/gzip")
        self.send_header("Content-Disposition", 'attachment; filename="sysWalletTracker-vps.tgz"')
        self.send_header("Cache-Control", "no-store")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        if include_body:
            self.wfile.write(body)

    def send_dashboard(self, include_body: bool = True) -> None:
        parsed = urllib.parse.urlparse(self.path)
        if parsed.path == CHART_ASSET_ROUTE:
            body = chart_asset_bytes()
            if body is None:
                self.send_error(404)
                return
            self.send_response(200)
            self.send_header("Content-Type", "application/javascript; charset=utf-8")
            self.send_header("Cache-Control", "public, max-age=86400")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            if include_body:
                self.wfile.write(body)
            return

        if parsed.path == "/sysWalletTracker-vps.tgz":
            self.send_install_bundle(include_body=include_body)
            return

        if parsed.path in LEGACY_MASTERNODE_PATHS:
            self.redirect_to_sentry_node()
            return

        if parsed.path not in (
            "/",
            "/index.html",
            "/api/index.py",
            *SENTRY_NODE_PATHS,
            *TOP_WALLETS_PATHS,
            TOP_WALLETS_JSON_PATH,
            *EMISSIONS_PATHS,
            EMISSIONS_JSON_PATH,
            *MINERS_PATHS,
            MINERS_JSON_PATH,
            *SN_COMP_PATHS,
        ):
            self.send_error(404)
            return

        force = urllib.parse.parse_qs(parsed.query).get("force", ["0"])[0] == "1"
        content_type = (
            "application/json; charset=utf-8"
            if parsed.path in (TOP_WALLETS_JSON_PATH, EMISSIONS_JSON_PATH, MINERS_JSON_PATH)
            else "text/html; charset=utf-8"
        )
        try:
            static_body = fetch_static_page(parsed.path, force=force)
            if static_body is not None:
                self.send_response(200)
                self.send_header("Content-Type", content_type)
                cache_header = (
                    "no-store"
                    if force
                    else "public, max-age=15, s-maxage=30, stale-while-revalidate=60"
                )
                self.send_header("Cache-Control", cache_header)
                self.send_header("Content-Length", str(len(static_body)))
                self.end_headers()
                if include_body:
                    self.wfile.write(static_body)
                return

            store, since_time, since_label = sync_for_request(force=force)
            label = f"{since_label} (from {os.getenv('SYS_TRACKER_SINCE_DATE', DEFAULT_SINCE_DATE)} {os.getenv('SYS_TRACKER_TIMEZONE', DEFAULT_TIMEZONE)})"
            if parsed.path in SENTRY_NODE_PATHS:
                html_body = masternodes_html(
                    store,
                    since_time=since_time,
                    since_label=label,
                    refresh_seconds=env_int("SYS_TRACKER_PAGE_REFRESH_SECONDS", 0),
                )
            elif parsed.path in TOP_WALLETS_PATHS:
                html_body = top_wallets_html(
                    store,
                    refresh_seconds=env_int("SYS_TRACKER_PAGE_REFRESH_SECONDS", 0),
                )
            elif parsed.path == TOP_WALLETS_JSON_PATH:
                html_body = json.dumps(top_wallets_snapshot(store), indent=2)
            elif parsed.path in EMISSIONS_PATHS:
                html_body = emissions_html(
                    store,
                    refresh_seconds=env_int("SYS_TRACKER_PAGE_REFRESH_SECONDS", 0),
                )
            elif parsed.path == EMISSIONS_JSON_PATH:
                html_body = json.dumps(emissions_snapshot(store), indent=2)
            elif parsed.path in MINERS_PATHS:
                html_body = miners_html(
                    refresh_seconds=env_int("SYS_TRACKER_PAGE_REFRESH_SECONDS", 0),
                )
            elif parsed.path == MINERS_JSON_PATH:
                html_body = json.dumps(miners_snapshot(), indent=2)
            elif parsed.path in SN_COMP_PATHS:
                html_body = sn_comp_html(store, refresh_seconds=env_int("SYS_TRACKER_PAGE_REFRESH_SECONDS", 0))
            else:
                html_body = dashboard_html(
                    store,
                    since_time=since_time,
                    since_label=label,
                    refresh_seconds=env_int("SYS_TRACKER_PAGE_REFRESH_SECONDS", 0),
                )
            body = html_body.encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", content_type)
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
