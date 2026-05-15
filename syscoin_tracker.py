#!/usr/bin/env python3
"""Track Syscoin UTXO movements from a watched address.

The default provider is Syscoin's public Blockbook explorer. The tracker stores
transactions in SQLite, summarizes first-hop destinations, and can optionally
follow spent outputs to later transactions.
"""

from __future__ import annotations

import argparse
import base64
import csv
import datetime as dt
import html
import http.server
import json
import os
import ssl
import sqlite3
import sys
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from decimal import Decimal
from pathlib import Path
from typing import Any, Iterable
from zoneinfo import ZoneInfo


DEFAULT_BLOCKBOOK_URL = "https://explorer-blockbook.syscoin.org"
DEFAULT_ADDRESS = "sys1qync7erear7cvpkysvv0a28mj45g2ps0kq9c6qs"
DEFAULT_TIMEZONE = "Australia/Sydney"
DEFAULT_EXCHANGE_TAGS_PATH = Path("exchange_tags.csv")
DEFAULT_EXCHANGE_ROUTES_PATH = Path("exchange_routes.csv")
DEFAULT_EXCHANGE_HOT_WALLETS_PATH = Path("exchange_hot_wallets.csv")
SENTRY_COLLATERAL_SATS = 100_000 * 100_000_000
SATOSHI = Decimal("100000000")


def utc(ts: int | None) -> str:
    if not ts:
        return ""
    return dt.datetime.fromtimestamp(int(ts), tz=dt.timezone.utc).isoformat(timespec="seconds")


def now_iso() -> str:
    return dt.datetime.now(tz=dt.timezone.utc).isoformat(timespec="seconds")


def sats(value: Any) -> int:
    if value is None or value == "":
        return 0
    return int(value)


def fmt_sys(sat: int) -> str:
    amount = Decimal(sat) / SATOSHI
    return f"{amount:,.8f}".rstrip("0").rstrip(".")


def fmt_compact_sys(sat: int) -> str:
    amount = Decimal(sat) / SATOSHI
    sign = "-" if amount < 0 else ""
    amount = abs(amount)
    if amount >= Decimal("1000000"):
        return f"{sign}{amount / Decimal('1000000'):,.2f}M SYS"
    if amount >= Decimal("1000"):
        return f"{sign}{amount / Decimal('1000'):,.2f}K SYS"
    return f"{sign}{amount:,.2f} SYS".rstrip("0").rstrip(".")


def fmt_percent(part: int, total: int) -> str:
    if not total:
        return "0%"
    value = Decimal(part) * Decimal("100") / Decimal(total)
    return f"{value:.2f}%"


def fmt_local_datetime(ts: int | None, timezone_name: str = DEFAULT_TIMEZONE) -> str:
    if not ts:
        return ""
    local = dt.datetime.fromtimestamp(int(ts), tz=dt.timezone.utc).astimezone(ZoneInfo(timezone_name))
    return local.strftime("%b %-d, %Y %-I:%M %p")


def fmt_table_datetime(ts: int | None, timezone_name: str = DEFAULT_TIMEZONE) -> str:
    if not ts:
        return ""
    local = dt.datetime.fromtimestamp(int(ts), tz=dt.timezone.utc).astimezone(ZoneInfo(timezone_name))
    return local.strftime("%b %-d, %-I:%M %p")


def fmt_iso_local_datetime(value: str | None, timezone_name: str = DEFAULT_TIMEZONE) -> str:
    if not value:
        return ""
    text = value.replace("Z", "+00:00")
    try:
        parsed = dt.datetime.fromisoformat(text)
    except ValueError:
        return value
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=dt.timezone.utc)
    return parsed.astimezone(ZoneInfo(timezone_name)).strftime("%b %-d, %-I:%M %p")


def short_address(address: str) -> str:
    if len(address) <= 18:
        return address
    return f"{address[:10]}...{address[-8:]}"


def short_txid(txid: str) -> str:
    if len(txid) <= 18:
        return txid
    return f"{txid[:10]}...{txid[-8:]}"


def explorer_address_url(address: str) -> str:
    return f"https://explorer-blockbook.syscoin.org/address/{urllib.parse.quote(address)}"


def explorer_tx_url(txid: str) -> str:
    return f"https://explorer-blockbook.syscoin.org/tx/{urllib.parse.quote(txid)}"


def exchange_labels_for_address(address: str, exchange_tags: dict[str, str], exchange_routes: dict[str, str]) -> set[str]:
    labels = set()
    if address in exchange_tags:
        labels.add(exchange_tags[address])
    if address in exchange_routes:
        labels.add(exchange_routes[address])
    return labels


def load_exchange_tags(path: Path = DEFAULT_EXCHANGE_TAGS_PATH) -> dict[str, str]:
    if not path.exists():
        return {}
    with path.open(newline="") as f:
        return {
            row["address"].strip(): row["label"].strip()
            for row in csv.DictReader(f)
            if row.get("address") and row.get("label")
        }


def load_exchange_routes(path: Path = DEFAULT_EXCHANGE_ROUTES_PATH) -> dict[str, str]:
    if not path.exists():
        return {}
    with path.open(newline="") as f:
        return {
            row["from_address"].strip(): row["label"].strip()
            for row in csv.DictReader(f)
            if row.get("from_address") and row.get("label")
        }


def load_exchange_hot_wallets(path: Path = DEFAULT_EXCHANGE_HOT_WALLETS_PATH) -> list[dict[str, str]]:
    if not path.exists():
        return []
    wallets = []
    with path.open(newline="") as f:
        for row in csv.DictReader(f):
            label = (row.get("label") or "").strip()
            address = (row.get("address") or "").strip()
            if not label or not address:
                continue
            wallets.append(
                {
                    "label": label,
                    "address": address,
                    "note": (row.get("note") or "").strip(),
                }
            )
    return wallets


def root_path(path: str) -> str:
    return path.split(" > ", 1)[0]


def fmt_height(height: Any) -> str:
    if height is None or height == "":
        return ""
    value = int(height)
    return "unconfirmed" if value < 0 else str(value)


def sys_to_sats(value: str | None) -> int:
    if value is None:
        return 0
    return int(Decimal(value.replace(",", "")) * SATOSHI)


def parse_since_date(value: str | None, timezone_name: str) -> tuple[int | None, str | None]:
    if not value:
        return None, None
    text = value.strip()
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"

    parsed: dt.datetime | None = None
    try:
        parsed = dt.datetime.fromisoformat(text)
    except ValueError:
        for fmt in ("%Y-%m-%d %H:%M", "%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
            try:
                parsed = dt.datetime.strptime(text, fmt)
                break
            except ValueError:
                pass
    if parsed is None:
        raise ValueError(f"could not parse date: {value!r}")
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=ZoneInfo(timezone_name))
    parsed_utc = parsed.astimezone(dt.timezone.utc)
    return int(parsed_utc.timestamp()), parsed_utc.isoformat(timespec="seconds")


def addresses_from(item: dict[str, Any]) -> list[str]:
    values = item.get("addresses") or []
    if isinstance(values, str):
        return [values]
    return [str(v) for v in values if v]


def input_prev_vout(vin: dict[str, Any]) -> int:
    # Blockbook omits vout when the previous output index is zero on some coins.
    return int(vin.get("vout", 0) or 0)


class BlockbookClient:
    def __init__(self, base_url: str, timeout: int = 30, retries: int = 3, insecure_tls: bool = False) -> None:
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        self.retries = retries
        self.insecure_tls = insecure_tls
        self.ssl_context = self._ssl_context()

    def _ssl_context(self) -> ssl.SSLContext | None:
        if not self.base_url.startswith("https://"):
            return None
        if self.insecure_tls:
            return ssl._create_unverified_context()

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
                return ssl.create_default_context(cafile=cafile)
        return ssl.create_default_context()

    def get_json(self, path: str, params: dict[str, Any] | None = None) -> Any:
        query = urllib.parse.urlencode({k: v for k, v in (params or {}).items() if v is not None})
        url = f"{self.base_url}{path}"
        if query:
            url += f"?{query}"
        last_error: Exception | None = None
        for attempt in range(1, self.retries + 1):
            try:
                req = urllib.request.Request(url, headers={"User-Agent": "syscoin-tracker/0.1"})
                with urllib.request.urlopen(req, timeout=self.timeout, context=self.ssl_context) as resp:
                    return json.loads(resp.read().decode("utf-8"))
            except (urllib.error.URLError, TimeoutError, json.JSONDecodeError) as exc:
                last_error = exc
                if attempt == self.retries:
                    break
                time.sleep(0.5 * attempt)
        raise RuntimeError(f"failed to fetch {url}: {last_error}") from last_error

    def address(
        self,
        address: str,
        *,
        page: int = 1,
        page_size: int = 1000,
        details: str = "txs",
        from_height: int | None = None,
        to_height: int | None = None,
    ) -> dict[str, Any]:
        return self.get_json(
            f"/api/v2/address/{urllib.parse.quote(address)}",
            {
                "details": details,
                "page": page,
                "pageSize": page_size,
                "from": from_height,
                "to": to_height,
            },
        )

    def tx(self, txid: str) -> dict[str, Any]:
        return self.get_json(f"/api/v2/tx/{urllib.parse.quote(txid)}")

    def info(self) -> dict[str, Any]:
        return self.get_json("/api/v2")

    def block(self, height: int) -> dict[str, Any]:
        return self.get_json(f"/api/v2/block/{height}", {"pageSize": 1})


class SyscoinRpcClient:
    def __init__(self, url: str, username: str | None, password: str | None, timeout: int = 15) -> None:
        self.url = url
        self.username = username
        self.password = password
        self.timeout = timeout

    def call(self, method: str, params: list[Any] | None = None) -> Any:
        payload = json.dumps({"jsonrpc": "1.0", "id": "syscoin-tracker", "method": method, "params": params or []}).encode()
        headers = {"Content-Type": "application/json", "User-Agent": "syscoin-tracker/0.1"}
        if self.username is not None and self.password is not None:
            token = base64.b64encode(f"{self.username}:{self.password}".encode()).decode()
            headers["Authorization"] = f"Basic {token}"
        req = urllib.request.Request(self.url, data=payload, headers=headers)
        with urllib.request.urlopen(req, timeout=self.timeout) as resp:
            data = json.loads(resp.read().decode("utf-8"))
        if data.get("error"):
            raise RuntimeError(data["error"])
        return data.get("result")


def build_rpc_client(args: argparse.Namespace) -> SyscoinRpcClient | None:
    url = args.rpc_url or os.getenv("SYS_RPC_URL")
    host = args.rpc_host or os.getenv("SYS_RPC_HOST")
    port = args.rpc_port or os.getenv("SYS_RPC_PORT") or "8370"
    if not url and host:
        url = f"http://{host}:{port}/"
    if not url:
        return None
    return SyscoinRpcClient(
        url,
        args.rpc_user or os.getenv("SYS_RPC_USER"),
        args.rpc_password or os.getenv("SYS_RPC_PASSWORD"),
    )


def normalize_outpoint(value: str) -> str:
    return value.replace("-", ":").replace(",", ":")


def collect_masternode_outpoints(rpc: SyscoinRpcClient) -> set[str]:
    result = rpc.call("masternode_list", ["json"])
    outpoints: set[str] = set()
    if isinstance(result, dict):
        for key, info in result.items():
            if isinstance(key, str):
                outpoints.add(normalize_outpoint(key))
            if isinstance(info, dict):
                txid = info.get("collateralHash") or info.get("collateralTxHash") or info.get("txhash")
                n = info.get("collateralIndex") or info.get("collateralOutpointIndex") or info.get("outputidx")
                if txid is not None and n is not None:
                    outpoints.add(f"{txid}:{n}")
    return outpoints


def verify_sentry_candidates(store: Store, rpc: SyscoinRpcClient, since_time: int | None = None) -> str:
    outpoints = collect_masternode_outpoints(rpc)
    where = "WHERE value_sats = ?"
    params: list[Any] = [SENTRY_COLLATERAL_SATS]
    if since_time:
        where += " AND block_time >= ?"
        params.append(since_time)
    rows = store.conn.execute(
        f"""
        SELECT source_txid, source_vout, depth, address, block_height, block_time, path
        FROM tracked_outputs
        {where}
        ORDER BY block_time DESC
        """,
        params,
    ).fetchall()
    matches = []
    for row in rows:
        outpoint = f"{row['source_txid']}:{row['source_vout']}"
        if outpoint in outpoints:
            store.save_verified_sentry(row)
            matches.append([outpoint, row["depth"], row["address"], fmt_height(row["block_height"]), utc(row["block_time"])])
    lines = [
        f"100k SYS candidates checked: {len(rows)}",
        f"Masternode outpoints from RPC: {len(outpoints)}",
        f"Verified matches: {len(matches)}",
    ]
    if matches:
        lines.append("")
        lines.append(rows_to_markdown(["outpoint", "depth", "address", "height", "time UTC"], matches))
    return "\n".join(lines)


def block_height_at_or_after(client: BlockbookClient, target_ts: int) -> int:
    info = client.info()
    best_height = int(info.get("blockbook", {}).get("bestHeight") or info.get("backend", {}).get("blocks"))
    lo = 0
    hi = best_height
    while lo < hi:
        mid = (lo + hi) // 2
        block = client.block(mid)
        if int(block.get("time", 0)) < target_ts:
            lo = mid + 1
        else:
            hi = mid
    return lo


class Store:
    def __init__(self, path: Path) -> None:
        self.path = path
        self.conn = sqlite3.connect(path, check_same_thread=False)
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.execute("PRAGMA foreign_keys=ON")
        self.init()

    def init(self) -> None:
        self.conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS metadata (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS transactions (
                txid TEXT PRIMARY KEY,
                block_height INTEGER,
                block_time INTEGER,
                confirmations INTEGER,
                value_sats INTEGER,
                value_in_sats INTEGER,
                fee_sats INTEGER,
                raw_json TEXT NOT NULL,
                first_seen_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS movements (
                txid TEXT PRIMARY KEY REFERENCES transactions(txid) ON DELETE CASCADE,
                direction TEXT NOT NULL,
                input_sats INTEGER NOT NULL,
                output_to_watch_sats INTEGER NOT NULL,
                external_sats INTEGER NOT NULL,
                fee_sats INTEGER NOT NULL,
                block_height INTEGER,
                block_time INTEGER,
                created_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS tracked_outputs (
                source_txid TEXT NOT NULL,
                source_vout INTEGER NOT NULL,
                depth INTEGER NOT NULL DEFAULT 1,
                parent_txid TEXT,
                parent_vout INTEGER,
                address TEXT NOT NULL,
                value_sats INTEGER NOT NULL,
                attributed_sats INTEGER NOT NULL,
                block_height INTEGER,
                block_time INTEGER,
                spent INTEGER,
                spent_txid TEXT,
                spent_height INTEGER,
                path TEXT NOT NULL,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                PRIMARY KEY (source_txid, source_vout, depth, address, path)
            );

            CREATE TABLE IF NOT EXISTS alerts (
                txid TEXT PRIMARY KEY,
                emitted_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS verified_sentries (
                outpoint TEXT PRIMARY KEY,
                source_txid TEXT NOT NULL,
                source_vout INTEGER NOT NULL,
                depth INTEGER NOT NULL,
                address TEXT NOT NULL,
                block_height INTEGER,
                block_time INTEGER,
                path TEXT NOT NULL,
                verified_at TEXT NOT NULL
            );

            CREATE INDEX IF NOT EXISTS idx_movements_block
                ON movements(block_height, block_time);
            CREATE INDEX IF NOT EXISTS idx_tracked_outputs_address
                ON tracked_outputs(address);
            CREATE INDEX IF NOT EXISTS idx_tracked_outputs_spent
                ON tracked_outputs(spent, spent_txid);
            """
        )
        self.conn.commit()

    def set_meta(self, key: str, value: Any) -> None:
        self.conn.execute(
            "INSERT INTO metadata(key, value) VALUES(?, ?) "
            "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            (key, json.dumps(value)),
        )
        self.conn.commit()

    def get_meta(self, key: str, default: Any = None) -> Any:
        row = self.conn.execute("SELECT value FROM metadata WHERE key = ?", (key,)).fetchone()
        if not row:
            return default
        return json.loads(row["value"])

    def save_tx(self, tx: dict[str, Any]) -> bool:
        txid = tx["txid"]
        exists = self.conn.execute("SELECT 1 FROM transactions WHERE txid = ?", (txid,)).fetchone()
        stamp = now_iso()
        self.conn.execute(
            """
            INSERT INTO transactions(
                txid, block_height, block_time, confirmations, value_sats,
                value_in_sats, fee_sats, raw_json, first_seen_at, updated_at
            )
            VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(txid) DO UPDATE SET
                block_height=excluded.block_height,
                block_time=excluded.block_time,
                confirmations=excluded.confirmations,
                value_sats=excluded.value_sats,
                value_in_sats=excluded.value_in_sats,
                fee_sats=excluded.fee_sats,
                raw_json=excluded.raw_json,
                updated_at=excluded.updated_at
            """,
            (
                txid,
                tx.get("blockHeight"),
                tx.get("blockTime"),
                tx.get("confirmations"),
                sats(tx.get("value")),
                sats(tx.get("valueIn")),
                sats(tx.get("fees")),
                json.dumps(tx, separators=(",", ":")),
                stamp,
                stamp,
            ),
        )
        return exists is None

    def save_movement(self, movement: dict[str, Any]) -> None:
        self.conn.execute(
            """
            INSERT INTO movements(
                txid, direction, input_sats, output_to_watch_sats, external_sats,
                fee_sats, block_height, block_time, created_at
            )
            VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(txid) DO UPDATE SET
                direction=excluded.direction,
                input_sats=excluded.input_sats,
                output_to_watch_sats=excluded.output_to_watch_sats,
                external_sats=excluded.external_sats,
                fee_sats=excluded.fee_sats,
                block_height=excluded.block_height,
                block_time=excluded.block_time
            """,
            (
                movement["txid"],
                movement["direction"],
                movement["input_sats"],
                movement["output_to_watch_sats"],
                movement["external_sats"],
                movement["fee_sats"],
                movement["block_height"],
                movement["block_time"],
                now_iso(),
            ),
        )

    def save_output(self, output: dict[str, Any]) -> None:
        stamp = now_iso()
        self.conn.execute(
            """
            INSERT INTO tracked_outputs(
                source_txid, source_vout, depth, parent_txid, parent_vout, address,
                value_sats, attributed_sats, block_height, block_time, spent,
                spent_txid, spent_height, path, created_at, updated_at
            )
            VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(source_txid, source_vout, depth, address, path) DO UPDATE SET
                parent_txid=excluded.parent_txid,
                parent_vout=excluded.parent_vout,
                value_sats=excluded.value_sats,
                attributed_sats=excluded.attributed_sats,
                block_height=excluded.block_height,
                block_time=excluded.block_time,
                spent=excluded.spent,
                spent_txid=COALESCE(excluded.spent_txid, tracked_outputs.spent_txid),
                spent_height=COALESCE(excluded.spent_height, tracked_outputs.spent_height),
                updated_at=excluded.updated_at
            """,
            (
                output["source_txid"],
                output["source_vout"],
                output.get("depth", 1),
                output.get("parent_txid"),
                output.get("parent_vout"),
                output["address"],
                output["value_sats"],
                output["attributed_sats"],
                output.get("block_height"),
                output.get("block_time"),
                output.get("spent"),
                output.get("spent_txid"),
                output.get("spent_height"),
                output["path"],
                stamp,
                stamp,
            ),
        )

    def mark_spent(self, source_txid: str, source_vout: int, depth: int, path: str, spent_txid: str, spent_height: int | None) -> None:
        self.conn.execute(
            """
            UPDATE tracked_outputs
            SET spent = 1, spent_txid = ?, spent_height = ?, updated_at = ?
            WHERE source_txid = ? AND source_vout = ? AND depth = ? AND path = ?
            """,
            (spent_txid, spent_height, now_iso(), source_txid, source_vout, depth, path),
        )

    def mark_unspent(self, source_txid: str, source_vout: int, depth: int, path: str) -> None:
        self.conn.execute(
            """
            UPDATE tracked_outputs
            SET spent = 0, spent_txid = NULL, spent_height = NULL, updated_at = ?
            WHERE source_txid = ? AND source_vout = ? AND depth = ? AND path = ?
            """,
            (now_iso(), source_txid, source_vout, depth, path),
        )

    def unsent_alerts(self) -> list[sqlite3.Row]:
        return self.conn.execute(
            """
            SELECT m.*, t.raw_json
            FROM movements m
            JOIN transactions t USING(txid)
            LEFT JOIN alerts a USING(txid)
            WHERE a.txid IS NULL AND m.direction = 'out'
            ORDER BY COALESCE(m.block_height, 999999999), m.txid
            """
        ).fetchall()

    def mark_alerted(self, txid: str) -> None:
        self.conn.execute(
            "INSERT OR IGNORE INTO alerts(txid, emitted_at) VALUES(?, ?)",
            (txid, now_iso()),
        )
        self.conn.commit()

    def save_verified_sentry(self, row: sqlite3.Row) -> None:
        outpoint = f"{row['source_txid']}:{row['source_vout']}"
        self.conn.execute(
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
                outpoint,
                row["source_txid"],
                row["source_vout"],
                row["depth"],
                row["address"],
                row["block_height"],
                row["block_time"],
                row["path"],
                now_iso(),
            ),
        )
        self.conn.commit()


def analyze_tx(tx: dict[str, Any], watched: set[str]) -> tuple[dict[str, Any] | None, list[dict[str, Any]]]:
    txid = tx["txid"]
    input_sats = 0
    for vin in tx.get("vin") or []:
        if watched.intersection(addresses_from(vin)):
            input_sats += sats(vin.get("value"))

    output_to_watch_sats = 0
    external_sats = 0
    outputs: list[dict[str, Any]] = []

    for vout in tx.get("vout") or []:
        out_addresses = addresses_from(vout)
        value = sats(vout.get("value"))
        is_change = bool(watched.intersection(out_addresses))
        if is_change:
            output_to_watch_sats += value
        elif input_sats > 0:
            external_sats += value
            for address in out_addresses or ["unknown"]:
                outputs.append(
                    {
                        "source_txid": txid,
                        "source_vout": int(vout.get("n", 0)),
                        "depth": 1,
                        "parent_txid": None,
                        "parent_vout": None,
                        "address": address,
                        "value_sats": value,
                        "attributed_sats": value,
                        "block_height": tx.get("blockHeight"),
                        "block_time": tx.get("blockTime"),
                        "spent": 1 if vout.get("spent") else 0 if vout.get("spent") is False else None,
                        "spent_txid": vout.get("spentTxId"),
                        "spent_height": vout.get("spentHeight"),
                        "path": f"{txid}:{int(vout.get('n', 0))}",
                    }
                )

    if input_sats == 0 and output_to_watch_sats == 0:
        return None, []

    if input_sats > 0 and external_sats > 0:
        direction = "out"
    elif input_sats > 0:
        direction = "self"
    else:
        direction = "in"

    movement = {
        "txid": txid,
        "direction": direction,
        "input_sats": input_sats,
        "output_to_watch_sats": output_to_watch_sats,
        "external_sats": external_sats,
        "fee_sats": sats(tx.get("fees")),
        "block_height": tx.get("blockHeight"),
        "block_time": tx.get("blockTime"),
    }
    return movement, outputs


def sync_address(
    store: Store,
    client: BlockbookClient,
    address: str,
    *,
    page_size: int,
    max_pages: int | None,
    from_height: int | None,
    watched: set[str],
    quiet: bool = False,
) -> dict[str, int]:
    page = 1
    seen = 0
    inserted = 0
    outbound = 0
    total_pages = 1
    latest_summary: dict[str, Any] | None = None

    while page <= total_pages:
        data = client.address(address, page=page, page_size=page_size, details="txs", from_height=from_height)
        latest_summary = {k: v for k, v in data.items() if k != "transactions"}
        total_pages = int(data.get("totalPages", 1) or 1)
        if max_pages is not None:
            total_pages = min(total_pages, max_pages)

        txs = data.get("transactions") or []
        if not quiet:
            print(f"Fetched page {page}/{total_pages}: {len(txs)} transactions", file=sys.stderr)

        for tx in txs:
            was_inserted = store.save_tx(tx)
            inserted += 1 if was_inserted else 0
            movement, outputs = analyze_tx(tx, watched)
            if movement:
                store.save_movement(movement)
                outbound += 1 if movement["direction"] == "out" else 0
            for output in outputs:
                store.save_output(output)
            seen += 1
        store.conn.commit()
        page += 1

    if latest_summary:
        store.set_meta(
            "last_summary",
            {
                "address": address,
                "synced_at": now_iso(),
                "balance_sats": sats(latest_summary.get("balance")),
                "total_received_sats": sats(latest_summary.get("totalReceived")),
                "total_sent_sats": sats(latest_summary.get("totalSent")),
                "txs": latest_summary.get("txs"),
                "block_height": latest_summary.get("blockHeight") or latest_summary.get("block"),
            },
        )
    return {"seen": seen, "inserted": inserted, "outbound": outbound}


def find_spending_tx(
    client: BlockbookClient,
    address: str,
    source_txid: str,
    source_vout: int,
    *,
    page_size: int,
    max_pages: int | None,
    from_height: int | None,
) -> dict[str, Any] | None:
    page = 1
    total_pages = 1
    while page <= total_pages:
        data = client.address(address, page=page, page_size=page_size, details="txs", from_height=from_height)
        total_pages = int(data.get("totalPages", 1) or 1)
        if max_pages is not None:
            total_pages = min(total_pages, max_pages)
        for tx in data.get("transactions") or []:
            if tx.get("txid") == source_txid:
                continue
            for vin in tx.get("vin") or []:
                if vin.get("txid") == source_txid and input_prev_vout(vin) == source_vout:
                    return tx
        page += 1
    return None


def follow_outputs(
    store: Store,
    client: BlockbookClient,
    watched: set[str],
    *,
    depth: int,
    limit: int,
    min_sats: int,
    page_size: int,
    max_pages_per_address: int | None,
) -> dict[str, int]:
    examined = 0
    found_spends = 0
    created = 0

    for current_depth in range(1, depth + 1):
        rows = store.conn.execute(
            """
            SELECT *
            FROM tracked_outputs
            WHERE depth = ?
              AND attributed_sats >= ?
              AND (spent_txid IS NULL OR spent_txid = '')
            ORDER BY attributed_sats DESC
            LIMIT ?
            """,
            (current_depth, min_sats, limit),
        ).fetchall()
        if not rows:
            continue

        for row in rows:
            examined += 1
            spend = find_spending_tx(
                client,
                row["address"],
                row["source_txid"],
                row["source_vout"],
                page_size=page_size,
                max_pages=max_pages_per_address,
                from_height=row["block_height"],
            )
            if not spend:
                continue

            found_spends += 1
            store.save_tx(spend)
            store.mark_spent(
                row["source_txid"],
                row["source_vout"],
                row["depth"],
                row["path"],
                spend["txid"],
                spend.get("blockHeight"),
            )

            if current_depth >= depth:
                continue

            value_in = max(sats(spend.get("valueIn")), 1)
            tracked_share = Decimal(row["attributed_sats"]) / Decimal(value_in)
            for vout in spend.get("vout") or []:
                out_addresses = addresses_from(vout)
                value = sats(vout.get("value"))
                if not value or watched.intersection(out_addresses) or row["address"] in out_addresses:
                    continue
                attributed = int(Decimal(value) * tracked_share)
                if attributed <= 0:
                    continue
                for address in out_addresses or ["unknown"]:
                    store.save_output(
                        {
                            "source_txid": spend["txid"],
                            "source_vout": int(vout.get("n", 0)),
                            "depth": current_depth + 1,
                            "parent_txid": row["source_txid"],
                            "parent_vout": row["source_vout"],
                            "address": address,
                            "value_sats": value,
                            "attributed_sats": attributed,
                            "block_height": spend.get("blockHeight"),
                            "block_time": spend.get("blockTime"),
                            "spent": 1 if vout.get("spent") else 0 if vout.get("spent") is False else None,
                            "spent_txid": vout.get("spentTxId"),
                            "spent_height": vout.get("spentHeight"),
                            "path": f"{row['path']} > {spend['txid']}:{int(vout.get('n', 0))}",
                        }
                    )
                    created += 1
            store.conn.commit()

    return {"examined": examined, "found_spends": found_spends, "created": created}


def refresh_spent_first_hops(
    store: Store,
    client: BlockbookClient,
    watched: set[str],
    *,
    since_time: int | None,
    limit: int,
    min_sats: int,
    page_size: int,
    max_pages_per_address: int,
) -> dict[str, int]:
    where = """
        t.depth = 1
        AND t.spent = 1
        AND t.attributed_sats >= ?
        AND NOT EXISTS (
            SELECT 1
            FROM tracked_outputs c
            WHERE c.depth = 2
              AND c.path LIKE t.path || ' > %'
        )
    """
    params: list[Any] = [min_sats]
    if since_time is not None:
        where += " AND t.block_time >= ?"
        params.append(since_time)
    params.append(limit)

    rows = store.conn.execute(
        f"""
        SELECT t.*
        FROM tracked_outputs t
        WHERE {where}
        ORDER BY t.block_time DESC, t.attributed_sats DESC
        LIMIT ?
        """,
        params,
    ).fetchall()

    examined = 0
    found_spends = 0
    created = 0
    errors = 0

    for row in rows:
        examined += 1
        spend = None
        if row["spent_txid"]:
            try:
                spend = client.tx(row["spent_txid"])
            except Exception:
                spend = None
        if spend is None:
            try:
                spend = find_spending_tx(
                    client,
                    row["address"],
                    row["source_txid"],
                    row["source_vout"],
                    page_size=page_size,
                    max_pages=max_pages_per_address,
                    from_height=row["block_height"],
                )
            except Exception:
                errors += 1
                continue
        if not spend:
            continue

        found_spends += 1
        store.save_tx(spend)
        store.mark_spent(
            row["source_txid"],
            row["source_vout"],
            row["depth"],
            row["path"],
            spend["txid"],
            spend.get("blockHeight"),
        )

        value_in = max(sats(spend.get("valueIn")), 1)
        tracked_share = Decimal(row["attributed_sats"]) / Decimal(value_in)
        for vout in spend.get("vout") or []:
            out_addresses = addresses_from(vout)
            value = sats(vout.get("value"))
            if not value or watched.intersection(out_addresses) or row["address"] in out_addresses:
                continue
            attributed = int(Decimal(value) * tracked_share)
            if attributed <= 0:
                continue
            for address in out_addresses or ["unknown"]:
                store.save_output(
                    {
                        "source_txid": spend["txid"],
                        "source_vout": int(vout.get("n", 0)),
                        "depth": 2,
                        "parent_txid": row["source_txid"],
                        "parent_vout": row["source_vout"],
                        "address": address,
                        "value_sats": value,
                        "attributed_sats": attributed,
                        "block_height": spend.get("blockHeight"),
                        "block_time": spend.get("blockTime"),
                        "spent": 1 if vout.get("spent") else 0 if vout.get("spent") is False else None,
                        "spent_txid": vout.get("spentTxId"),
                        "spent_height": vout.get("spentHeight"),
                        "path": f"{row['path']} > {spend['txid']}:{int(vout.get('n', 0))}",
                    }
                )
                created += 1
        store.conn.commit()

    return {"examined": examined, "found_spends": found_spends, "created": created, "errors": errors}


def refresh_node_spends(
    store: Store,
    client: BlockbookClient,
    watched: set[str],
    *,
    limit: int,
    page_size: int,
    max_pages_per_address: int,
) -> dict[str, int]:
    rows = store.conn.execute(
        """
        SELECT t.*
        FROM tracked_outputs t
        LEFT JOIN verified_sentries v
          ON v.source_txid = t.source_txid
         AND v.source_vout = t.source_vout
         AND v.depth = t.depth
         AND v.path = t.path
        WHERE t.value_sats = ?
          AND (
            t.spent IS NULL
            OR t.spent = 0
            OR (
              t.spent = 1
              AND t.spent_txid IS NOT NULL
              AND NOT EXISTS (
                SELECT 1
                FROM tracked_outputs c
                WHERE c.parent_txid = t.source_txid
                  AND c.parent_vout = t.source_vout
                  AND c.source_txid = t.spent_txid
              )
            )
          )
        ORDER BY CASE WHEN v.outpoint IS NULL THEN 1 ELSE 0 END,
                 COALESCE(t.block_time, 0) DESC
        LIMIT ?
        """,
        (SENTRY_COLLATERAL_SATS, limit),
    ).fetchall()

    examined = 0
    found_spends = 0
    active = 0
    created = 0
    errors = 0

    for row in rows:
        examined += 1
        spend = None
        spent_txid = row["spent_txid"]
        spent_height = row["spent_height"]

        if row["spent"] != 1 or not spent_txid:
            try:
                source_tx = client.tx(row["source_txid"])
                store.save_tx(source_tx)
                source_vout = next(
                    (vout for vout in source_tx.get("vout") or [] if int(vout.get("n", 0)) == int(row["source_vout"])),
                    None,
                )
            except Exception:
                errors += 1
                continue

            if not source_vout:
                errors += 1
                continue

            if source_vout.get("spent") is False:
                store.mark_unspent(row["source_txid"], row["source_vout"], row["depth"], row["path"])
                store.conn.commit()
                active += 1
                continue

            if source_vout.get("spent") is True:
                spent_txid = source_vout.get("spentTxId") or spent_txid
                spent_height = source_vout.get("spentHeight") or spent_height

        if spent_txid:
            try:
                spend = client.tx(spent_txid)
            except Exception:
                spend = None

        if spend is None:
            try:
                spend = find_spending_tx(
                    client,
                    row["address"],
                    row["source_txid"],
                    row["source_vout"],
                    page_size=page_size,
                    max_pages=max_pages_per_address,
                    from_height=row["block_height"],
                )
            except Exception:
                errors += 1
                continue

        if not spend:
            continue

        found_spends += 1
        store.save_tx(spend)
        store.mark_spent(
            row["source_txid"],
            row["source_vout"],
            row["depth"],
            row["path"],
            spend["txid"],
            spend.get("blockHeight") or spent_height,
        )

        value_in = max(sats(spend.get("valueIn")), 1)
        tracked_share = Decimal(row["attributed_sats"]) / Decimal(value_in)
        for vout in spend.get("vout") or []:
            out_addresses = addresses_from(vout)
            value = sats(vout.get("value"))
            if not value or watched.intersection(out_addresses):
                continue
            attributed = int(Decimal(value) * tracked_share)
            if attributed <= 0:
                continue
            for address in out_addresses or ["unknown"]:
                store.save_output(
                    {
                        "source_txid": spend["txid"],
                        "source_vout": int(vout.get("n", 0)),
                        "depth": int(row["depth"]) + 1,
                        "parent_txid": row["source_txid"],
                        "parent_vout": row["source_vout"],
                        "address": address,
                        "value_sats": value,
                        "attributed_sats": attributed,
                        "block_height": spend.get("blockHeight"),
                        "block_time": spend.get("blockTime"),
                        "spent": 1 if vout.get("spent") else 0 if vout.get("spent") is False else None,
                        "spent_txid": vout.get("spentTxId"),
                        "spent_height": vout.get("spentHeight"),
                        "path": f"{row['path']} > {spend['txid']}:{int(vout.get('n', 0))}",
                    }
                )
                created += 1
        store.conn.commit()

    return {"examined": examined, "found_spends": found_spends, "active": active, "created": created, "errors": errors}


def rows_to_markdown(headers: list[str], rows: Iterable[Iterable[Any]]) -> str:
    table = ["| " + " | ".join(headers) + " |", "| " + " | ".join(["---"] * len(headers)) + " |"]
    for row in rows:
        table.append("| " + " | ".join(str(v) for v in row) + " |")
    return "\n".join(table)


def report(
    store: Store,
    *,
    top: int,
    all_destinations: bool,
    since_height: int | None,
    since_time: int | None,
    since_label: str | None,
    min_sats: int,
    csv_path: Path | None = None,
) -> str:
    params: list[Any] = []
    where = "WHERE direction = 'out'"
    if since_height:
        where += " AND block_height >= ?"
        params.append(since_height)
    if since_time:
        where += " AND block_time >= ?"
        params.append(since_time)

    summary = store.conn.execute(
        f"""
        SELECT COUNT(*) AS txs,
               COALESCE(SUM(external_sats), 0) AS external_sats,
               COALESCE(SUM(fee_sats), 0) AS fees,
               MIN(CASE WHEN block_height >= 0 THEN block_height END) AS first_height,
               MAX(CASE WHEN block_height >= 0 THEN block_height END) AS last_height,
               SUM(CASE WHEN block_height < 0 THEN 1 ELSE 0 END) AS unconfirmed_txs
        FROM movements
        {where}
        """,
        params,
    ).fetchone()

    top_where = "WHERE depth = 1 AND attributed_sats >= ?"
    top_params: list[Any] = [min_sats]
    if since_height:
        top_where += " AND block_height >= ?"
        top_params.append(since_height)
    if since_time:
        top_where += " AND block_time >= ?"
        top_params.append(since_time)
    destination_limit = ""
    if not all_destinations:
        destination_limit = "LIMIT ?"
        top_params.append(top)

    top_rows = store.conn.execute(
        f"""
        SELECT address,
               COUNT(*) AS outputs,
               SUM(attributed_sats) AS total_sats,
               MAX(block_height) AS last_height,
               MAX(block_time) AS last_time
        FROM tracked_outputs
        {top_where}
        GROUP BY address
        ORDER BY total_sats DESC
        {destination_limit}
        """,
        top_params,
    ).fetchall()

    recent_rows = store.conn.execute(
        f"""
        SELECT m.txid, m.block_height, m.block_time, m.external_sats
        FROM movements m
        {where}
        ORDER BY COALESCE(m.block_height, 0) DESC, m.block_time DESC
        LIMIT ?
        """,
        params + [top],
    ).fetchall()

    if csv_path:
        with csv_path.open("w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(["rank", "address", "outputs", "total_sys", "last_height", "last_time_utc"])
            for rank, row in enumerate(top_rows, 1):
                writer.writerow([rank, row["address"], row["outputs"], fmt_sys(row["total_sats"]), fmt_height(row["last_height"]), utc(row["last_time"])])

    last_summary = store.get_meta("last_summary", {})
    lines = []
    if last_summary:
        lines.append(f"Address: {last_summary.get('address')}")
        lines.append(
            "Balance: "
            f"{fmt_sys(last_summary.get('balance_sats', 0))} SYS; "
            f"transactions: {last_summary.get('txs')}; "
            f"synced: {last_summary.get('synced_at')}"
        )
        lines.append("")
    if since_label:
        lines.append(f"Window starts: {since_label}")
        lines.append("")

    lines.append(
        f"Outbound transactions: {summary['txs']} | "
        f"external amount: {fmt_sys(summary['external_sats'])} SYS | "
        f"fees: {fmt_sys(summary['fees'])} SYS | "
        f"height range: {fmt_height(summary['first_height'])}..{fmt_height(summary['last_height'])}"
    )
    if summary["unconfirmed_txs"]:
        lines[-1] += f" | unconfirmed outbound: {summary['unconfirmed_txs']}"
    lines.append("")
    heading = "All first-hop destinations ranked by SYS amount" if all_destinations else "Top first-hop destinations"
    lines.append(heading)
    lines.append(
        rows_to_markdown(
            ["rank", "address", "outputs", "total SYS", "last height", "last seen UTC"],
            [
                [rank, row["address"], row["outputs"], fmt_sys(row["total_sats"]), fmt_height(row["last_height"]), utc(row["last_time"])]
                for rank, row in enumerate(top_rows, 1)
            ],
        )
    )
    lines.append("")
    lines.append("Recent outbound transactions")
    lines.append(
        rows_to_markdown(
            ["txid", "height", "time UTC", "external SYS"],
            [[row["txid"], fmt_height(row["block_height"]), utc(row["block_time"]), fmt_sys(row["external_sats"])] for row in recent_rows],
        )
    )
    if csv_path:
        lines.append("")
        lines.append(f"Wrote CSV: {csv_path}")
    return "\n".join(lines)


def post_webhook(url: str, payload: dict[str, Any]) -> None:
    body = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(url, data=body, headers={"Content-Type": "application/json", "User-Agent": "syscoin-tracker/0.1"})
    with urllib.request.urlopen(req, timeout=20) as resp:
        resp.read()


def emit_alerts(store: Store, webhook_url: str | None) -> int:
    count = 0
    for row in store.unsent_alerts():
        tx = json.loads(row["raw_json"])
        payload = {
            "text": f"Syscoin hot wallet outbound: {fmt_sys(row['external_sats'])} SYS in {row['txid']}",
            "txid": row["txid"],
            "block_height": row["block_height"],
            "block_time_utc": utc(row["block_time"]),
            "external_sats": row["external_sats"],
            "external_sys": fmt_sys(row["external_sats"]),
            "destinations": [
                {
                    "address": addr,
                    "value_sys": fmt_sys(sats(vout.get("value"))),
                    "vout": vout.get("n"),
                }
                for vout in tx.get("vout") or []
                for addr in addresses_from(vout)
            ],
        }
        if webhook_url:
            post_webhook(webhook_url, payload)
        else:
            print(json.dumps(payload, indent=2))
        store.mark_alerted(row["txid"])
        count += 1
    return count


def mark_existing_alerts(store: Store) -> int:
    rows = store.unsent_alerts()
    for row in rows:
        store.mark_alerted(row["txid"])
    return len(rows)


def dashboard_html(
    store: Store,
    since_time: int | None = None,
    since_label: str | None = None,
    refresh_seconds: int = 60,
) -> str:
    last_summary = store.get_meta("last_summary", {})
    top_where = "WHERE depth = 1"
    top_params: list[Any] = []
    recent_where = "WHERE direction = 'out'"
    recent_params: list[Any] = []
    if since_time:
        top_where += " AND block_time >= ?"
        top_params.append(since_time)
        recent_where += " AND block_time >= ?"
        recent_params.append(since_time)

    window_summary = store.conn.execute(
        f"""
        SELECT COUNT(*) AS txs,
               COALESCE(SUM(external_sats), 0) AS external_sats,
               MIN(CASE WHEN block_height >= 0 THEN block_height END) AS first_height,
               MAX(CASE WHEN block_height >= 0 THEN block_height END) AS last_height
        FROM movements
        {recent_where}
        """,
        recent_params,
    ).fetchone()
    destination_count = store.conn.execute(
        f"""
        SELECT COUNT(*) AS addresses
        FROM (
            SELECT address
            FROM tracked_outputs
            {top_where}
            GROUP BY address
        )
        """,
        top_params,
    ).fetchone()["addresses"]
    top_rows = store.conn.execute(
        f"""
        SELECT address,
               COUNT(*) AS outputs,
               SUM(attributed_sats) AS total_sats,
               SUM(CASE WHEN spent = 1 THEN attributed_sats ELSE 0 END) AS moved_sats,
               MIN(block_time) AS first_time,
               MAX(block_time) AS last_time
        FROM tracked_outputs
        {top_where}
        GROUP BY address
        ORDER BY total_sats DESC
        """,
        top_params,
    ).fetchall()
    first_hop_outputs = store.conn.execute(
        f"""
        SELECT address, path, value_sats
        FROM tracked_outputs
        {top_where}
        """,
        top_params,
    ).fetchall()
    roots_by_address: dict[str, set[str]] = {}
    address_by_root: dict[str, str] = {}
    sentry_candidates_by_address: dict[str, int] = {}
    for row in first_hop_outputs:
        roots_by_address.setdefault(row["address"], set()).add(row["path"])
        address_by_root[row["path"]] = row["address"]
        if row["value_sats"] == SENTRY_COLLATERAL_SATS:
            sentry_candidates_by_address[row["address"]] = sentry_candidates_by_address.get(row["address"], 0) + 1

    exchange_tags = load_exchange_tags()
    exchange_routes = load_exchange_routes()
    exchange_hot_wallets = load_exchange_hot_wallets()
    later_exchanges_by_address: dict[str, set[str]] = {}
    downstream_sentry_by_address: dict[str, int] = {}
    verified_sentry_by_address: dict[str, int] = {}
    downstream_sats_by_address: dict[str, int] = {}
    if first_hop_outputs:
        downstream_rows = store.conn.execute(
            """
            SELECT address, value_sats, path
            FROM tracked_outputs
            WHERE depth > 1
            """
        ).fetchall()
        for row in downstream_rows:
            origin = address_by_root.get(root_path(row["path"]))
            if not origin:
                continue
            if row["address"] in exchange_tags:
                later_exchanges_by_address.setdefault(origin, set()).add(exchange_tags[row["address"]])
            if row["value_sats"] == SENTRY_COLLATERAL_SATS:
                downstream_sentry_by_address[origin] = downstream_sentry_by_address.get(origin, 0) + 1
            downstream_sats_by_address[origin] = downstream_sats_by_address.get(origin, 0) + int(row["value_sats"])
        verified_rows = store.conn.execute(
            """
            SELECT path
            FROM verified_sentries
            """
        ).fetchall()
        for row in verified_rows:
            origin = address_by_root.get(root_path(row["path"]))
            if origin:
                verified_sentry_by_address[origin] = verified_sentry_by_address.get(origin, 0) + 1

    def explorer_addr(addr: str) -> str:
        return f"https://explorer-blockbook.syscoin.org/address/{urllib.parse.quote(addr)}"

    hot_wallet_rows = []
    for wallet in exchange_hot_wallets:
        note = wallet["note"]
        note_html = f"<span>{html.escape(note)}</span>" if note else ""
        hot_wallet_rows.append(
            f"<article class='wallet-card'>"
            f"<strong>{html.escape(wallet['label'])}</strong>"
            f"{note_html}"
            f"<a href='{explorer_addr(wallet['address'])}'>{html.escape(wallet['address'])}</a>"
            f"</article>"
        )
    hot_wallet_html = "\n".join(hot_wallet_rows)

    html_rows = []
    for rank, row in enumerate(top_rows, 1):
        exchange_bits = set()
        if row["address"] in exchange_routes:
            exchange_bits.add(exchange_routes[row["address"]])
        if row["address"] in exchange_tags:
            exchange_bits.add(exchange_tags[row["address"]])
        exchange_bits.update(later_exchanges_by_address.get(row["address"], set()))
        downstream_sats = downstream_sats_by_address.get(row["address"], 0)
        if (
            not exchange_bits
            and row["total_sats"] >= sys_to_sats("100")
            and downstream_sats >= int(Decimal(row["total_sats"]) * Decimal("0.98"))
            and not sentry_candidates_by_address.get(row["address"], 0)
            and not downstream_sentry_by_address.get(row["address"], 0)
            and not verified_sentry_by_address.get(row["address"], 0)
        ):
            exchange_bits.add("Ex-like")
        exchange_text = ", ".join(sorted(exchange_bits)) if exchange_bits else "-"

        direct_sentry = sentry_candidates_by_address.get(row["address"], 0)
        downstream_sentry = downstream_sentry_by_address.get(row["address"], 0)
        verified_sentry = verified_sentry_by_address.get(row["address"], 0)
        sentry_bits = []
        if verified_sentry:
            sentry_bits.append(f"verified {verified_sentry}")
        if direct_sentry:
            sentry_bits.append(f"{direct_sentry} direct")
        if downstream_sentry:
            sentry_bits.append(f"{downstream_sentry} later")
        sentry_text = ", ".join(sentry_bits) if sentry_bits else "-"
        sentry_sort = verified_sentry * 10000 + direct_sentry * 100 + downstream_sentry
        full_address = row["address"]
        display_address = short_address(full_address)
        last_seen_full = fmt_local_datetime(row["last_time"])
        last_seen_short = fmt_table_datetime(row["last_time"])

        html_rows.append(
            f"<tr><td class='rank' data-sort='{rank}'>{rank}</td>"
            f"<td class='address' data-sort='{html.escape(full_address.lower())}'><a href='{explorer_addr(full_address)}' title='{html.escape(full_address)}'>{html.escape(display_address)}</a></td>"
            f"<td class='amount' data-sort='{row['total_sats']}'>{fmt_compact_sys(row['total_sats'])}</td>"
            f"<td data-sort='{row['total_sats']}'>{fmt_percent(row['total_sats'], window_summary['external_sats'])}</td>"
            f"<td data-sort='{row['outputs']}'>{row['outputs']}</td>"
            f"<td class='amount subtle' data-sort='{row['moved_sats'] or 0}'>{fmt_compact_sys(row['moved_sats']) if row['moved_sats'] else '-'}</td>"
            f"<td data-sort='{html.escape(exchange_text.lower())}'>{html.escape(exchange_text)}</td>"
            f"<td data-sort='{sentry_sort}'>{html.escape(sentry_text)}</td>"
            f"<td data-sort='{row['last_time'] or 0}' title='{html.escape(last_seen_full)}'>{html.escape(last_seen_short)}</td></tr>"
        )
    top_html = "\n".join(html_rows)
    since_text = f"{fmt_local_datetime(since_time)} Sydney" if since_time else "all tracked history"
    updated_text = fmt_iso_local_datetime(last_summary.get("synced_at"))
    total_sent_full = fmt_sys(window_summary["external_sats"])

    return f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <meta http-equiv="refresh" content="{max(refresh_seconds, 1)}">
  <title>Syscoin Hot Wallet Tracker</title>
  <style>
    :root {{ color-scheme: light dark; font-family: Inter, ui-sans-serif, system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif; --page-gutter: clamp(24px, 3.8vw, 80px); }}
    *, *::before, *::after {{ box-sizing: border-box; }}
    html, body {{ margin: 0; max-width: 100%; overflow-x: hidden; }}
    body {{ background: #f7f5f0; color: #1c2227; }}
    header {{ background: #142026; color: #f8fafc; padding: 24px 0 22px; width: 100%; }}
    .header-inner, main {{ margin-left: var(--page-gutter); margin-right: var(--page-gutter); width: auto; }}
    .topbar {{ display: flex; align-items: end; justify-content: space-between; gap: 16px; }}
    h1 {{ font-size: 1.8rem; margin: 0 0 8px; letter-spacing: 0; }}
    .subtitle {{ color: #c9d5d8; font-size: 0.98rem; line-height: 1.45; }}
    .nav {{ display: flex; flex-wrap: wrap; gap: 8px; justify-content: flex-end; }}
    .nav a {{ border: 1px solid rgba(248, 250, 252, 0.28); border-radius: 999px; color: #dbe6e9; font-size: 0.84rem; padding: 7px 11px; }}
    .nav a.active {{ background: #f8fafc; color: #142026; }}
    main {{ display: grid; gap: 22px; margin-top: 22px; margin-bottom: 22px; padding: 0; }}
    main > * {{ min-width: 0; }}
    .metrics {{ display: grid; grid-template-columns: repeat(4, minmax(150px, 1fr)); gap: 12px; }}
    .metric {{ background: #fff; border: 1px solid #d9ded8; border-radius: 8px; padding: 14px 16px; min-width: 0; }}
    .metric span {{ display: block; color: #687177; font-size: 0.84rem; margin-bottom: 6px; }}
    .metric b {{ display: block; font-size: clamp(1.25rem, 2vw, 1.65rem); line-height: 1.1; overflow-wrap: anywhere; }}
    .panel-title {{ display: flex; align-items: end; justify-content: space-between; gap: 16px; }}
    .panel-title h2 {{ margin: 0; font-size: 1.25rem; }}
    .panel-title p {{ margin: 0; color: #687177; font-size: 0.9rem; }}
    .wallet-list {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(310px, 1fr)); gap: 10px; margin-top: 10px; }}
    .wallet-card {{ background: #fff; border: 1px solid #d9ded8; border-radius: 8px; padding: 12px 14px; min-width: 0; display: grid; gap: 5px; }}
    .wallet-card strong {{ font-size: 0.95rem; }}
    .wallet-card span {{ color: #687177; font-size: 0.82rem; }}
    .wallet-card a {{ font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, "Liberation Mono", monospace; font-size: 0.78rem; overflow-wrap: anywhere; }}
    .table-wrap {{ background: #fff; border: 1px solid #d9ded8; border-radius: 8px; max-width: 100%; min-width: 0; overflow-x: auto; width: 100%; }}
    table {{ width: 100%; min-width: 1100px; border-collapse: separate; border-spacing: 0; background: #fff; table-layout: fixed; }}
    th, td {{ padding: 8px 10px; border-bottom: 1px solid #e4e8e2; text-align: left; font-size: 0.9rem; overflow: hidden; text-overflow: ellipsis; }}
    thead {{ position: sticky; top: 0; z-index: 10; }}
    th {{ background: #eaf0ec; position: sticky; top: 0; z-index: 11; box-shadow: 0 1px 0 #d9ded8; }}
    .sort-button {{ appearance: none; border: 0; background: transparent; color: inherit; cursor: pointer; display: inline-flex; align-items: center; gap: 4px; font: inherit; font-weight: 700; padding: 0; text-align: inherit; white-space: nowrap; }}
    .sort-button:hover, .sort-button:focus-visible {{ color: #086788; outline: none; }}
    .sort-icon {{ color: #687177; display: inline-block; font-size: 0.75rem; min-width: 1ch; }}
    th[aria-sort="ascending"] .sort-icon::before {{ content: "^"; }}
    th[aria-sort="descending"] .sort-icon::before {{ content: "v"; }}
    th[aria-sort="none"] .sort-icon::before {{ content: ""; }}
    .floating-table-header {{ background: #fff; border: 1px solid #d9ded8; border-top: 0; box-shadow: 0 5px 14px rgba(20, 32, 38, 0.12); display: none; left: 0; position: fixed; top: 0; z-index: 1000; }}
    .floating-table-header table {{ border-collapse: separate; border-spacing: 0; table-layout: fixed; width: 100%; }}
    .floating-table-header th {{ position: static; top: auto; }}
    th:nth-child(1), td:nth-child(1) {{ width: 58px; text-align: right; color: #687177; }}
    th:nth-child(2), td:nth-child(2) {{ width: 250px; }}
    th:nth-child(3), td:nth-child(3) {{ width: 112px; }}
    th:nth-child(4), td:nth-child(4) {{ width: 82px; }}
    th:nth-child(5), td:nth-child(5) {{ width: 74px; }}
    th:nth-child(6), td:nth-child(6) {{ width: 112px; }}
    th:nth-child(7), td:nth-child(7) {{ width: 142px; }}
    th:nth-child(8), td:nth-child(8) {{ width: 138px; }}
    th:nth-child(9), td:nth-child(9) {{ width: 132px; }}
    th:nth-child(3), td:nth-child(3),
    th:nth-child(4), td:nth-child(4),
    th:nth-child(5), td:nth-child(5),
    th:nth-child(6), td:nth-child(6) {{ text-align: right; white-space: nowrap; }}
    th:nth-child(7), td:nth-child(7),
    th:nth-child(8), td:nth-child(8),
    th:nth-child(9), td:nth-child(9) {{ white-space: nowrap; }}
    .address {{ font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, "Liberation Mono", monospace; white-space: nowrap; }}
    .amount {{ font-weight: 700; }}
    .subtle {{ color: #687177; font-weight: 600; }}
    a {{ color: #086788; text-decoration: none; }}
    code {{ overflow-wrap: anywhere; }}
    @media(max-width: 820px) {{
      .metrics {{ grid-template-columns: repeat(2, minmax(0, 1fr)); }}
      .panel-title {{ align-items: start; flex-direction: column; }}
      .topbar {{ align-items: start; flex-direction: column; }}
      .nav {{ justify-content: flex-start; }}
      .header-inner, main {{ margin-left: 12px; margin-right: 12px; }}
      table {{ min-width: 1060px; }}
    }}
    @media (prefers-color-scheme: dark) {{
      body {{ background: #121619; color: #f3f4f6; }}
      .metric, .wallet-card, .table-wrap, table, .floating-table-header {{ background: #1c2328; border-color: #334047; }}
      th {{ background: #263139; }}
      th, td {{ border-color: #334047; }}
      a {{ color: #67d7ff; }}
      .subtitle {{ color: #b6c3c7; }}
      .metric span, .wallet-card span, .panel-title p, th:nth-child(1), td:nth-child(1), .subtle, .sort-icon {{ color: #a7b0b5; }}
      .sort-button:hover, .sort-button:focus-visible {{ color: #67d7ff; }}
    }}
  </style>
</head>
<body>
  <header>
    <div class="header-inner">
      <div class="topbar">
        <div>
          <h1>Syscoin Hot Wallet Tracker</h1>
          <div class="subtitle">Binance hot wallet movements since {html.escape(since_text)}</div>
        </div>
        <nav class="nav" aria-label="Dashboard pages">
          <a class="active" href="/">Wallet Flows</a>
          <a href="/masternodes">Masternodes</a>
        </nav>
      </div>
    </div>
  </header>
  <main>
    <section class="metrics">
      <div class="metric"><span>SYS Sent</span><b title="{html.escape(total_sent_full)} SYS">{fmt_compact_sys(window_summary["external_sats"])}</b></div>
      <div class="metric"><span>Transfers</span><b>{window_summary["txs"]}</b></div>
      <div class="metric"><span>Recipients</span><b>{destination_count}</b></div>
      <div class="metric"><span>Updated</span><b>{html.escape(updated_text)}</b></div>
    </section>
    <section class="hot-wallets">
      <div class="panel-title">
        <h2>Known Exchange Hot Wallets</h2>
        <p>{len(exchange_hot_wallets)} Blockbook links</p>
      </div>
      <div class="wallet-list">{hot_wallet_html}</div>
    </section>
    <section>
      <div class="panel-title">
        <h2>Recipients Ranked By SYS</h2>
        <p>{destination_count} addresses, high to low</p>
      </div>
      <div class="table-wrap"><table id="destinations-table"><thead><tr><th data-sort="number" data-default-dir="asc" aria-sort="ascending"><button class="sort-button" type="button">Rank<span class="sort-icon" aria-hidden="true"></span></button></th><th data-sort="text" data-default-dir="asc" aria-sort="none"><button class="sort-button" type="button">Address<span class="sort-icon" aria-hidden="true"></span></button></th><th data-sort="number" data-default-dir="desc" aria-sort="none"><button class="sort-button" type="button">SYS Sent<span class="sort-icon" aria-hidden="true"></span></button></th><th data-sort="number" data-default-dir="desc" aria-sort="none"><button class="sort-button" type="button">Share<span class="sort-icon" aria-hidden="true"></span></button></th><th data-sort="number" data-default-dir="desc" aria-sort="none"><button class="sort-button" type="button">Sends<span class="sort-icon" aria-hidden="true"></span></button></th><th data-sort="number" data-default-dir="desc" title="Amount already spent by the first recipient address" aria-sort="none"><button class="sort-button" type="button">Sent Again<span class="sort-icon" aria-hidden="true"></span></button></th><th data-sort="text" data-default-dir="asc" aria-sort="none"><button class="sort-button" type="button">Exchange<span class="sort-icon" aria-hidden="true"></span></button></th><th data-sort="number" data-default-dir="desc" title="Exact 100,000 SYS output seen at this hop or the next traced hop" aria-sort="none"><button class="sort-button" type="button">100k Node<span class="sort-icon" aria-hidden="true"></span></button></th><th data-sort="number" data-default-dir="desc" aria-sort="none"><button class="sort-button" type="button">Last Seen<span class="sort-icon" aria-hidden="true"></span></button></th></tr></thead><tbody>{top_html}</tbody></table></div>
    </section>
  </main>
  <script>
    (() => {{
      const table = document.getElementById("destinations-table");
      if (!table) return;
      const headers = Array.from(table.querySelectorAll("th[data-sort]"));
      const tbody = table.tBodies[0];
      const storageKey = "syscoin-destinations-sort";
      let activeIndex = 0;
      let activeDirection = "asc";
      const floatingWrap = document.createElement("div");
      floatingWrap.className = "floating-table-header";
      const floatingTable = document.createElement("table");
      const floatingHead = table.tHead.cloneNode(true);
      floatingTable.appendChild(floatingHead);
      floatingWrap.appendChild(floatingTable);
      document.body.appendChild(floatingWrap);
      const floatingHeaders = Array.from(floatingHead.querySelectorAll("th[data-sort]"));

      const cellValue = (row, index, type) => {{
        const raw = row.cells[index]?.dataset.sort ?? row.cells[index]?.textContent ?? "";
        if (type === "number") return Number(raw) || 0;
        return raw.toLowerCase();
      }};

      const updateHeaderState = (index, direction) => {{
        [headers, floatingHeaders].forEach((headerGroup) => {{
          headerGroup.forEach((header, headerIndex) => {{
            header.setAttribute(
              "aria-sort",
              headerIndex === index ? (direction === "asc" ? "ascending" : "descending") : "none",
            );
          }});
        }});
      }};

      const syncFloatingHeader = () => {{
        const tableRect = table.getBoundingClientRect();
        const headRect = table.tHead.getBoundingClientRect();
        const shouldShow = tableRect.top < 0 && tableRect.bottom > headRect.height;
        floatingWrap.style.display = shouldShow ? "block" : "none";
        if (!shouldShow) return;
        floatingWrap.style.left = `${{tableRect.left}}px`;
        floatingWrap.style.width = `${{tableRect.width}}px`;
        headers.forEach((header, index) => {{
          const width = header.getBoundingClientRect().width;
          floatingHeaders[index].style.width = `${{width}}px`;
        }});
      }};

      const sortRows = (index, direction, persist = true) => {{
        const type = headers[index].dataset.sort;
        const multiplier = direction === "asc" ? 1 : -1;
        const rows = Array.from(tbody.rows);
        rows.sort((left, right) => {{
          const leftValue = cellValue(left, index, type);
          const rightValue = cellValue(right, index, type);
          if (type === "number") return (leftValue - rightValue) * multiplier;
          return String(leftValue).localeCompare(String(rightValue)) * multiplier;
        }});
        rows.forEach((row) => tbody.appendChild(row));
        activeIndex = index;
        activeDirection = direction;
        updateHeaderState(index, direction);
        syncFloatingHeader();
        if (persist) {{
          localStorage.setItem(storageKey, JSON.stringify({{ index, direction }}));
        }}
      }};

      const bindSortButtons = (headerGroup) => {{
        headerGroup.forEach((header, index) => {{
          const button = header.querySelector("button");
          button?.addEventListener("click", () => {{
            const nextDirection =
              activeIndex === index
                ? (activeDirection === "asc" ? "desc" : "asc")
                : (header.dataset.defaultDir || "asc");
            sortRows(index, nextDirection);
          }});
        }});
      }};

      bindSortButtons(headers);
      bindSortButtons(floatingHeaders);
      window.addEventListener("scroll", syncFloatingHeader, {{ passive: true }});
      window.addEventListener("resize", syncFloatingHeader);
      syncFloatingHeader();

      try {{
        const saved = JSON.parse(localStorage.getItem(storageKey) || "null");
        if (
          saved &&
          Number.isInteger(saved.index) &&
          headers[saved.index] &&
          (saved.direction === "asc" || saved.direction === "desc")
        ) {{
          sortRows(saved.index, saved.direction, false);
        }}
      }} catch (_error) {{
        localStorage.removeItem(storageKey);
      }}
    }})();
  </script>
</body>
</html>"""


def masternodes_html(
    store: Store,
    since_time: int | None = None,
    since_label: str | None = None,
    refresh_seconds: int = 60,
) -> str:
    exchange_tags = load_exchange_tags()
    exchange_routes = load_exchange_routes()
    for wallet in load_exchange_hot_wallets():
        exchange_tags.setdefault(wallet["address"], wallet["label"])

    roots = store.conn.execute(
        """
        SELECT path, address
        FROM tracked_outputs
        WHERE depth = 1
        """
    ).fetchall()
    address_by_root = {row["path"]: row["address"] for row in roots}

    node_rows = store.conn.execute(
        """
        SELECT t.*,
               CASE WHEN v.outpoint IS NULL THEN 0 ELSE 1 END AS verified
        FROM tracked_outputs t
        LEFT JOIN verified_sentries v
          ON v.source_txid = t.source_txid
         AND v.source_vout = t.source_vout
         AND v.depth = t.depth
         AND v.path = t.path
        WHERE t.value_sats = ?
        ORDER BY COALESCE(t.block_time, 0) DESC, t.address
        """,
        (SENTRY_COLLATERAL_SATS,),
    ).fetchall()

    spend_times = {
        row["txid"]: row["block_time"]
        for row in store.conn.execute(
            """
            SELECT txid, block_time
            FROM transactions
            WHERE txid IN (
                SELECT spent_txid
                FROM tracked_outputs
                WHERE value_sats = ? AND spent_txid IS NOT NULL
            )
            """,
            (SENTRY_COLLATERAL_SATS,),
        ).fetchall()
    }

    def child_outputs_for(row: sqlite3.Row) -> list[dict[str, Any]]:
        if not row["spent_txid"]:
            return []
        tracked_children = store.conn.execute(
            """
            SELECT address, value_sats, attributed_sats, block_time
            FROM tracked_outputs
            WHERE parent_txid = ?
              AND parent_vout = ?
              AND source_txid = ?
            ORDER BY value_sats DESC
            """,
            (row["source_txid"], row["source_vout"], row["spent_txid"]),
        ).fetchall()
        if tracked_children:
            return [dict(child) for child in tracked_children]

        tx_row = store.conn.execute("SELECT raw_json FROM transactions WHERE txid = ?", (row["spent_txid"],)).fetchone()
        if not tx_row:
            return []
        try:
            tx = json.loads(tx_row["raw_json"])
        except json.JSONDecodeError:
            return []
        children = []
        tx_time = tx.get("blockTime")
        for vout in tx.get("vout") or []:
            value = sats(vout.get("value"))
            if not value:
                continue
            for address in addresses_from(vout) or ["unknown"]:
                children.append({"address": address, "value_sats": value, "attributed_sats": value, "block_time": tx_time})
        return children

    prepared_rows = []
    for row in node_rows:
        origin_address = address_by_root.get(root_path(row["path"]), "")
        origin_labels = exchange_labels_for_address(origin_address, exchange_tags, exchange_routes) if origin_address else set()
        children = child_outputs_for(row)
        exit_labels: set[str] = set()
        largest_exit = 0
        for child in children:
            child_address = str(child["address"])
            child_value = int(child["value_sats"])
            largest_exit = max(largest_exit, child_value)
            if child_address == "unknown":
                continue
            exit_labels.update(exchange_labels_for_address(child_address, exchange_tags, exchange_routes))
        if not exit_labels and row["spent"] == 1 and largest_exit >= int(SENTRY_COLLATERAL_SATS * Decimal("0.98")):
            exit_labels.add("Ex-like")

        child_times = [int(child["block_time"]) for child in children if child.get("block_time")]
        spend_time = spend_times.get(row["spent_txid"]) if row["spent_txid"] else None
        if spend_time is None and child_times:
            spend_time = max(child_times)
        taken_down_sort = spend_time or row["spent_height"] or 0
        is_taken_down = row["spent"] == 1
        prepared_rows.append(
            {
                "row": row,
                "type": "Verified" if row["verified"] else "Candidate",
                "type_sort": 1 if row["verified"] else 0,
                "status": "Taken down" if is_taken_down else "Active",
                "status_sort": 0 if is_taken_down else 1,
                "origin_address": origin_address,
                "origin_labels": ", ".join(sorted(origin_labels)) if origin_labels else "-",
                "exit": ", ".join(sorted(exit_labels)) if exit_labels else "-",
                "exit_sort": ",".join(sorted(exit_labels)).lower() if exit_labels else "",
                "setup_time": row["block_time"],
                "taken_down_time": spend_time,
                "taken_down_sort": taken_down_sort,
            }
        )

    verified_count = sum(1 for item in prepared_rows if item["type"] == "Verified")
    taken_down_count = sum(1 for item in prepared_rows if item["status"] == "Taken down")
    exchange_exit_count = sum(1 for item in prepared_rows if item["status"] == "Taken down" and item["exit"] != "-")
    active_count = len(prepared_rows) - taken_down_count

    html_rows = []
    for item in prepared_rows:
        row = item["row"]
        outpoint = f"{row['source_txid']}:{row['source_vout']}"
        origin_address = item["origin_address"]
        origin_html = (
            f"<a href='{explorer_address_url(origin_address)}' title='{html.escape(origin_address)}'>{html.escape(short_address(origin_address))}</a>"
            if origin_address
            else "-"
        )
        spend_html = (
            f"<a href='{explorer_tx_url(row['spent_txid'])}' title='{html.escape(row['spent_txid'])}'>{html.escape(short_txid(row['spent_txid']))}</a>"
            if row["spent_txid"]
            else "-"
        )
        html_rows.append(
            f"<tr>"
            f"<td data-sort='{item['type_sort']}'><span class='badge {'verified' if item['type'] == 'Verified' else 'candidate'}'>{html.escape(item['type'])}</span></td>"
            f"<td data-sort='{item['status_sort']}'><span class='status {'down' if item['status'] == 'Taken down' else 'active'}'>{html.escape(item['status'])}</span></td>"
            f"<td data-sort='{html.escape(item['origin_labels'].lower())}'>{html.escape(item['origin_labels'])}</td>"
            f"<td class='address' data-sort='{html.escape(origin_address.lower())}'>{origin_html}</td>"
            f"<td class='address' data-sort='{html.escape(row['address'].lower())}'><a href='{explorer_address_url(row['address'])}' title='{html.escape(row['address'])}'>{html.escape(short_address(row['address']))}</a></td>"
            f"<td data-sort='{row['block_time'] or 0}' title='{html.escape(fmt_local_datetime(row['block_time']))}'>{html.escape(fmt_table_datetime(row['block_time']))}</td>"
            f"<td data-sort='{item['taken_down_sort']}' title='{html.escape(fmt_local_datetime(item['taken_down_time']))}'>{html.escape(fmt_table_datetime(item['taken_down_time'])) if item['taken_down_time'] else '-'}</td>"
            f"<td data-sort='{html.escape(item['exit_sort'])}'>{html.escape(item['exit'])}</td>"
            f"<td class='address' data-sort='{html.escape(outpoint.lower())}'><a href='{explorer_tx_url(row['source_txid'])}' title='{html.escape(outpoint)}'>{html.escape(short_txid(outpoint))}</a></td>"
            f"<td class='address' data-sort='{html.escape((row['spent_txid'] or '').lower())}'>{spend_html}</td>"
            f"</tr>"
        )

    rows_html = "\n".join(html_rows)
    since_text = f"{fmt_local_datetime(since_time)} Sydney" if since_time else "all tracked history"
    updated_text = fmt_iso_local_datetime(store.get_meta("last_summary", {}).get("synced_at"))
    total_count = len(prepared_rows)

    return f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <meta http-equiv="refresh" content="{max(refresh_seconds, 1)}">
  <title>Syscoin Masternode Tracker</title>
  <style>
    :root {{ color-scheme: light dark; font-family: Inter, ui-sans-serif, system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif; --page-gutter: clamp(24px, 3.8vw, 80px); }}
    *, *::before, *::after {{ box-sizing: border-box; }}
    html, body {{ margin: 0; max-width: 100%; overflow-x: hidden; }}
    body {{ background: #f7f5f0; color: #1c2227; }}
    header {{ background: #142026; color: #f8fafc; padding: 24px 0 22px; width: 100%; }}
    .header-inner, main {{ margin-left: var(--page-gutter); margin-right: var(--page-gutter); width: auto; }}
    .topbar {{ display: flex; align-items: end; justify-content: space-between; gap: 16px; }}
    h1 {{ font-size: 1.8rem; margin: 0 0 8px; letter-spacing: 0; }}
    .subtitle {{ color: #c9d5d8; font-size: 0.98rem; line-height: 1.45; }}
    .nav {{ display: flex; flex-wrap: wrap; gap: 8px; justify-content: flex-end; }}
    .nav a {{ border: 1px solid rgba(248, 250, 252, 0.28); border-radius: 999px; color: #dbe6e9; font-size: 0.84rem; padding: 7px 11px; text-decoration: none; }}
    .nav a.active {{ background: #f8fafc; color: #142026; }}
    main {{ display: grid; gap: 22px; margin-top: 22px; margin-bottom: 22px; padding: 0; }}
    main > * {{ min-width: 0; }}
    .metrics {{ display: grid; grid-template-columns: repeat(5, minmax(130px, 1fr)); gap: 12px; }}
    .metric {{ background: #fff; border: 1px solid #d9ded8; border-radius: 8px; padding: 14px 16px; min-width: 0; }}
    .metric span {{ display: block; color: #687177; font-size: 0.84rem; margin-bottom: 6px; }}
    .metric b {{ display: block; font-size: clamp(1.25rem, 2vw, 1.65rem); line-height: 1.1; overflow-wrap: anywhere; }}
    .panel-title {{ display: flex; align-items: end; justify-content: space-between; gap: 16px; }}
    .panel-title h2 {{ margin: 0; font-size: 1.25rem; }}
    .panel-title p {{ margin: 0; color: #687177; font-size: 0.9rem; }}
    .table-wrap {{ background: #fff; border: 1px solid #d9ded8; border-radius: 8px; max-width: 100%; min-width: 0; overflow-x: auto; width: 100%; }}
    table {{ width: 100%; min-width: 1250px; border-collapse: separate; border-spacing: 0; background: #fff; table-layout: fixed; }}
    th, td {{ padding: 8px 10px; border-bottom: 1px solid #e4e8e2; text-align: left; font-size: 0.88rem; overflow: hidden; text-overflow: ellipsis; }}
    th {{ background: #eaf0ec; position: sticky; top: 0; z-index: 10; box-shadow: 0 1px 0 #d9ded8; }}
    .sort-button {{ appearance: none; border: 0; background: transparent; color: inherit; cursor: pointer; display: inline-flex; align-items: center; gap: 4px; font: inherit; font-weight: 700; padding: 0; text-align: inherit; white-space: nowrap; }}
    .sort-button:hover, .sort-button:focus-visible {{ color: #086788; outline: none; }}
    .sort-icon {{ color: #687177; display: inline-block; font-size: 0.75rem; min-width: 1ch; }}
    th[aria-sort="ascending"] .sort-icon::before {{ content: "^"; }}
    th[aria-sort="descending"] .sort-icon::before {{ content: "v"; }}
    th[aria-sort="none"] .sort-icon::before {{ content: ""; }}
    th:nth-child(1), td:nth-child(1) {{ width: 92px; }}
    th:nth-child(2), td:nth-child(2) {{ width: 110px; }}
    th:nth-child(3), td:nth-child(3) {{ width: 110px; }}
    th:nth-child(4), td:nth-child(4) {{ width: 205px; }}
    th:nth-child(5), td:nth-child(5) {{ width: 205px; }}
    th:nth-child(6), td:nth-child(6) {{ width: 122px; }}
    th:nth-child(7), td:nth-child(7) {{ width: 122px; }}
    th:nth-child(8), td:nth-child(8) {{ width: 100px; }}
    th:nth-child(9), td:nth-child(9) {{ width: 150px; }}
    th:nth-child(10), td:nth-child(10) {{ width: 150px; }}
    .address {{ font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, "Liberation Mono", monospace; white-space: nowrap; }}
    .badge, .status {{ border-radius: 999px; display: inline-flex; font-size: 0.78rem; font-weight: 700; padding: 4px 8px; white-space: nowrap; }}
    .badge.verified {{ background: #dceee7; color: #0d5d40; }}
    .badge.candidate {{ background: #eef1f3; color: #48535a; }}
    .status.active {{ background: #e7f2ff; color: #095c9f; }}
    .status.down {{ background: #fff0df; color: #8a4c00; }}
    a {{ color: #086788; text-decoration: none; }}
    @media(max-width: 920px) {{
      .metrics {{ grid-template-columns: repeat(2, minmax(0, 1fr)); }}
      .panel-title {{ align-items: start; flex-direction: column; }}
      .topbar {{ align-items: start; flex-direction: column; }}
      .nav {{ justify-content: flex-start; }}
      .header-inner, main {{ margin-left: 12px; margin-right: 12px; }}
    }}
    @media (prefers-color-scheme: dark) {{
      body {{ background: #121619; color: #f3f4f6; }}
      .metric, .table-wrap, table {{ background: #1c2328; border-color: #334047; }}
      th {{ background: #263139; }}
      th, td {{ border-color: #334047; }}
      a {{ color: #67d7ff; }}
      .subtitle {{ color: #b6c3c7; }}
      .metric span, .panel-title p, .sort-icon {{ color: #a7b0b5; }}
      .sort-button:hover, .sort-button:focus-visible {{ color: #67d7ff; }}
      .badge.verified {{ background: #15382b; color: #b4f0d8; }}
      .badge.candidate {{ background: #2a3339; color: #d7dee2; }}
      .status.active {{ background: #15304a; color: #b9dcff; }}
      .status.down {{ background: #442d13; color: #ffd9a8; }}
    }}
  </style>
</head>
<body>
  <header>
    <div class="header-inner">
      <div class="topbar">
        <div>
          <h1>Syscoin Masternode Tracker</h1>
          <div class="subtitle">100k SYS collateral outputs tied to Binance wallet flows since {html.escape(since_text)}</div>
        </div>
        <nav class="nav" aria-label="Dashboard pages">
          <a href="/">Wallet Flows</a>
          <a class="active" href="/masternodes">Masternodes</a>
        </nav>
      </div>
    </div>
  </header>
  <main>
    <section class="metrics">
      <div class="metric"><span>Total 100k Outputs</span><b>{total_count}</b></div>
      <div class="metric"><span>Verified Nodes</span><b>{verified_count}</b></div>
      <div class="metric"><span>Active</span><b>{active_count}</b></div>
      <div class="metric"><span>Taken Down</span><b>{taken_down_count}</b></div>
      <div class="metric"><span>Exit To Exchange</span><b>{exchange_exit_count}</b></div>
    </section>
    <section>
      <div class="panel-title">
        <h2>Masternode Collateral Timeline</h2>
        <p>Updated {html.escape(updated_text)}</p>
      </div>
      <div class="table-wrap">
        <table>
          <thead>
            <tr>
              <th data-sort="number" data-default-dir="desc" aria-sort="none"><button class="sort-button" type="button">Type<span class="sort-icon" aria-hidden="true"></span></button></th>
              <th data-sort="number" data-default-dir="asc" aria-sort="none"><button class="sort-button" type="button">Status<span class="sort-icon" aria-hidden="true"></span></button></th>
              <th data-sort="text" data-default-dir="asc" aria-sort="none"><button class="sort-button" type="button">Funding Trail<span class="sort-icon" aria-hidden="true"></span></button></th>
              <th data-sort="text" data-default-dir="asc" aria-sort="none"><button class="sort-button" type="button">Funding Address<span class="sort-icon" aria-hidden="true"></span></button></th>
              <th data-sort="text" data-default-dir="asc" aria-sort="none"><button class="sort-button" type="button">Node Address<span class="sort-icon" aria-hidden="true"></span></button></th>
              <th data-sort="number" data-default-dir="desc" aria-sort="descending"><button class="sort-button" type="button">Setup<span class="sort-icon" aria-hidden="true"></span></button></th>
              <th data-sort="number" data-default-dir="desc" aria-sort="none"><button class="sort-button" type="button">Taken Down<span class="sort-icon" aria-hidden="true"></span></button></th>
              <th data-sort="text" data-default-dir="asc" aria-sort="none"><button class="sort-button" type="button">Exit<span class="sort-icon" aria-hidden="true"></span></button></th>
              <th data-sort="text" data-default-dir="asc" aria-sort="none"><button class="sort-button" type="button">Outpoint<span class="sort-icon" aria-hidden="true"></span></button></th>
              <th data-sort="text" data-default-dir="asc" aria-sort="none"><button class="sort-button" type="button">Spend Tx<span class="sort-icon" aria-hidden="true"></span></button></th>
            </tr>
          </thead>
          <tbody>{rows_html}</tbody>
        </table>
      </div>
    </section>
  </main>
  <script>
    (() => {{
      const table = document.querySelector("table");
      if (!table) return;
      const headers = Array.from(table.querySelectorAll("th[data-sort]"));
      const tbody = table.tBodies[0];
      let activeIndex = 5;
      let activeDirection = "desc";

      const cellValue = (row, index, type) => {{
        const raw = row.cells[index]?.dataset.sort ?? row.cells[index]?.textContent ?? "";
        if (type === "number") return Number(raw) || 0;
        return raw.toLowerCase();
      }};

      const updateHeaderState = (index, direction) => {{
        headers.forEach((header, headerIndex) => {{
          header.setAttribute(
            "aria-sort",
            headerIndex === index ? (direction === "asc" ? "ascending" : "descending") : "none",
          );
        }});
      }};

      const sortRows = (index, direction) => {{
        const type = headers[index].dataset.sort;
        const multiplier = direction === "asc" ? 1 : -1;
        const rows = Array.from(tbody.rows);
        rows.sort((left, right) => {{
          const leftValue = cellValue(left, index, type);
          const rightValue = cellValue(right, index, type);
          if (type === "number") return (leftValue - rightValue) * multiplier;
          return String(leftValue).localeCompare(String(rightValue)) * multiplier;
        }});
        rows.forEach((row) => tbody.appendChild(row));
        activeIndex = index;
        activeDirection = direction;
        updateHeaderState(index, direction);
      }};

      headers.forEach((header, index) => {{
        header.querySelector("button")?.addEventListener("click", () => {{
          const nextDirection =
            activeIndex === index
              ? (activeDirection === "asc" ? "desc" : "asc")
              : (header.dataset.defaultDir || "asc");
          sortRows(index, nextDirection);
        }});
      }});
    }})();
  </script>
</body>
</html>"""


def dashboard_sync_loop(
    db_path: Path,
    blockbook_url: str,
    insecure_tls: bool,
    address: str,
    page_size: int,
    max_pages: int | None,
    from_height: int | None,
    since_time: int | None,
    interval: int,
) -> None:
    sync_store = Store(db_path)
    sync_client = BlockbookClient(blockbook_url, insecure_tls=insecure_tls)
    watched = {address}
    while True:
        started = time.monotonic()
        try:
            stats = sync_address(
                sync_store,
                sync_client,
                address,
                page_size=page_size,
                max_pages=max_pages,
                from_height=from_height,
                watched=watched,
                quiet=True,
            )
            follow_stats = refresh_spent_first_hops(
                sync_store,
                sync_client,
                watched,
                since_time=since_time,
                limit=8,
                min_sats=sys_to_sats("100"),
                page_size=min(page_size, 100),
                max_pages_per_address=1,
            )
            node_stats = refresh_node_spends(
                sync_store,
                sync_client,
                watched,
                limit=12,
                page_size=min(page_size, 100),
                max_pages_per_address=1,
            )
            print(
                f"{now_iso()} dashboard sync seen={stats['seen']} new={stats['inserted']} "
                f"next_hop_found={follow_stats['found_spends']} next_hop_outputs={follow_stats['created']} "
                f"next_hop_errors={follow_stats['errors']} "
                f"node_spends={node_stats['found_spends']} node_outputs={node_stats['created']} "
                f"node_errors={node_stats['errors']}",
                file=sys.stderr,
            )
        except Exception as exc:
            print(f"{now_iso()} dashboard sync failed: {exc}", file=sys.stderr)
        elapsed = time.monotonic() - started
        time.sleep(max(1, interval - int(elapsed)))


def serve(
    store: Store,
    host: str,
    port: int,
    since_time: int | None = None,
    since_label: str | None = None,
    refresh_seconds: int = 60,
) -> None:
    class Handler(http.server.BaseHTTPRequestHandler):
        def send_page(self, include_body: bool = True) -> None:
            parsed = urllib.parse.urlparse(self.path)
            if parsed.path in ("/", "/index.html"):
                html_body = dashboard_html(
                    store,
                    since_time=since_time,
                    since_label=since_label,
                    refresh_seconds=refresh_seconds,
                )
            elif parsed.path in ("/masternodes", "/masternodes.html"):
                html_body = masternodes_html(
                    store,
                    since_time=since_time,
                    since_label=since_label,
                    refresh_seconds=refresh_seconds,
                )
            else:
                self.send_error(404)
                return

            body = html_body.encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            if include_body:
                self.wfile.write(body)

        def do_GET(self) -> None:  # noqa: N802
            self.send_page(include_body=True)

        def do_HEAD(self) -> None:  # noqa: N802
            self.send_page(include_body=False)

        def log_message(self, fmt: str, *args: Any) -> None:
            print(fmt % args, file=sys.stderr)

    server = http.server.ThreadingHTTPServer((host, port), Handler)
    print(f"Serving dashboard at http://{host}:{port}")
    server.serve_forever()


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Track where Syscoin leaves a watched UTXO address.")
    parser.add_argument("--db", default="syscoin_tracker.sqlite", help="SQLite database path")
    parser.add_argument("--address", default=DEFAULT_ADDRESS, help="Watched Syscoin address")
    parser.add_argument("--blockbook-url", default=os.getenv("SYS_BLOCKBOOK_URL", DEFAULT_BLOCKBOOK_URL), help="Blockbook base URL")
    parser.add_argument("--page-size", type=int, default=1000, help="Blockbook page size")
    parser.add_argument("--insecure-tls", action="store_true", help="Disable TLS verification for a private/self-signed Blockbook")
    parser.add_argument("--timezone", default=os.getenv("SYS_TRACKER_TIMEZONE", DEFAULT_TIMEZONE), help="Timezone for date arguments without an offset")
    parser.add_argument("--rpc-url", help="Syscoin Core RPC URL, e.g. http://127.0.0.1:8370/")
    parser.add_argument("--rpc-host", help="Syscoin Core RPC host, used with --rpc-port")
    parser.add_argument("--rpc-port", help="Syscoin Core RPC port, default 8370")
    parser.add_argument("--rpc-user", help="Syscoin Core RPC username")
    parser.add_argument("--rpc-password", help="Syscoin Core RPC password; prefer SYS_RPC_PASSWORD env var")
    sub = parser.add_subparsers(dest="command", required=True)

    sync_p = sub.add_parser("sync", help="Fetch transactions for the watched address")
    sync_p.add_argument("--max-pages", type=int, help="Limit pages for a quick incremental sync")
    sync_p.add_argument("--from-height", type=int, help="Only fetch address transactions from this height")
    sync_p.add_argument("--since-date", help="Only fetch address transactions from this date/time; e.g. '2026-04-14 12:30'")
    sync_p.add_argument("--quiet", action="store_true")

    report_p = sub.add_parser("report", help="Print a destination report")
    report_p.add_argument("--top", type=int, default=20)
    report_p.add_argument("--all-destinations", action="store_true", help="Show/export every ranked destination address")
    report_p.add_argument("--since-height", type=int)
    report_p.add_argument("--since-date", help="Only report movements from this date/time; e.g. '2026-04-14 12:30'")
    report_p.add_argument("--min-sys", default="0")
    report_p.add_argument("--csv", type=Path, help="Write top destinations to CSV")

    follow_p = sub.add_parser("follow", help="Follow spent first-hop outputs")
    follow_p.add_argument("--depth", type=int, default=2)
    follow_p.add_argument("--limit", type=int, default=100)
    follow_p.add_argument("--min-sys", default="100")
    follow_p.add_argument("--max-pages-per-address", type=int, default=5)

    rpc_p = sub.add_parser("rpc-check", help="Check Syscoin Core RPC connectivity")
    rpc_p.set_defaults(rpc_command=True)

    verify_p = sub.add_parser("verify-sentries", help="Verify exact 100k SYS candidates against masternode_list RPC")
    verify_p.add_argument("--since-date", help="Only verify candidates from this date/time; e.g. '2026-04-14 12:30'")

    watch_p = sub.add_parser("watch", help="Poll repeatedly and emit outbound alerts")
    watch_p.add_argument("--interval", type=int, default=60)
    watch_p.add_argument("--max-pages", type=int, default=2)
    watch_p.add_argument("--webhook-url", default=os.getenv("SYS_TRACKER_WEBHOOK_URL"))
    watch_p.add_argument("--alert-history", action="store_true", help="Emit alerts for already-synced historical outbound txs")

    serve_p = sub.add_parser("serve", help="Serve a local dashboard")
    serve_p.add_argument("--host", default="127.0.0.1")
    serve_p.add_argument("--port", type=int, default=8787)
    serve_p.add_argument("--since-date", help="Only show dashboard movements from this date/time; e.g. '2026-04-14 12:30'")
    serve_p.add_argument("--sync-interval", type=int, default=60, help="Seconds between Blockbook syncs and page refreshes; use 0 to disable auto-sync")
    serve_p.add_argument("--sync-max-pages", type=int, help="Limit address pages per dashboard sync")

    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    store = Store(Path(args.db))
    client = BlockbookClient(args.blockbook_url, insecure_tls=args.insecure_tls)
    watched = {args.address}

    if args.command == "sync":
        since_time, since_label = parse_since_date(args.since_date, args.timezone)
        from_height = args.from_height
        if since_time:
            date_height = block_height_at_or_after(client, since_time)
            from_height = max(from_height or 0, date_height)
            if not args.quiet:
                print(f"Resolved {args.since_date!r} in {args.timezone} to {since_label}, block >= {date_height}", file=sys.stderr)
        stats = sync_address(
            store,
            client,
            args.address,
            page_size=args.page_size,
            max_pages=args.max_pages,
            from_height=from_height,
            watched=watched,
            quiet=args.quiet,
        )
        print(f"Synced {stats['seen']} transactions ({stats['inserted']} new); outbound movements seen: {stats['outbound']}")
        return 0

    if args.command == "report":
        since_time, since_label = parse_since_date(args.since_date, args.timezone)
        display_label = f"{since_label} (from {args.since_date} {args.timezone})" if since_label else None
        print(
            report(
                store,
                top=args.top,
                all_destinations=args.all_destinations,
                since_height=args.since_height,
                since_time=since_time,
                since_label=display_label,
                min_sats=sys_to_sats(args.min_sys),
                csv_path=args.csv,
            )
        )
        return 0

    if args.command == "follow":
        stats = follow_outputs(
            store,
            client,
            watched,
            depth=args.depth,
            limit=args.limit,
            min_sats=sys_to_sats(args.min_sys),
            page_size=args.page_size,
            max_pages_per_address=args.max_pages_per_address,
        )
        print(
            f"Examined {stats['examined']} outputs; found {stats['found_spends']} spends; "
            f"created {stats['created']} next-hop outputs"
        )
        return 0

    if args.command == "rpc-check":
        rpc = build_rpc_client(args)
        if rpc is None:
            print("No RPC URL/host supplied. Use --rpc-host/--rpc-url or SYS_RPC_URL.")
            return 1
        info = rpc.call("getblockchaininfo")
        network = rpc.call("getnetworkinfo")
        print(f"RPC OK: chain={info.get('chain')} blocks={info.get('blocks')} version={network.get('subversion')}")
        return 0

    if args.command == "verify-sentries":
        rpc = build_rpc_client(args)
        if rpc is None:
            print("No RPC URL/host supplied. Use --rpc-host/--rpc-url or SYS_RPC_URL.")
            return 1
        since_time, _since_label = parse_since_date(args.since_date, args.timezone)
        print(verify_sentry_candidates(store, rpc, since_time=since_time))
        return 0

    if args.command == "watch":
        print(f"Watching {args.address} every {args.interval}s via {args.blockbook_url}")
        if not args.alert_history:
            stats = sync_address(
                store,
                client,
                args.address,
                page_size=args.page_size,
                max_pages=args.max_pages,
                from_height=None,
                watched=watched,
                quiet=True,
            )
            marked = mark_existing_alerts(store)
            print(f"{now_iso()} baseline seen={stats['seen']} new={stats['inserted']} marked_existing={marked}")
        while True:
            stats = sync_address(
                store,
                client,
                args.address,
                page_size=args.page_size,
                max_pages=args.max_pages,
                from_height=None,
                watched=watched,
                quiet=True,
            )
            alerts = emit_alerts(store, args.webhook_url)
            print(f"{now_iso()} sync seen={stats['seen']} new={stats['inserted']} alerts={alerts}")
            time.sleep(args.interval)

    if args.command == "serve":
        since_time, since_label = parse_since_date(args.since_date, args.timezone)
        display_label = f"{since_label} (from {args.since_date} {args.timezone})" if since_label else None
        from_height = None
        if since_time:
            from_height = block_height_at_or_after(client, since_time)
            print(f"Dashboard window starts at block >= {from_height}", file=sys.stderr)
        if args.sync_interval > 0:
            sync_thread = threading.Thread(
                target=dashboard_sync_loop,
                args=(
                    Path(args.db),
                    args.blockbook_url,
                    args.insecure_tls,
                    args.address,
                    args.page_size,
                    args.sync_max_pages,
                    from_height,
                    since_time,
                    args.sync_interval,
                ),
                daemon=True,
            )
            sync_thread.start()
        serve(
            store,
            args.host,
            args.port,
            since_time=since_time,
            since_label=display_label,
            refresh_seconds=args.sync_interval if args.sync_interval > 0 else 60,
        )
        return 0

    return 1


if __name__ == "__main__":
    raise SystemExit(main())
