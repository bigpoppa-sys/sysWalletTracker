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

try:
    from zoneinfo import ZoneInfo
except ImportError:  # pragma: no cover - compatibility for Python 3.8 VPS cron
    class ZoneInfo(dt.tzinfo):
        def __init__(self, key: str) -> None:
            self.key = key

        def utcoffset(self, value: dt.datetime | None) -> dt.timedelta:
            if self.key == "Australia/Sydney":
                return dt.timedelta(hours=10)
            return dt.timedelta(0)

        def dst(self, value: dt.datetime | None) -> dt.timedelta:
            return dt.timedelta(0)

        def tzname(self, value: dt.datetime | None) -> str:
            return self.key


DEFAULT_BLOCKBOOK_URL = "https://explorer-blockbook.syscoin.org"
DEFAULT_ADDRESS = "sys1qync7erear7cvpkysvv0a28mj45g2ps0kq9c6qs"
DEFAULT_TIMEZONE = "Australia/Sydney"
DEFAULT_EXCHANGE_TAGS_PATH = Path("exchange_tags.csv")
DEFAULT_EXCHANGE_ROUTES_PATH = Path("exchange_routes.csv")
DEFAULT_EXCHANGE_HOT_WALLETS_PATH = Path("exchange_hot_wallets.csv")
DEFAULT_EXCHANGE_COLD_WALLETS_PATH = Path("exchange_cold_wallets.csv")
DEFAULT_WALLET_LABELS_PATH = Path("wallet_labels.csv")
DEFAULT_NETWORK_MASTERNODES_PATH = Path("network_masternodes.csv")
DEFAULT_NODE_OUTPUTS_PATH = Path("node_outputs.csv")
DEFAULT_VERIFIED_SENTRIES_PATH = Path("verified_sentries.csv")
DEFAULT_MONITORING_FROM_HEIGHT = 2221358
SENTRY_NODE_PATHS = ("/sentrynode", "/sentrynode.html")
LEGACY_MASTERNODE_PATHS = ("/masternodes", "/masternodes.html")
TOP_WALLETS_PATHS = ("/top-wallets", "/top-wallets.html")
TOP_WALLETS_JSON = "top-wallets.json"
TOP_WALLETS_HTML = "top-wallets.html"
TOP_WALLETS_JSON_PATH = f"/{TOP_WALLETS_JSON}"
CHART_ASSET_ROUTE = "/assets/chart.umd.js"
CHART_ASSET_PATH = Path("static/assets/chart.umd.js")
NETWORK_MASTERNODE_HEADERS = [
    "outpoint",
    "source_txid",
    "source_vout",
    "pro_tx_hash",
    "service",
    "payee",
    "status",
    "collateral_address",
    "owner_address",
    "voting_address",
    "collateral_height",
    "collateral_time",
    "registered_height",
    "registered_time",
    "last_paid_time",
    "last_paid_block",
    "first_seen_at",
    "last_seen_at",
    "removed_at",
    "taken_down_txid",
    "taken_down_time",
    "moved_to_address",
]
SENTRY_COLLATERAL_SATS = 100_000 * 100_000_000
SENIORITY_LEVEL_1_BLOCKS = 210_240
SENIORITY_LEVEL_2_BLOCKS = 525_600
SATOSHI = Decimal("100000000")
DB_WRITE_LOCK = threading.Lock()


def utc(ts: int | None) -> str:
    if not ts:
        return ""
    return dt.datetime.fromtimestamp(int(ts), tz=dt.timezone.utc).isoformat(timespec="seconds")


def now_iso() -> str:
    return dt.datetime.now(tz=dt.timezone.utc).isoformat(timespec="seconds")


def chart_asset_bytes() -> bytes | None:
    if not CHART_ASSET_PATH.exists():
        return None
    return CHART_ASSET_PATH.read_bytes()


def sats(value: Any) -> int:
    if value is None or value == "":
        return 0
    return int(value)


def int_or_none(value: Any) -> int | None:
    if value is None or value == "":
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


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


def fmt_local_date(ts: int | None, timezone_name: str = DEFAULT_TIMEZONE) -> str:
    if not ts:
        return "-"
    local = dt.datetime.fromtimestamp(int(ts), tz=dt.timezone.utc).astimezone(ZoneInfo(timezone_name))
    return local.strftime("%b %-d, %Y")


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


def sentry_status_label(status: str) -> str:
    normalized = status.upper()
    if normalized == "POSE_BANNED":
        return "Banned"
    if normalized == "ENABLED":
        return "Enabled"
    return status or "Unknown"


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


def load_wallet_labels(path: Path = DEFAULT_WALLET_LABELS_PATH) -> dict[str, dict[str, str]]:
    if not path.exists():
        return {}
    labels: dict[str, dict[str, str]] = {}
    with path.open(newline="") as f:
        for row in csv.DictReader(f):
            address = (row.get("address") or "").strip()
            if not address:
                continue
            labels[address] = {
                "name": (row.get("name") or "").strip(),
                "label": (row.get("label") or "").strip(),
            }
    return labels


def wallet_identity_for_address(
    address: str,
    wallet_labels: dict[str, dict[str, str]],
    exchange_tags: dict[str, str],
) -> dict[str, str]:
    custom = wallet_labels.get(address)
    if custom:
        name = custom.get("name") or short_address(address)
        label = custom.get("label") or "Private Wallet"
        return {"name": name, "label": label}

    exchange_name = exchange_tags.get(address)
    if exchange_name:
        return {"name": exchange_name, "label": "Exchange"}

    return {"name": short_address(address), "label": "Unknown"}


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


def load_exchange_cold_wallets(path: Path = DEFAULT_EXCHANGE_COLD_WALLETS_PATH) -> list[dict[str, str]]:
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


def sats_to_sys_string(value: int) -> str:
    return fmt_sys(value)


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

    def batch_call(self, calls: list[tuple[str, list[Any]]]) -> list[Any]:
        payload = json.dumps(
            [
                {"jsonrpc": "1.0", "id": str(index), "method": method, "params": params}
                for index, (method, params) in enumerate(calls)
            ]
        ).encode()
        headers = {"Content-Type": "application/json", "User-Agent": "syscoin-tracker/0.1"}
        if self.username is not None and self.password is not None:
            token = base64.b64encode(f"{self.username}:{self.password}".encode()).decode()
            headers["Authorization"] = f"Basic {token}"
        req = urllib.request.Request(self.url, data=payload, headers=headers)
        with urllib.request.urlopen(req, timeout=max(self.timeout, 60)) as resp:
            data = json.loads(resp.read().decode("utf-8"))
        if not isinstance(data, list):
            raise RuntimeError("RPC batch response was not a list")
        by_id = {int(item["id"]): item for item in data}
        results = []
        for index in range(len(calls)):
            item = by_id[index]
            if item.get("error"):
                raise RuntimeError(item["error"])
            results.append(item.get("result"))
        return results


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


def block_time_from_height(rpc: SyscoinRpcClient, height: int | None, cache: dict[int, int | None]) -> int | None:
    if not height:
        return None
    if height in cache:
        return cache[height]
    try:
        block_hash = rpc.call("getblockhash", [height])
        header = rpc.call("getblockheader", [block_hash])
        cache[height] = int(header.get("time")) if isinstance(header, dict) and header.get("time") else None
    except Exception:
        cache[height] = None
    return cache[height]


def block_times_from_heights(rpc: SyscoinRpcClient, heights: Iterable[int]) -> dict[int, int | None]:
    unique_heights = sorted(set(int(height) for height in heights if height))
    times: dict[int, int | None] = {}
    chunk_size = 500
    for index in range(0, len(unique_heights), chunk_size):
        chunk = unique_heights[index : index + chunk_size]
        try:
            hashes = rpc.batch_call([("getblockhash", [height]) for height in chunk])
            headers = rpc.batch_call([("getblockheader", [block_hash]) for block_hash in hashes])
            for height, header in zip(chunk, headers):
                times[height] = int(header.get("time")) if isinstance(header, dict) and header.get("time") else None
        except Exception:
            cache: dict[int, int | None] = {}
            for height in chunk:
                times[height] = block_time_from_height(rpc, height, cache)
    return times


def network_masternode_rows_from_rpc(rpc: SyscoinRpcClient) -> list[dict[str, Any]]:
    result = rpc.call("masternode_list", ["json"])
    if not isinstance(result, dict):
        return []

    parsed_rows: list[tuple[str, int, dict[str, Any], int | None, int | None]] = []
    heights: set[int] = set()
    for outpoint_key, info in result.items():
        if not isinstance(info, dict):
            continue
        outpoint = normalize_outpoint(str(outpoint_key))
        if ":" not in outpoint:
            continue
        source_txid, source_vout_text = outpoint.rsplit(":", 1)
        try:
            source_vout = int(source_vout_text)
        except ValueError:
            continue
        collateral_height = int(info["collateralheight"]) if info.get("collateralheight") is not None else None
        registered_height = int(info["registeredheight"]) if info.get("registeredheight") is not None else None
        if collateral_height:
            heights.add(collateral_height)
        if registered_height:
            heights.add(registered_height)
        parsed_rows.append((source_txid, source_vout, info, collateral_height, registered_height))

    height_times = block_times_from_heights(rpc, heights)
    rows: list[dict[str, Any]] = []
    seen_at = now_iso()
    for source_txid, source_vout, info, collateral_height, registered_height in parsed_rows:
        outpoint = f"{source_txid}:{source_vout}"
        rows.append(
            {
                "outpoint": outpoint,
                "source_txid": source_txid,
                "source_vout": source_vout,
                "pro_tx_hash": info.get("proTxHash") or "",
                "service": info.get("address") or "",
                "payee": info.get("payee") or "",
                "status": info.get("status") or "",
                "collateral_address": info.get("collateraladdress") or "",
                "owner_address": info.get("owneraddress") or "",
                "voting_address": info.get("votingaddress") or "",
                "collateral_height": collateral_height,
                "collateral_time": height_times.get(collateral_height) if collateral_height else None,
                "registered_height": registered_height,
                "registered_time": height_times.get(registered_height) if registered_height else None,
                "last_paid_time": int(info["lastpaidtime"]) if info.get("lastpaidtime") else None,
                "last_paid_block": int(info["lastpaidblock"]) if info.get("lastpaidblock") else None,
                "first_seen_at": seen_at,
                "last_seen_at": seen_at,
                "removed_at": "",
                "taken_down_txid": "",
                "taken_down_time": None,
                "moved_to_address": "",
            }
        )
    return rows


def write_network_masternodes_csv(rows: list[dict[str, Any]], path: Path) -> None:
    with path.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=NETWORK_MASTERNODE_HEADERS, extrasaction="ignore", lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)


def network_masternode_rows_from_store(store: Store) -> list[dict[str, Any]]:
    rows = store.conn.execute(
        f"""
        SELECT {", ".join(NETWORK_MASTERNODE_HEADERS)}
        FROM network_masternodes
        ORDER BY COALESCE(registered_time, collateral_time, 0) DESC, collateral_address
        """
    ).fetchall()
    return [dict(row) for row in rows]


def load_network_masternodes_csv_rows(
    store: Store,
    rows: Iterable[dict[str, str]],
    *,
    source: str = "csv",
) -> dict[str, Any]:
    def maybe_int(value: str | None) -> int | None:
        if value is None or value == "":
            return None
        return int(value)

    count = 0
    enabled = 0
    latest_seen_at = ""
    for row in rows:
        status = row.get("status") or ""
        last_seen_at = row.get("last_seen_at") or now_iso()
        latest_seen_at = max(latest_seen_at, last_seen_at)
        if status.upper() == "ENABLED":
            enabled += 1
        count += 1
        store.save_network_masternode(
            {
                "outpoint": row["outpoint"],
                "source_txid": row["source_txid"],
                "source_vout": int(row["source_vout"]),
                "pro_tx_hash": row.get("pro_tx_hash") or "",
                "service": row.get("service") or "",
                "payee": row.get("payee") or "",
                "status": status,
                "collateral_address": row.get("collateral_address") or "",
                "owner_address": row.get("owner_address") or "",
                "voting_address": row.get("voting_address") or "",
                "collateral_height": maybe_int(row.get("collateral_height")),
                "collateral_time": maybe_int(row.get("collateral_time")),
                "registered_height": maybe_int(row.get("registered_height")),
                "registered_time": maybe_int(row.get("registered_time")),
                "last_paid_time": maybe_int(row.get("last_paid_time")),
                "last_paid_block": maybe_int(row.get("last_paid_block")),
                "first_seen_at": row.get("first_seen_at") or now_iso(),
                "last_seen_at": last_seen_at,
                "removed_at": row.get("removed_at") or "",
                "taken_down_txid": row.get("taken_down_txid") or "",
                "taken_down_time": maybe_int(row.get("taken_down_time")),
                "moved_to_address": row.get("moved_to_address") or "",
            }
        )
    store.conn.commit()
    if count:
        stats = {
            "synced_at": latest_seen_at or now_iso(),
            "current": count,
            "enabled": enabled,
            "source": source,
        }
        store.set_meta("last_masternode_sync", stats)
        return stats
    return {"synced_at": None, "current": 0, "enabled": 0, "source": source}


def load_network_masternodes_csv(store: Store, path: Path = DEFAULT_NETWORK_MASTERNODES_PATH) -> dict[str, Any] | None:
    if not path.exists():
        return None

    with path.open(newline="") as f:
        return load_network_masternodes_csv_rows(store, csv.DictReader(f), source=str(path))


def load_verified_sentries_csv(store: Store, path: Path = DEFAULT_VERIFIED_SENTRIES_PATH) -> None:
    if not path.exists():
        return
    with path.open(newline="") as f:
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


def load_node_outputs_csv(store: Store, path: Path = DEFAULT_NODE_OUTPUTS_PATH) -> None:
    if not path.exists():
        return

    def maybe_int(value: str | None) -> int | None:
        if value is None or value == "":
            return None
        return int(value)

    with path.open(newline="") as f:
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
        f"Sentry node outpoints from RPC: {len(outpoints)}",
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
        self.conn = sqlite3.connect(path, check_same_thread=False, timeout=30, isolation_level=None)
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA busy_timeout=30000")
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

            CREATE TABLE IF NOT EXISTS network_masternodes (
                outpoint TEXT PRIMARY KEY,
                source_txid TEXT NOT NULL,
                source_vout INTEGER NOT NULL,
                pro_tx_hash TEXT,
                service TEXT,
                payee TEXT,
                status TEXT,
                collateral_address TEXT NOT NULL,
                owner_address TEXT,
                voting_address TEXT,
                collateral_height INTEGER,
                collateral_time INTEGER,
                registered_height INTEGER,
                registered_time INTEGER,
                last_paid_time INTEGER,
                last_paid_block INTEGER,
                first_seen_at TEXT NOT NULL,
                last_seen_at TEXT NOT NULL,
                removed_at TEXT,
                taken_down_txid TEXT,
                taken_down_time INTEGER,
                moved_to_address TEXT
            );

            CREATE TABLE IF NOT EXISTS top_wallet_utxos (
                txid TEXT NOT NULL,
                vout INTEGER NOT NULL,
                address TEXT NOT NULL,
                value_sats INTEGER NOT NULL,
                block_height INTEGER NOT NULL,
                block_time INTEGER,
                PRIMARY KEY (txid, vout)
            );

            CREATE TABLE IF NOT EXISTS top_wallet_balances (
                address TEXT PRIMARY KEY,
                balance_sats INTEGER NOT NULL DEFAULT 0,
                utxo_count INTEGER NOT NULL DEFAULT 0,
                last_seen_height INTEGER,
                last_seen_time INTEGER,
                updated_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS top_wallet_cluster_utxos (
                txid TEXT NOT NULL,
                vout INTEGER NOT NULL,
                address TEXT NOT NULL,
                value_sats INTEGER NOT NULL,
                block_height INTEGER NOT NULL,
                block_time INTEGER,
                PRIMARY KEY (txid, vout)
            );

            CREATE TABLE IF NOT EXISTS top_wallet_cluster_edges (
                address_a TEXT NOT NULL,
                address_b TEXT NOT NULL,
                tx_count INTEGER NOT NULL DEFAULT 0,
                first_seen_height INTEGER,
                last_seen_height INTEGER,
                last_seen_time INTEGER,
                updated_at TEXT NOT NULL,
                PRIMARY KEY (address_a, address_b)
            );

            CREATE INDEX IF NOT EXISTS idx_movements_block
                ON movements(block_height, block_time);
            CREATE INDEX IF NOT EXISTS idx_tracked_outputs_address
                ON tracked_outputs(address);
            CREATE INDEX IF NOT EXISTS idx_tracked_outputs_spent
                ON tracked_outputs(spent, spent_txid);
            CREATE INDEX IF NOT EXISTS idx_network_masternodes_status
                ON network_masternodes(status);
            CREATE INDEX IF NOT EXISTS idx_top_wallet_balances_balance
                ON top_wallet_balances(balance_sats DESC);
            CREATE INDEX IF NOT EXISTS idx_top_wallet_utxos_address
                ON top_wallet_utxos(address);
            CREATE INDEX IF NOT EXISTS idx_top_wallet_cluster_utxos_address
                ON top_wallet_cluster_utxos(address);
            CREATE INDEX IF NOT EXISTS idx_top_wallet_cluster_edges_b
                ON top_wallet_cluster_edges(address_b);
            """
        )
        self.ensure_network_masternode_columns()
        self.conn.commit()

    def ensure_network_masternode_columns(self) -> None:
        columns = {row["name"] for row in self.conn.execute("PRAGMA table_info(network_masternodes)").fetchall()}
        additions = {
            "taken_down_txid": "taken_down_txid TEXT",
            "taken_down_time": "taken_down_time INTEGER",
            "moved_to_address": "moved_to_address TEXT",
        }
        for name, definition in additions.items():
            if name not in columns:
                self.conn.execute(f"ALTER TABLE network_masternodes ADD COLUMN {definition}")

    def set_meta(self, key: str, value: Any, commit: bool = True) -> None:
        self.conn.execute(
            "INSERT INTO metadata(key, value) VALUES(?, ?) "
            "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            (key, json.dumps(value)),
        )
        if commit:
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

    def save_network_masternode(self, row: dict[str, Any]) -> None:
        self.conn.execute(
            """
            INSERT INTO network_masternodes(
                outpoint, source_txid, source_vout, pro_tx_hash, service, payee,
                status, collateral_address, owner_address, voting_address,
                collateral_height, collateral_time, registered_height,
                registered_time, last_paid_time, last_paid_block, first_seen_at,
                last_seen_at, removed_at, taken_down_txid, taken_down_time,
                moved_to_address
            )
            VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(outpoint) DO UPDATE SET
                pro_tx_hash=excluded.pro_tx_hash,
                service=excluded.service,
                payee=excluded.payee,
                status=excluded.status,
                collateral_address=excluded.collateral_address,
                owner_address=excluded.owner_address,
                voting_address=excluded.voting_address,
                collateral_height=excluded.collateral_height,
                collateral_time=excluded.collateral_time,
                registered_height=excluded.registered_height,
                registered_time=excluded.registered_time,
                last_paid_time=excluded.last_paid_time,
                last_paid_block=excluded.last_paid_block,
                last_seen_at=excluded.last_seen_at,
                removed_at=excluded.removed_at,
                taken_down_txid=excluded.taken_down_txid,
                taken_down_time=excluded.taken_down_time,
                moved_to_address=excluded.moved_to_address
            """,
            (
                row["outpoint"],
                row["source_txid"],
                row["source_vout"],
                row.get("pro_tx_hash"),
                row.get("service"),
                row.get("payee"),
                row.get("status"),
                row["collateral_address"],
                row.get("owner_address"),
                row.get("voting_address"),
                row.get("collateral_height"),
                row.get("collateral_time"),
                row.get("registered_height"),
                row.get("registered_time"),
                row.get("last_paid_time"),
                row.get("last_paid_block"),
                row.get("first_seen_at") or now_iso(),
                row.get("last_seen_at") or now_iso(),
                row.get("removed_at") or None,
                row.get("taken_down_txid") or None,
                row.get("taken_down_time"),
                row.get("moved_to_address") or None,
            ),
        )

    def mark_network_masternode_removed(
        self,
        outpoint: str,
        *,
        removed_at: str,
        taken_down_txid: str | None = None,
        taken_down_time: int | None = None,
        moved_to_address: str | None = None,
    ) -> None:
        self.conn.execute(
            """
            UPDATE network_masternodes
            SET status = ?,
                removed_at = ?,
                last_seen_at = ?,
                taken_down_txid = COALESCE(?, taken_down_txid),
                taken_down_time = COALESCE(?, taken_down_time),
                moved_to_address = COALESCE(?, moved_to_address)
            WHERE outpoint = ?
            """,
            (
                "REMOVED",
                removed_at,
                removed_at,
                taken_down_txid,
                taken_down_time,
                moved_to_address,
                outpoint,
            ),
        )


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


def trace_masternode_collateral_spend(client: BlockbookClient, row: sqlite3.Row | dict[str, Any]) -> dict[str, Any]:
    address = str(row["collateral_address"] or "")
    source_txid = str(row["source_txid"])
    source_vout = int(row["source_vout"])
    if not address or not source_txid:
        return {}

    spend: dict[str, Any] | None = None
    try:
        source_tx = client.tx(source_txid)
        for vout in source_tx.get("vout") or []:
            if int(vout.get("n", 0) or 0) == source_vout and vout.get("spentTxId"):
                spend = client.tx(str(vout["spentTxId"]))
                break
    except Exception:
        spend = None

    if spend is None:
        try:
            spend = find_spending_tx(
                client,
                address,
                source_txid,
                source_vout,
                page_size=100,
                max_pages=5,
                from_height=row["collateral_height"] or row["registered_height"],
            )
        except Exception:
            spend = None
    if not spend:
        return {}

    threshold = int(Decimal(SENTRY_COLLATERAL_SATS) * Decimal("0.95"))
    candidates: list[tuple[int, str]] = []
    fallback: list[tuple[int, str]] = []
    for vout in spend.get("vout") or []:
        value = sats(vout.get("value"))
        out_addresses = [addr for addr in addresses_from(vout) if addr != address]
        if not out_addresses:
            continue
        fallback.append((value, out_addresses[0]))
        if value >= threshold:
            candidates.append((value, out_addresses[0]))

    moved_to_address = ""
    if candidates:
        moved_to_address = max(candidates, key=lambda item: item[0])[1]
    elif fallback:
        moved_to_address = max(fallback, key=lambda item: item[0])[1]

    return {
        "taken_down_txid": spend.get("txid") or "",
        "taken_down_time": spend.get("blockTime"),
        "moved_to_address": moved_to_address,
    }


def sync_network_masternodes(
    store: Store,
    rpc: SyscoinRpcClient,
    client: BlockbookClient | None = None,
) -> dict[str, int]:
    rows = network_masternode_rows_from_rpc(rpc)
    try:
        chain_height = int(rpc.call("getblockcount"))
    except Exception:
        chain_height = max(
            (
                int_or_none(row.get(key)) or 0
                for row in rows
                for key in ("collateral_height", "registered_height", "last_paid_block")
            ),
            default=0,
        )
    current_outpoints = {row["outpoint"] for row in rows}
    existing_rows = store.conn.execute("SELECT * FROM network_masternodes").fetchall()
    existing_outpoints = {row["outpoint"] for row in existing_rows}

    for row in rows:
        store.save_network_masternode(row)

    removed = 0
    traced = 0
    synced_at = now_iso()
    removed_at = synced_at
    for row in existing_rows:
        if row["outpoint"] in current_outpoints or row["removed_at"]:
            continue
        trace = trace_masternode_collateral_spend(client, row) if client is not None else {}
        if trace.get("taken_down_txid"):
            traced += 1
        store.mark_network_masternode_removed(
            row["outpoint"],
            removed_at=removed_at,
            taken_down_txid=trace.get("taken_down_txid"),
            taken_down_time=trace.get("taken_down_time"),
            moved_to_address=trace.get("moved_to_address"),
        )
        removed += 1

    store.conn.commit()
    stats = {
        "current": len(rows),
        "added": len(current_outpoints - existing_outpoints),
        "removed": removed,
        "traced": traced,
        "enabled": sum(1 for row in rows if str(row.get("status", "")).upper() == "ENABLED"),
        "chain_height": chain_height,
    }
    store.set_meta("last_masternode_sync", {"synced_at": synced_at, **stats})
    return stats


def rpc_output_address(vout: dict[str, Any]) -> str | None:
    script = vout.get("scriptPubKey") or {}
    address = script.get("address")
    if address:
        return str(address)
    addresses = script.get("addresses") or []
    if isinstance(addresses, str):
        return addresses
    if isinstance(addresses, list) and len(addresses) == 1 and addresses[0]:
        return str(addresses[0])
    return None


def rpc_output_sats(vout: dict[str, Any]) -> int:
    value = vout.get("value")
    if value is None:
        return 0
    return sys_to_sats(str(value))


def top_wallet_progress(store: Store) -> dict[str, Any]:
    progress = store.get_meta("top_wallet_index", {}) or {}
    if "last_height" not in progress:
        progress["last_height"] = -1
    return progress


def top_wallet_cluster_progress(store: Store) -> dict[str, Any]:
    progress = store.get_meta("top_wallet_cluster_index", {}) or {}
    if "last_height" not in progress:
        progress["last_height"] = -1
    return progress


def reset_top_wallet_index(store: Store, start_height: int = 0) -> None:
    store.conn.execute("DELETE FROM top_wallet_utxos")
    store.conn.execute("DELETE FROM top_wallet_balances")
    store.set_meta(
        "top_wallet_index",
        {
            "last_height": start_height - 1,
            "last_hash": "",
            "chain_height": None,
            "synced_at": now_iso(),
            "reset_at": now_iso(),
        },
        commit=False,
    )
    store.conn.commit()


def reset_top_wallet_cluster_index(store: Store, start_height: int = 0) -> None:
    store.conn.execute("DELETE FROM top_wallet_cluster_utxos")
    store.conn.execute("DELETE FROM top_wallet_cluster_edges")
    store.set_meta(
        "top_wallet_cluster_index",
        {
            "last_height": start_height - 1,
            "last_hash": "",
            "chain_height": None,
            "synced_at": now_iso(),
            "reset_at": now_iso(),
        },
        commit=False,
    )
    store.conn.commit()


def top_wallet_sentry_collateral_outpoints(store: Store) -> dict[tuple[str, int], str]:
    if store.conn.execute("SELECT COUNT(*) AS count FROM network_masternodes").fetchone()["count"] == 0:
        load_network_masternodes_csv(store)

    outpoints: dict[tuple[str, int], str] = {}
    for row in store.conn.execute(
        """
        SELECT source_txid, source_vout, collateral_address
        FROM network_masternodes
        WHERE source_txid != '' AND collateral_address != ''
        """
    ):
        txid = str(row["source_txid"] or "")
        vout = int_or_none(row["source_vout"])
        address = str(row["collateral_address"] or "")
        if txid and vout is not None and address:
            outpoints[(txid, vout)] = address

    for row in store.conn.execute(
        """
        SELECT source_txid, source_vout, address
        FROM verified_sentries
        WHERE source_txid != '' AND address != ''
        """
    ):
        txid = str(row["source_txid"] or "")
        vout = int_or_none(row["source_vout"])
        address = str(row["address"] or "")
        if txid and vout is not None and address:
            outpoints[(txid, vout)] = address

    return outpoints


def adjust_top_wallet_balance(
    store: Store,
    address: str,
    delta_sats: int,
    delta_utxos: int,
    block_height: int,
    block_time: int | None,
) -> None:
    stamp = now_iso()
    store.conn.execute(
        """
        INSERT INTO top_wallet_balances(
            address, balance_sats, utxo_count, last_seen_height, last_seen_time, updated_at
        )
        VALUES(?, ?, ?, ?, ?, ?)
        ON CONFLICT(address) DO UPDATE SET
            balance_sats = top_wallet_balances.balance_sats + excluded.balance_sats,
            utxo_count = top_wallet_balances.utxo_count + excluded.utxo_count,
            last_seen_height = CASE
                WHEN excluded.last_seen_height >= COALESCE(top_wallet_balances.last_seen_height, -1)
                THEN excluded.last_seen_height
                ELSE top_wallet_balances.last_seen_height
            END,
            last_seen_time = CASE
                WHEN excluded.last_seen_height >= COALESCE(top_wallet_balances.last_seen_height, -1)
                THEN excluded.last_seen_time
                ELSE top_wallet_balances.last_seen_time
            END,
            updated_at = excluded.updated_at
        """,
        (address, delta_sats, delta_utxos, block_height, block_time, stamp),
    )


def process_top_wallet_block(store: Store, block: dict[str, Any]) -> dict[str, int]:
    block_height = int(block.get("height") or 0)
    block_time = int_or_none(block.get("time"))
    stats = {"txs": 0, "outputs": 0, "spends": 0, "missing_spends": 0}

    for tx in block.get("tx") or []:
        txid = tx.get("txid")
        if not txid:
            continue
        stats["txs"] += 1

        for vin in tx.get("vin") or []:
            if vin.get("coinbase"):
                continue
            prev_txid = vin.get("txid")
            if not prev_txid:
                continue
            prev_vout = input_prev_vout(vin)
            previous = store.conn.execute(
                """
                SELECT address, value_sats
                FROM top_wallet_utxos
                WHERE txid = ? AND vout = ?
                """,
                (prev_txid, prev_vout),
            ).fetchone()
            if not previous:
                stats["missing_spends"] += 1
                continue
            store.conn.execute(
                "DELETE FROM top_wallet_utxos WHERE txid = ? AND vout = ?",
                (prev_txid, prev_vout),
            )
            adjust_top_wallet_balance(
                store,
                previous["address"],
                -int(previous["value_sats"]),
                -1,
                block_height,
                block_time,
            )
            stats["spends"] += 1

        for vout in tx.get("vout") or []:
            address = rpc_output_address(vout)
            if not address:
                continue
            value_sats = rpc_output_sats(vout)
            if value_sats <= 0:
                continue
            output_index = int(vout.get("n", 0) or 0)
            cursor = store.conn.execute(
                """
                INSERT OR IGNORE INTO top_wallet_utxos(
                    txid, vout, address, value_sats, block_height, block_time
                )
                VALUES(?, ?, ?, ?, ?, ?)
                """,
                (txid, output_index, address, value_sats, block_height, block_time),
            )
            if cursor.rowcount:
                adjust_top_wallet_balance(store, address, value_sats, 1, block_height, block_time)
                stats["outputs"] += 1

    return stats


def save_top_wallet_cluster_edge(
    store: Store,
    address_a: str,
    address_b: str,
    block_height: int,
    block_time: int | None,
) -> None:
    if address_a == address_b:
        return
    left, right = sorted((address_a, address_b))
    stamp = now_iso()
    store.conn.execute(
        """
        INSERT INTO top_wallet_cluster_edges(
            address_a, address_b, tx_count, first_seen_height, last_seen_height, last_seen_time, updated_at
        )
        VALUES(?, ?, 1, ?, ?, ?, ?)
        ON CONFLICT(address_a, address_b) DO UPDATE SET
            tx_count = top_wallet_cluster_edges.tx_count + 1,
            first_seen_height = MIN(
                COALESCE(top_wallet_cluster_edges.first_seen_height, excluded.first_seen_height),
                excluded.first_seen_height
            ),
            last_seen_height = CASE
                WHEN excluded.last_seen_height >= COALESCE(top_wallet_cluster_edges.last_seen_height, -1)
                THEN excluded.last_seen_height
                ELSE top_wallet_cluster_edges.last_seen_height
            END,
            last_seen_time = CASE
                WHEN excluded.last_seen_height >= COALESCE(top_wallet_cluster_edges.last_seen_height, -1)
                THEN excluded.last_seen_time
                ELSE top_wallet_cluster_edges.last_seen_time
            END,
            updated_at = excluded.updated_at
        """,
        (left, right, block_height, block_height, block_time, stamp),
    )


def process_top_wallet_cluster_block(
    store: Store,
    block: dict[str, Any],
    sentry_collateral_outpoints: dict[tuple[str, int], str] | None = None,
) -> dict[str, int]:
    block_height = int(block.get("height") or 0)
    block_time = int_or_none(block.get("time"))
    sentry_collateral_outpoints = sentry_collateral_outpoints or {}
    stats = {
        "txs": 0,
        "outputs": 0,
        "spends": 0,
        "missing_spends": 0,
        "groups": 0,
        "sentry_groups": 0,
        "edges": 0,
        "sentry_edges": 0,
    }

    for tx in block.get("tx") or []:
        txid = tx.get("txid")
        if not txid:
            continue
        stats["txs"] += 1
        input_addresses: list[str] = []

        for vin in tx.get("vin") or []:
            if vin.get("coinbase"):
                continue
            prev_txid = vin.get("txid")
            if not prev_txid:
                continue
            prev_vout = input_prev_vout(vin)
            previous = store.conn.execute(
                """
                SELECT address
                FROM top_wallet_cluster_utxos
                WHERE txid = ? AND vout = ?
                """,
                (prev_txid, prev_vout),
            ).fetchone()
            if not previous:
                stats["missing_spends"] += 1
                continue
            store.conn.execute(
                "DELETE FROM top_wallet_cluster_utxos WHERE txid = ? AND vout = ?",
                (prev_txid, prev_vout),
            )
            input_addresses.append(previous["address"])
            stats["spends"] += 1

        unique_input_addresses = sorted(set(input_addresses))
        if len(unique_input_addresses) > 1:
            anchor = unique_input_addresses[0]
            for address in unique_input_addresses[1:]:
                save_top_wallet_cluster_edge(store, anchor, address, block_height, block_time)
                stats["edges"] += 1
            stats["groups"] += 1

        for vout in tx.get("vout") or []:
            address = rpc_output_address(vout)
            if not address:
                continue
            value_sats = rpc_output_sats(vout)
            if value_sats <= 0:
                continue
            output_index = int(vout.get("n", 0) or 0)
            collateral_address = sentry_collateral_outpoints.get((txid, output_index))
            if (
                collateral_address
                and value_sats == SENTRY_COLLATERAL_SATS
                and unique_input_addresses
            ):
                sentry_edges = 0
                for input_address in unique_input_addresses:
                    if input_address == collateral_address:
                        continue
                    save_top_wallet_cluster_edge(store, input_address, collateral_address, block_height, block_time)
                    stats["edges"] += 1
                    stats["sentry_edges"] += 1
                    sentry_edges += 1
                if sentry_edges:
                    stats["sentry_groups"] += 1
            cursor = store.conn.execute(
                """
                INSERT OR IGNORE INTO top_wallet_cluster_utxos(
                    txid, vout, address, value_sats, block_height, block_time
                )
                VALUES(?, ?, ?, ?, ?, ?)
                """,
                (txid, output_index, address, value_sats, block_height, block_time),
            )
            if cursor.rowcount:
                stats["outputs"] += 1

    return stats


def sync_top_wallet_index(
    store: Store,
    rpc: SyscoinRpcClient,
    *,
    start_height: int = 0,
    max_blocks: int | None = None,
    to_height: int | None = None,
    reset: bool = False,
    batch_size: int = 50,
) -> dict[str, Any]:
    if reset:
        reset_top_wallet_index(store, start_height=start_height)

    progress = top_wallet_progress(store)
    last_height = int_or_none(progress.get("last_height"))
    if last_height is None or last_height < start_height - 1:
        last_height = start_height - 1
    chain_height = int(rpc.call("getblockcount"))
    target_height = min(chain_height, to_height if to_height is not None else chain_height)
    if max_blocks is not None:
        target_height = min(target_height, last_height + max_blocks)

    totals = {
        "blocks": 0,
        "txs": 0,
        "outputs": 0,
        "spends": 0,
        "missing_spends": 0,
        "start_height": last_height + 1,
        "last_height": last_height,
        "target_height": target_height,
        "chain_height": chain_height,
    }
    if target_height <= last_height:
        progress.update({"chain_height": chain_height, "synced_at": now_iso()})
        store.set_meta("top_wallet_index", progress)
        return totals

    batch_size = max(1, batch_size)
    height = last_height + 1
    while height <= target_height:
        heights = list(range(height, min(target_height, height + batch_size - 1) + 1))
        block_hashes = rpc.batch_call([("getblockhash", [item]) for item in heights])
        blocks = rpc.batch_call([("getblock", [block_hash, 2]) for block_hash in block_hashes])

        if len(block_hashes) != len(heights) or len(blocks) != len(heights):
            raise RuntimeError("top wallet RPC batch returned an unexpected number of results")

        for block_height, block_hash, block in zip(heights, block_hashes, blocks):
            previous_hash = progress.get("last_hash")
            if block_height > 0 and previous_hash and block.get("previousblockhash") != previous_hash:
                raise RuntimeError(
                    "top wallet index hit a chain reorg; rerun sync-top-wallets with --reset to rebuild"
                )

            store.conn.execute("BEGIN")
            block_stats = process_top_wallet_block(store, block)
            progress = {
                "last_height": block_height,
                "last_hash": block_hash,
                "chain_height": chain_height,
                "synced_at": now_iso(),
            }
            store.set_meta("top_wallet_index", progress, commit=False)
            store.conn.commit()

            totals["blocks"] += 1
            totals["last_height"] = block_height
            for key in ("txs", "outputs", "spends", "missing_spends"):
                totals[key] += block_stats[key]

        height = heights[-1] + 1

    return totals


def sync_top_wallet_cluster_index(
    store: Store,
    rpc: SyscoinRpcClient,
    *,
    start_height: int = 0,
    max_blocks: int | None = None,
    to_height: int | None = None,
    reset: bool = False,
    batch_size: int = 50,
) -> dict[str, Any]:
    if reset:
        reset_top_wallet_cluster_index(store, start_height=start_height)

    progress = top_wallet_cluster_progress(store)
    last_height = int_or_none(progress.get("last_height"))
    if last_height is None or last_height < start_height - 1:
        last_height = start_height - 1
    chain_height = int(rpc.call("getblockcount"))
    target_height = min(chain_height, to_height if to_height is not None else chain_height)
    if max_blocks is not None:
        target_height = min(target_height, last_height + max_blocks)
    sentry_collateral_outpoints = top_wallet_sentry_collateral_outpoints(store)

    totals = {
        "blocks": 0,
        "txs": 0,
        "outputs": 0,
        "spends": 0,
        "missing_spends": 0,
        "groups": 0,
        "sentry_groups": 0,
        "edges": 0,
        "sentry_edges": 0,
        "sentry_collateral_outpoints": len(sentry_collateral_outpoints),
        "start_height": last_height + 1,
        "last_height": last_height,
        "target_height": target_height,
        "chain_height": chain_height,
    }
    if target_height <= last_height:
        progress.update({"chain_height": chain_height, "synced_at": now_iso()})
        store.set_meta("top_wallet_cluster_index", progress)
        return totals

    batch_size = max(1, batch_size)
    height = last_height + 1
    while height <= target_height:
        heights = list(range(height, min(target_height, height + batch_size - 1) + 1))
        block_hashes = rpc.batch_call([("getblockhash", [item]) for item in heights])
        blocks = rpc.batch_call([("getblock", [block_hash, 2]) for block_hash in block_hashes])

        if len(block_hashes) != len(heights) or len(blocks) != len(heights):
            raise RuntimeError("top wallet cluster RPC batch returned an unexpected number of results")

        for block_height, block_hash, block in zip(heights, block_hashes, blocks):
            previous_hash = progress.get("last_hash")
            if block_height > 0 and previous_hash and block.get("previousblockhash") != previous_hash:
                raise RuntimeError(
                    "top wallet cluster index hit a chain reorg; rerun sync-top-wallet-clusters with --reset to rebuild"
                )

            store.conn.execute("BEGIN")
            block_stats = process_top_wallet_cluster_block(store, block, sentry_collateral_outpoints)
            progress = {
                "last_height": block_height,
                "last_hash": block_hash,
                "chain_height": chain_height,
                "synced_at": now_iso(),
            }
            store.set_meta("top_wallet_cluster_index", progress, commit=False)
            store.conn.commit()

            totals["blocks"] += 1
            totals["last_height"] = block_height
            for key in ("txs", "outputs", "spends", "missing_spends", "groups", "sentry_groups", "edges", "sentry_edges"):
                totals[key] += block_stats[key]

        height = heights[-1] + 1

    return totals


class UnionFind:
    def __init__(self) -> None:
        self.parent: dict[str, str] = {}

    def find(self, item: str) -> str:
        parent = self.parent.setdefault(item, item)
        if parent != item:
            self.parent[item] = self.find(parent)
        return self.parent[item]

    def union(self, left: str, right: str) -> None:
        left_root = self.find(left)
        right_root = self.find(right)
        if left_root == right_root:
            return
        if right_root < left_root:
            left_root, right_root = right_root, left_root
        self.parent[right_root] = left_root


def top_wallet_cluster_snapshot(store: Store, limit: int = 100) -> dict[str, Any]:
    progress = top_wallet_cluster_progress(store)
    last_height = int_or_none(progress.get("last_height"))
    chain_height = int_or_none(progress.get("chain_height"))
    complete = bool(chain_height is not None and last_height is not None and last_height >= chain_height)
    edge_totals = store.conn.execute(
        """
        SELECT COUNT(*) AS edges,
               COALESCE(SUM(tx_count), 0) AS edge_observations
        FROM top_wallet_cluster_edges
        """
    ).fetchone()
    balance_rows = store.conn.execute(
        """
        SELECT address, balance_sats, utxo_count, last_seen_height, last_seen_time
        FROM top_wallet_balances
        WHERE balance_sats > 0
        """
    ).fetchall()
    balance_total = sum(int(row["balance_sats"] or 0) for row in balance_rows)

    if not edge_totals["edges"]:
        return {
            "generated_at": now_iso(),
            "type": "estimated_wallet_clusters",
            "stage": "forensic-clusters",
            "limit": limit,
            "index": {
                "last_height": last_height,
                "chain_height": chain_height,
                "complete": complete,
                "synced_at": progress.get("synced_at"),
            },
            "totals": {
                "clusters": 0,
                "funded_addresses": len(balance_rows),
                "known_exchange_clusters": 0,
                "estimated_private_holders": 0,
                "balance_sats": balance_total,
                "balance_sys": sats_to_sys_string(balance_total),
                "known_exchange_sats": 0,
                "known_exchange_sys": "0",
                "private_unknown_sats": balance_total,
                "private_unknown_sys": sats_to_sys_string(balance_total),
                "edges": 0,
                "edge_observations": 0,
            },
            "wallets": [],
        }

    union = UnionFind()
    for row in store.conn.execute("SELECT address_a, address_b FROM top_wallet_cluster_edges"):
        union.union(row["address_a"], row["address_b"])
    member_counts: dict[str, int] = {}
    for address in list(union.parent):
        root = union.find(address)
        member_counts[root] = member_counts.get(root, 0) + 1
    node_counts: dict[str, int] = {}
    if store.conn.execute("SELECT COUNT(*) AS count FROM network_masternodes").fetchone()["count"] == 0:
        load_network_masternodes_csv(store)
    for row in store.conn.execute(
        """
        SELECT collateral_address
        FROM network_masternodes
        WHERE collateral_address != '' AND COALESCE(removed_at, '') = ''
        """
    ):
        collateral_address = str(row["collateral_address"] or "")
        if not collateral_address:
            continue
        root = union.find(collateral_address)
        node_counts[root] = node_counts.get(root, 0) + 1

    exchange_tags = load_exchange_tags()
    wallet_labels = load_wallet_labels()
    clusters: dict[str, dict[str, Any]] = {}
    for row in balance_rows:
        address = row["address"]
        root = union.find(address)
        balance_sats = int(row["balance_sats"] or 0)
        cluster = clusters.setdefault(
            root,
            {
                "address": address,
                "address_count": member_counts.get(root, 0),
                "funded_address_count": 0,
                "balance_sats": 0,
                "utxo_count": 0,
                "last_seen_height": None,
                "last_seen_time": None,
                "representative_balance_sats": -1,
                "name": short_address(address),
                "label": "Unknown",
                "label_priority": 0,
                "node_count": node_counts.get(root, 0),
            },
        )
        cluster["funded_address_count"] += 1
        if int(cluster["address_count"]) < int(cluster["funded_address_count"]):
            cluster["address_count"] = int(cluster["funded_address_count"])
        cluster["balance_sats"] += balance_sats
        cluster["utxo_count"] += int(row["utxo_count"] or 0)
        if balance_sats > int(cluster["representative_balance_sats"]):
            cluster["address"] = address
            cluster["representative_balance_sats"] = balance_sats
        row_height = int_or_none(row["last_seen_height"])
        if row_height is not None and row_height >= (int_or_none(cluster["last_seen_height"]) or -1):
            cluster["last_seen_height"] = row_height
            cluster["last_seen_time"] = row["last_seen_time"]

        identity = wallet_identity_for_address(address, wallet_labels, exchange_tags)
        label = identity["label"]
        priority = 2 if label == "Exchange" else 1 if label != "Unknown" else 0
        if priority > int(cluster["label_priority"]):
            cluster["name"] = identity["name"]
            cluster["label"] = label
            cluster["label_priority"] = priority

    for root, node_count in node_counts.items():
        cluster = clusters.get(root)
        if not cluster:
            continue
        cluster["node_count"] = node_count
        if cluster["label"] == "Unknown":
            cluster["label"] = "Node Operator"
            cluster["label_priority"] = 1

    ordered = sorted(clusters.values(), key=lambda item: (-int(item["balance_sats"]), str(item["address"])))
    wallets = []
    for rank, cluster in enumerate(ordered[:limit], 1):
        balance_sats = int(cluster["balance_sats"])
        wallets.append(
            {
                "rank": rank,
                "name": cluster["name"],
                "label": cluster["label"],
                "address": cluster["address"],
                "addresses": [cluster["address"]],
                "address_count": int(cluster["address_count"]),
                "balance_sats": balance_sats,
                "balance_sys": sats_to_sys_string(balance_sats),
                "utxo_count": int(cluster["utxo_count"]),
                "node_count": int(cluster.get("node_count") or 0),
                "last_seen_height": cluster["last_seen_height"],
                "last_seen_time": cluster["last_seen_time"],
                "percent_text": fmt_percent(balance_sats, balance_total),
            }
        )

    known_exchange_clusters = sum(1 for item in clusters.values() if item["label"] == "Exchange")
    known_exchange_sats = sum(int(item["balance_sats"]) for item in clusters.values() if item["label"] == "Exchange")
    private_unknown_sats = balance_total - known_exchange_sats
    return {
        "generated_at": now_iso(),
        "type": "estimated_wallet_clusters",
        "stage": "forensic-clusters",
        "limit": limit,
        "index": {
            "last_height": last_height,
            "chain_height": chain_height,
            "complete": complete,
            "synced_at": progress.get("synced_at"),
        },
        "totals": {
            "clusters": len(clusters),
            "funded_addresses": len(balance_rows),
            "known_exchange_clusters": known_exchange_clusters,
            "estimated_private_holders": len(clusters) - known_exchange_clusters,
            "balance_sats": balance_total,
            "balance_sys": sats_to_sys_string(balance_total),
            "known_exchange_sats": known_exchange_sats,
            "known_exchange_sys": sats_to_sys_string(known_exchange_sats),
            "private_unknown_sats": private_unknown_sats,
            "private_unknown_sys": sats_to_sys_string(private_unknown_sats),
            "edges": int(edge_totals["edges"] or 0),
            "edge_observations": int(edge_totals["edge_observations"] or 0),
        },
        "wallets": wallets,
    }


def top_wallets_snapshot(store: Store, limit: int = 100) -> dict[str, Any]:
    progress = top_wallet_progress(store)
    exchange_tags = load_exchange_tags()
    wallet_labels = load_wallet_labels()
    rows = store.conn.execute(
        """
        SELECT address, balance_sats, utxo_count, last_seen_height, last_seen_time
        FROM top_wallet_balances
        WHERE balance_sats > 0
        ORDER BY balance_sats DESC, address
        LIMIT ?
        """,
        (limit,),
    ).fetchall()
    totals = store.conn.execute(
        """
        SELECT COUNT(*) AS addresses,
               COALESCE(SUM(balance_sats), 0) AS balance_sats,
               COALESCE(SUM(utxo_count), 0) AS utxos
        FROM top_wallet_balances
        WHERE balance_sats > 0
        """
    ).fetchone()
    wallets = []
    for rank, row in enumerate(rows, 1):
        balance_sats = int(row["balance_sats"] or 0)
        address = row["address"]
        identity = wallet_identity_for_address(address, wallet_labels, exchange_tags)
        wallets.append(
            {
                "rank": rank,
                "name": identity["name"],
                "label": identity["label"],
                "address": address,
                "addresses": [address],
                "address_count": 1,
                "balance_sats": balance_sats,
                "balance_sys": sats_to_sys_string(balance_sats),
                "utxo_count": int(row["utxo_count"] or 0),
                "last_seen_height": row["last_seen_height"],
                "last_seen_time": row["last_seen_time"],
            }
        )

    last_height = int_or_none(progress.get("last_height"))
    chain_height = int_or_none(progress.get("chain_height"))
    complete = bool(chain_height is not None and last_height is not None and last_height >= chain_height)
    return {
        "generated_at": now_iso(),
        "type": "address_balances",
        "stage": "exact-addresses",
        "limit": limit,
        "index": {
            "last_height": last_height,
            "chain_height": chain_height,
            "complete": complete,
            "synced_at": progress.get("synced_at"),
        },
        "totals": {
            "addresses": int(totals["addresses"] or 0),
            "balance_sats": int(totals["balance_sats"] or 0),
            "balance_sys": sats_to_sys_string(int(totals["balance_sats"] or 0)),
            "utxos": int(totals["utxos"] or 0),
        },
        "wallets": wallets,
        "estimated_clusters": top_wallet_cluster_snapshot(store, limit=limit),
    }


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


def refresh_exchange_hot_wallet_balances(
    store: Store,
    client: BlockbookClient,
    wallets: list[dict[str, str]] | None = None,
) -> dict[str, dict[str, Any]]:
    wallets = wallets if wallets is not None else load_exchange_hot_wallets() + load_exchange_cold_wallets()
    known_addresses = {wallet["address"] for wallet in wallets}
    previous = store.get_meta("exchange_hot_wallet_balances", {}) or {}
    updated: dict[str, dict[str, Any]] = {
        address: values
        for address, values in previous.items()
        if address in known_addresses and isinstance(values, dict)
    }
    synced_at = now_iso()

    for wallet in wallets:
        address = wallet["address"]
        try:
            payload = client.address(address, details="basic", page_size=1)
            updated[address] = {
                "label": wallet["label"],
                "address": address,
                "balance_sats": sats(payload.get("balance")),
                "total_received_sats": sats(payload.get("totalReceived")),
                "total_sent_sats": sats(payload.get("totalSent")),
                "txs": payload.get("txs"),
                "synced_at": synced_at,
            }
        except Exception as exc:
            cached = dict(updated.get(address, {}))
            cached.update(
                {
                    "label": wallet["label"],
                    "address": address,
                    "error": str(exc),
                }
            )
            updated[address] = cached

    store.set_meta("exchange_hot_wallet_balances", updated)
    return updated


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


def atomic_write_text(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp_path = path.with_name(f".{path.name}.tmp")
    temp_path.write_text(content, encoding="utf-8")
    temp_path.replace(path)


def atomic_write_json(path: Path, payload: Any) -> None:
    atomic_write_text(path, json.dumps(payload, indent=2, sort_keys=True) + "\n")


def sync_static_snapshot(
    store: Store,
    client: BlockbookClient,
    rpc: SyscoinRpcClient | None,
    *,
    address: str,
    page_size: int,
    max_pages: int | None,
    from_height: int | None,
    since_time: int | None,
    csv_path: Path,
    next_hop_limit: int,
    node_spend_limit: int,
) -> dict[str, Any]:
    watched = {address}
    stats = sync_address(
        store,
        client,
        address,
        page_size=page_size,
        max_pages=max_pages,
        from_height=from_height,
        watched=watched,
        quiet=True,
    )
    load_node_outputs_csv(store)
    load_verified_sentries_csv(store)
    follow_stats = refresh_spent_first_hops(
        store,
        client,
        watched,
        since_time=since_time,
        limit=next_hop_limit,
        min_sats=sys_to_sats("100"),
        page_size=min(page_size, 100),
        max_pages_per_address=1,
    )
    node_stats = refresh_node_spends(
        store,
        client,
        watched,
        limit=node_spend_limit,
        page_size=min(page_size, 100),
        max_pages_per_address=1,
    )
    exchange_balances = refresh_exchange_hot_wallet_balances(store, client)

    masternode_stats: dict[str, Any] | None = None
    if rpc is not None:
        if store.conn.execute("SELECT COUNT(*) AS count FROM network_masternodes").fetchone()["count"] == 0:
            load_network_masternodes_csv(store, csv_path)
        masternode_stats = sync_network_masternodes(store, rpc, client)
        write_network_masternodes_csv(network_masternode_rows_from_store(store), csv_path)
    else:
        load_network_masternodes_csv(store, csv_path)

    return {
        "synced_at": now_iso(),
        "wallet": stats,
        "next_hop": follow_stats,
        "node_spends": node_stats,
        "exchange_wallets": len(exchange_balances),
        "masternodes": masternode_stats,
    }


def publish_static_snapshot(
    store: Store,
    client: BlockbookClient,
    rpc: SyscoinRpcClient | None,
    *,
    output_dir: Path,
    address: str,
    page_size: int,
    max_pages: int | None,
    from_height: int | None,
    since_time: int | None,
    since_label: str | None,
    refresh_seconds: int,
    csv_path: Path,
    next_hop_limit: int,
    node_spend_limit: int,
) -> dict[str, Any]:
    output_dir.mkdir(parents=True, exist_ok=True)
    csv_path = csv_path if csv_path.is_absolute() else Path.cwd() / csv_path
    sync_stats = sync_static_snapshot(
        store,
        client,
        rpc,
        address=address,
        page_size=page_size,
        max_pages=max_pages,
        from_height=from_height,
        since_time=since_time,
        csv_path=csv_path,
        next_hop_limit=next_hop_limit,
        node_spend_limit=node_spend_limit,
    )
    index_html = dashboard_html(store, since_time=since_time, since_label=since_label, refresh_seconds=refresh_seconds)
    masternode_page = masternodes_html(store, since_time=since_time, since_label=since_label, refresh_seconds=refresh_seconds)
    top_wallets = top_wallets_snapshot(store)
    top_wallets_page = top_wallets_html(store, refresh_seconds=refresh_seconds)
    atomic_write_text(output_dir / "index.html", index_html)
    atomic_write_text(output_dir / "wallet-flows.html", index_html)
    atomic_write_text(output_dir / "sentrynode.html", masternode_page)
    atomic_write_text(output_dir / "masternodes.html", masternode_page)
    atomic_write_text(output_dir / TOP_WALLETS_HTML, top_wallets_page)
    atomic_write_json(output_dir / TOP_WALLETS_JSON, top_wallets)
    if CHART_ASSET_PATH.exists():
        asset_target = output_dir / CHART_ASSET_ROUTE.lstrip("/")
        asset_target.parent.mkdir(parents=True, exist_ok=True)
        temp_asset = asset_target.with_name(f".{asset_target.name}.tmp")
        temp_asset.write_bytes(CHART_ASSET_PATH.read_bytes())
        temp_asset.replace(asset_target)
    if csv_path.exists():
        network_csv = output_dir / "network_masternodes.csv"
        temp_csv = network_csv.with_name(f".{network_csv.name}.tmp")
        temp_csv.write_bytes(csv_path.read_bytes())
        temp_csv.replace(network_csv)
    atomic_write_json(
        output_dir / "status.json",
        {
            **sync_stats,
            "pages": {
                "wallet_flows": "index.html",
                "sentry_node": "sentrynode.html",
                "legacy_masternodes": "masternodes.html",
                "top_wallets": TOP_WALLETS_HTML,
                "top_wallets_json": TOP_WALLETS_JSON,
                "network_masternodes": "network_masternodes.csv",
            },
        },
    )
    return sync_stats


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
    exchange_cold_wallets = load_exchange_cold_wallets()
    exchange_hot_wallet_balances = store.get_meta("exchange_hot_wallet_balances", {}) or {}
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

    exchange_sent_sats_by_label: dict[str, int] = {}
    for row in top_rows:
        labels = set()
        if row["address"] in exchange_routes:
            labels.add(exchange_routes[row["address"]])
        if row["address"] in exchange_tags:
            labels.add(exchange_tags[row["address"]])
        labels.update(later_exchanges_by_address.get(row["address"], set()))
        for label in labels:
            exchange_sent_sats_by_label[label] = exchange_sent_sats_by_label.get(label, 0) + int(row["total_sats"] or 0)

    def wallet_card_rows(wallets: list[dict[str, str]], include_sent_total: bool = False) -> list[str]:
        rows = []
        for wallet in wallets:
            note = wallet["note"]
            note_html = f"<span>{html.escape(note)}</span>" if note else ""
            if include_sent_total:
                sent_sats = exchange_sent_sats_by_label.get(wallet["label"], 0)
                sent_html = (
                    f"<span class='wallet-flow'>From Binance: "
                    f"<b title='{fmt_sys(sent_sats)} SYS'>{fmt_compact_sys(sent_sats)}</b></span>"
                    if sent_sats
                    else "<span class='wallet-flow muted'>From Binance: -</span>"
                )
            else:
                sent_html = ""
            wallet_balance = exchange_hot_wallet_balances.get(wallet["address"], {})
            if isinstance(wallet_balance, dict) and "balance_sats" in wallet_balance:
                balance_sats = int(wallet_balance.get("balance_sats") or 0)
                if balance_sats < SATOSHI:
                    continue
                balance_text = fmt_compact_sys(balance_sats)
                balance_html = (
                    f"<span class='wallet-balance'>Final balance: "
                    f"<b title='{fmt_sys(balance_sats)} SYS'>{html.escape(balance_text)}</b></span>"
                )
            else:
                balance_html = "<span class='wallet-balance muted'>Final balance: -</span>"
            rows.append(
                f"<article class='wallet-card'>"
                f"<strong>{html.escape(wallet['label'])}</strong>"
                f"{note_html}"
                f"<a href='{explorer_addr(wallet['address'])}' title='{html.escape(wallet['address'])}'>"
                f"{html.escape(short_address(wallet['address']))}</a>"
                f"{sent_html}"
                f"{balance_html}"
                f"</article>"
            )
        return rows

    hot_wallet_rows = wallet_card_rows(exchange_hot_wallets, include_sent_total=True)
    cold_wallet_rows = wallet_card_rows(exchange_cold_wallets)
    hot_wallet_html = "\n".join(hot_wallet_rows)
    cold_wallet_html = "\n".join(cold_wallet_rows)
    visible_hot_wallet_count = len(hot_wallet_rows)
    visible_cold_wallet_count = len(cold_wallet_rows)

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
    since_text = dt.datetime.fromtimestamp(since_time, tz=dt.timezone.utc).astimezone(ZoneInfo(DEFAULT_TIMEZONE)).strftime("%b %-d, %Y") if since_time else "all tracked history"
    updated_text = fmt_iso_local_datetime(last_summary.get("synced_at"))
    total_sent_full = fmt_sys(window_summary["external_sats"])
    wallet_balance_sats = int(last_summary.get("balance_sats") or 0)
    wallet_balance_full = fmt_sys(wallet_balance_sats)
    hot_wallet_address = last_summary.get("address") or DEFAULT_ADDRESS

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
    .metrics {{ display: grid; grid-template-columns: repeat(5, minmax(150px, 1fr)); gap: 12px; }}
    .metric {{ background: #fff; border: 1px solid #d9ded8; border-radius: 8px; padding: 14px 16px; min-width: 0; }}
    .metric span {{ display: block; color: #687177; font-size: 0.84rem; margin-bottom: 6px; }}
    .metric b {{ display: block; font-size: clamp(1.25rem, 2vw, 1.65rem); line-height: 1.1; overflow-wrap: anywhere; }}
    .metric .metric-address {{ display: block; font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, "Liberation Mono", monospace; font-size: 0.72rem; margin-top: 7px; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }}
    .panel-title {{ display: flex; align-items: end; justify-content: space-between; gap: 16px; }}
    .panel-title h2 {{ margin: 0; font-size: 1.25rem; }}
    .panel-title p {{ margin: 0; color: #687177; font-size: 0.9rem; }}
    .table-controls {{ align-items: end; display: flex; justify-content: flex-end; margin: 10px 0; }}
    .pagination-controls {{ align-items: end; display: flex; flex-wrap: wrap; gap: 10px; justify-content: flex-end; margin-left: auto; }}
    .page-size-control {{ color: #687177; display: grid; font-size: 0.78rem; font-weight: 700; gap: 4px; width: 110px; }}
    .page-size-control select {{ background: #fff; border: 1px solid #cfd7d1; border-radius: 6px; color: #1c2227; font: inherit; min-height: 36px; padding: 7px 9px; }}
    .pager {{ align-items: center; color: #687177; display: flex; gap: 8px; justify-content: flex-end; }}
    .pager button {{ background: #fff; border: 1px solid #cfd7d1; border-radius: 6px; color: #1c2227; cursor: pointer; font: inherit; min-height: 36px; padding: 7px 10px; }}
    .pager button:disabled {{ cursor: default; opacity: 0.45; }}
    .page-status {{ color: #687177; font-size: 0.86rem; min-width: 96px; text-align: center; }}
    .wallet-list {{ display: grid; grid-template-columns: repeat(5, minmax(0, 1fr)); gap: 10px; margin-top: 10px; }}
    .wallet-card {{ background: #fff; border: 1px solid #d9ded8; border-radius: 8px; padding: 12px 12px; min-width: 0; display: grid; gap: 5px; }}
    .wallet-card strong {{ font-size: 0.95rem; }}
    .wallet-card span {{ color: #687177; font-size: 0.82rem; }}
    .wallet-card a {{ display: block; max-width: 100%; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, "Liberation Mono", monospace; font-size: 0.72rem; }}
    .wallet-card .wallet-balance, .wallet-card .wallet-flow {{ color: #1c2227; font-size: 0.82rem; }}
    .wallet-card .wallet-balance b, .wallet-card .wallet-flow b {{ font-weight: 800; }}
    .wallet-card .muted {{ color: #7b858a; }}
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
      .wallet-list {{ grid-template-columns: repeat(2, minmax(0, 1fr)); }}
      .panel-title {{ align-items: start; flex-direction: column; }}
      .table-controls {{ align-items: stretch; flex-direction: column; }}
      .pagination-controls {{ align-items: stretch; flex-direction: column; margin-left: 0; width: 100%; }}
      .page-size-control {{ width: 100%; }}
      .pager {{ justify-content: space-between; }}
      .topbar {{ align-items: start; flex-direction: column; }}
      .nav {{ justify-content: flex-start; }}
      .header-inner, main {{ margin-left: 12px; margin-right: 12px; }}
      table {{ min-width: 1060px; }}
    }}
    @media(max-width: 520px) {{
      .metrics, .wallet-list {{ grid-template-columns: 1fr; }}
    }}
    @media (prefers-color-scheme: dark) {{
      body {{ background: #121619; color: #f3f4f6; }}
      .metric, .wallet-card, .table-wrap, table, .floating-table-header {{ background: #1c2328; border-color: #334047; }}
      th {{ background: #263139; }}
      th, td {{ border-color: #334047; }}
      .page-size-control select, .pager button {{ background: #1c2328; border-color: #46555e; color: #f3f4f6; }}
      a {{ color: #67d7ff; }}
      .subtitle {{ color: #b6c3c7; }}
      .metric span, .wallet-card span, .panel-title p, th:nth-child(1), td:nth-child(1), .subtle, .sort-icon, .page-size-control, .pager, .page-status {{ color: #a7b0b5; }}
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
          <div class="subtitle">Experimental view. Useful for spotting patterns, not proof of ownership or intent.</div>
        </div>
        <nav class="nav" aria-label="Dashboard pages">
          <a class="active" href="/">Wallet Flows</a>
          <a href="/sentrynode">Sentry Nodes</a>
          <a href="/top-wallets">Top Wallets</a>
        </nav>
      </div>
    </div>
  </header>
  <main>
    <section class="metrics">
      <div class="metric"><span>SYS Sent</span><b title="{html.escape(total_sent_full)} SYS">{fmt_compact_sys(window_summary["external_sats"])}</b></div>
      <div class="metric"><span>Binance Hot Wallet</span><b title="{html.escape(wallet_balance_full)} SYS">{fmt_compact_sys(wallet_balance_sats)}</b><a class="metric-address" href="{explorer_addr(hot_wallet_address)}" title="{html.escape(hot_wallet_address)}">{html.escape(short_address(hot_wallet_address))}</a></div>
      <div class="metric"><span>Transfers</span><b>{window_summary["txs"]}</b></div>
      <div class="metric"><span>Recipients</span><b>{destination_count}</b></div>
      <div class="metric"><span>Updated</span><b>{html.escape(updated_text)}</b></div>
    </section>
    <section class="hot-wallets">
      <div class="panel-title">
        <h2>Known Exchange Hot Wallets</h2>
        <p>{visible_hot_wallet_count} Blockbook links</p>
      </div>
      <div class="wallet-list">{hot_wallet_html}</div>
    </section>
    <section class="cold-wallets">
      <div class="panel-title">
        <h2>Known Cold Wallets</h2>
        <p>{visible_cold_wallet_count} Blockbook links</p>
      </div>
      <div class="wallet-list">{cold_wallet_html}</div>
    </section>
    <section>
      <div class="panel-title">
        <h2>Recipients Ranked By SYS</h2>
        <p>{destination_count} addresses, high to low</p>
      </div>
      <div class="table-controls" aria-label="Recipients table controls">
        <div class="pagination-controls">
          <label class="page-size-control" for="destinations-page-size">
            <span>Rows</span>
            <select id="destinations-page-size">
              <option value="20" selected>20</option>
              <option value="50">50</option>
              <option value="100">100</option>
            </select>
          </label>
          <div class="pager" aria-label="Recipients pagination">
            <button id="destinations-first" type="button">First</button>
            <button id="destinations-prev" type="button">Prev</button>
            <span class="page-status" id="destinations-page-status">0 of 0</span>
            <button id="destinations-next" type="button">Next</button>
            <button id="destinations-last" type="button">Last</button>
          </div>
        </div>
      </div>
      <div class="table-wrap"><table id="destinations-table"><thead><tr><th data-sort="number" data-default-dir="asc" aria-sort="ascending"><button class="sort-button" type="button">Rank<span class="sort-icon" aria-hidden="true"></span></button></th><th data-sort="text" data-default-dir="asc" aria-sort="none"><button class="sort-button" type="button">Address<span class="sort-icon" aria-hidden="true"></span></button></th><th data-sort="number" data-default-dir="desc" aria-sort="none"><button class="sort-button" type="button">SYS Sent<span class="sort-icon" aria-hidden="true"></span></button></th><th data-sort="number" data-default-dir="desc" aria-sort="none"><button class="sort-button" type="button">Share<span class="sort-icon" aria-hidden="true"></span></button></th><th data-sort="number" data-default-dir="desc" aria-sort="none"><button class="sort-button" type="button">Sends<span class="sort-icon" aria-hidden="true"></span></button></th><th data-sort="number" data-default-dir="desc" title="Amount already spent by the first recipient address" aria-sort="none"><button class="sort-button" type="button">Sent Again<span class="sort-icon" aria-hidden="true"></span></button></th><th data-sort="text" data-default-dir="asc" aria-sort="none"><button class="sort-button" type="button">Exchange<span class="sort-icon" aria-hidden="true"></span></button></th><th data-sort="number" data-default-dir="desc" title="Exact 100,000 SYS output seen at this hop or the next traced hop" aria-sort="none"><button class="sort-button" type="button">100k Sentry<span class="sort-icon" aria-hidden="true"></span></button></th><th data-sort="number" data-default-dir="desc" aria-sort="none"><button class="sort-button" type="button">Last Seen<span class="sort-icon" aria-hidden="true"></span></button></th></tr></thead><tbody>{top_html}</tbody></table></div>
    </section>
  </main>
  <script>
    (() => {{
      const table = document.getElementById("destinations-table");
      if (!table) return;
      const headers = Array.from(table.querySelectorAll("th[data-sort]"));
      const tbody = table.tBodies[0];
      const pageSizeSelect = document.getElementById("destinations-page-size");
      const firstButton = document.getElementById("destinations-first");
      const prevButton = document.getElementById("destinations-prev");
      const nextButton = document.getElementById("destinations-next");
      const lastButton = document.getElementById("destinations-last");
      const pageStatus = document.getElementById("destinations-page-status");
      const storageKey = "syscoin-destinations-sort";
      let rows = Array.from(tbody.rows);
      let page = 1;
      let pageSize = Number(pageSizeSelect?.value || 20);
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
        rows.sort((left, right) => {{
          const leftValue = cellValue(left, index, type);
          const rightValue = cellValue(right, index, type);
          if (type === "number") return (leftValue - rightValue) * multiplier;
          return String(leftValue).localeCompare(String(rightValue)) * multiplier;
        }});
        rows.forEach((row) => tbody.appendChild(row));
        activeIndex = index;
        activeDirection = direction;
        page = 1;
        updateHeaderState(index, direction);
        renderPage();
        syncFloatingHeader();
        if (persist) {{
          localStorage.setItem(storageKey, JSON.stringify({{ index, direction }}));
        }}
      }};

      const renderPage = () => {{
        const total = rows.length;
        const totalPages = Math.max(1, Math.ceil(total / pageSize));
        page = Math.min(Math.max(page, 1), totalPages);
        const start = (page - 1) * pageSize;
        const end = Math.min(start + pageSize, total);
        rows.forEach((row, index) => {{
          row.hidden = index < start || index >= end;
        }});
        if (pageStatus) {{
          pageStatus.textContent = total ? `${{start + 1}}-${{end}} of ${{total}}` : "0 of 0";
        }}
        if (firstButton) firstButton.disabled = page <= 1 || total === 0;
        if (prevButton) prevButton.disabled = page <= 1 || total === 0;
        if (nextButton) nextButton.disabled = page >= totalPages || total === 0;
        if (lastButton) lastButton.disabled = page >= totalPages || total === 0;
        syncFloatingHeader();
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
      pageSizeSelect?.addEventListener("change", () => {{
        pageSize = Number(pageSizeSelect.value || 20);
        page = 1;
        renderPage();
      }});
      firstButton?.addEventListener("click", () => {{
        page = 1;
        renderPage();
      }});
      prevButton?.addEventListener("click", () => {{
        page -= 1;
        renderPage();
      }});
      nextButton?.addEventListener("click", () => {{
        page += 1;
        renderPage();
      }});
      lastButton?.addEventListener("click", () => {{
        page = Math.max(1, Math.ceil(rows.length / pageSize));
        renderPage();
      }});
      window.addEventListener("scroll", syncFloatingHeader, {{ passive: true }});
      window.addEventListener("resize", syncFloatingHeader);
      renderPage();

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


def top_wallets_html(store: Store, refresh_seconds: int = 60, limit: int = 100) -> str:
    snapshot = top_wallets_snapshot(store, limit=limit)
    wallets = snapshot["wallets"]
    index = snapshot["index"]
    totals = snapshot["totals"]
    last_height = index.get("last_height")
    chain_height = index.get("chain_height")
    indexed_blocks = (int(last_height) + 1) if isinstance(last_height, int) and last_height >= 0 else 0
    remaining_blocks = (
        max(int(chain_height) - int(last_height), 0)
        if isinstance(chain_height, int) and isinstance(last_height, int)
        else None
    )
    block_height_text = (
        f"{int(chain_height):,}"
        if isinstance(chain_height, int)
        else f"{int(last_height):,}"
        if isinstance(last_height, int) and last_height >= 0
        else "not started"
    )
    remaining_text = f"{remaining_blocks:,}" if remaining_blocks is not None else "-"
    updated_text = fmt_iso_local_datetime(index.get("synced_at"))
    indexed_total_sats = int(totals["balance_sats"] or 0)

    def explorer_addr(addr: str) -> str:
        return f"https://explorer-blockbook.syscoin.org/address/{urllib.parse.quote(addr)}"

    def render_wallet_row(row: dict[str, Any]) -> str:
        last_seen = fmt_table_datetime(row["last_seen_time"]) if row.get("last_seen_time") else "-"
        last_seen_full = fmt_local_datetime(row["last_seen_time"]) if row.get("last_seen_time") else ""
        address = row["address"]
        balance_sats = int(row["balance_sats"])
        name = row.get("name") or short_address(address)
        label = row.get("label") or "Unknown"
        label_lower = label.lower()
        label_class = (
            "exchange"
            if label_lower.startswith("exchange")
            else "operator"
            if "operator" in label_lower
            else "private"
            if "private" in label_lower or "holder" in label_lower or "user" in label_lower or "person" in label_lower
            else "unknown"
        )
        address_count = int(row.get("address_count") or 1)
        address_word = "address" if address_count == 1 else "addresses"
        return (
            "<tr>"
            f"<td class='rank' data-sort='{row['rank']}'>{row['rank']}</td>"
            f"<td class='wallet-name' data-sort='{html.escape(name.lower())}'>{html.escape(name)}</td>"
            f"<td data-sort='{html.escape(label.lower())}'><span class='label-pill {label_class}'>{html.escape(label)}</span></td>"
            f"<td class='address-list' data-sort='{address_count}'><a href='{explorer_addr(address)}' title='{html.escape(address)}'>{address_count:,} {address_word}</a><span>{html.escape(short_address(address))}</span></td>"
            f"<td class='amount' data-sort='{balance_sats}' title='{html.escape(row['balance_sys'])} SYS'>{fmt_compact_sys(balance_sats)}</td>"
            f"<td data-sort='{balance_sats}'>{row.get('percent_text') or fmt_percent(balance_sats, indexed_total_sats)}</td>"
            f"<td data-sort='{row['last_seen_time'] or 0}' title='{html.escape(last_seen_full)}'>{html.escape(last_seen)}</td>"
            f"</tr>"
        )

    rows = [render_wallet_row(row) for row in wallets]
    if not rows:
        rows = ["<tr><td class='empty' colspan='7'>No top wallet index data yet.</td></tr>"]
    rows_html = "\n".join(rows)
    panel_status = f"Updated {html.escape(updated_text or '-')}"
    cluster_snapshot = snapshot["estimated_clusters"]
    cluster_wallets = cluster_snapshot["wallets"]
    cluster_index = cluster_snapshot["index"]
    cluster_totals = cluster_snapshot["totals"]
    cluster_last_height = cluster_index.get("last_height")
    cluster_chain_height = cluster_index.get("chain_height")
    cluster_progress_text = (
        f"{int(cluster_last_height):,} / {int(cluster_chain_height):,}"
        if isinstance(cluster_last_height, int) and isinstance(cluster_chain_height, int)
        else "not started"
    )
    cluster_status = (
        "Forensic cluster index complete"
        if cluster_index.get("complete")
        else f"Building forensic cluster index: {cluster_progress_text}"
        if cluster_last_height is not None and int(cluster_last_height) >= 0
        else "Forensic cluster index not started"
    )
    cluster_rows_html = "\n".join(render_wallet_row(row) for row in cluster_wallets)
    if not cluster_rows_html:
        cluster_rows_html = "<tr><td class='empty' colspan='7'>No estimated address clusters yet. Run the forensic cluster index to build this table.</td></tr>"

    def wallet_table_controls(control_id: str, label: str) -> str:
        return f"""
      <div class="table-controls" aria-label="{html.escape(label)} table controls">
        <div class="pagination-controls">
          <label class="page-size-control" for="{control_id}-page-size">
            <span>Rows</span>
            <select id="{control_id}-page-size">
              <option value="20" selected>20</option>
              <option value="50">50</option>
              <option value="100">100</option>
            </select>
          </label>
          <div class="pager" aria-label="{html.escape(label)} pagination">
            <button id="{control_id}-first" type="button">First</button>
            <button id="{control_id}-prev" type="button">Prev</button>
            <span class="page-status" id="{control_id}-page-status">0 of 0</span>
            <button id="{control_id}-next" type="button">Next</button>
            <button id="{control_id}-last" type="button">Last</button>
          </div>
        </div>
      </div>"""

    def operator_label_for_count(node_count: int) -> str:
        if node_count >= 100:
            return "Major Sentry Operator"
        if node_count >= 10:
            return "Large Sentry Operator"
        if node_count == 1:
            return "Solo Sentry Operator"
        return "Sentry Operator"

    def build_operator_rows() -> list[dict[str, Any]]:
        edge_count = store.conn.execute("SELECT COUNT(*) AS count FROM top_wallet_cluster_edges").fetchone()["count"]
        balance_count = store.conn.execute("SELECT COUNT(*) AS count FROM top_wallet_balances WHERE balance_sats > 0").fetchone()["count"]
        if not edge_count or not balance_count:
            return []

        union = UnionFind()
        for edge in store.conn.execute("SELECT address_a, address_b FROM top_wallet_cluster_edges"):
            union.union(edge["address_a"], edge["address_b"])

        wallet_labels = load_wallet_labels()
        exchange_tags = load_exchange_tags()
        clusters: dict[str, dict[str, Any]] = {}
        for balance_row in store.conn.execute(
            """
            SELECT address, balance_sats
            FROM top_wallet_balances
            WHERE balance_sats > 0
            """
        ):
            address = str(balance_row["address"] or "")
            balance_sats = int(balance_row["balance_sats"] or 0)
            root = union.find(address)
            cluster = clusters.setdefault(
                root,
                {
                    "address": address,
                    "balance_sats": 0,
                    "representative_balance_sats": -1,
                    "name": short_address(address),
                    "label": "Unknown",
                    "label_priority": 0,
                },
            )
            cluster["balance_sats"] += balance_sats
            if balance_sats > int(cluster["representative_balance_sats"]):
                cluster["address"] = address
                cluster["representative_balance_sats"] = balance_sats
            identity = wallet_identity_for_address(address, wallet_labels, exchange_tags)
            label = identity["label"]
            priority = 2 if label == "Exchange" else 1 if label != "Unknown" else 0
            if priority > int(cluster["label_priority"]):
                cluster["name"] = identity["name"]
                cluster["label"] = label
                cluster["label_priority"] = priority

        if store.conn.execute("SELECT COUNT(*) AS count FROM network_masternodes").fetchone()["count"] == 0:
            load_network_masternodes_csv(store)

        operators: dict[str, dict[str, Any]] = {}
        for node_row in store.conn.execute(
            """
            SELECT collateral_address, status, collateral_height
            FROM network_masternodes
            WHERE collateral_address != '' AND COALESCE(removed_at, '') = ''
            """
        ):
            collateral_address = str(node_row["collateral_address"] or "")
            if not collateral_address:
                continue
            root = union.find(collateral_address)
            cluster = clusters.get(root)
            if not cluster:
                continue
            operator = operators.setdefault(
                root,
                {
                    "name": cluster["name"],
                    "address": cluster["address"],
                    "nodes": 0,
                    "locked_sats": 0,
                    "net_sats": int(cluster["balance_sats"]),
                    "seniority_counts": {"Base": 0, "Level 1": 0, "Level 2": 0},
                    "status_counts": {},
                },
            )
            operator["nodes"] += 1
            operator["locked_sats"] += SENTRY_COLLATERAL_SATS
            operator["net_sats"] = int(cluster["balance_sats"])

            collateral_height = int_or_none(node_row["collateral_height"])
            if collateral_height is not None and isinstance(cluster_chain_height, int):
                blocks_since_collateral = max(cluster_chain_height - collateral_height, 0)
                if blocks_since_collateral >= SENIORITY_LEVEL_2_BLOCKS:
                    seniority_label = "Level 2"
                elif blocks_since_collateral >= SENIORITY_LEVEL_1_BLOCKS:
                    seniority_label = "Level 1"
                else:
                    seniority_label = "Base"
                operator["seniority_counts"][seniority_label] += 1

            status = str(node_row["status"] or "").upper()
            status_key = "Banned" if "BANNED" in status else "Enabled" if status == "ENABLED" else status.title() or "Unknown"
            operator["status_counts"][status_key] = operator["status_counts"].get(status_key, 0) + 1

        def mix_text(counts: dict[str, int]) -> str:
            parts = []
            for label in ("Base", "Level 1", "Level 2"):
                count = int(counts.get(label) or 0)
                if count:
                    short_label = "L1" if label == "Level 1" else "L2" if label == "Level 2" else "Base"
                    parts.append(f"{count:,} {short_label}")
            return ", ".join(parts) if parts else "-"

        def status_text(counts: dict[str, int]) -> str:
            parts = []
            for label in ("Enabled", "Banned", "Unknown"):
                count = int(counts.get(label) or 0)
                if count:
                    parts.append(f"{count:,} {label.lower()}")
            for label, count in sorted(counts.items()):
                if label not in {"Enabled", "Banned", "Unknown"} and count:
                    parts.append(f"{count:,} {label.lower()}")
            return ", ".join(parts) if parts else "-"

        rows = []
        ordered = sorted(
            operators.values(),
            key=lambda item: (-int(item["nodes"]), -int(item["net_sats"]), str(item["name"]).lower()),
        )
        for rank, item in enumerate(ordered, 1):
            node_count = int(item["nodes"])
            rows.append(
                {
                    "rank": rank,
                    "name": item["name"],
                    "label": operator_label_for_count(node_count),
                    "nodes": node_count,
                    "locked_sats": int(item["locked_sats"]),
                    "net_sats": int(item["net_sats"]),
                    "seniority": mix_text(item["seniority_counts"]),
                    "status": status_text(item["status_counts"]),
                }
            )
        return rows

    operator_rows = build_operator_rows()

    def operator_label_class(label: str) -> str:
        label_lower = label.lower()
        if label_lower.startswith("major"):
            return "major"
        if label_lower.startswith("large"):
            return "large"
        if label_lower.startswith("solo"):
            return "solo"
        return "operator"

    operator_rows_html = "\n".join(
        (
            "<tr>"
            f"<td class='rank' data-sort='{row['rank']}'>{row['rank']}</td>"
            f"<td class='wallet-name' data-sort='{html.escape(row['name'].lower())}'>{html.escape(row['name'])}</td>"
            f"<td data-sort='{html.escape(row['label'].lower())}'><span class='label-pill operator-label {operator_label_class(row['label'])}'>{html.escape(row['label'])}</span></td>"
            f"<td class='amount' data-sort='{row['nodes']}'>{row['nodes']:,}</td>"
            f"<td class='amount' data-sort='{row['locked_sats']}'>{fmt_compact_sys(row['locked_sats'])}</td>"
            f"<td class='amount' data-sort='{row['net_sats']}'>{fmt_compact_sys(row['net_sats'])}</td>"
            f"<td data-sort='{html.escape(row['seniority'].lower())}'>{html.escape(row['seniority'])}</td>"
            f"<td data-sort='{html.escape(row['status'].lower())}'>{html.escape(row['status'])}</td>"
            "</tr>"
        )
        for row in operator_rows
    )
    if not operator_rows_html:
        operator_rows_html = "<tr><td class='empty' colspan='8'>No sentry operator clusters found yet. Run the forensic cluster index to build this table.</td></tr>"

    return f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <meta http-equiv="refresh" content="{max(refresh_seconds, 1)}">
  <title>Syscoin Top Wallets</title>
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
    .metrics {{ display: grid; grid-template-columns: repeat(5, minmax(140px, 1fr)); gap: 12px; }}
    .metric {{ background: #fff; border: 1px solid #d9ded8; border-radius: 8px; padding: 14px 16px; min-width: 0; }}
    .metric span {{ display: block; color: #687177; font-size: 0.84rem; margin-bottom: 6px; }}
    .metric b {{ display: block; font-size: clamp(1.2rem, 1.8vw, 1.55rem); line-height: 1.1; overflow-wrap: anywhere; }}
    .panel-title {{ display: flex; align-items: end; justify-content: space-between; gap: 16px; }}
    .panel-title h2 {{ margin: 0; font-size: 1.25rem; }}
    .panel-title p {{ margin: 0; color: #687177; font-size: 0.9rem; }}
    .phase-note {{ color: #687177; font-size: 0.9rem; line-height: 1.45; margin: 8px 0 10px; max-width: 880px; }}
    .table-controls {{ align-items: end; display: flex; justify-content: flex-end; margin: 10px 0; }}
    .pagination-controls {{ align-items: end; display: flex; flex-wrap: wrap; gap: 10px; justify-content: flex-end; margin-left: auto; }}
    .page-size-control {{ color: #687177; display: grid; font-size: 0.78rem; font-weight: 700; gap: 4px; width: 110px; }}
    .page-size-control select {{ background: #fff; border: 1px solid #cfd7d1; border-radius: 6px; color: #1c2227; font: inherit; min-height: 36px; padding: 7px 9px; }}
    .pager {{ align-items: center; color: #687177; display: flex; gap: 8px; justify-content: flex-end; }}
    .pager button {{ background: #fff; border: 1px solid #cfd7d1; border-radius: 6px; color: #1c2227; cursor: pointer; font: inherit; min-height: 36px; padding: 7px 10px; }}
    .pager button:disabled {{ cursor: default; opacity: 0.45; }}
    .page-status {{ color: #687177; font-size: 0.86rem; min-width: 96px; text-align: center; }}
    .table-wrap {{ background: #fff; border: 1px solid #d9ded8; border-radius: 8px; max-width: 100%; min-width: 0; overflow-x: auto; width: 100%; }}
    table {{ width: 100%; min-width: 980px; border-collapse: separate; border-spacing: 0; background: #fff; table-layout: fixed; }}
    th, td {{ padding: 8px 10px; border-bottom: 1px solid #e4e8e2; text-align: left; font-size: 0.9rem; overflow: hidden; text-overflow: ellipsis; }}
    th {{ background: #eaf0ec; position: sticky; top: 0; z-index: 10; box-shadow: 0 1px 0 #d9ded8; }}
    .sort-button {{ appearance: none; border: 0; background: transparent; color: inherit; cursor: pointer; display: inline-flex; align-items: center; gap: 4px; font: inherit; font-weight: 700; padding: 0; text-align: inherit; white-space: nowrap; }}
    .sort-button:hover, .sort-button:focus-visible {{ color: #086788; outline: none; }}
    .sort-icon {{ color: #687177; display: inline-block; font-size: 0.75rem; min-width: 1ch; }}
    th[aria-sort="ascending"] .sort-icon::before {{ content: "^"; }}
    th[aria-sort="descending"] .sort-icon::before {{ content: "v"; }}
    th[aria-sort="none"] .sort-icon::before {{ content: ""; }}
    th:nth-child(1), td:nth-child(1) {{ width: 70px; text-align: right; color: #687177; }}
    th:nth-child(2), td:nth-child(2) {{ width: 220px; }}
    th:nth-child(3), td:nth-child(3) {{ width: 130px; }}
    th:nth-child(4), td:nth-child(4) {{ width: 220px; }}
    th:nth-child(5), td:nth-child(5) {{ width: 145px; text-align: right; }}
    th:nth-child(6), td:nth-child(6) {{ width: 160px; text-align: right; }}
    th:nth-child(7), td:nth-child(7) {{ width: 145px; }}
    .wallet-name {{ font-weight: 800; }}
    .address-list {{ white-space: nowrap; }}
    .address-list a {{ font-weight: 700; }}
    .address-list span {{ color: #687177; display: block; font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, "Liberation Mono", monospace; font-size: 0.78rem; margin-top: 2px; overflow: hidden; text-overflow: ellipsis; }}
    .label-pill {{ border-radius: 999px; display: inline-flex; font-size: 0.78rem; font-weight: 800; line-height: 1; padding: 5px 9px; white-space: nowrap; }}
    .label-pill.exchange {{ background: #e8f3ff; color: #14589c; }}
    .label-pill.operator {{ background: #fff4dd; color: #805313; }}
    .label-pill.private {{ background: #f4edff; color: #6840a3; }}
    .label-pill.unknown {{ background: #eef1ef; color: #596268; }}
    .label-pill.operator-label.major {{ background: #e9f8ef; color: #126237; }}
    .label-pill.operator-label.large {{ background: #eef5ff; color: #1c5f96; }}
    .label-pill.operator-label.operator {{ background: #fff4dd; color: #805313; }}
    .label-pill.operator-label.solo {{ background: #f2efff; color: #5f45a6; }}
    .amount {{ font-weight: 800; white-space: nowrap; }}
    .operator-table {{ min-width: 1120px; }}
    .operator-table th:nth-child(1), .operator-table td:nth-child(1) {{ width: 70px; text-align: right; }}
    .operator-table th:nth-child(2), .operator-table td:nth-child(2) {{ width: 210px; }}
    .operator-table th:nth-child(3), .operator-table td:nth-child(3) {{ width: 210px; }}
    .operator-table th:nth-child(4), .operator-table td:nth-child(4) {{ width: 125px; text-align: right; }}
    .operator-table th:nth-child(5), .operator-table td:nth-child(5) {{ width: 145px; text-align: right; }}
    .operator-table th:nth-child(6), .operator-table td:nth-child(6) {{ width: 145px; text-align: right; }}
    .operator-table th:nth-child(7), .operator-table td:nth-child(7) {{ width: 210px; }}
    .operator-table th:nth-child(8), .operator-table td:nth-child(8) {{ width: 160px; }}
    .empty {{ color: #687177; padding: 18px 14px; text-align: center; }}
    a {{ color: #086788; text-decoration: none; }}
    @media(max-width: 820px) {{
      .metrics {{ grid-template-columns: repeat(2, minmax(0, 1fr)); }}
      .panel-title {{ align-items: start; flex-direction: column; }}
      .table-controls {{ align-items: stretch; flex-direction: column; }}
      .pagination-controls {{ align-items: stretch; flex-direction: column; margin-left: 0; width: 100%; }}
      .page-size-control {{ width: 100%; }}
      .pager {{ justify-content: space-between; }}
      .topbar {{ align-items: start; flex-direction: column; }}
      .nav {{ justify-content: flex-start; }}
      .header-inner, main {{ margin-left: 12px; margin-right: 12px; }}
    }}
    @media(max-width: 520px) {{
      .metrics {{ grid-template-columns: 1fr; }}
    }}
    @media (prefers-color-scheme: dark) {{
      body {{ background: #121619; color: #f3f4f6; }}
      .metric, .table-wrap, table {{ background: #1c2328; border-color: #334047; }}
      .page-size-control select, .pager button {{ background: #1c2328; border-color: #46555e; color: #f3f4f6; }}
      th {{ background: #263139; }}
      th, td {{ border-color: #334047; }}
      a {{ color: #67d7ff; }}
      .subtitle {{ color: #b6c3c7; }}
      .metric span, .panel-title p, .phase-note, th:nth-child(1), td:nth-child(1), .address-list span, .sort-icon, .empty, .page-size-control, .pager, .page-status {{ color: #a7b0b5; }}
      .label-pill.exchange {{ background: #173754; color: #9ed2ff; }}
      .label-pill.operator {{ background: #443111; color: #ffd98a; }}
      .label-pill.private {{ background: #352451; color: #d6bcff; }}
      .label-pill.unknown {{ background: #30383d; color: #c0c9ce; }}
      .label-pill.operator-label.major {{ background: #143e28; color: #aef0c6; }}
      .label-pill.operator-label.large {{ background: #173754; color: #9ed2ff; }}
      .label-pill.operator-label.operator {{ background: #443111; color: #ffd98a; }}
      .label-pill.operator-label.solo {{ background: #332652; color: #d8c7ff; }}
      .sort-button:hover, .sort-button:focus-visible {{ color: #67d7ff; }}
    }}
  </style>
</head>
<body>
  <header>
    <div class="header-inner">
      <div class="topbar">
        <div>
          <h1>Syscoin Top Wallets</h1>
          <div class="subtitle">Experimental on-chain forensics from our own RPC index. Labels and wallet clusters are estimates, not proof of ownership.</div>
        </div>
        <nav class="nav" aria-label="Dashboard pages">
          <a href="/">Wallet Flows</a>
          <a href="/sentrynode">Sentry Nodes</a>
          <a class="active" href="/top-wallets">Top Wallets</a>
        </nav>
      </div>
    </div>
  </header>
  <main>
    <section class="metrics">
      <div class="metric"><span>Block Height</span><b>{html.escape(block_height_text)}</b></div>
      <div class="metric"><span>Blocks Indexed</span><b>{indexed_blocks:,}</b></div>
      <div class="metric"><span>Blocks Remaining</span><b>{html.escape(remaining_text)}</b></div>
      <div class="metric"><span>Addresses</span><b>{int(totals['addresses']):,}</b></div>
      <div class="metric"><span>Indexed Balance</span><b title="{html.escape(str(totals['balance_sys']))} SYS">{fmt_compact_sys(indexed_total_sats)}</b></div>
    </section>
    <section>
      <div class="panel-title">
        <h2>Phase 2 Address Cluster Estimate</h2>
        <p>{html.escape(cluster_status)}</p>
      </div>
      <p class="phase-note">Common-input plus sentry-collateral clustering estimate. Address balances are exact; holder grouping is a forensic estimate, not proof of ownership.</p>
      <div class="metrics">
        <div class="metric"><span>Estimated Holders</span><b>{int(cluster_totals['clusters']):,}</b></div>
        <div class="metric"><span>Known Exchange Clusters</span><b>{int(cluster_totals['known_exchange_clusters']):,}</b></div>
        <div class="metric"><span>Estimated Private Holders</span><b>{int(cluster_totals['estimated_private_holders']):,}</b></div>
        <div class="metric"><span>Known Exchange SYS</span><b title="{html.escape(cluster_totals['known_exchange_sys'])} SYS">{fmt_compact_sys(int(cluster_totals['known_exchange_sats']))}</b></div>
        <div class="metric"><span>Private / Unknown SYS</span><b title="{html.escape(cluster_totals['private_unknown_sys'])} SYS">{fmt_compact_sys(int(cluster_totals['private_unknown_sats']))}</b></div>
      </div>
    </section>
    <section>
      <div class="panel-title">
        <h2>Estimated Address Clusters</h2>
        <p>{int(cluster_totals['edges']):,} forensic links</p>
      </div>
      {wallet_table_controls("estimated-clusters", "Estimated address clusters")}
      <div class="table-wrap">
        <table class="sortable-wallet-table" id="estimated-clusters-table">
          <thead>
            <tr>
              <th data-sort="number" data-default-dir="asc" aria-sort="ascending"><button class="sort-button" type="button">Rank<span class="sort-icon" aria-hidden="true"></span></button></th>
              <th data-sort="text" data-default-dir="asc" aria-sort="none"><button class="sort-button" type="button">Name<span class="sort-icon" aria-hidden="true"></span></button></th>
              <th data-sort="text" data-default-dir="asc" aria-sort="none"><button class="sort-button" type="button">Label<span class="sort-icon" aria-hidden="true"></span></button></th>
              <th data-sort="number" data-default-dir="desc" aria-sort="none"><button class="sort-button" type="button">Addresses<span class="sort-icon" aria-hidden="true"></span></button></th>
              <th data-sort="number" data-default-dir="desc" aria-sort="none"><button class="sort-button" type="button">Net Worth<span class="sort-icon" aria-hidden="true"></span></button></th>
              <th data-sort="number" data-default-dir="desc" aria-sort="none"><button class="sort-button" type="button">Percent of coins<span class="sort-icon" aria-hidden="true"></span></button></th>
              <th data-sort="number" data-default-dir="desc" aria-sort="none"><button class="sort-button" type="button">Last Change<span class="sort-icon" aria-hidden="true"></span></button></th>
            </tr>
          </thead>
          <tbody>{cluster_rows_html}</tbody>
        </table>
      </div>
    </section>
    <section>
      <div class="panel-title">
        <h2>Sentry Operator View</h2>
        <p>Derived from indexed clusters</p>
      </div>
      <p class="phase-note">Derived operator labels from estimated wallet clusters that contain active 100,000 SYS sentry collateral. Labels are thresholds, not identity proof.</p>
      {wallet_table_controls("sentry-operators", "Sentry operator view")}
      <div class="table-wrap">
        <table class="sortable-wallet-table operator-table" id="sentry-operators-table">
          <thead>
            <tr>
              <th data-sort="number" data-default-dir="asc" aria-sort="ascending"><button class="sort-button" type="button">Rank<span class="sort-icon" aria-hidden="true"></span></button></th>
              <th data-sort="text" data-default-dir="asc" aria-sort="none"><button class="sort-button" type="button">Name<span class="sort-icon" aria-hidden="true"></span></button></th>
              <th data-sort="text" data-default-dir="asc" aria-sort="none"><button class="sort-button" type="button">Label<span class="sort-icon" aria-hidden="true"></span></button></th>
              <th data-sort="number" data-default-dir="desc" aria-sort="none"><button class="sort-button" type="button">Sentry Nodes<span class="sort-icon" aria-hidden="true"></span></button></th>
              <th data-sort="number" data-default-dir="desc" aria-sort="none"><button class="sort-button" type="button">Locked SYS<span class="sort-icon" aria-hidden="true"></span></button></th>
              <th data-sort="number" data-default-dir="desc" aria-sort="none"><button class="sort-button" type="button">Net Worth<span class="sort-icon" aria-hidden="true"></span></button></th>
              <th data-sort="text" data-default-dir="asc" aria-sort="none"><button class="sort-button" type="button">Seniority Mix<span class="sort-icon" aria-hidden="true"></span></button></th>
              <th data-sort="text" data-default-dir="asc" aria-sort="none"><button class="sort-button" type="button">Status<span class="sort-icon" aria-hidden="true"></span></button></th>
            </tr>
          </thead>
          <tbody>{operator_rows_html}</tbody>
        </table>
      </div>
    </section>
    <section>
      <div class="panel-title">
        <h2>Largest Addresses</h2>
        <p>{panel_status}</p>
      </div>
      {wallet_table_controls("top-wallets", "Largest addresses")}
      <div class="table-wrap">
        <table class="sortable-wallet-table" id="top-wallets-table">
          <thead>
            <tr>
              <th data-sort="number" data-default-dir="asc" aria-sort="ascending"><button class="sort-button" type="button">Rank<span class="sort-icon" aria-hidden="true"></span></button></th>
              <th data-sort="text" data-default-dir="asc" aria-sort="none"><button class="sort-button" type="button">Name<span class="sort-icon" aria-hidden="true"></span></button></th>
              <th data-sort="text" data-default-dir="asc" aria-sort="none"><button class="sort-button" type="button">Label<span class="sort-icon" aria-hidden="true"></span></button></th>
              <th data-sort="number" data-default-dir="desc" aria-sort="none"><button class="sort-button" type="button">Addresses<span class="sort-icon" aria-hidden="true"></span></button></th>
              <th data-sort="number" data-default-dir="desc" aria-sort="none"><button class="sort-button" type="button">Net Worth<span class="sort-icon" aria-hidden="true"></span></button></th>
              <th data-sort="number" data-default-dir="desc" aria-sort="none"><button class="sort-button" type="button">Percent of coins<span class="sort-icon" aria-hidden="true"></span></button></th>
              <th data-sort="number" data-default-dir="desc" aria-sort="none"><button class="sort-button" type="button">Last Change<span class="sort-icon" aria-hidden="true"></span></button></th>
            </tr>
          </thead>
          <tbody>{rows_html}</tbody>
        </table>
      </div>
    </section>
  </main>
  <script>
    (() => {{
      document.querySelectorAll(".sortable-wallet-table").forEach((table) => {{
        const headers = Array.from(table.querySelectorAll("th[data-sort]"));
        const tbody = table.tBodies[0];
        const controlId = table.id ? table.id.replace(/-table$/, "") : "";
        const pageSizeSelect = controlId ? document.getElementById(`${{controlId}}-page-size`) : null;
        const firstButton = controlId ? document.getElementById(`${{controlId}}-first`) : null;
        const prevButton = controlId ? document.getElementById(`${{controlId}}-prev`) : null;
        const nextButton = controlId ? document.getElementById(`${{controlId}}-next`) : null;
        const lastButton = controlId ? document.getElementById(`${{controlId}}-last`) : null;
        const pageStatus = controlId ? document.getElementById(`${{controlId}}-page-status`) : null;
        const emptyRows = Array.from(tbody.rows).filter((row) => row.querySelector(".empty"));
        let dataRows = Array.from(tbody.rows).filter((row) => !emptyRows.includes(row));
        let page = 1;
        let pageSize = Number(pageSizeSelect?.value || 20);
        let activeIndex = 0;
        let activeDirection = "asc";
        const cellValue = (row, index, type) => {{
          const raw = row.cells[index]?.dataset.sort ?? row.cells[index]?.textContent ?? "";
          if (type === "number") return Number(raw) || 0;
          return raw.toLowerCase();
        }};
        const renderPage = () => {{
          const total = dataRows.length;
          const totalPages = Math.max(1, Math.ceil(total / pageSize));
          page = Math.min(Math.max(page, 1), totalPages);
          const start = (page - 1) * pageSize;
          const end = Math.min(start + pageSize, total);
          dataRows.forEach((row, index) => {{
            row.hidden = index < start || index >= end;
          }});
          emptyRows.forEach((row) => {{
            row.hidden = total > 0;
          }});
          if (pageStatus) {{
            pageStatus.textContent = total ? `${{start + 1}}-${{end}} of ${{total}}` : "0 of 0";
          }}
          if (firstButton) firstButton.disabled = page <= 1 || total === 0;
          if (prevButton) prevButton.disabled = page <= 1 || total === 0;
          if (nextButton) nextButton.disabled = page >= totalPages || total === 0;
          if (lastButton) lastButton.disabled = page >= totalPages || total === 0;
        }};
        const updateHeaderState = (index, direction) => {{
          headers.forEach((header, headerIndex) => {{
            header.setAttribute("aria-sort", headerIndex === index ? (direction === "asc" ? "ascending" : "descending") : "none");
          }});
        }};
        headers.forEach((header, index) => {{
          header.querySelector("button")?.addEventListener("click", () => {{
            const direction = activeIndex === index ? (activeDirection === "asc" ? "desc" : "asc") : (header.dataset.defaultDir || "asc");
            const type = header.dataset.sort;
            const multiplier = direction === "asc" ? 1 : -1;
            dataRows.sort((left, right) => {{
              const leftValue = cellValue(left, index, type);
              const rightValue = cellValue(right, index, type);
              if (type === "number") return (leftValue - rightValue) * multiplier;
              return String(leftValue).localeCompare(String(rightValue)) * multiplier;
            }});
            dataRows.forEach((row) => tbody.appendChild(row));
            emptyRows.forEach((row) => tbody.appendChild(row));
            activeIndex = index;
            activeDirection = direction;
            page = 1;
            updateHeaderState(index, direction);
            renderPage();
          }});
        }});
        pageSizeSelect?.addEventListener("change", () => {{
          pageSize = Number(pageSizeSelect.value || 20);
          page = 1;
          renderPage();
        }});
        firstButton?.addEventListener("click", () => {{
          page = 1;
          renderPage();
        }});
        prevButton?.addEventListener("click", () => {{
          page -= 1;
          renderPage();
        }});
        nextButton?.addEventListener("click", () => {{
          page += 1;
          renderPage();
        }});
        lastButton?.addEventListener("click", () => {{
          page = Math.max(1, Math.ceil(dataRows.length / pageSize));
          renderPage();
        }});
        renderPage();
      }});
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
    if store.conn.execute("SELECT COUNT(*) AS count FROM network_masternodes").fetchone()["count"] == 0:
        load_network_masternodes_csv(store)

    prepared_rows = []
    exchange_tags = load_exchange_tags()
    exchange_routes = load_exchange_routes()
    network_rows = store.conn.execute(
        """
        SELECT *
        FROM network_masternodes
        ORDER BY COALESCE(registered_time, collateral_time, 0) DESC, collateral_address
        """
    ).fetchall()
    masternode_meta = store.get_meta("last_masternode_sync", {})
    chain_height = int_or_none(masternode_meta.get("chain_height")) or max(
        (
            int_or_none(row[key]) or 0
            for row in network_rows
            for key in ("collateral_height", "registered_height", "last_paid_block")
        ),
        default=0,
    )

    def iso_timestamp(value: str | None) -> int:
        if not value:
            return 0
        try:
            parsed = dt.datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError:
            return 0
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=dt.timezone.utc)
        return int(parsed.timestamp())

    baseline_seen = [
        (iso_timestamp(row["first_seen_at"]), row["first_seen_at"])
        for row in network_rows
        if iso_timestamp(row["first_seen_at"]) > 0
    ]
    baseline_ts, baseline_iso = min(baseline_seen, default=(0, ""))

    def seniority_for(row: sqlite3.Row) -> dict[str, Any]:
        collateral_height = int_or_none(row["collateral_height"])
        if not collateral_height or not chain_height:
            return {"label": "-", "sort": -1, "class": "unknown", "title": "Collateral height unavailable"}

        blocks_since_collateral = max(chain_height - collateral_height, 0)
        if blocks_since_collateral >= SENIORITY_LEVEL_2_BLOCKS:
            label = "Level 2"
            sort = 2
            css_class = "level-2"
        elif blocks_since_collateral >= SENIORITY_LEVEL_1_BLOCKS:
            label = "Level 1"
            sort = 1
            css_class = "level-1"
        else:
            label = "Base"
            sort = 0
            css_class = "base"

        return {
            "label": label,
            "sort": sort,
            "class": css_class,
            "title": f"{blocks_since_collateral:,} blocks since collateral height {collateral_height:,}",
        }

    for row in network_rows:
        setup_time = row["registered_time"] or row["collateral_time"]
        taken_down_sort = row["taken_down_time"] or iso_timestamp(row["removed_at"])
        taken_down_text = fmt_table_datetime(row["taken_down_time"]) if row["taken_down_time"] else fmt_iso_local_datetime(row["removed_at"])
        first_seen_sort = iso_timestamp(row["first_seen_at"])
        is_new = bool(baseline_ts and first_seen_sort > baseline_ts)
        is_removed = bool(row["removed_at"])
        change_type = "Taken down" if is_removed else "New setup" if is_new else ""
        change_sort = taken_down_sort if is_removed else first_seen_sort
        status = row["status"] or ("Taken down" if row["removed_at"] else "Unknown")
        seniority = seniority_for(row)
        moved_to_address = row["moved_to_address"] or ""
        exchange_labels = exchange_labels_for_address(moved_to_address, exchange_tags, exchange_routes) if moved_to_address else set()
        exchange_text = ", ".join(sorted(exchange_labels)) if exchange_labels else "-"
        prepared_rows.append(
            {
                "row": row,
                "change_type": change_type,
                "change_sort": change_sort,
                "status": status,
                "status_label": sentry_status_label(status),
                "status_sort": status.lower(),
                "setup_time": setup_time,
                "taken_down_text": taken_down_text,
                "taken_down_sort": taken_down_sort,
                "seniority": seniority,
                "moved_to_address": moved_to_address,
                "exchange_text": exchange_text,
            }
        )

    current_items = [item for item in prepared_rows if not item["row"]["removed_at"]]
    enabled_count = sum(1 for item in current_items if item["status"].upper() == "ENABLED")
    banned_count = sum(1 for item in current_items if item["status"].upper() == "POSE_BANNED")
    seniority_counts = {"Base": 0, "Level 1": 0, "Level 2": 0}
    for item in current_items:
        label = item["seniority"]["label"]
        if label in seniority_counts:
            seniority_counts[label] += 1

    def masternode_row_html(item: dict[str, Any], include_change: bool = False) -> str:
        row = item["row"]
        collateral_address = row["collateral_address"]
        source_txid = row["source_txid"]
        source_vout = int(row["source_vout"])
        tx_outpoint = f"{source_txid}:{source_vout}"
        tx_label = f"{short_txid(source_txid)}:{source_vout}" if source_txid else "-"
        tx_html = (
            f"<a href='{explorer_tx_url(source_txid)}' title='{html.escape(tx_outpoint)}'>{html.escape(tx_label)}</a>"
            if source_txid
            else "-"
        )
        service = row["service"] or "-"
        change_cell = (
            f"<td data-sort='{item['change_sort']}'><span class='status {'down' if item['change_type'] == 'Taken down' else 'active'}'>{html.escape(item['change_type'])}</span></td>"
            if include_change
            else ""
        )
        moved_to_address = item["moved_to_address"]
        moved_to_html = (
            f"<a href='{explorer_address_url(moved_to_address)}' title='{html.escape(moved_to_address)}'>{html.escape(short_address(moved_to_address))}</a>"
            if moved_to_address
            else "-"
        )
        taken_down_display = item["taken_down_text"] if item["change_type"] == "Taken down" and item["taken_down_text"] else "-"
        change_detail_cells = (
            f"<td data-sort='{item['taken_down_sort'] or 0}'>{html.escape(taken_down_display)}</td>"
            f"<td class='address' data-sort='{html.escape(moved_to_address.lower())}'>{moved_to_html}</td>"
            f"<td data-sort='{html.escape(item['exchange_text'].lower())}'>{html.escape(item['exchange_text'])}</td>"
            if include_change
            else ""
        )
        setup_cell = (
            f"<td data-sort='{item['setup_time'] or 0}' title='{html.escape(fmt_local_datetime(item['setup_time']))}'>"
            f"{html.escape(fmt_table_datetime(item['setup_time']))}</td>"
        )
        status_cell = (
            f"<td data-sort='{html.escape(item['status_sort'])}'>"
            f"<span class='status {'active' if item['status'].upper() == 'ENABLED' else 'down'}'>{html.escape(item['status_label'])}</span>"
            f"</td>"
        )
        if include_change:
            return f"<tr>{change_cell}{setup_cell}{change_detail_cells}{status_cell}</tr>"

        seniority = item["seniority"]
        search_terms = html.escape(f"{collateral_address} {service}".lower(), quote=True)
        return (
            f"<tr data-search='{search_terms}'>"
            f"{setup_cell}"
            f"<td data-sort='{seniority['sort']}'><span class='seniority {html.escape(seniority['class'])}' title='{html.escape(seniority['title'])}'>{html.escape(seniority['label'])}</span></td>"
            f"<td class='address' data-sort='{html.escape(collateral_address.lower())}'><a href='{explorer_address_url(collateral_address)}' title='{html.escape(collateral_address)}'>{html.escape(short_address(collateral_address))}</a></td>"
            f"<td class='address' data-sort='{html.escape(tx_outpoint.lower())}'>{tx_html}</td>"
            f"<td data-sort='{html.escape(str(service).lower())}'>{html.escape(str(service))}</td>"
            f"{status_cell}"
            f"</tr>"
        )

    rows_html = "\n".join(masternode_row_html(item) for item in current_items)
    change_items = sorted((item for item in prepared_rows if item["change_type"]), key=lambda item: item["change_sort"], reverse=True)
    new_setup_count = sum(1 for item in change_items if item["change_type"] == "New setup")
    taken_down_count = sum(1 for item in change_items if item["change_type"] == "Taken down")
    last_setup_time = max(
        (int(item["setup_time"] or item["change_sort"] or 0) for item in change_items if item["change_type"] == "New setup"),
        default=0,
    )
    last_taken_down_time = max(
        (int(item["taken_down_sort"] or item["change_sort"] or 0) for item in change_items if item["change_type"] == "Taken down"),
        default=0,
    )
    last_setup_date = fmt_local_date(last_setup_time)
    last_taken_down_date = fmt_local_date(last_taken_down_time)
    change_rows_html = "\n".join(masternode_row_html(item, include_change=True) for item in change_items)
    if not change_rows_html:
        change_rows_html = "<tr class='mn-empty'><td class='empty' colspan='6'>No new setups or takedowns since the banked snapshot.</td></tr>"
    current_no_results_html = "<tr class='mn-no-results' hidden><td class='empty' colspan='6'>No matching sentry nodes.</td></tr>"
    since_text = f"{fmt_local_datetime(since_time)} Sydney" if since_time else "all tracked history"
    updated_text = fmt_iso_local_datetime(masternode_meta.get("synced_at") or store.get_meta("last_summary", {}).get("synced_at"))
    baseline_text = fmt_iso_local_datetime(baseline_iso) if baseline_iso else "not set"
    total_count = len(current_items)

    def chart_legend(parts: list[tuple[str, int, str]], total: int) -> str:
        rows = []
        for label, count, color in parts:
            percent = fmt_percent(count, total) if total else "0%"
            rows.append(
                "<li>"
                f"<span class='legend-swatch' style='background:{html.escape(color)}'></span>"
                f"<span>{html.escape(label)}</span>"
                f"<strong>{count:,}</strong>"
                f"<em>{html.escape(percent)}</em>"
                "</li>"
            )
        return "".join(rows)

    status_chart_parts = [
        ("Enabled", enabled_count, "#2f7fb8"),
        ("Banned", banned_count, "#c66a2e"),
    ]
    seniority_chart_parts = [
        ("Base", seniority_counts["Base"], "#6f7d84"),
        ("Level 1", seniority_counts["Level 1"], "#2b78b8"),
        ("Level 2", seniority_counts["Level 2"], "#2b8a57"),
    ]
    status_legend = chart_legend(status_chart_parts, total_count)
    seniority_legend = chart_legend(seniority_chart_parts, sum(seniority_counts.values()))
    chart_data_json = json.dumps(
        {
            "status": {
                "labels": [label for label, _, _ in status_chart_parts],
                "values": [count for _, count, _ in status_chart_parts],
                "colors": [color for _, _, color in status_chart_parts],
            },
            "seniority": {
                "labels": [label for label, _, _ in seniority_chart_parts],
                "values": [count for _, count, _ in seniority_chart_parts],
                "colors": [color for _, _, color in seniority_chart_parts],
            },
        }
    )
    chart_data_json = chart_data_json.replace("</", "<\\/")

    return f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <meta http-equiv="refresh" content="{max(refresh_seconds, 1)}">
  <title>Syscoin Sentry Node Tracker</title>
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
    .metrics {{ display: grid; grid-template-columns: repeat(5, minmax(120px, 1fr)); gap: 12px; }}
    .metric {{ background: #fff; border: 1px solid #d9ded8; border-radius: 8px; padding: 14px 16px; min-width: 0; }}
    .metric span {{ display: block; color: #687177; font-size: 0.84rem; margin-bottom: 6px; }}
    .metric b {{ display: block; font-size: clamp(1.25rem, 2vw, 1.65rem); line-height: 1.1; overflow-wrap: anywhere; }}
    .metric small {{ color: #687177; display: block; font-size: 0.78rem; font-weight: 700; margin-top: 8px; }}
    .metric small strong {{ color: #1c2227; font-weight: 800; }}
    .chart-grid {{ display: grid; grid-template-columns: repeat(2, minmax(0, 1fr)); gap: 12px; }}
    .chart-card {{ background: #fff; border: 1px solid #d9ded8; border-radius: 8px; display: grid; gap: 12px; min-width: 0; padding: 18px; }}
    .chart-card h2 {{ font-size: 1rem; margin: 0; }}
    .chart-body {{ align-items: center; display: grid; gap: 20px; grid-template-columns: minmax(220px, 280px) minmax(0, 1fr); min-width: 0; }}
    .chart-canvas-wrap {{ align-items: center; display: flex; height: 280px; justify-content: center; min-width: 0; overflow: visible; }}
    .chart-canvas-wrap canvas {{ height: 280px !important; width: 280px !important; }}
    .chart-legend {{ display: grid; flex: 1 1 auto; gap: 8px; list-style: none; margin: 0; min-width: 0; padding: 0; }}
    .chart-legend li {{ align-items: center; display: grid; gap: 8px; grid-template-columns: 12px minmax(80px, 1fr) auto auto; }}
    .legend-swatch {{ border-radius: 999px; display: inline-block; height: 12px; width: 12px; }}
    .chart-legend span, .chart-legend em {{ color: #687177; font-size: 0.84rem; font-style: normal; }}
    .chart-legend strong {{ font-size: 0.92rem; }}
    .panel-title {{ display: flex; align-items: end; justify-content: space-between; gap: 16px; }}
    .panel-title h2 {{ margin: 0; font-size: 1.25rem; }}
    .panel-title p {{ margin: 0; color: #687177; font-size: 0.9rem; }}
    .table-controls {{ align-items: end; display: flex; flex-wrap: wrap; gap: 10px; justify-content: space-between; margin: 10px 0; }}
    .table-controls label {{ color: #687177; display: grid; font-size: 0.78rem; font-weight: 700; gap: 4px; }}
    .table-controls input, .table-controls select {{ background: #fff; border: 1px solid #cfd7d1; border-radius: 6px; color: #1c2227; font: inherit; min-height: 36px; padding: 7px 9px; }}
    .search-control {{ flex: 1 1 300px; max-width: 520px; }}
    .page-size-control {{ width: 110px; }}
    .pagination-controls {{ align-items: end; display: flex; flex-wrap: wrap; gap: 10px; justify-content: flex-end; margin-left: auto; }}
    .pager {{ align-items: center; color: #687177; display: flex; gap: 8px; }}
    .pager button {{ background: #fff; border: 1px solid #cfd7d1; border-radius: 6px; color: #1c2227; cursor: pointer; font: inherit; min-height: 36px; padding: 7px 10px; }}
    .pager button:disabled {{ cursor: default; opacity: 0.45; }}
    .page-status {{ color: #687177; font-size: 0.86rem; min-width: 96px; text-align: center; }}
    .table-wrap {{ background: #fff; border: 1px solid #d9ded8; border-radius: 8px; max-width: 100%; min-width: 0; overflow-x: auto; width: 100%; }}
    table {{ width: 100%; min-width: 920px; border-collapse: separate; border-spacing: 0; background: #fff; table-layout: fixed; }}
    .mn-current {{ min-width: 935px; }}
    .mn-changes {{ min-width: 850px; }}
    th, td {{ padding: 8px 10px; border-bottom: 1px solid #e4e8e2; text-align: left; font-size: 0.88rem; overflow: hidden; text-overflow: ellipsis; }}
    th {{ background: #eaf0ec; position: sticky; top: 0; z-index: 10; box-shadow: 0 1px 0 #d9ded8; }}
    .sort-button {{ appearance: none; border: 0; background: transparent; color: inherit; cursor: pointer; display: inline-flex; align-items: center; gap: 4px; font: inherit; font-weight: 700; padding: 0; text-align: inherit; white-space: nowrap; }}
    .sort-button:hover, .sort-button:focus-visible {{ color: #086788; outline: none; }}
    .sort-icon {{ color: #687177; display: inline-block; font-size: 0.75rem; min-width: 1ch; }}
    th[aria-sort="ascending"] .sort-icon::before {{ content: "^"; }}
    th[aria-sort="descending"] .sort-icon::before {{ content: "v"; }}
    th[aria-sort="none"] .sort-icon::before {{ content: ""; }}
    .mn-current th:nth-child(1), .mn-current td:nth-child(1) {{ width: 125px; }}
    .mn-current th:nth-child(2), .mn-current td:nth-child(2) {{ width: 100px; }}
    .mn-current th:nth-child(3), .mn-current td:nth-child(3) {{ width: 220px; }}
    .mn-current th:nth-child(4), .mn-current td:nth-child(4) {{ width: 220px; }}
    .mn-current th:nth-child(5), .mn-current td:nth-child(5) {{ width: 155px; }}
    .mn-current th:nth-child(6), .mn-current td:nth-child(6) {{ width: 115px; }}
    .mn-changes th:nth-child(1), .mn-changes td:nth-child(1) {{ width: 125px; }}
    .mn-changes th:nth-child(2), .mn-changes td:nth-child(2) {{ width: 125px; }}
    .mn-changes th:nth-child(3), .mn-changes td:nth-child(3) {{ width: 135px; }}
    .mn-changes th:nth-child(4), .mn-changes td:nth-child(4) {{ width: 220px; }}
    .mn-changes th:nth-child(5), .mn-changes td:nth-child(5) {{ width: 130px; }}
    .mn-changes th:nth-child(6), .mn-changes td:nth-child(6) {{ width: 115px; }}
    .address {{ font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, "Liberation Mono", monospace; white-space: nowrap; }}
    .empty {{ color: #687177; padding: 18px 14px; text-align: center; }}
    .status {{ border-radius: 999px; display: inline-flex; font-size: 0.78rem; font-weight: 700; padding: 4px 8px; white-space: nowrap; }}
    .status.active {{ background: #e7f2ff; color: #095c9f; }}
    .status.down {{ background: #fff0df; color: #8a4c00; }}
    .seniority {{ border-radius: 999px; display: inline-flex; font-size: 0.78rem; font-weight: 700; padding: 4px 8px; white-space: nowrap; }}
    .seniority.base {{ background: #edf1ed; color: #405045; }}
    .seniority.level-1 {{ background: #e7f2ff; color: #095c9f; }}
    .seniority.level-2 {{ background: #e8f6ed; color: #1f6d3a; }}
    .seniority.unknown {{ background: #f2f3f3; color: #687177; }}
    a {{ color: #086788; text-decoration: none; }}
    @media(max-width: 920px) {{
      .metrics {{ grid-template-columns: repeat(2, minmax(0, 1fr)); }}
      .chart-grid {{ grid-template-columns: 1fr; }}
      .chart-body {{ grid-template-columns: 1fr; }}
      .chart-canvas-wrap {{ height: 250px; }}
      .chart-canvas-wrap canvas {{ height: 250px !important; width: 250px !important; }}
      .panel-title {{ align-items: start; flex-direction: column; }}
      .table-controls {{ align-items: stretch; flex-direction: column; }}
      .search-control, .page-size-control {{ max-width: none; width: 100%; }}
      .pagination-controls {{ align-items: stretch; flex-direction: column; margin-left: 0; width: 100%; }}
      .pager {{ justify-content: space-between; }}
      .topbar {{ align-items: start; flex-direction: column; }}
      .nav {{ justify-content: flex-start; }}
      .header-inner, main {{ margin-left: 12px; margin-right: 12px; }}
    }}
    @media (prefers-color-scheme: dark) {{
      body {{ background: #121619; color: #f3f4f6; }}
      .metric, .table-wrap, table {{ background: #1c2328; border-color: #334047; }}
      .chart-card {{ background: #1c2328; border-color: #334047; }}
      .table-controls input, .table-controls select, .pager button {{ background: #1c2328; border-color: #46555e; color: #f3f4f6; }}
      th {{ background: #263139; }}
      th, td {{ border-color: #334047; }}
      a {{ color: #67d7ff; }}
      .subtitle {{ color: #b6c3c7; }}
      .metric span, .metric small, .panel-title p, .sort-icon, .empty, .table-controls label, .pager, .page-status, .chart-legend span, .chart-legend em {{ color: #a7b0b5; }}
      .metric small strong {{ color: #f3f4f6; }}
      .sort-button:hover, .sort-button:focus-visible {{ color: #67d7ff; }}
      .status.active {{ background: #15304a; color: #b9dcff; }}
      .status.down {{ background: #442d13; color: #ffd9a8; }}
      .seniority.base {{ background: #273234; color: #d7e1dd; }}
      .seniority.level-1 {{ background: #15304a; color: #b9dcff; }}
      .seniority.level-2 {{ background: #173823; color: #b7efc7; }}
      .seniority.unknown {{ background: #2d3337; color: #a7b0b5; }}
    }}
  </style>
</head>
<body>
  <header>
    <div class="header-inner">
      <div class="topbar">
        <div>
          <h1>Syscoin Sentry Node Tracker</h1>
          <div class="subtitle">Experimental view. Useful for spotting patterns, not proof of ownership or intent.</div>
        </div>
        <nav class="nav" aria-label="Dashboard pages">
          <a href="/">Wallet Flows</a>
          <a class="active" href="/sentrynode">Sentry Nodes</a>
          <a href="/top-wallets">Top Wallets</a>
        </nav>
      </div>
    </div>
  </header>
  <main>
    <section class="metrics">
      <div class="metric"><span>Sentry Nodes</span><b>{total_count}</b></div>
      <div class="metric"><span>Enabled</span><b>{enabled_count}</b></div>
      <div class="metric"><span>Banned</span><b>{banned_count}</b></div>
      <div class="metric"><span>New Setups</span><b>{new_setup_count}</b><small>Last Setup: <strong>{html.escape(last_setup_date)}</strong></small></div>
      <div class="metric"><span>Taken Down</span><b>{taken_down_count}</b><small>Last Taken Down: <strong>{html.escape(last_taken_down_date)}</strong></small></div>
    </section>
    <section class="chart-grid" aria-label="Sentry node charts">
      <article class="chart-card">
        <h2>Enabled vs Banned</h2>
        <div class="chart-body">
          <div class="chart-canvas-wrap"><canvas id="status-chart" aria-label="Enabled {enabled_count}, Banned {banned_count}" role="img"></canvas></div>
          <ul class="chart-legend">{status_legend}</ul>
        </div>
      </article>
      <article class="chart-card">
        <h2>Seniority Amounts</h2>
        <div class="chart-body">
          <div class="chart-canvas-wrap"><canvas id="seniority-chart" aria-label="Base {seniority_counts['Base']}, Level 1 {seniority_counts['Level 1']}, Level 2 {seniority_counts['Level 2']}" role="img"></canvas></div>
          <ul class="chart-legend">{seniority_legend}</ul>
        </div>
      </article>
    </section>
    <section>
      <div class="panel-title">
        <h2>Changes Since Snapshot</h2>
        <p>Snapshot banked {html.escape(baseline_text)}</p>
      </div>
      <div class="table-controls" aria-label="Changes since snapshot table controls">
        <div class="pagination-controls">
          <label class="page-size-control" for="mn-changes-page-size">
            <span>Rows</span>
            <select id="mn-changes-page-size">
              <option value="20" selected>20</option>
              <option value="50">50</option>
              <option value="100">100</option>
            </select>
          </label>
          <div class="pager" aria-label="Changes since snapshot pagination">
            <button id="mn-changes-first" type="button">First</button>
            <button id="mn-changes-prev" type="button">Prev</button>
            <span class="page-status" id="mn-changes-page-status">0 of 0</span>
            <button id="mn-changes-next" type="button">Next</button>
            <button id="mn-changes-last" type="button">Last</button>
          </div>
        </div>
      </div>
      <div class="table-wrap">
        <table class="mn-table mn-changes">
          <thead>
            <tr>
              <th data-sort="number" data-default-dir="desc" aria-sort="descending"><button class="sort-button" type="button">Change<span class="sort-icon" aria-hidden="true"></span></button></th>
              <th data-sort="number" data-default-dir="desc" aria-sort="none"><button class="sort-button" type="button">Date Setup<span class="sort-icon" aria-hidden="true"></span></button></th>
              <th data-sort="number" data-default-dir="desc" aria-sort="none"><button class="sort-button" type="button">Date Taken Down<span class="sort-icon" aria-hidden="true"></span></button></th>
              <th data-sort="text" data-default-dir="asc" aria-sort="none"><button class="sort-button" type="button">100k Moved To<span class="sort-icon" aria-hidden="true"></span></button></th>
              <th data-sort="text" data-default-dir="asc" aria-sort="none"><button class="sort-button" type="button">Exchange<span class="sort-icon" aria-hidden="true"></span></button></th>
              <th data-sort="text" data-default-dir="asc" aria-sort="none"><button class="sort-button" type="button">Status<span class="sort-icon" aria-hidden="true"></span></button></th>
            </tr>
          </thead>
          <tbody>{change_rows_html}</tbody>
        </table>
      </div>
    </section>
    <section>
      <div class="panel-title">
        <h2>Current Sentry Node List</h2>
        <p>Updated {html.escape(updated_text)}</p>
      </div>
      <div class="table-controls" aria-label="Current sentry node table controls">
        <label class="search-control" for="mn-current-search">
          <span>Search</span>
          <input id="mn-current-search" type="search" placeholder="IP or collateral address" autocomplete="off">
        </label>
        <div class="pagination-controls">
          <label class="page-size-control" for="mn-current-page-size">
            <span>Rows</span>
            <select id="mn-current-page-size">
              <option value="20" selected>20</option>
              <option value="50">50</option>
              <option value="100">100</option>
            </select>
          </label>
          <div class="pager" aria-label="Current sentry node pagination">
            <button id="mn-current-first" type="button">First</button>
            <button id="mn-current-prev" type="button">Prev</button>
            <span class="page-status" id="mn-current-page-status">0 of 0</span>
            <button id="mn-current-next" type="button">Next</button>
            <button id="mn-current-last" type="button">Last</button>
          </div>
        </div>
      </div>
      <div class="table-wrap">
        <table class="mn-table mn-current">
          <thead>
            <tr>
              <th data-sort="number" data-default-dir="desc" aria-sort="descending"><button class="sort-button" type="button">Date Setup<span class="sort-icon" aria-hidden="true"></span></button></th>
              <th data-sort="number" data-default-dir="desc" aria-sort="none"><button class="sort-button" type="button">Seniority<span class="sort-icon" aria-hidden="true"></span></button></th>
              <th data-sort="text" data-default-dir="asc" aria-sort="none"><button class="sort-button" type="button">Collateral Address<span class="sort-icon" aria-hidden="true"></span></button></th>
              <th data-sort="text" data-default-dir="asc" aria-sort="none"><button class="sort-button" type="button">Collateral Tx<span class="sort-icon" aria-hidden="true"></span></button></th>
              <th data-sort="text" data-default-dir="asc" aria-sort="none"><button class="sort-button" type="button">IP Address<span class="sort-icon" aria-hidden="true"></span></button></th>
              <th data-sort="text" data-default-dir="asc" aria-sort="none"><button class="sort-button" type="button">Status<span class="sort-icon" aria-hidden="true"></span></button></th>
            </tr>
          </thead>
          <tbody>{rows_html}{current_no_results_html}</tbody>
        </table>
      </div>
    </section>
  </main>
  <script src="{CHART_ASSET_ROUTE.lstrip('/')}"></script>
  <script type="application/json" id="sentry-chart-data">{chart_data_json}</script>
  <script>
    (() => {{
      const makeSentryChart = (id, data) => {{
        const canvas = document.getElementById(id);
        if (!canvas || !window.Chart || !data) return;
        const total = data.values.reduce((sum, value) => sum + Number(value || 0), 0);
        new Chart(canvas, {{
          type: "pie",
          data: {{
            labels: data.labels,
            datasets: [{{
              data: data.values,
              backgroundColor: data.colors,
              borderColor: getComputedStyle(document.body).backgroundColor,
              borderWidth: 2,
              hoverOffset: 18,
            }}],
          }},
          options: {{
            animation: {{ duration: 450 }},
            layout: {{ padding: 24 }},
            maintainAspectRatio: false,
            plugins: {{
              legend: {{ display: false }},
              tooltip: {{
                backgroundColor: "rgba(20, 32, 38, 0.94)",
                boxPadding: 5,
                callbacks: {{
                  label: (context) => {{
                    const value = Number(context.parsed || 0);
                    const percent = total ? ((value * 100) / total).toFixed(2) : "0.00";
                    return ` ${{context.label}}: ${{value.toLocaleString()}} (${{percent}}%)`;
                  }},
                }},
                padding: 10,
              }},
            }},
          }},
        }});
      }};

      try {{
        const chartData = JSON.parse(document.getElementById("sentry-chart-data")?.textContent || "{{}}");
        makeSentryChart("status-chart", chartData.status);
        makeSentryChart("seniority-chart", chartData.seniority);
      }} catch (_error) {{}}

      const setupPaginator = (config) => {{
        const table = document.querySelector(config.tableSelector);
        const searchInput = config.searchSelector ? document.querySelector(config.searchSelector) : null;
        const pageSizeSelect = document.querySelector(config.pageSizeSelector);
        const pageStatus = document.querySelector(config.statusSelector);
        const firstButton = document.querySelector(config.firstSelector);
        const prevButton = document.querySelector(config.prevSelector);
        const nextButton = document.querySelector(config.nextSelector);
        const lastButton = document.querySelector(config.lastSelector);
        if (!table || !pageSizeSelect || !pageStatus || !firstButton || !prevButton || !nextButton || !lastButton) return null;
        let page = 1;

        const dataRows = () =>
          Array.from(table.tBodies[0].rows).filter(
            (row) => !row.classList.contains("mn-no-results") && !row.classList.contains("mn-empty"),
          );

        const apply = () => {{
          const query = (searchInput?.value || "").trim().toLowerCase();
          const pageSize = Number(pageSizeSelect.value) || 20;
          const rows = dataRows();
          const matchedRows = rows.filter((row) => !query || (row.dataset.search || "").includes(query));
          const total = matchedRows.length;
          const totalPages = Math.max(1, Math.ceil(total / pageSize));
          page = Math.min(Math.max(page, 1), totalPages);
          const start = total ? (page - 1) * pageSize : 0;
          const end = Math.min(start + pageSize, total);
          rows.forEach((row) => {{ row.hidden = true; }});
          matchedRows.slice(start, end).forEach((row) => {{ row.hidden = false; }});
          table.querySelectorAll(".mn-empty").forEach((row) => {{ row.hidden = total !== 0; }});
          table.querySelectorAll(".mn-no-results").forEach((row) => {{ row.hidden = total !== 0; }});
          pageStatus.textContent = total ? `${{start + 1}}-${{end}} of ${{total}}` : "0 of 0";
          firstButton.disabled = page <= 1 || total === 0;
          prevButton.disabled = page <= 1;
          nextButton.disabled = page >= totalPages;
          lastButton.disabled = page >= totalPages || total === 0;
        }};

        searchInput?.addEventListener("input", () => {{
          page = 1;
          apply();
        }});
        pageSizeSelect.addEventListener("change", () => {{
          page = 1;
          apply();
        }});
        firstButton.addEventListener("click", () => {{
          page = 1;
          apply();
        }});
        prevButton.addEventListener("click", () => {{
          page -= 1;
          apply();
        }});
        nextButton.addEventListener("click", () => {{
          page += 1;
          apply();
        }});
        lastButton.addEventListener("click", () => {{
          const query = (searchInput?.value || "").trim().toLowerCase();
          const pageSize = Number(pageSizeSelect.value) || 20;
          const total = dataRows().filter((row) => !query || (row.dataset.search || "").includes(query)).length;
          page = Math.max(1, Math.ceil(total / pageSize));
          apply();
        }});

        return {{ table, apply }};
      }};

      const paginators = [
        setupPaginator({{
          tableSelector: ".mn-changes",
          pageSizeSelector: "#mn-changes-page-size",
          statusSelector: "#mn-changes-page-status",
          firstSelector: "#mn-changes-first",
          prevSelector: "#mn-changes-prev",
          nextSelector: "#mn-changes-next",
          lastSelector: "#mn-changes-last",
        }}),
        setupPaginator({{
          tableSelector: ".mn-current",
          searchSelector: "#mn-current-search",
          pageSizeSelector: "#mn-current-page-size",
          statusSelector: "#mn-current-page-status",
          firstSelector: "#mn-current-first",
          prevSelector: "#mn-current-prev",
          nextSelector: "#mn-current-next",
          lastSelector: "#mn-current-last",
        }}),
      ].filter(Boolean);

      const applyPaginationForTable = (table) => {{
        paginators.find((paginator) => paginator.table === table)?.apply();
      }};

      document.querySelectorAll("table").forEach((table) => {{
        const headers = Array.from(table.querySelectorAll("th[data-sort]"));
        const tbody = table.tBodies[0];
        let activeIndex = 0;
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
          const trailingRows = Array.from(tbody.rows).filter(
            (row) => row.classList.contains("mn-no-results") || row.classList.contains("mn-empty"),
          );
          const rows = Array.from(tbody.rows).filter((row) => !trailingRows.includes(row));
          rows.sort((left, right) => {{
            const leftValue = cellValue(left, index, type);
            const rightValue = cellValue(right, index, type);
            if (type === "number") return (leftValue - rightValue) * multiplier;
            return String(leftValue).localeCompare(String(rightValue)) * multiplier;
          }});
          rows.forEach((row) => tbody.appendChild(row));
          trailingRows.forEach((row) => tbody.appendChild(row));
          activeIndex = index;
          activeDirection = direction;
          updateHeaderState(index, direction);
          applyPaginationForTable(table);
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
      }});
      paginators.forEach((paginator) => paginator.apply());
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
            with DB_WRITE_LOCK:
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
                exchange_balance_stats = refresh_exchange_hot_wallet_balances(sync_store, sync_client)
            print(
                f"{now_iso()} dashboard sync seen={stats['seen']} new={stats['inserted']} "
                f"next_hop_found={follow_stats['found_spends']} next_hop_outputs={follow_stats['created']} "
                f"next_hop_errors={follow_stats['errors']} "
                f"node_spends={node_stats['found_spends']} node_outputs={node_stats['created']} "
                f"node_errors={node_stats['errors']} "
                f"exchange_wallets={len(exchange_balance_stats)}",
                file=sys.stderr,
            )
        except Exception as exc:
            sync_store.conn.rollback()
            print(f"{now_iso()} dashboard sync failed: {exc}", file=sys.stderr)
        elapsed = time.monotonic() - started
        time.sleep(max(1, interval - int(elapsed)))


def masternode_sync_loop(
    db_path: Path,
    rpc: SyscoinRpcClient,
    blockbook_url: str,
    insecure_tls: bool,
    interval: int,
) -> None:
    sync_store = Store(db_path)
    sync_client = BlockbookClient(blockbook_url, insecure_tls=insecure_tls)
    while True:
        started = time.monotonic()
        try:
            with DB_WRITE_LOCK:
                stats = sync_network_masternodes(sync_store, rpc, sync_client)
            print(
                f"{now_iso()} sentry node sync current={stats['current']} enabled={stats['enabled']} "
                f"added={stats['added']} removed={stats['removed']} traced={stats['traced']}",
                file=sys.stderr,
            )
        except Exception as exc:
            sync_store.conn.rollback()
            print(f"{now_iso()} sentry node sync failed: {exc}", file=sys.stderr)
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
        def redirect_to_sentry_node(self) -> None:
            parsed = urllib.parse.urlparse(self.path)
            query = f"?{parsed.query}" if parsed.query else ""
            self.send_response(308)
            self.send_header("Location", f"/sentrynode{query}")
            self.send_header("Content-Length", "0")
            self.end_headers()

        def send_page(self, include_body: bool = True) -> None:
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
            if parsed.path in LEGACY_MASTERNODE_PATHS:
                self.redirect_to_sentry_node()
                return
            with DB_WRITE_LOCK:
                content_type = "text/html; charset=utf-8"
                if parsed.path == TOP_WALLETS_JSON_PATH:
                    html_body = json.dumps(top_wallets_snapshot(store), indent=2)
                    content_type = "application/json; charset=utf-8"
                elif parsed.path in ("/", "/index.html"):
                    html_body = dashboard_html(
                        store,
                        since_time=since_time,
                        since_label=since_label,
                        refresh_seconds=refresh_seconds,
                    )
                elif parsed.path in SENTRY_NODE_PATHS:
                    html_body = masternodes_html(
                        store,
                        since_time=since_time,
                        since_label=since_label,
                        refresh_seconds=refresh_seconds,
                    )
                elif parsed.path in TOP_WALLETS_PATHS:
                    html_body = top_wallets_html(store, refresh_seconds=refresh_seconds)
                else:
                    self.send_error(404)
                    return

            body = html_body.encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", content_type)
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

    verify_p = sub.add_parser("verify-sentries", help="Verify exact 100k SYS candidates against Syscoin Core masternode_list RPC")
    verify_p.add_argument("--since-date", help="Only verify candidates from this date/time; e.g. '2026-04-14 12:30'")

    mn_p = sub.add_parser("sync-masternodes", help="Fetch the full network sentry node snapshot from RPC")
    mn_p.add_argument("--csv", type=Path, default=DEFAULT_NETWORK_MASTERNODES_PATH, help="Write sentry node snapshot CSV")

    top_wallet_p = sub.add_parser("sync-top-wallets", help="Index exact address balances from Syscoin Core blocks")
    top_wallet_p.add_argument("--start-height", type=int, default=0, help="Height to start from when the index is empty or reset")
    top_wallet_p.add_argument("--to-height", type=int, help="Stop at this block height")
    top_wallet_p.add_argument("--max-blocks", type=int, default=1000, help="Maximum blocks to index in this run")
    top_wallet_p.add_argument("--reset", action="store_true", help="Clear the top-wallet index before syncing")
    top_wallet_p.add_argument("--top", type=int, default=100, help="Number of addresses to include in the JSON snapshot")
    top_wallet_p.add_argument("--json", type=Path, default=Path(TOP_WALLETS_JSON), help="Write top-wallet snapshot JSON")
    top_wallet_p.add_argument("--batch-size", type=int, default=50, help="RPC blocks to fetch per batch")

    cluster_p = sub.add_parser("sync-top-wallet-clusters", help="Build estimated holder clusters from common-input and sentry-collateral history")
    cluster_p.add_argument("--start-height", type=int, default=0, help="Height to start from when the cluster index is empty or reset")
    cluster_p.add_argument("--to-height", type=int, help="Stop at this block height")
    cluster_p.add_argument("--max-blocks", type=int, default=1000, help="Maximum blocks to index in this run")
    cluster_p.add_argument("--reset", action="store_true", help="Clear the top-wallet cluster index before syncing")
    cluster_p.add_argument("--top", type=int, default=100, help="Number of estimated clusters to include in the JSON snapshot")
    cluster_p.add_argument("--json", type=Path, help="Write estimated cluster snapshot JSON")
    cluster_p.add_argument("--batch-size", type=int, default=50, help="RPC blocks to fetch per batch")

    static_p = sub.add_parser("publish-static", help="Sync data and write pre-rendered dashboard pages")
    static_p.add_argument("--output-dir", type=Path, required=True, help="Directory to publish static HTML/data files")
    static_p.add_argument("--since-date", default="2026-04-14 12:30", help="Only show dashboard movements from this date/time")
    static_p.add_argument("--from-height", type=int, help="Only fetch address transactions from this height")
    static_p.add_argument("--refresh-seconds", type=int, default=60, help="HTML meta refresh interval")
    static_p.add_argument("--sync-max-pages", type=int, help="Limit address pages during dashboard sync")
    static_p.add_argument("--next-hop-limit", type=int, default=8, help="Number of first-hop spends to trace per publish")
    static_p.add_argument("--node-spend-limit", type=int, default=12, help="Number of possible node spends to trace per publish")
    static_p.add_argument("--csv", type=Path, default=DEFAULT_NETWORK_MASTERNODES_PATH, help="Sentry node snapshot CSV path")

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
    serve_p.add_argument("--masternode-sync-interval", type=int, default=60, help="Seconds between sentry node RPC checks; use 0 to disable")
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

    if args.command == "sync-masternodes":
        rpc = build_rpc_client(args)
        if rpc is None:
            print("No RPC URL/host supplied. Use --rpc-host/--rpc-url or SYS_RPC_URL.")
            return 1
        if store.conn.execute("SELECT COUNT(*) AS count FROM network_masternodes").fetchone()["count"] == 0 and args.csv:
            load_network_masternodes_csv(store, args.csv)
        stats = sync_network_masternodes(store, rpc, client)
        if args.csv:
            write_network_masternodes_csv(network_masternode_rows_from_store(store), args.csv)
        print(
            f"Synced {stats['current']} sentry nodes ({stats['enabled']} enabled); "
            f"added {stats['added']}, removed {stats['removed']}, traced {stats['traced']}"
        )
        return 0

    if args.command == "sync-top-wallets":
        rpc = build_rpc_client(args)
        if rpc is None:
            print("No RPC URL/host supplied. Use --rpc-host/--rpc-url or SYS_RPC_URL.")
            return 1
        stats = sync_top_wallet_index(
            store,
            rpc,
            start_height=args.start_height,
            max_blocks=args.max_blocks,
            to_height=args.to_height,
            reset=args.reset,
            batch_size=args.batch_size,
        )
        snapshot = top_wallets_snapshot(store, limit=args.top)
        if args.json:
            atomic_write_json(args.json, snapshot)
        print(
            f"Indexed top wallets blocks={stats['blocks']} txs={stats['txs']} "
            f"outputs={stats['outputs']} spends={stats['spends']} "
            f"missing_spends={stats['missing_spends']} "
            f"height={stats['last_height']}/{stats['chain_height']} "
            f"top_addresses={len(snapshot['wallets'])}"
        )
        return 0

    if args.command == "sync-top-wallet-clusters":
        rpc = build_rpc_client(args)
        if rpc is None:
            print("No RPC URL/host supplied. Use --rpc-host/--rpc-url or SYS_RPC_URL.")
            return 1
        stats = sync_top_wallet_cluster_index(
            store,
            rpc,
            start_height=args.start_height,
            max_blocks=args.max_blocks,
            to_height=args.to_height,
            reset=args.reset,
            batch_size=args.batch_size,
        )
        snapshot = top_wallet_cluster_snapshot(store, limit=args.top)
        if args.json:
            atomic_write_json(args.json, snapshot)
        print(
            f"Indexed top wallet clusters blocks={stats['blocks']} txs={stats['txs']} "
            f"outputs={stats['outputs']} spends={stats['spends']} "
            f"missing_spends={stats['missing_spends']} groups={stats['groups']} "
            f"sentry_groups={stats['sentry_groups']} edges={stats['edges']} "
            f"sentry_edges={stats['sentry_edges']} "
            f"sentry_outpoints={stats['sentry_collateral_outpoints']} "
            f"height={stats['last_height']}/{stats['chain_height']} "
            f"clusters={snapshot['totals']['clusters']}"
        )
        return 0

    if args.command == "publish-static":
        since_time, since_label = parse_since_date(args.since_date, args.timezone)
        display_label = f"{since_label} (from {args.since_date} {args.timezone})" if since_label else None
        from_height = args.from_height
        if from_height is None and args.since_date == "2026-04-14 12:30":
            from_height = DEFAULT_MONITORING_FROM_HEIGHT
        elif from_height is None and since_time:
            from_height = block_height_at_or_after(client, since_time)
        rpc = build_rpc_client(args)
        stats = publish_static_snapshot(
            store,
            client,
            rpc,
            output_dir=args.output_dir,
            address=args.address,
            page_size=args.page_size,
            max_pages=args.sync_max_pages,
            from_height=from_height,
            since_time=since_time,
            since_label=display_label,
            refresh_seconds=args.refresh_seconds,
            csv_path=args.csv,
            next_hop_limit=args.next_hop_limit,
            node_spend_limit=args.node_spend_limit,
        )
        masternode_stats = stats.get("masternodes") or {}
        print(
            f"Published static snapshot to {args.output_dir}; "
            f"wallet_seen={stats['wallet']['seen']} wallet_new={stats['wallet']['inserted']} "
            f"next_hop_found={stats['next_hop']['found_spends']} "
            f"node_spends={stats['node_spends']['found_spends']} "
            f"sentry_nodes={masternode_stats.get('current', '-')}"
        )
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
        rpc = build_rpc_client(args)
        if args.masternode_sync_interval > 0 and rpc is not None:
            masternode_thread = threading.Thread(
                target=masternode_sync_loop,
                args=(
                    Path(args.db),
                    rpc,
                    args.blockbook_url,
                    args.insecure_tls,
                    args.masternode_sync_interval,
                ),
                daemon=True,
            )
            masternode_thread.start()
        elif args.masternode_sync_interval > 0:
            print("Sentry node live sync disabled: no RPC URL/host supplied.", file=sys.stderr)
        refresh_candidates = [value for value in (args.sync_interval, args.masternode_sync_interval) if value > 0]
        serve(
            store,
            args.host,
            args.port,
            since_time=since_time,
            since_label=display_label,
            refresh_seconds=min(refresh_candidates) if refresh_candidates else 60,
        )
        return 0

    return 1


if __name__ == "__main__":
    raise SystemExit(main())
