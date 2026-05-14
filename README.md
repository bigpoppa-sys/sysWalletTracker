# Syscoin Hot Wallet Tracker

Track where SYS leaves the Binance hot wallet:

`sys1qync7erear7cvpkysvv0a28mj45g2ps0kq9c6qs`

The tracker uses the official Syscoin Native UTXO Blockbook explorer by default:

`https://explorer-blockbook.syscoin.org/`

It stores fetched transactions in SQLite, classifies hot-wallet outbound movements,
aggregates first-hop destinations, and can optionally follow spent outputs to later
hops.

## Quick Start

```sh
python3 syscoin_tracker.py sync
python3 syscoin_tracker.py report --top 25 --csv destinations.csv
python3 syscoin_tracker.py serve
```

Then open:

`http://127.0.0.1:8787`

## Vercel

This repo includes a Vercel Python function at `api/index.py`, so the same
dashboard can be deployed without running a local process forever.

```sh
npx vercel
npx vercel --prod
```

On Vercel, the dashboard syncs on page request and uses `/tmp` SQLite caching
while the function instance is warm. The page still refreshes every 60 seconds.
Use the local `serve` command or an external database/cron worker if you need a
fully persistent background indexer.

Useful Vercel environment variables:

```sh
SYS_TRACKER_SINCE_DATE="2026-04-14 12:30"
SYS_TRACKER_FROM_HEIGHT="2221358"
SYS_TRACKER_SYNC_INTERVAL="60"
SYS_TRACKER_NEXT_HOP_LIMIT="8"
```

## Watch Mode

Poll the official Blockbook endpoint and print JSON alerts for new outbound sends:

```sh
python3 syscoin_tracker.py watch --interval 60
```

Post alerts to a Discord, Slack, or custom webhook:

```sh
SYS_TRACKER_WEBHOOK_URL="https://example.com/webhook" python3 syscoin_tracker.py watch
```

The webhook payload includes the transaction id, block height, UTC time, external
SYS amount, and destination outputs.

## Useful Reports

Recent-only report from a block height:

```sh
python3 syscoin_tracker.py report --since-height 2238000 --top 50
```

Report from the monitoring announcement timestamp onward. Date/time values without
an explicit offset use `Australia/Sydney` by default:

```sh
python3 syscoin_tracker.py sync --since-date "2026-04-14 12:30"
python3 syscoin_tracker.py report --since-date "2026-04-14 12:30" --top 50 --csv destinations_since_monitoring.csv
python3 syscoin_tracker.py serve --since-date "2026-04-14 12:30"
```

The dashboard auto-syncs from Blockbook and refreshes the browser every 60 seconds
by default. Change that cadence with:

```sh
python3 syscoin_tracker.py serve --since-date "2026-04-14 12:30" --sync-interval 30
```

Export every recipient address, ranked high to low by SYS amount:

```sh
python3 syscoin_tracker.py report --since-date "2026-04-14 12:30" --all-destinations --csv destinations_since_monitoring_all_ranked.csv
```

## Exchange And Sentry Checks

Known exchange labels live in `exchange_tags.csv`. Add rows as you identify more
addresses:

```csv
address,label
sys1qs05qfuw06dja0vglclkuz58nscmj579jll7lzr,Bitget
```

To use Syscoin Core RPC without storing credentials in the repo:

```sh
export SYS_RPC_HOST="127.0.0.1"
export SYS_RPC_USER="your-user"
export SYS_RPC_PASSWORD="your-password"
python3 syscoin_tracker.py rpc-check
python3 syscoin_tracker.py verify-sentries --since-date "2026-04-14 12:30"
```

`verify-sentries` compares exact 100,000 SYS candidates against
`masternode_list` outpoints. If RPC is only bound locally on a remote node, use an
SSH tunnel or adjust `rpcbind` / `rpcallowip` carefully.

Ignore small outputs:

```sh
python3 syscoin_tracker.py report --min-sys 1000 --top 50
```

Follow large first-hop outputs one additional hop:

```sh
python3 syscoin_tracker.py follow --depth 2 --min-sys 1000 --limit 100
```

## Custom Provider

Use another Blockbook instance:

```sh
python3 syscoin_tracker.py --blockbook-url "https://your-blockbook.example" sync
```

The default public endpoint is enough for tracking, but a private Blockbook instance
is better for heavy polling or deeper multi-hop tracing.
