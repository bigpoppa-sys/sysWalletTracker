#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BUNDLE_URL="${SYS_TRACKER_BUNDLE_URL:-https://syswallettracker.vercel.app/sysWalletTracker-vps.tgz}"
[[ $# -eq 0 || ( $# -eq 1 && "$1" == --check ) ]] || {
  echo "Usage: $0 [--check]" >&2; exit 2;
}
work="$(mktemp -d "${TMPDIR:-/tmp}/syswallettracker-vercel.XXXXXXXX")"
trap 'rm -rf -- "$work"' EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

# Fetch to a private local directory, then reuse the allowlisted, staged installer.
# --check downloads and validates only; it never contacts the VPS.
curl --fail --show-error --silent --location --proto '=https' --proto-redir '=https' \
  --connect-timeout 15 --max-time 180 "$BUNDLE_URL" -o "$work/vercel.tgz"
bash "$ROOT/scripts/update_vps_from_local.sh" --bundle "$work/vercel.tgz" "$@"
