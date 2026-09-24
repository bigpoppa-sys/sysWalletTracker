#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
VPS_HOST="${SYS_TRACKER_VPS_HOST:-hb-sys-prod-01}"
SSH_KEY="${SYS_TRACKER_SSH_KEY:-$HOME/.ssh/id_ed25519_hetzner_sysbot}"
LOCK_TIMEOUT_SECONDS="${SYS_TRACKER_LOCK_TIMEOUT_SECONDS:-1800}"
BUNDLE=""
CHECK_ONLY=0

while (( $# )); do
  case "$1" in
    --check) CHECK_ONLY=1; shift ;;
    --bundle)
      [[ $# -ge 2 && -f "$2" ]] || { echo "--bundle requires an existing archive" >&2; exit 2; }
      BUNDLE="$2"; shift 2 ;;
    *) echo "Usage: $0 [--check] [--bundle FILE.tgz]" >&2; exit 2 ;;
  esac
done
[[ "$VPS_HOST" =~ ^[a-zA-Z0-9_@.-]+$ && "$VPS_HOST" != -* ]] || exit 2
[[ "$LOCK_TIMEOUT_SECONDS" =~ ^[1-9][0-9]{0,4}$ ]] || exit 2

work="$(mktemp -d "${TMPDIR:-/tmp}/syswallettracker-local.XXXXXXXX")"
remote=""
ssh_opts=(-i "$SSH_KEY" -o IdentitiesOnly=yes -o BatchMode=yes -o StrictHostKeyChecking=yes)
cleanup() {
  if [[ -n "$remote" ]]; then
    ssh "${ssh_opts[@]}" "$VPS_HOST" "rm -rf -- '$remote'" >/dev/null 2>&1 || true
  fi
  rm -rf -- "$work"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

# Construct a new archive; never extract an untrusted Vercel archive wholesale.
python3 - "$ROOT" "$BUNDLE" "$work/app.tgz" <<'PY'
import io
from pathlib import Path, PurePosixPath
import sys
import tarfile

root, bundle, output = Path(sys.argv[1]), sys.argv[2], sys.argv[3]
runtime = {
    "syscoin_tracker.py", "api/index.py", "static/assets/chart.umd.js",
    "exchange_hot_wallets.csv", "exchange_cold_wallets.csv", "exchange_routes.csv",
    "exchange_tags.csv", "wallet_labels.csv", "miner_addresses.csv",
    "node_outputs.csv", "verified_sentries.csv",
}
metadata = {
    "DEPLOYMENT.md", "README.md", ".python-version", "pyproject.toml", "uv.lock",
    "package.json", "package-lock.json", "vercel.json", ".gitignore", ".vercelignore",
}
seen = set()

def add(archive, name, body):
    if name in seen:
        raise ValueError(f"Duplicate archive entry: {name}")
    if name.endswith(".py"):
        compile(body, name, "exec")
    info = tarfile.TarInfo(name)
    info.size = len(body)
    info.mode = 0o755 if name.endswith(".sh") else 0o644
    archive.addfile(info, io.BytesIO(body))
    seen.add(name)
    print(name)

with tarfile.open(output, "w:gz") as target:
    if bundle:
        with tarfile.open(bundle, "r:gz") as source:
            for member in source:
                path = PurePosixPath(member.name)
                if path.is_absolute() or ".." in path.parts or str(path) != member.name:
                    raise ValueError(f"Unsafe archive path: {member.name}")
                if not member.isfile():
                    if member.isdir():
                        continue
                    raise ValueError(f"Links/special files are not allowed: {member.name}")
                # Recovery bundles must not downgrade installed workers, units or helpers.
                if member.name in runtime:
                    if member.size > 32 * 1024 * 1024:
                        raise ValueError(f"Oversized runtime file: {member.name}")
                    with source.extractfile(member) as stream:
                        add(target, member.name, stream.read())
    else:
        paths = runtime | metadata
        for pattern in ("scripts/*.py", "scripts/*.sh", "deploy/*.service",
                        "deploy/*.timer", "deploy/*.slice", "deploy/*.swap", "deploy/*.logrotate"):
            paths.update(str(path.relative_to(root)) for path in root.glob(pattern))
        # Deliberately exclude deploy/worker.env and all mutable or private data.
        paths.update({"deploy/tracker.nginx.conf", "deploy/90-tracker-swap.conf"})
        for name in sorted(paths):
            path = root / name
            if path.is_symlink() or any(parent.is_symlink() for parent in path.parents if parent != root):
                raise ValueError(f"Symlinks are not allowed: {name}")
            if path.is_file():
                add(target, name, path.read_bytes())
    required = {"syscoin_tracker.py", "api/index.py"}
    if not bundle:
        required.add("scripts/tracker_worker.py")
    if not required <= seen:
        raise ValueError(f"Missing required files: {sorted(required - seen)}")
print(f"Validated {len(seen)} application files; no environment, database or mutable CSV included.")
PY

if (( CHECK_ONLY )); then
  echo "Local archive check complete; no SSH or deployment performed."
  exit 0
fi

[[ -r "$SSH_KEY" ]] || { echo "SSH key not readable: $SSH_KEY" >&2; exit 1; }
candidate="$(ssh "${ssh_opts[@]}" "$VPS_HOST" 'sudo -n true && mktemp -d /tmp/syswallettracker-deploy.XXXXXXXX')"
[[ "$candidate" =~ ^/tmp/syswallettracker-deploy\.[a-zA-Z0-9]+$ ]] || {
  echo "Unexpected remote staging path; refusing to upload." >&2; exit 1;
}
remote="$candidate"
scp "${ssh_opts[@]}" "$work/app.tgz" "$VPS_HOST:$remote/app.tgz"
printf -v command 'sudo -n bash -s -- %q %q' "$remote" "$LOCK_TIMEOUT_SECONDS"
ssh "${ssh_opts[@]}" "$VPS_HOST" "$command" <<'REMOTE'
set -euo pipefail
UPLOAD="$1"
LOCK_TIMEOUT_SECONDS="$2"
APP=/srv/syswallettracker/app
DATA=/srv/syswallettracker/data
PUBLIC=/var/www/html/syswallettracker
SERVICE=syswallettracker@publish.service

[[ -d "$APP" && ! -L "$APP" && -d "$DATA" && -d "$PUBLIC" ]] || {
  echo "Bootstrap the dedicated account, directories and systemd worker first (DEPLOYMENT.md)." >&2
  exit 1
}
id syswallettracker >/dev/null
systemctl cat "$SERVICE" >/dev/null
[[ "$(systemctl show -p User --value "$SERVICE")" == syswallettracker ]] || {
  echo "$SERVICE must run as syswallettracker" >&2; exit 1;
}
exec 9>"$DATA/.deploy.lock"
flock -w "$LOCK_TIMEOUT_SECONDS" 9
STAGE="$(mktemp -d /srv/syswallettracker/.deploy.XXXXXXXX)"
finish() {
  status=$?
  trap - EXIT
  if (( status != 0 )) && [[ -d "$STAGE/previous-app" ]]; then
    echo "Deployment failed; restoring previous application. Public output may need republishing." >&2
    if [[ -e "$APP" ]]; then mv "$APP" "$STAGE/failed-app"; fi
    mv "$STAGE/previous-app" "$APP"
    echo "Failed release retained at $STAGE/failed-app" >&2
  elif [[ ! -d "$STAGE/previous-app" ]]; then
    rm -rf -- "$STAGE"
  fi
  exit "$status"
}
trap finish EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

mkdir "$STAGE/app"
cp -a "$APP/." "$STAGE/app/"
/usr/bin/python3 - "$UPLOAD/app.tgz" "$STAGE/app" <<'PY'
from pathlib import Path, PurePosixPath
import shutil
import sys
import tarfile

root = Path(sys.argv[2])
with tarfile.open(sys.argv[1], "r:gz") as archive:
    for member in archive:
        path = PurePosixPath(member.name)
        if (not member.isfile() or path.is_absolute() or ".." in path.parts
                or str(path) != member.name):
            raise ValueError(f"Unsafe release member: {member.name}")
        destination = root / member.name
        if destination.is_symlink() or any(parent.is_symlink() for parent in destination.parents):
            raise ValueError(f"Refusing to overwrite a symlink: {member.name}")
        destination.parent.mkdir(parents=True, exist_ok=True)
        with archive.extractfile(member) as source, destination.open("wb") as target:
            shutil.copyfileobj(source, target)
        destination.chmod(member.mode & 0o755)
PY

# Validate using the host interpreter before touching the installed app.
test -s "$STAGE/app/scripts/tracker_worker.py"
test -s "$STAGE/app/static/assets/chart.umd.js"
/usr/bin/python3 -m compileall -f -q "$STAGE/app/syscoin_tracker.py" "$STAGE/app/api" "$STAGE/app/scripts"
for script in "$STAGE/app"/scripts/*.sh; do bash -n "$script"; done
chown -R syswallettracker:syswallettracker "$STAGE/app"
chmod 755 "$STAGE/app"

# Wait for in-flight jobs, without killing services or starting network sync.
fds=()
deadline=$((SECONDS + LOCK_TIMEOUT_SECONDS))
for job in wallet traces sentry times miners publish top clusters emissions nevm backup health; do
  lock="$DATA/.$job.lock"
  touch "$lock"
  chown syswallettracker:syswallettracker "$lock"
  exec {fd}>"$lock"
  remaining=$((deadline - SECONDS))
  (( remaining > 0 )) || { echo "Timed out waiting for worker locks" >&2; exit 1; }
  flock -w "$remaining" "$fd"
  fds+=("$fd")
done

mv "$APP" "$STAGE/previous-app"
mv "$STAGE/app" "$APP"
for fd in "${fds[@]}"; do exec {fd}>&-; done

# An earlier oneshot may still be exiting after releasing its file lock.
deadline=$((SECONDS + LOCK_TIMEOUT_SECONDS))
while :; do
  state="$(systemctl show -p ActiveState --value "$SERVICE")"
  case "$state" in inactive|failed) break ;; esac
  (( SECONDS < deadline )) || { echo "Timed out waiting for $SERVICE" >&2; exit 1; }
  sleep 1
done
previous_status="$(/usr/bin/python3 - "$DATA/job-publish.json" <<'PY'
from pathlib import Path
import sys
path = Path(sys.argv[1])
print(path.stat().st_mtime_ns if path.exists() else 0)
PY
)"
sudo -n systemctl start syswallettracker@publish.service
/usr/bin/python3 - "$DATA/job-publish.json" "$previous_status" <<'PY'
import json
from pathlib import Path
import sys
path = Path(sys.argv[1])
if path.stat().st_mtime_ns <= int(sys.argv[2]) or json.loads(path.read_text()).get("ok") is not True:
    raise SystemExit("Publisher did not record a new successful run; inspect its journal and job-publish.json")
PY
echo "Application updated; stored-data-only publication completed at $PUBLIC"
echo "Previous application retained for rollback: $STAGE/previous-app"
echo "Unit templates were copied only; no systemd/nginx configuration was installed or enabled."
REMOTE
