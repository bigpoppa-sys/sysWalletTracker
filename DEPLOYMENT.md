# Hetzner Deployment and Recovery

This is the current runbook for SysWalletTracker. Run deployment commands only
when deployment is explicitly authorized. Documentation or script changes alone
do not authorize a commit, push, server change, or Vercel deployment.

## Architecture

| Component | Location / owner |
| --- | --- |
| GitHub | `bigpoppa-sys/sysWalletTracker`, production branch `main` |
| Vercel | Project `syswallettracker`, https://syswallettracker.vercel.app |
| SSH | `hb-sys-prod-01` = `chris@5.223.55.80` |
| Local SSH key | `~/.ssh/id_ed25519_hetzner_sysbot` |
| Privileged operations | `sudo -n` from the `chris` account |
| Application | `/srv/syswallettracker/app` |
| Persistent SQLite | `/srv/syswallettracker/data/syscoin_tracker.sqlite` |
| Mutable sentry CSV | `/srv/syswallettracker/data/network_masternodes.csv` |
| Worker environment | `/srv/syswallettracker/data/worker.env` |
| Core data / local RPC cookie | `/srv/syswallettracker/core` |
| Local database backups | `/srv/syswallettracker/backups` |
| Public static files | `/var/www/html/syswallettracker` |
| Static origin | https://tracker.syscoin.dev/syswallettracker |

The dedicated `syswallettracker` account owns the worker application, persistent
data, Core data, backups and static output. Keep it separate from the colocated MM
bot. Nginx reads the public directory; it must never expose the app, data, backup,
environment or Core directories. The app is a file/bundle installation, not a Git
checkout; do not run `git pull` there.

GitHub's Vercel integration automatically deploys `main`. It does **not** need a
new `VERCEL_TOKEN` or GitHub Actions secrets. The static origin is a separate
deployment step: normally run the local helper after the matching Vercel
production deployment is ready. The optional GitHub mirror workflow is
**manual-only**, not a requirement for the normal GitHub-to-Vercel deployment.
Do not use the legacy Vercel project `syswallettracker-sentry`.

## Recovery Prerequisites

These are provisioning tasks, not actions performed by either update helper.
Review the checked-in `deploy/` templates before installing them.

1. Confirm `hb-sys-prod-01` resolves to `chris@5.223.55.80`, with the key above,
   and verify the SSH host-key fingerprint independently through Hetzner's
   console. Helpers require a matching `known_hosts` entry and never disable
   host-key checking. Without a local alias, set
   `SYS_TRACKER_VPS_HOST=chris@5.223.55.80`.
2. Confirm passwordless `sudo -n`, Python 3 with the application's syntax,
   Bash 4+, `flock`, `tar`, and systemd on the host. The staged compile uses the
   service's `/usr/bin/python3`, so an incompatible interpreter blocks replacement.
3. Create the `syswallettracker` system account and the directories in the table,
   with the application initially a real directory (not a symlink). Set the
   appropriate ownership before deploying. The helpers require app, data and
   public directories to exist.
4. Restore SQLite only from a confirmed, integrity-checked backup using SQLite's
   backup API or a consistent offline copy. Do not copy a live database without
   accounting for WAL files. Seed the mutable CSV separately, only when no
   server CSV exists; routine deployments never upload it. Protect `worker.env`
   and RPC credentials. A historical local snapshot is not a current backup.
5. Install the reviewed `deploy/syswallettracker@.service` and
   `deploy/syswallettracker.slice` into `/etc/systemd/system`, then run
   `sudo -n systemctl daemon-reload`. The service must run as `syswallettracker`
   with `WorkingDirectory=/srv/syswallettracker/app` and load the environment
   from `/srv/syswallettracker/data/worker.env`. Install reviewed timers
   separately, enabling them only after the initial publish is verified.
6. Provision Core, swap and resource/disk limits separately using the relevant
   `deploy/` templates. `deploy/syscoin.conf` and `deploy/worker.env` are not
   uploaded by the helpers: review and provision their live counterparts
   explicitly. Keep Core RPC on loopback. The indexer needs chain data, enough
   storage for the index and backups, and monitoring of the shared host.
7. Configure the `tracker.syscoin.dev` DNS record and nginx virtual host, then
   provision and verify HTTPS. `deploy/tracker.nginx.conf` is an HTTP template;
   copying it alone does not install TLS. Do not replace unrelated nginx sites.

If no confirmed database backup survived the deleted server, recovery is a
rebuild, not restoration of lost history. Sentry feeds and historical CSVs do not
reconstruct the full wallet, clustering or emissions indexes. Let the relevant
worker jobs rebuild from chain data and report incomplete indexes honestly.
Do not run the old cron installer alongside the systemd workers.

## Local Verification

From the repository, inspect intentional existing changes before selecting any
commit scope. Do not stage unrelated edits or generated/private data.

```sh
git status --short
git diff --stat
for script in scripts/*.sh; do bash -n "$script"; done
bash scripts/update_vps_from_local.sh --check
git diff --check
```

`--check` constructs and validates the safe local archive, compiles its Python
sources without importing them, and lists its contents. It performs no SSH,
push or deployment. The helper works from any working directory. For UI changes,
also verify the affected pages locally before deployment.

## Authorized Deployment

1. Commit only the reviewed change and push `main` when authorized. Check that
   Vercel project `syswallettracker` is connected to the expected repository.
   Use Vercel's dashboard/GitHub deployment checks to verify the exact commit is
   ready in production. A ready older deployment does not verify a new commit.
2. Deploy that same reviewed local checkout to Hetzner:

   ```sh
   bash scripts/update_vps_from_local.sh
   ```

   The helper packages current working-tree files, including intentional
   uncommitted application changes, **not** just `HEAD`. For a production release,
   confirm these are the files intended for the matching Vercel deployment.
3. Verify the static origin, Vercel pages, worker status and expected content as
   described below. A push alone, Vercel build alone or HTTP 200 alone is not a
   verified end-to-end release.

Vercel CLI deployment is an explicitly authorized fallback when its GitHub
integration is unavailable, not the normal path. Use the operator's authenticated
CLI session and confirm the linked project before doing so. A standalone Vercel
token is not a normal-deployment prerequisite. Do not repeatedly create
deployments to work around an unclear build state.

### What the Local Helper Does

- Builds a positive allowlist of application sources, immutable lookup CSVs,
  manifests, `scripts/*.py`, `scripts/*.sh`, systemd unit/timer/slice templates,
  swap/logrotate templates and the reviewed nginx/sysctl templates. It includes new worker
  scripts and units even before they are Git-tracked.
- Excludes environment files, SQLite/WAL files, logs, credentials, `.git`,
  `.ssh`, reports, Core configuration, generated snapshots and
  `network_masternodes.csv`. It rejects symlink inputs.
- Transfers a tar archive with `scp` into a unique private SSH staging directory;
  privileged installation uses `sudo -n`.
- Serializes deployments with `/srv/syswallettracker/data/.deploy.lock`, copies
  the existing app into staging, then overlays the allowed files. Files absent
  from a bundle are not deleted, including existing charts, scripts and units.
- Compiles the staged Python with the **remote** interpreter and validates shell
  syntax before replacing the app. It requires the installed/staged worker and
  Chart.js asset, applies `syswallettracker` ownership, and waits for the worker
  job locks (including `times`) before swapping directories. It does not kill running jobs.
- Keeps the previous app in a unique `/srv/syswallettracker/.deploy.*/previous-app`
  directory and restores it if replacement or publication fails. It never
  installs/enables units, reloads nginx or overwrites persistent data/configuration.
- Publishes through exactly:

  ```sh
  sudo -n systemctl start syswallettracker@publish.service
  ```

  The helper verifies the service user and requires a fresh successful
  `job-publish.json`; a skipped/no-op run is not reported as a successful release.

The oneshot calls `scripts/tracker_worker.py publish` as `syswallettracker`, with
`--db /srv/syswallettracker/data/syscoin_tracker.sqlite`, the data-directory CSV,
and `publish-static --skip-sync`. This render uses stored data and makes no
network calls; network synchronization remains in separate scheduled jobs.
Missing/stale data is a sync/recovery issue, not a reason to run an indexing cron
from the deployment helper. A successful render does not mean the indexes are
complete or fresh.

Supported overrides are `SYS_TRACKER_VPS_HOST`, `SYS_TRACKER_SSH_KEY` (a filesystem
path, not private-key text), and `SYS_TRACKER_LOCK_TIMEOUT_SECONDS` (default
`1800`). App/data/public paths are deliberately fixed to the systemd architecture.
Normal helpers neither source `.env` nor run the publisher as root.

### Vercel Bundle Recovery

Use only after confirming the production Vercel deployment contains the intended
runtime code and the Hetzner worker/unit prerequisites are already installed:

```sh
bash scripts/update_vps_from_vercel_bundle.sh --check
bash scripts/update_vps_from_vercel_bundle.sh
```

The wrapper downloads `https://syswallettracker.vercel.app/sysWalletTracker-vps.tgz`
over HTTPS into a private local temporary directory, then uses the same installer.
Unlike the local `--check`, bundle `--check` downloads from Vercel, but still never
contacts the VPS. `SYS_TRACKER_BUNDLE_URL` can select another trusted HTTPS source;
its content is executable application code, so do not use untrusted URLs.

Only runtime Python, immutable lookup CSVs and the chart asset are accepted from
the bundle. Old bundled deployment helpers, worker files, unit files, environment,
mutable CSV and generated snapshots are ignored. Absolute/traversal paths,
links, duplicate accepted files and invalid Python abort before upload. Missing
core runtime files also abort. The installed worker, deployment tooling and
charts absent from the bundle survive. This is not a complete bootstrap source
and does not prove which Git commit Vercel packaged.

### Optional GitHub Mirror Job

`.github/workflows/publish-static.yml` has only `workflow_dispatch`. It does not
run on pushes, wait on Vercel's API, or use `VERCEL_TOKEN`. To opt into a manual
run later, an operator must first verify Vercel production matches the selected
`main` commit and check the workflow's `vercel_ready` confirmation.

Only that optional workflow requires repository secrets:

- `SYS_TRACKER_SSH_KEY`: the private key authorized for `chris@5.223.55.80`.
- `SYS_TRACKER_SSH_KNOWN_HOSTS`: known-hosts entries matching `5.223.55.80`, verified
  independently against the Hetzner console. Do not trust unverified `ssh-keyscan`
  output on first use.

The workflow uses the direct IP, a key path under the runner's actual `$HOME`,
strict host-key checking, and removes its credentials afterward. Missing secrets
cannot break normal Vercel GitHub deployments because this job is not automatic.

## Vercel Origin and Verification

Set Vercel production `SYS_TRACKER_STATIC_BASE_URL` to
`https://tracker.syscoin.dev/syswallettracker` only after the replacement origin
is reachable over valid HTTPS and has verified output. Keep
`SYS_TRACKER_STATIC_TIMEOUT_SECONDS=5` and `SYS_TRACKER_REQUEST_SYNC=0` to bound
origin requests without full request-time sync. A failed configured origin serves
the last good in-process cache for up to 24 hours with stale headers, or an explicit
503 when no usable cache remains; it never silently serves an empty local database.
Environment changes need a new Vercel
deployment to take effect. Check for an existing override pointing at the retired
origin; a code default does not override project environment variables.

From the operator's machine, after an authorized release:

```sh
ssh -i ~/.ssh/id_ed25519_hetzner_sysbot -o IdentitiesOnly=yes hb-sys-prod-01 \
  'sudo -n systemctl status syswallettracker@publish.service --no-pager; sudo -n journalctl -u syswallettracker@publish.service -n 40 --no-pager'
ssh -i ~/.ssh/id_ed25519_hetzner_sysbot -o IdentitiesOnly=yes hb-sys-prod-01 \
  'sudo -n cat /srv/syswallettracker/data/job-publish.json'
curl -fsS --max-time 30 https://tracker.syscoin.dev/syswallettracker/sentrynode.html
curl -fsS --max-time 30 https://tracker.syscoin.dev/syswallettracker/sn-comp.html
curl -fsS --max-time 30 https://tracker.syscoin.dev/syswallettracker/assets/chart.umd.js
curl -fsS --max-time 30 https://tracker.syscoin.dev/syswallettracker/health.json
curl -fsS --max-time 30 'https://syswallettracker.vercel.app/sentrynode?force=1'
```

A completed oneshot normally becomes inactive; inspect its exit result, journal
and `job-publish.json` (`ok`, `started_at`, `finished_at`), not just active status.
Inspect timestamps and index heights/completeness on the actual changed pages,
and visually verify UI changes. `health.json` comes from the separate health job;
its freshness does not by itself prove all data jobs succeeded. The workflow's
HTTP checks are smoke checks, not a substitute for these data/content checks.

## Failures and Rollback

- A packaging, missing prerequisite, lock, compile or shell-syntax failure before
  replacement leaves installed code untouched. Fix the reported cause locally;
  do not bypass validation or replace the mutable CSV with a bundled snapshot.
- Failed replacement/publication restores the previous application and returns
  nonzero. Inspect the error and worker journal before retrying. Restoration is
  **code-only**: SQLite schema/data and any already-written public files are not
  rolled back. After diagnosing publication, explicitly republish the restored
  application with the same systemd command and verify its output.
- A hard interruption (host crash, SIGKILL, lost disk) may prevent shell traps
  from completing. Inspect app and the printed `.deploy.*` release directory,
  wait for worker jobs to finish and hold the same deployment/job locks before
  restoring `previous-app`. Do not blindly delete or copy a release over an
  active deployment. Retain the failed app for diagnosis.
- Keep the printed previous-app directory until the release is verified; then
  remove only reviewed obsolete release directories to reclaim disk. They are
  rollback copies, not database backups. Never prune data/Core/backups as part
  of a code update.
- Configure durable off-host backups and test restoration. Local backups on the
  same server do not protect against server deletion; archive the mutable CSV
  and protected configuration separately from the SQLite backup job.

## Archived Architecture

The deleted `root@142.93.241.64` server, `/root/sysWalletTracker`,
`~/.ssh/codex_syswallettracker_ed25519`, `https://syscoin.dev/syswallettracker`,
and the old `.static-snapshot.lock`/cron deployment path are retired. References
to them elsewhere are historical, not recovery instructions. Do not reinstall
those cron jobs or point Vercel back at that dead origin.
