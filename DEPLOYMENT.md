# Deployment Runbook

Use this every time the user says `deploy`.

## Live Architecture

- GitHub repo: `bigpoppa-sys/sysWalletTracker`, branch `main`.
- Vercel app: serves `api/index.py` and exposes `/sysWalletTracker-vps.tgz`.
- VPS static publisher: `root@142.93.241.64`, app dir `/root/sysWalletTracker`.
- Public static output: `/var/www/html/syswallettracker`, served as `https://syscoin.dev/syswallettracker/`.
- Vercel pages normally proxy the static `syscoin.dev` HTML/JSON first. Therefore a live deploy is not complete until both Vercel and the VPS static output are updated.
- The VPS install is not a git checkout. Do not use `git pull` inside `/root/sysWalletTracker`.

## Deploy Checklist

1. Check scope locally.

```sh
git status --short
git diff --stat
```

Only stage files related to the requested change. Do not stage `reports/`, SQLite files, logs, or user scratch files unless explicitly requested.

2. Run local verification.

```sh
python3 -m py_compile syscoin_tracker.py api/index.py
```

For UI changes, run the local server and verify the changed page before deploying:

```sh
python3 syscoin_tracker.py serve --host 127.0.0.1 --port 8787 --sync-interval 0 --masternode-sync-interval 0
```

3. Commit and push.

```sh
git add <changed-files>
git commit -m "<clear message>"
git push origin main
```

4. Deploy Vercel production.

```sh
npx vercel --prod --yes
```

Wait for the command to finish and record the production URL. Do not continue while the Vercel CLI is still running.

5. Update the VPS from the fresh Vercel bundle and publish static pages.

```sh
./scripts/update_vps_from_vercel_bundle.sh
```

This script:

- Uses `~/.ssh/codex_syswallettracker_ed25519`.
- Waits for the VPS static snapshot lock instead of racing the cron.
- Downloads `https://syswallettracker.vercel.app/sysWalletTracker-vps.tgz`.
- Extracts code into `/root/sysWalletTracker` without touching `.env`, the database, or logs.
- Runs `python3 -m py_compile`.
- Runs `publish-static` with the current VPS database so `syscoin.dev` updates immediately.

6. Verify live.

Use forced fresh static reads first:

```sh
curl -fsSL "https://syscoin.dev/syswallettracker/sentrynode.html?t=$(date +%s)" | rg "expected text"
curl -fsSL "https://syswallettracker.vercel.app/sentrynode?force=1" | rg "expected text"
```

For browser/UI changes, open the relevant live page after the curl check.

## Important Rules

- GitHub push alone is not a complete deploy.
- Vercel deploy alone is not a complete deploy.
- VPS `git pull` is wrong because the VPS app dir is a bundle install, not a repo.
- Do not kill the static cron unless the user explicitly asks. Wait on the lock.
- For page-rendering changes, use `publish-static`; do not run the full indexing cron manually.
- For indexer changes, update the VPS bundle first, then let the cron continue unless an immediate static refresh is needed.
- If any step fails, stop and report the exact failed step before trying a different path.
