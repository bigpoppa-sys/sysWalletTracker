# Deployment Runbook

Use this exact runbook every time the user says `deploy`, `push live`, or `push it live`.

`deploy` means the full job is complete only when:

- The requested change is committed to `main`.
- `main` is pushed to GitHub.
- Vercel production has been deployed or checked.
- The VPS static pages at `syscoin.dev` have been refreshed.
- The live site has been verified from the public URL.

Do not call a deploy done after only a commit, only a push, or only a Vercel command.

## Live Architecture

- GitHub repo: `bigpoppa-sys/sysWalletTracker`, branch `main`.
- Vercel app: serves `api/index.py`, canonical URL `https://syswallettracker.vercel.app`.
- Vercel install bundle route: `https://syswallettracker.vercel.app/sysWalletTracker-vps.tgz`.
- VPS publisher: `root@142.93.241.64`, app dir `/root/sysWalletTracker`.
- Static output: `/var/www/html/syswallettracker`.
- Public static URL: `https://syscoin.dev/syswallettracker/`.
- Vercel pages normally proxy the static `syscoin.dev` HTML/JSON first.
- The VPS app dir is a bundle install, not a git checkout. Never use `git pull` inside `/root/sysWalletTracker`.

## Deploy Checklist

1. Check the local scope.

```sh
git status --short
git diff --stat
```

Only stage files related to the requested change. Do not stage `reports/`, SQLite files, logs, temp screenshots, or user scratch files unless explicitly requested.

2. Run local verification.

```sh
python3 -m py_compile syscoin_tracker.py api/index.py
bash -n scripts/*.sh
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

If this finishes cleanly, continue to the VPS publish step.

If the Vercel CLI hangs in `Building...`, becomes silent for more than a few minutes, or returns an unclear status, do not stop the deployment. Inspect the canonical production app and continue with the VPS local publish fallback:

```sh
npx vercel inspect https://syswallettracker.vercel.app --timeout 20
curl -fsSL "https://syswallettracker.vercel.app/sentrynode?force=1&t=$(date +%s)" | head
```

If Vercel reports the new deployment as `UNKNOWN`, `BLOCKED`, or refuses `vercel promote` because the deployment is not ready, treat Vercel as checked but not promotable. Continue with the VPS local publish fallback and verify the public pages. Do not keep creating new Vercel deployments unless the user specifically asks to debug Vercel.

Do not use random Vercel deployment URLs as the primary verification target; they can return `401` when deployment protection is enabled. Use the canonical production URL.

5. Refresh the VPS static pages.

Preferred path, when the fresh Vercel bundle is available:

```sh
./scripts/update_vps_from_vercel_bundle.sh
```

Fallback path, when Vercel is slow, protected, or the bundle freshness is unclear:

```sh
./scripts/update_vps_from_local.sh
```

Both scripts:

- Use `~/.ssh/codex_syswallettracker_ed25519`.
- Wait for `/root/sysWalletTracker/.static-snapshot.lock` instead of racing cron.
- Preserve `/root/sysWalletTracker/.env`, database files, and logs.
- Run `python3 -m py_compile`.
- Run `publish-static` with the current VPS database so `syscoin.dev` updates immediately.

6. Verify the live site.

Use the changed page and expected text from the request. Example for a sentry snapshot table change:

```sh
curl -fsSL "https://syscoin.dev/syswallettracker/sentrynode.html?t=$(date +%s)" | rg "Seniority|Date Taken Down|100k Moved To"
curl -fsSL "https://syswallettracker.vercel.app/sentrynode?force=1&t=$(date +%s)" | rg "Seniority|Date Taken Down|100k Moved To"
```

For browser-visible UI changes, open the relevant live page after the curl check and confirm the page visually.

## Failure Handling

- If the Vercel deploy path fails or hangs, use `scripts/update_vps_from_local.sh` and verify the public pages.
- If the Vercel bundle publish fails, use `scripts/update_vps_from_local.sh`.
- If the local VPS publish fails, fix that exact failure and rerun it.
- Only report a deployment as blocked after both the primary path and documented fallback path fail, or after live verification still fails.
- Include the exact failed command and the exact live verification result when blocked.

## Important Rules

- GitHub push alone is not a complete deploy.
- Vercel deploy alone is not a complete deploy.
- VPS static publish alone is not a complete deploy unless the code is already committed and pushed.
- Do not kill the static cron unless the user explicitly asks. Wait on the lock.
- For page-rendering changes, use `publish-static`; do not run the full indexing cron manually.
- For indexer changes, update the VPS bundle first, then let the cron continue unless an immediate static refresh is needed.
