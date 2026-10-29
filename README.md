# ContestHunt daily scraper orchestrator (Cloudflare Worker)

Cron-triggered Worker that coordinates the **daily full scraper run** through the
backend's protected operations API. The backend (FastAPI + Supabase `scraper_runs`)
is authoritative for run claiming, overlap prevention and the retry limit; this
Worker only inspects history and decides **skip / start / retry**.

It is **separate from, and does not touch,** the existing 10-minute Render ping Worker.

## Flow (per cron tick)

1. `schedule_key = daily-YYYY-MM-DD` (UTC date of the cron's scheduled time).
2. `GET /api/v1/operations/scraper-runs/latest?schedule_key=...`
3. Decide:
   | History | Action |
   |---|---|
   | a run for this key succeeded | skip |
   | any run queued/running (backend also recovers stale ones) | skip |
   | attempts exhausted (`>= max_attempts`, none succeeded) | log `retries_exhausted` (error); no trigger |
   | last attempt failed/timeout, cool-down (5, 10, 20 min…) not elapsed | wait for a later tick |
   | no run, or failed/timeout and cool-down elapsed | `POST` with `attempt = attempts_used + 1` |
4. A lost/timed-out/5xx `POST` is **never blindly re-sent**: history is re-read first. If the run exists it is
   treated as accepted. Even a replay carries `attempt`, so the backend answers `409 attempt_mismatch/active_run`.
5. Transient errors (network, 5xx, 408/425/429) use bounded exponential backoff with jitter.
   401/403 and 400/404/422 are **never retried**. If coordination can't complete, nothing is lost: the next tick
   re-evaluates from the persisted history.

The default cron (`*/15 2-6 * * *`, UTC) gives the 02:00 tick plus reconciliation ticks for ~5 hours so that failed
runs get retried with cool-down. Use `crons = ["0 2 * * *"]` for a single daily invocation (then only
transient-failure backoff inside one invocation applies; no later retry ticks).

## Configuration

| Name | Kind | Required | Notes |
|---|---|---|---|
| `SCRAPER_TRIGGER_SECRET` | **Worker secret** | yes | Same value as Render's `SCRAPER_TRIGGER_SECRET` (≥ 32 chars). Sent only in `X-Scraper-Secret`. |
| `BACKEND_URL` | var | yes | `https://<service>.onrender.com` (must be https, no trailing path). |
| `RETRY_BASE_DELAY_MINUTES` | var | no (5) | Cool-down after a failed run, doubled per attempt. |
| `REQUEST_TIMEOUT_MS`, `MAX_REQUEST_TRIES`, `BACKOFF_BASE_MS`, `BACKOFF_CAP_MS` | var | no | HTTP timeout / transient retry tuning. |

The per-schedule attempt limit (`SCRAPER_RUN_MAX_ATTEMPTS`, default 3) is enforced by the **backend**.

## Deploy (manual - not done by this repo)

```bash
cd cloudflare/scraper-orchestrator
npm i -D wrangler                         # or: npx wrangler ...
# edit BACKEND_URL in wrangler.toml
npx wrangler secret put SCRAPER_TRIGGER_SECRET
npx wrangler deploy
npx wrangler tail                         # watch structured logs
```

Local test of the scheduled handler: `npx wrangler dev --test-scheduled`, then
`curl "http://localhost:8787/__scheduled?cron=*/15+2-6+*+*+*"` (use a throwaway
`.dev.vars` containing `SCRAPER_TRIGGER_SECRET=...`; never commit it).

Logs contain run IDs, statuses and short error summaries - never secrets.
