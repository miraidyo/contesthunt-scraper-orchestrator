/**
 * ContestHunt daily scraper orchestrator (Cloudflare Worker, Cron Trigger).
 *
 * The backend is authoritative: it persists every run in Supabase (scraper_runs),
 * enforces "one active run" and "bounded attempts per schedule key" atomically.
 * This Worker only inspects run history through the protected API and decides
 * whether to start, skip or retry. It is safe to invoke repeatedly (idempotent).
 *
 *   GET  {BACKEND_URL}/api/v1/operations/scraper-runs/latest?schedule_key=daily-YYYY-MM-DD
 *   POST {BACKEND_URL}/api/v1/operations/scraper-runs
 *
 * Secret: env.SCRAPER_TRIGGER_SECRET (Worker secret) -> `X-Scraper-Secret` header only.
 * It is never put in a URL and never logged.
 *
 * This Worker does NOT replace the separate 10-minute Render keep-alive ping.
 */

const API_PATH = "/api/v1/operations/scraper-runs";

const DEFAULTS = {
  REQUEST_TIMEOUT_MS: 15000, // per HTTP attempt (Render free may be cold-starting)
  MAX_REQUEST_TRIES: 4, // per logical call, for transient failures
  BACKOFF_BASE_MS: 2000, // 2s, 4s, 8s (+ jitter), capped
  BACKOFF_CAP_MS: 20000,
  RETRY_BASE_DELAY_MINUTES: 5, // cool-down after a failed run: 5, 10, 20 ... min
};

/** Error with a classification the caller can act on. */
class ApiError extends Error {
  constructor(message, { kind, status } = {}) {
    super(message);
    this.kind = kind; // "auth" | "validation" | "transient" | "unexpected"
    this.status = status;
  }
}

/** UTC calendar day -> stable schedule key shared by all attempts for that day. */
export function scheduleKeyFor(scheduledTimeMs) {
  const d = new Date(scheduledTimeMs);
  return `daily-${d.toISOString().slice(0, 10)}`;
}

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

function backoffMs(attemptIndex, cfg) {
  const exp = Math.min(cfg.BACKOFF_CAP_MS, cfg.BACKOFF_BASE_MS * 2 ** attemptIndex);
  return Math.round(exp / 2 + Math.random() * (exp / 2)); // equal jitter
}

function log(level, event, fields = {}) {
  // Structured, secret-free logging. Never pass headers/env here.
  const line = JSON.stringify({ level, event, ts: new Date().toISOString(), ...fields });
  if (level === "error") console.error(line);
  else if (level === "warn") console.warn(line);
  else console.log(line);
}

function buildConfig(env) {
  const num = (v, d) => (Number.isFinite(Number(v)) && Number(v) > 0 ? Number(v) : d);
  return {
    BACKEND_URL: String(env.BACKEND_URL || "").replace(/\/+$/, ""),
    REQUEST_TIMEOUT_MS: num(env.REQUEST_TIMEOUT_MS, DEFAULTS.REQUEST_TIMEOUT_MS),
    MAX_REQUEST_TRIES: Math.min(8, num(env.MAX_REQUEST_TRIES, DEFAULTS.MAX_REQUEST_TRIES)),
    BACKOFF_BASE_MS: num(env.BACKOFF_BASE_MS, DEFAULTS.BACKOFF_BASE_MS),
    BACKOFF_CAP_MS: num(env.BACKOFF_CAP_MS, DEFAULTS.BACKOFF_CAP_MS),
    RETRY_BASE_DELAY_MINUTES: num(env.RETRY_BASE_DELAY_MINUTES, DEFAULTS.RETRY_BASE_DELAY_MINUTES),
  };
}

/**
 * One HTTP call with timeout. Returns {status, body}. Throws ApiError("transient")
 * for network errors/timeouts - i.e. the outcome at the server is UNKNOWN.
 */
async function httpOnce(cfg, env, fetchImpl, method, path, jsonBody) {
  const headers = { "X-Scraper-Secret": env.SCRAPER_TRIGGER_SECRET, Accept: "application/json" };
  if (jsonBody !== undefined) headers["Content-Type"] = "application/json";
  let res;
  try {
    res = await fetchImpl(`${cfg.BACKEND_URL}${path}`, {
      method,
      headers,
      body: jsonBody !== undefined ? JSON.stringify(jsonBody) : undefined,
      signal: AbortSignal.timeout(cfg.REQUEST_TIMEOUT_MS),
    });
  } catch (err) {
    throw new ApiError(`network/timeout: ${err?.name || "Error"}`, { kind: "transient" });
  }
  let body = null;
  try {
    body = await res.json();
  } catch {
    /* non-JSON body (e.g. a proxy error page) */
  }
  return { status: res.status, body };
}

/** Classify a response status for retry purposes. */
function classify(status) {
  if (status === 401 || status === 403) return "auth"; // permanent: never retried
  if (status === 400 || status === 404 || status === 422) return "validation"; // permanent
  if (status === 408 || status === 425 || status === 429 || status >= 500) return "transient";
  return "unexpected";
}

/** GET latest with bounded exponential backoff on transient failures. */
async function getLatest(cfg, env, fetchImpl, scheduleKey) {
  const path = `${API_PATH}/latest?schedule_key=${encodeURIComponent(scheduleKey)}`;
  let lastErr;
  for (let i = 0; i < cfg.MAX_REQUEST_TRIES; i++) {
    try {
      const { status, body } = await httpOnce(cfg, env, fetchImpl, "GET", path);
      if (status === 200 && body?.summary) return body;
      const kind = classify(status);
      lastErr = new ApiError(`GET latest -> HTTP ${status}`, { kind, status });
      if (kind !== "transient") throw lastErr; // auth/validation/unexpected: stop now
    } catch (err) {
      if (!(err instanceof ApiError)) throw err;
      lastErr = err;
      if (err.kind !== "transient") throw err;
    }
    if (i < cfg.MAX_REQUEST_TRIES - 1) await sleep(backoffMs(i, cfg));
  }
  throw lastErr;
}

/**
 * Request a run. Distinguishes:
 *   - accepted (202)               -> {outcome: "accepted", run}
 *   - refused by backend (409)     -> {outcome: "refused", reason, run}
 *   - delivery uncertain / 5xx     -> re-read history BEFORE any re-send
 * The POST carries the expected `attempt`, so even a blind replay cannot create
 * a duplicate (backend answers attempt_mismatch / active_run).
 */
async function requestRun(cfg, env, fetchImpl, scheduleKey, expectedAttempt) {
  const payload = { schedule_key: scheduleKey, attempt: expectedAttempt, trigger_source: "cloudflare" };
  let lastErr;
  for (let i = 0; i < cfg.MAX_REQUEST_TRIES; i++) {
    let uncertain = false;
    try {
      const { status, body } = await httpOnce(cfg, env, fetchImpl, "POST", API_PATH, payload);
      if (status === 202 && body?.run_id) {
        return { outcome: "accepted", run: body };
      }
      if (status === 409) {
        return { outcome: "refused", reason: body?.reason || "conflict", run: body?.run || null };
      }
      const kind = classify(status);
      lastErr = new ApiError(`POST -> HTTP ${status}`, { kind, status });
      if (kind !== "transient") throw lastErr;
      uncertain = true; // 5xx/429: server may or may not have created the run
    } catch (err) {
      if (!(err instanceof ApiError)) throw err;
      lastErr = err;
      if (err.kind !== "transient") throw err;
      uncertain = true; // timeout / lost response
    }

    if (uncertain) {
      // Check authoritative history before ever sending again.
      await sleep(backoffMs(i, cfg));
      try {
        const latest = await getLatest(cfg, env, fetchImpl, scheduleKey);
        const run = latest.run;
        if (run && run.attempt >= expectedAttempt) {
          log("info", "trigger_confirmed_via_history", {
            schedule_key: scheduleKey, run_id: run.run_id, attempt: run.attempt, status: run.status,
          });
          return { outcome: "accepted_unconfirmed_response", run };
        }
      } catch (err) {
        if (err instanceof ApiError && err.kind !== "transient") throw err;
        // History unavailable too: do NOT resend blindly beyond the bounded loop.
        log("warn", "history_check_failed_after_uncertain_trigger", { schedule_key: scheduleKey });
      }
    }
  }
  throw lastErr;
}

/**
 * Decide and act. Returns a short outcome string (also logged).
 * Exported for ad-hoc verification with a mocked fetch.
 */
export async function orchestrate(env, scheduledTimeMs, fetchImpl = fetch, nowMs = Date.now()) {
  const cfg = buildConfig(env);
  if (!cfg.BACKEND_URL || !env.SCRAPER_TRIGGER_SECRET) {
    log("error", "misconfigured", { missing: !cfg.BACKEND_URL ? "BACKEND_URL" : "SCRAPER_TRIGGER_SECRET" });
    return "misconfigured";
  }
  if (!cfg.BACKEND_URL.startsWith("https://")) {
    log("error", "backend_url_must_be_https");
    return "misconfigured";
  }

  const scheduleKey = scheduleKeyFor(scheduledTimeMs);

  try {
    const latest = await getLatest(cfg, env, fetchImpl, scheduleKey);
    const { run, active_run: activeRun, summary } = latest;

    if (summary.succeeded) {
      log("info", "skip_already_succeeded", { schedule_key: scheduleKey, run_id: run?.run_id });
      return "skipped_succeeded";
    }
    if (summary.active) {
      log("info", "skip_run_active", {
        schedule_key: scheduleKey, active_run_id: activeRun?.run_id,
        active_schedule_key: activeRun?.schedule_key, status: activeRun?.status,
      });
      return "skipped_active";
    }
    if (summary.exhausted) {
      // Failure state is preserved in Supabase; surface it loudly in Worker logs.
      log("error", "retries_exhausted", {
        schedule_key: scheduleKey, attempts: summary.attempts_used, max_attempts: summary.max_attempts,
        last_run_id: run?.run_id, last_status: run?.status, last_error: run?.error_summary,
      });
      return "exhausted";
    }

    // Retry cool-down (exponential across attempts): base * 2^(attempts_used-1)
    if (run && (run.status === "failed" || run.status === "timeout")) {
      const waitMs = cfg.RETRY_BASE_DELAY_MINUTES * 60000 * 2 ** (summary.attempts_used - 1);
      const finishedMs = run.completed_at ? Date.parse(run.completed_at) : 0;
      if (finishedMs && nowMs - finishedMs < waitMs) {
        log("info", "retry_cooldown", {
          schedule_key: scheduleKey, last_run_id: run.run_id, attempt: run.attempt,
          retry_in_seconds: Math.ceil((waitMs - (nowMs - finishedMs)) / 1000),
        });
        return "cooldown";
      }
      log("warn", "previous_attempt_failed_retrying", {
        schedule_key: scheduleKey, last_run_id: run.run_id, last_status: run.status,
        last_error: run.error_summary,
      });
    }

    const expectedAttempt = summary.attempts_used + 1;
    const result = await requestRun(cfg, env, fetchImpl, scheduleKey, expectedAttempt);

    if (result.outcome === "refused") {
      // Backend is authoritative; these are normal, non-error outcomes.
      log("info", "trigger_refused_by_backend", {
        schedule_key: scheduleKey, reason: result.reason, run_id: result.run?.run_id,
      });
      return `refused_${result.reason}`;
    }
    log("info", "trigger_accepted", {
      schedule_key: scheduleKey, run_id: result.run.run_id, attempt: result.run.attempt,
      status: result.run.status, parent_run_id: result.run.parent_run_id ?? null,
      confirmed_by: result.outcome === "accepted" ? "202" : "history",
    });
    return "triggered";
  } catch (err) {
    if (err instanceof ApiError && err.kind === "auth") {
      log("error", "auth_rejected_not_retrying", { status: err.status });
      return "auth_failed";
    }
    if (err instanceof ApiError && err.kind === "validation") {
      log("error", "request_invalid_not_retrying", { status: err.status });
      return "invalid_request";
    }
    // Transient exhaustion: nothing was recorded here; the next cron tick re-evaluates from history.
    log("error", "coordination_failed_will_retry_next_tick", {
      schedule_key: scheduleKey, kind: err?.kind || "unexpected", status: err?.status ?? null,
      message: String(err?.message || err).slice(0, 200),
    });
    return "deferred";
  }
}

export default {
  async scheduled(event, env, ctx) {
    ctx.waitUntil(orchestrate(env, event.scheduledTime));
  },
};
