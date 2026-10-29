import { createHash } from "node:crypto";
import { getRedisConnection } from "@/lib/queue/client";
import { DM_JOB_MAX_DURATION_SECONDS } from "@/lib/queue/backoff";

const PREFIX = "qstash:dm:";
// Outlives the function (maxDuration) and ends before the first QStash retry
// (5 minutes), so a crashed invocation does not block the retry.
const INFLIGHT_TTL_SECONDS = DM_JOB_MAX_DURATION_SECONDS + 30;
const DONE_TTL_SECONDS = 24 * 60 * 60;

export type DedupeClaim = "claimed" | "inflight" | "done";

function redisKey(key: string): string {
  return `${PREFIX}${key}`;
}

export function zernioDedupeKey(workspaceId: string, payload: unknown): string {
  const id =
    payload && typeof payload === "object" && "id" in payload
      ? (payload as { id: unknown }).id
      : undefined;
  if (typeof id === "string" && id.trim()) {
    return `zernio:${workspaceId}:${id.trim()}`;
  }
  const digest = createHash("sha256")
    .update(JSON.stringify(payload))
    .digest("hex");
  return `zernio:${workspaceId}:${digest}`;
}

export function processDedupeKey(body: {
  name: string;
  data: unknown;
  jobId?: string;
}): { key: string; durable: boolean } {
  if (body.jobId) return { key: `job:${body.jobId}`, durable: true };
  const digest = createHash("sha256")
    .update(JSON.stringify({ name: body.name, data: body.data }))
    .digest("hex");
  return { key: `job:${digest}`, durable: false };
}

export async function claimDedupe(key: string): Promise<DedupeClaim> {
  const redis = getRedisConnection();
  const stored = redisKey(key);
  const created = await redis.set(
    stored,
    "inflight",
    "EX",
    INFLIGHT_TTL_SECONDS,
    "NX"
  );
  if (created === "OK") return "claimed";
  const current = await redis.get(stored);
  return current === "done" ? "done" : "inflight";
}

export async function completeDedupe(key: string): Promise<void> {
  await getRedisConnection().set(
    redisKey(key),
    "done",
    "EX",
    DONE_TTL_SECONDS
  );
}

export async function releaseDedupe(key: string): Promise<void> {
  try {
    await getRedisConnection().eval(
      `if redis.call("GET", KEYS[1]) == "inflight" then return redis.call("DEL", KEYS[1]) end return 0`,
      1,
      redisKey(key)
    );
  } catch (error) {
    const message = error instanceof Error ? error.message : "Unknown error";
    console.error("[qstash] failed to release dedupe lock:", message);
  }
}

function isUnrecoverable(error: unknown): boolean {
  return error instanceof Error && error.name === "UnrecoverableError";
}

export type DedupeRun<T> =
  | { status: "done" }
  | { status: "inflight" }
  | { status: "ok"; value: T };

/**
 * Durable keys (webhook event ids and deterministic job ids) stay "done" for
 * a day so a replay does not send twice. Keys without a job id (reconciler
 * sweeps) only hold the lock while the invocation runs, because a later sweep
 * must be able to enqueue the same comment again. An unrecoverable send is
 * marked done either way: retrying it is how duplicate DMs happen.
 */
export async function runWithDedupe<T>(
  key: string,
  durable: boolean,
  fn: () => Promise<T>
): Promise<DedupeRun<T>> {
  const claim = await claimDedupe(key);
  if (claim === "done") return { status: "done" };
  if (claim === "inflight") return { status: "inflight" };

  let finished = false;
  try {
    const value = await fn();
    finished = true;
    if (durable) await completeDedupe(key);
    else await releaseDedupe(key);
    return { status: "ok", value };
  } catch (error) {
    if (!finished) {
      if (isUnrecoverable(error)) await completeDedupe(key);
      else await releaseDedupe(key);
    }
    throw error;
  }
}
