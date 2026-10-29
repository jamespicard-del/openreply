import { Client, Receiver } from "@upstash/qstash";
import { createHash } from "node:crypto";
import { getBaseUrl } from "@/lib/env";
import type { DmQueueJob } from "@/lib/queue/client";
import {
  DM_JOB_MAX_DURATION_SECONDS,
  QSTASH_RETRIES,
  QSTASH_RETRY_DELAY,
  RECONCILE_MAX_DURATION_SECONDS,
} from "@/lib/queue/backoff";

export {
  QSTASH_RETRIES,
  QSTASH_RETRY_DELAY,
  DM_JOB_MAX_DURATION_SECONDS,
  RECONCILE_MAX_DURATION_SECONDS,
};

export const RECONCILE_SCHEDULE_ID = "openreply-comment-reconcile";
export const RECONCILE_CRON = "*/5 * * * *";

const QSTASH_ENV = [
  "QSTASH_TOKEN",
  "QSTASH_CURRENT_SIGNING_KEY",
  "QSTASH_NEXT_SIGNING_KEY",
] as const;

export function missingQStashEnv(): string[] {
  return QSTASH_ENV.filter((name) => !process.env[name]);
}

export function dmJobUrl(): string {
  return new URL("/api/jobs/dm", getBaseUrl()).toString();
}

export function reconcileUrl(): string {
  return new URL("/api/cron/reconcile-comments", getBaseUrl()).toString();
}

function qstashClient(): Client {
  const token = process.env.QSTASH_TOKEN;
  if (!token) throw new Error("QSTASH_TOKEN is required");
  return new Client({
    token,
    baseUrl: process.env.QSTASH_URL,
    devMode: false,
  });
}

export function getReceiver(): Receiver {
  const currentSigningKey = process.env.QSTASH_CURRENT_SIGNING_KEY;
  const nextSigningKey = process.env.QSTASH_NEXT_SIGNING_KEY;
  if (!currentSigningKey || !nextSigningKey) {
    throw new Error(
      "QSTASH_CURRENT_SIGNING_KEY and QSTASH_NEXT_SIGNING_KEY are required"
    );
  }
  return new Receiver({
    currentSigningKey,
    nextSigningKey,
    devMode: false,
  });
}

export async function verifyQStashRequest(input: {
  request: Request;
  rawBody: string;
  url: string;
}): Promise<void> {
  const signature = input.request.headers.get("upstash-signature");
  if (!signature) throw new Error("Missing QStash signature");
  await getReceiver().verify({
    signature,
    body: input.rawBody,
    url: input.url,
    upstashRegion: input.request.headers.get("upstash-region") ?? undefined,
    clockTolerance: 10,
  });
}

// The SDK keeps a deduplication id for 90 days. A raw event id would then
// refuse a republish long after retries were exhausted. Bucket the webhook
// id so a burst collapses and a later redelivery can still be published.
// Processing idempotency is the Redis lock, which is released on failure.
const DEDUPE_BUCKET_MS = 10 * 60 * 1000;

export function toDeduplicationId(value: string): string {
  const cleaned = value.replace(/[^A-Za-z0-9_-]/g, "_");
  if (cleaned.length <= 128) return cleaned;
  return createHash("sha256").update(value).digest("hex");
}

export function bucketedDeduplicationId(key: string, now = Date.now()): string {
  const bucket = Math.floor(now / DEDUPE_BUCKET_MS);
  return toDeduplicationId(`${key}_${bucket}`);
}

type PublishBody =
  | {
      kind: "ingest";
      provider: "ZERNIO" | "META";
      workspaceId?: string;
      payload: unknown;
      dedupeKey: string;
    }
  | {
      kind: "process";
      name: string;
      data: DmQueueJob;
      jobId?: string;
    };

async function publish(input: {
  body: PublishBody;
  deduplicationId?: string;
  delaySeconds?: number;
}): Promise<void> {
  await qstashClient().publishJSON({
    url: dmJobUrl(),
    body: input.body,
    retries: QSTASH_RETRIES,
    retryDelay: QSTASH_RETRY_DELAY,
    timeout: DM_JOB_MAX_DURATION_SECONDS,
    label: "openreply-dm",
    redact: { body: true },
    ...(input.deduplicationId
      ? { deduplicationId: input.deduplicationId }
      : {}),
    ...(input.delaySeconds ? { delay: input.delaySeconds } : {}),
  });
}

export async function publishIngestJob(input: {
  provider: "ZERNIO" | "META";
  workspaceId?: string;
  payload: unknown;
  dedupeKey: string;
}): Promise<void> {
  await publish({
    body: { kind: "ingest", ...input },
    deduplicationId: bucketedDeduplicationId(input.dedupeKey),
  });
}

export async function publishDmJob(input: {
  name: string;
  data: DmQueueJob;
  jobId?: string;
  delayMs?: number;
}): Promise<void> {
  const delaySeconds =
    input.delayMs && input.delayMs > 0
      ? Math.max(1, Math.ceil(input.delayMs / 1000))
      : undefined;
  await publish({
    body: {
      kind: "process",
      name: input.name,
      data: input.data,
      jobId: input.jobId,
    },
    // Stable ids (follow-up, read fallback, rate-limit retry) keep BullMQ's
    // "one job per id" behavior. The SDK retains the id for 90 days, which
    // is the point of those ids. Reconciler enqueues omit jobId on purpose.
    deduplicationId: input.jobId
      ? toDeduplicationId(input.jobId)
      : undefined,
    delaySeconds,
  });
}

export function logQStashError(error: unknown): string {
  const message = error instanceof Error ? error.message : "Unknown error";
  const token = process.env.QSTASH_TOKEN;
  return token ? message.replaceAll(token, "[redacted]") : message;
}
