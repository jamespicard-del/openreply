import { NextResponse } from "next/server";
import { UnrecoverableError } from "bullmq";
import { z } from "zod";
import { executeDmJob } from "@/lib/queue/dm-worker";
import { processDedupeKey, runWithDedupe } from "@/lib/queue/idempotency";
import {
  FOLLOWUP_JOB_NAME,
  MESSAGE_JOB_NAME,
  POSTBACK_JOB_NAME,
  type DmQueueJob,
} from "@/lib/queue/client";
import { processInstagramWebhook } from "@/lib/queue/process-webhook";
import { dmJobUrl, logQStashError, verifyQStashRequest } from "@/lib/queue/qstash";

export const runtime = "nodejs";
export const dynamic = "force-dynamic";
// A single Zernio event runs inline: keyword match, rate-limit reserve, and
// one or two provider sends. That finishes well under a minute. The matching
// QStash publish timeout is DM_JOB_MAX_DURATION_SECONDS (kept equal to this
// literal; Next reads maxDuration statically). 60 is also Vercel Hobby's cap.
export const maxDuration = 60;

const PROCESS_NAMES = new Set([
  "process-comment",
  POSTBACK_JOB_NAME,
  FOLLOWUP_JOB_NAME,
  MESSAGE_JOB_NAME,
]);

const jobBodySchema = z.discriminatedUnion("kind", [
  z.object({
    kind: z.literal("ingest"),
    provider: z.enum(["ZERNIO", "META"]),
    workspaceId: z.string().min(1).optional(),
    payload: z.unknown(),
    dedupeKey: z.string().min(1),
  }),
  z.object({
    kind: z.literal("process"),
    name: z.string().min(1),
    data: z
      .object({ instagramAccountId: z.string().min(1) })
      .passthrough(),
    jobId: z.string().min(1).optional(),
  }),
]);

function attemptsMade(request: Request): number {
  const raw = Number(request.headers.get("upstash-retried") ?? "0");
  return Number.isFinite(raw) && raw > 0 ? Math.floor(raw) : 0;
}

export async function POST(request: Request) {
  const rawBody = await request.text();
  try {
    await verifyQStashRequest({ request, rawBody, url: dmJobUrl() });
  } catch (error) {
    console.error("[qstash] signature rejected:", logQStashError(error));
    return NextResponse.json({ ok: false }, { status: 401 });
  }

  let json: unknown;
  try {
    json = JSON.parse(rawBody);
  } catch {
    return NextResponse.json({ ok: false }, { status: 489 });
  }
  const parsed = jobBodySchema.safeParse(json);
  if (!parsed.success) {
    return NextResponse.json({ ok: false }, { status: 489 });
  }
  if (
    parsed.data.kind === "process" &&
    !PROCESS_NAMES.has(parsed.data.name)
  ) {
    return NextResponse.json({ ok: false }, { status: 489 });
  }

  const attempt = attemptsMade(request);

  try {
    if (parsed.data.kind === "ingest") {
      const body = parsed.data;
      const result = await runWithDedupe(body.dedupeKey, true, () =>
        processInstagramWebhook(
          {
            payload: body.payload as Parameters<
              typeof processInstagramWebhook
            >[0]["payload"],
            provider: body.provider,
            workspaceId: body.workspaceId,
          },
          {
            runInline: (job) =>
              executeDmJob({
                name: job.name,
                data: job.data,
                id: job.jobId,
                attemptsMade: attempt,
              }),
          }
        )
      );
      if (result.status === "done") {
        return NextResponse.json({ ok: true, duplicate: true });
      }
      if (result.status === "inflight") {
        return NextResponse.json({ ok: false }, { status: 500 });
      }
      return NextResponse.json({ ok: true });
    }

    const body = parsed.data;
    const { key, durable } = processDedupeKey(body);
    const result = await runWithDedupe(key, durable, () =>
      executeDmJob({
        name: body.name,
        data: body.data as unknown as DmQueueJob,
        id: body.jobId,
        attemptsMade: attempt,
      })
    );
    if (result.status === "done") {
      return NextResponse.json({ ok: true, duplicate: true });
    }
    if (result.status === "inflight") {
      return NextResponse.json({ ok: false }, { status: 500 });
    }
    return NextResponse.json({ ok: true });
  } catch (error) {
    console.error("[qstash] job failed:", logQStashError(error));
    if (error instanceof UnrecoverableError) {
      return NextResponse.json({ ok: false }, { status: 489 });
    }
    return NextResponse.json({ ok: false }, { status: 500 });
  }
}
