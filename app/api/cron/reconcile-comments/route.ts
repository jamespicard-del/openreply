import { NextResponse } from "next/server";
import { attachPendingNextReels } from "@/lib/automation/attach-next-reel";
import { getQueueBackend } from "@/lib/queue/backend";
import { reconcileComments } from "@/lib/polling/comment-reconciler";
import {
  logQStashError,
  reconcileUrl,
  verifyQStashRequest,
} from "@/lib/queue/qstash";

export const runtime = "nodejs";
export const dynamic = "force-dynamic";
// Same bound as the DM job route so a Hobby project still deploys. A large
// workspace can outrun 60s; the sweep is idempotent and the next run continues.
// Literal on purpose: Next reads maxDuration statically. Keep it equal to
// RECONCILE_MAX_DURATION_SECONDS, which is also the QStash schedule timeout.
export const maxDuration = 60;

async function authorized(request: Request, rawBody: string): Promise<boolean> {
  const cronSecret = process.env.CRON_SECRET || process.env.NEXTAUTH_SECRET;
  if (
    cronSecret &&
    request.headers.get("authorization") === `Bearer ${cronSecret}`
  ) {
    return true;
  }
  if (!request.headers.get("upstash-signature")) return false;
  try {
    await verifyQStashRequest({ request, rawBody, url: reconcileUrl() });
    return true;
  } catch (error) {
    console.error("[qstash] reconcile signature rejected:", logQStashError(error));
    return false;
  }
}

async function handle(request: Request) {
  const rawBody = request.method === "POST" ? await request.text() : "";
  if (!(await authorized(request, rawBody))) {
    return NextResponse.json(
      { success: false, error: "Unauthorized" },
      { status: 401 }
    );
  }

  // While BullMQ is still the backend the worker already sweeps. Running here
  // too would double the Instagram reads. The QStash schedule can exist before
  // cutover; it no-ops until the flag flips.
  if (getQueueBackend() !== "qstash") {
    return NextResponse.json({
      success: true,
      skipped: true,
      backend: "bullmq",
    });
  }

  const attached = await attachPendingNextReels();
  await reconcileComments();
  return NextResponse.json({ success: true, attached });
}

export async function GET(request: Request) {
  return handle(request);
}

export async function POST(request: Request) {
  return handle(request);
}
