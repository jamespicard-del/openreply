import { createHmac } from "node:crypto";
import { beforeEach, describe, expect, it, vi } from "vitest";
import { UnrecoverableError } from "bullmq";

const mocks = vi.hoisted(() => ({
  verifyQStashRequest: vi.fn(),
  publishIngestJob: vi.fn(),
  executeDmJob: vi.fn(),
  processInstagramWebhook: vi.fn(),
  runWithDedupe: vi.fn(),
  reconcileComments: vi.fn(),
  attachPendingNextReels: vi.fn(),
  findUnique: vi.fn(),
  findFirst: vi.fn(),
  queryRaw: vi.fn(),
  getJobCounts: vi.fn(),
  ping: vi.fn(),
  getWorkerHealth: vi.fn(),
  getRedisConnection: vi.fn(),
  getDMQueue: vi.fn(),
}));

vi.mock("@/lib/queue/qstash", async (importOriginal) => {
  const actual = await importOriginal<typeof import("@/lib/queue/qstash")>();
  return {
    ...actual,
    verifyQStashRequest: mocks.verifyQStashRequest,
    publishIngestJob: mocks.publishIngestJob,
  };
});

vi.mock("@/lib/queue/dm-worker", () => ({
  executeDmJob: mocks.executeDmJob,
}));

vi.mock("@/lib/queue/process-webhook", () => ({
  processInstagramWebhook: mocks.processInstagramWebhook,
}));

vi.mock("@/lib/queue/idempotency", async (importOriginal) => {
  const actual = await importOriginal<typeof import("@/lib/queue/idempotency")>();
  return {
    ...actual,
    runWithDedupe: mocks.runWithDedupe,
  };
});

vi.mock("@/lib/polling/comment-reconciler", () => ({
  reconcileComments: mocks.reconcileComments,
}));

vi.mock("@/lib/automation/attach-next-reel", () => ({
  attachPendingNextReels: mocks.attachPendingNextReels,
}));

vi.mock("@/lib/db/client", () => ({
  prisma: {
    $queryRaw: mocks.queryRaw,
    zernioConnection: { findUnique: mocks.findUnique },
    instagramAccount: { findFirst: mocks.findFirst },
  },
}));

vi.mock("@/lib/meta/oauth", () => ({
  decryptToken: () => "secret",
}));

vi.mock("@/lib/queue/client", () => ({
  getDMQueue: mocks.getDMQueue,
  getRedisConnection: mocks.getRedisConnection,
  POSTBACK_JOB_NAME: "process-postback",
  FOLLOWUP_JOB_NAME: "process-followup",
  MESSAGE_JOB_NAME: "process-message",
}));

vi.mock("@/lib/ops/worker-health", () => ({
  getWorkerHealth: mocks.getWorkerHealth,
}));

import { POST as jobPost, maxDuration as jobMaxDuration } from "@/app/api/jobs/dm/route";
import { POST as zernioPost } from "@/app/api/zernio/webhook/[workspaceId]/route";
import { GET as healthGet } from "@/app/api/health/route";
import {
  POST as reconcilePost,
  maxDuration as reconcileMaxDuration,
} from "@/app/api/cron/reconcile-comments/route";
import {
  DM_JOB_MAX_DURATION_SECONDS,
  RECONCILE_MAX_DURATION_SECONDS,
} from "@/lib/queue/backoff";

const zernioPayload = {
  id: "evt1",
  event: "comment.received",
  account: { id: "za1", platform: "instagram" },
  comment: {
    id: "c1",
    text: "LINK",
    author: { id: "p1", username: "reader" },
    platformPostId: "m1",
  },
};

function signedZernioRequest() {
  const raw = JSON.stringify(zernioPayload);
  const signature = createHmac("sha256", "secret").update(raw).digest("hex");
  return new Request("https://openreply.example/api/zernio/webhook/ws", {
    method: "POST",
    headers: { "x-zernio-signature": signature },
    body: raw,
  });
}

describe("QStash job route", () => {
  it("keeps the route timeout equal to the QStash publish timeout", () => {
    expect(jobMaxDuration).toBe(DM_JOB_MAX_DURATION_SECONDS);
    expect(jobMaxDuration).toBe(60);
    expect(reconcileMaxDuration).toBe(RECONCILE_MAX_DURATION_SECONDS);
  });

  beforeEach(() => {
    vi.clearAllMocks();
    mocks.verifyQStashRequest.mockResolvedValue(undefined);
    mocks.runWithDedupe.mockImplementation(
      async (_key: string, _durable: boolean, fn: () => Promise<unknown>) => ({
        status: "ok",
        value: await fn(),
      })
    );
    mocks.executeDmJob.mockResolvedValue(undefined);
    mocks.processInstagramWebhook.mockImplementation(
      async (
        _args: unknown,
        options?: { runInline?: (job: { name: string; data: object; jobId?: string }) => Promise<void> }
      ) => {
        await options?.runInline?.({
          name: "process-comment",
          data: { instagramAccountId: "ig1" },
          jobId: "comment_ig1_c1",
        });
      }
    );
  });

  it("rejects a missing signature", async () => {
    mocks.verifyQStashRequest.mockRejectedValue(new Error("Missing QStash signature"));
    const response = await jobPost(
      new Request("https://openreply.example/api/jobs/dm", {
        method: "POST",
        body: "{}",
      })
    );
    expect(response.status).toBe(401);
    expect(mocks.executeDmJob).not.toHaveBeenCalled();
  });

  it("stops retrying a body that is not a job", async () => {
    const response = await jobPost(
      new Request("https://openreply.example/api/jobs/dm", {
        method: "POST",
        headers: { "upstash-signature": "sig" },
        body: JSON.stringify({ kind: "process", name: "drop-table", data: {} }),
      })
    );
    expect(response.status).toBe(489);
  });

  it("runs an ingest inline and passes the QStash retry count", async () => {
    const response = await jobPost(
      new Request("https://openreply.example/api/jobs/dm", {
        method: "POST",
        headers: { "upstash-signature": "sig", "upstash-retried": "1" },
        body: JSON.stringify({
          kind: "ingest",
          provider: "ZERNIO",
          workspaceId: "ws",
          dedupeKey: "zernio:ws:evt1",
          payload: { object: "instagram", entry: [] },
        }),
      })
    );
    expect(response.status).toBe(200);
    expect(mocks.runWithDedupe).toHaveBeenCalledWith(
      "zernio:ws:evt1",
      true,
      expect.any(Function)
    );
    expect(mocks.executeDmJob).toHaveBeenCalledWith(
      expect.objectContaining({ id: "comment_ig1_c1", attemptsMade: 1 })
    );
  });

  it("acks a finished duplicate without running the processor again", async () => {
    mocks.runWithDedupe.mockResolvedValue({ status: "done" });
    const response = await jobPost(
      new Request("https://openreply.example/api/jobs/dm", {
        method: "POST",
        headers: { "upstash-signature": "sig" },
        body: JSON.stringify({
          kind: "process",
          name: "process-message",
          jobId: "message_ig_mid",
          data: { instagramAccountId: "ig" },
        }),
      })
    );
    expect(response.status).toBe(200);
    await expect(response.json()).resolves.toEqual({ ok: true, duplicate: true });
    expect(mocks.executeDmJob).not.toHaveBeenCalled();
  });

  it("asks QStash to retry when the same job is already in flight", async () => {
    mocks.runWithDedupe.mockResolvedValue({ status: "inflight" });
    const response = await jobPost(
      new Request("https://openreply.example/api/jobs/dm", {
        method: "POST",
        headers: { "upstash-signature": "sig" },
        body: JSON.stringify({
          kind: "process",
          name: "process-comment",
          jobId: "comment_ig_c1",
          data: { instagramAccountId: "ig" },
        }),
      })
    );
    expect(response.status).toBe(500);
  });

  it("cancels retries when delivery is unrecoverable", async () => {
    mocks.executeDmJob.mockRejectedValue(new UnrecoverableError("uncertain"));
    const response = await jobPost(
      new Request("https://openreply.example/api/jobs/dm", {
        method: "POST",
        headers: { "upstash-signature": "sig" },
        body: JSON.stringify({
          kind: "process",
          name: "process-comment",
          jobId: "comment_ig_c1",
          data: { instagramAccountId: "ig" },
        }),
      })
    );
    expect(response.status).toBe(489);
  });
});

describe("Zernio webhook", () => {
  beforeEach(() => {
    vi.unstubAllEnvs();
    vi.clearAllMocks();
    mocks.findUnique.mockResolvedValue({
      webhookId: "wh",
      webhookSecret: "encrypted",
    });
    mocks.findFirst.mockResolvedValue({
      zernioAccountId: "za1",
      instagramId: "ig1",
    });
    mocks.publishIngestJob.mockResolvedValue(undefined);
    mocks.processInstagramWebhook.mockResolvedValue(undefined);
  });

  it("acks after publishing and does not process inline", async () => {
    vi.stubEnv("QUEUE_BACKEND", "qstash");
    const response = await zernioPost(signedZernioRequest(), {
      params: Promise.resolve({ workspaceId: "ws" }),
    });
    expect(response.status).toBe(200);
    expect(mocks.publishIngestJob).toHaveBeenCalledWith(
      expect.objectContaining({
        provider: "ZERNIO",
        workspaceId: "ws",
        dedupeKey: "zernio:ws:evt1",
      })
    );
    expect(mocks.processInstagramWebhook).not.toHaveBeenCalled();
  });

  it("keeps the BullMQ path on the request", async () => {
    vi.stubEnv("QUEUE_BACKEND", "bullmq");
    const response = await zernioPost(signedZernioRequest(), {
      params: Promise.resolve({ workspaceId: "ws" }),
    });
    expect(response.status).toBe(200);
    expect(mocks.processInstagramWebhook).toHaveBeenCalled();
    expect(mocks.publishIngestJob).not.toHaveBeenCalled();
  });

  it("does not publish an event for an unknown account", async () => {
    vi.stubEnv("QUEUE_BACKEND", "qstash");
    mocks.findFirst.mockResolvedValue(null);
    const response = await zernioPost(signedZernioRequest(), {
      params: Promise.resolve({ workspaceId: "ws" }),
    });
    expect(response.status).toBe(200);
    await expect(response.json()).resolves.toEqual({ success: true, ignored: true });
    expect(mocks.publishIngestJob).not.toHaveBeenCalled();
  });
});

describe("health", () => {
  beforeEach(() => {
    vi.unstubAllEnvs();
    vi.clearAllMocks();
    mocks.queryRaw.mockResolvedValue([{ "?column?": 1 }]);
    mocks.ping.mockResolvedValue("PONG");
    mocks.getRedisConnection.mockReturnValue({ ping: mocks.ping });
    mocks.getJobCounts.mockResolvedValue({
      waiting: 0,
      active: 0,
      delayed: 0,
      failed: 0,
    });
    mocks.getDMQueue.mockReturnValue({ getJobCounts: mocks.getJobCounts });
    mocks.getWorkerHealth.mockResolvedValue({
      healthy: true,
      heartbeat: { checkedAt: new Date().toISOString() },
      ageMs: 1000,
    });
  });

  it("does not read Redis or the worker heartbeat on the QStash backend", async () => {
    vi.stubEnv("QUEUE_BACKEND", "qstash");
    vi.stubEnv("QSTASH_TOKEN", "token");
    vi.stubEnv("QSTASH_CURRENT_SIGNING_KEY", "current");
    vi.stubEnv("QSTASH_NEXT_SIGNING_KEY", "next");
    const response = await healthGet();
    expect(response.status).toBe(200);
    const body = await response.json();
    expect(body.checks.worker).toMatchObject({
      healthy: true,
      heartbeat: null,
      mode: "qstash",
    });
    expect(mocks.getRedisConnection).not.toHaveBeenCalled();
    expect(mocks.getDMQueue).not.toHaveBeenCalled();
    expect(mocks.getWorkerHealth).not.toHaveBeenCalled();
  });

  it("still requires the worker heartbeat on BullMQ", async () => {
    vi.stubEnv("QUEUE_BACKEND", "bullmq");
    mocks.getWorkerHealth.mockResolvedValue({
      healthy: false,
      heartbeat: null,
      ageMs: null,
    });
    const response = await healthGet();
    expect(response.status).toBe(503);
    expect(mocks.getWorkerHealth).toHaveBeenCalled();
    expect(mocks.ping).toHaveBeenCalled();
  });
});

describe("comment reconcile schedule route", () => {
  beforeEach(() => {
    vi.unstubAllEnvs();
    vi.clearAllMocks();
    vi.stubEnv("CRON_SECRET", "cron-secret");
    mocks.attachPendingNextReels.mockResolvedValue({ bound: 0, failedAccounts: 0 });
    mocks.reconcileComments.mockResolvedValue(undefined);
    mocks.verifyQStashRequest.mockResolvedValue(undefined);
  });

  function request(headers: Record<string, string> = {}) {
    return new Request("https://openreply.example/api/cron/reconcile-comments", {
      method: "POST",
      headers,
    });
  }

  it("no-ops while BullMQ is still sweeping", async () => {
    vi.stubEnv("QUEUE_BACKEND", "bullmq");
    const response = await reconcilePost(
      request({ authorization: "Bearer cron-secret" })
    );
    expect(response.status).toBe(200);
    await expect(response.json()).resolves.toMatchObject({ skipped: true });
    expect(mocks.reconcileComments).not.toHaveBeenCalled();
  });

  it("runs the worker's sweep when QStash is the backend", async () => {
    vi.stubEnv("QUEUE_BACKEND", "qstash");
    const response = await reconcilePost(
      request({ authorization: "Bearer cron-secret" })
    );
    expect(response.status).toBe(200);
    expect(mocks.attachPendingNextReels).toHaveBeenCalled();
    expect(mocks.reconcileComments).toHaveBeenCalled();
  });

  it("accepts a QStash signature in place of the cron secret", async () => {
    vi.stubEnv("QUEUE_BACKEND", "qstash");
    const response = await reconcilePost(
      request({ "upstash-signature": "sig" })
    );
    expect(response.status).toBe(200);
    expect(mocks.verifyQStashRequest).toHaveBeenCalled();
    expect(mocks.reconcileComments).toHaveBeenCalled();
  });

  it("rejects an unsigned request", async () => {
    const response = await reconcilePost(request());
    expect(response.status).toBe(401);
  });
});
