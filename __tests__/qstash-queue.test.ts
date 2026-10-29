import { beforeEach, describe, expect, it, vi } from "vitest";

const { publishJSON, queueAdd, clientConfig } = vi.hoisted(() => ({
  publishJSON: vi.fn(),
  queueAdd: vi.fn(),
  clientConfig: [] as unknown[],
}));

vi.mock("@upstash/qstash", () => ({
  Client: class {
    constructor(config: unknown) {
      clientConfig.push(config);
    }
    publishJSON = publishJSON;
  },
  Receiver: class {
    verify() {
      return Promise.resolve(true);
    }
  },
}));

vi.mock("@/lib/queue/client", () => ({
  getDMQueue: () => ({ add: queueAdd }),
  getRedisConnection: vi.fn(),
}));

import { getQueueBackend } from "@/lib/queue/backend";
import { enqueueDmJob } from "@/lib/queue/enqueue";
import {
  bucketedDeduplicationId,
  toDeduplicationId,
} from "@/lib/queue/qstash";
import { QSTASH_RETRY_DELAY, QSTASH_RETRIES } from "@/lib/queue/backoff";

const comment = {
  instagramAccountId: "ig",
  commentId: "c1",
  commentText: "LINK",
  commenterId: "person",
  mediaId: "media",
};

describe("queue backend", () => {
  beforeEach(() => {
    vi.unstubAllEnvs();
    publishJSON.mockReset().mockResolvedValue({ messageId: "m1" });
    queueAdd.mockReset().mockResolvedValue(undefined);
    clientConfig.length = 0;
  });

  it("stays on BullMQ unless QUEUE_BACKEND is qstash", () => {
    vi.stubEnv("QUEUE_BACKEND", "");
    expect(getQueueBackend()).toBe("bullmq");
    vi.stubEnv("QUEUE_BACKEND", "surprise");
    expect(getQueueBackend()).toBe("bullmq");
    vi.stubEnv("QUEUE_BACKEND", "QStash");
    expect(getQueueBackend()).toBe("qstash");
  });

  it("enqueues through BullMQ with the same job id and delay", async () => {
    vi.stubEnv("QUEUE_BACKEND", "bullmq");
    await enqueueDmJob("process-comment", comment, {
      jobId: "comment_ig_c1",
      delay: 1_800_000,
    });
    expect(queueAdd).toHaveBeenCalledWith("process-comment", comment, {
      jobId: "comment_ig_c1",
      delay: 1_800_000,
    });
    expect(publishJSON).not.toHaveBeenCalled();
  });

  it("publishes to the job route with BullMQ's retry schedule", async () => {
    vi.stubEnv("QUEUE_BACKEND", "qstash");
    vi.stubEnv("QSTASH_TOKEN", "token-value");
    vi.stubEnv("NEXTAUTH_URL", "https://openreply.example");

    await enqueueDmJob("process-comment", comment, {
      jobId: "comment_ig_c1_retry_1",
      delay: 20_000,
    });

    expect(clientConfig[0]).toMatchObject({
      token: "token-value",
      devMode: false,
    });
    expect(publishJSON).toHaveBeenCalledWith(
      expect.objectContaining({
        url: "https://openreply.example/api/jobs/dm",
        retries: QSTASH_RETRIES,
        retryDelay: QSTASH_RETRY_DELAY,
        timeout: 60,
        delay: 20,
        deduplicationId: "comment_ig_c1_retry_1",
        redact: { body: true },
        body: {
          kind: "process",
          name: "process-comment",
          data: comment,
          jobId: "comment_ig_c1_retry_1",
        },
      })
    );
    expect(publishJSON.mock.calls[0][0].contentBasedDeduplication).toBeUndefined();
  });

  it("omits deduplication when the reconciler has no job id", async () => {
    vi.stubEnv("QUEUE_BACKEND", "qstash");
    vi.stubEnv("QSTASH_TOKEN", "token-value");
    vi.stubEnv("NEXTAUTH_URL", "https://openreply.example");
    await enqueueDmJob("process-comment", comment);
    expect(publishJSON.mock.calls[0][0].deduplicationId).toBeUndefined();
    expect(publishJSON.mock.calls[0][0].delay).toBeUndefined();
  });

  it("rounds a sub-second delay up to one second", async () => {
    vi.stubEnv("QUEUE_BACKEND", "qstash");
    vi.stubEnv("QSTASH_TOKEN", "token-value");
    vi.stubEnv("NEXTAUTH_URL", "https://openreply.example");
    await enqueueDmJob("process-postback", {
      instagramAccountId: "ig",
      userId: "person",
      payload: "reveal:auto",
    }, { delay: 1_500, jobId: "postback_1" });
    expect(publishJSON.mock.calls[0][0].delay).toBe(2);
  });
});

describe("deduplication ids", () => {
  it("buckets a webhook id so a later redelivery is not locked for 90 days", () => {
    const start = 1_700_000_400_000;
    const first = bucketedDeduplicationId("zernio:ws:evt", start);
    const sameWindow = bucketedDeduplicationId("zernio:ws:evt", start + 9 * 60 * 1000);
    const nextWindow = bucketedDeduplicationId("zernio:ws:evt", start + 10 * 60 * 1000);
    expect(first).toBe(sameWindow);
    expect(nextWindow).not.toBe(first);
    expect(first.startsWith("zernio_ws_evt_")).toBe(true);
  });

  it("hashes ids that would not fit in a header", () => {
    expect(toDeduplicationId("a".repeat(200))).toMatch(/^[a-f0-9]{64}$/);
  });
});
