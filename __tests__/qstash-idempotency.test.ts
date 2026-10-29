import { beforeEach, describe, expect, it, vi } from "vitest";

const redis = vi.hoisted(() => {
  const store = new Map<string, string>();
  return {
    store,
    set: vi.fn(
      async (key: string, value: string, ...args: Array<string | number>) => {
        if (args.includes("NX") && store.has(key)) return null;
        store.set(key, value);
        return "OK";
      }
    ),
    get: vi.fn(async (key: string) => store.get(key) ?? null),
    eval: vi.fn(async (_script: string, _keys: number, key: string) => {
      if (store.get(key) === "inflight") {
        store.delete(key);
        return 1;
      }
      return 0;
    }),
  };
});

vi.mock("@/lib/queue/client", () => ({
  getRedisConnection: () => redis,
}));

import { UnrecoverableError } from "bullmq";
import {
  processDedupeKey,
  runWithDedupe,
  zernioDedupeKey,
} from "@/lib/queue/idempotency";

describe("qstash dedupe lock", () => {
  beforeEach(() => {
    redis.store.clear();
    redis.set.mockClear();
    redis.get.mockClear();
    redis.eval.mockClear();
  });

  it("keys a Zernio delivery on the event id", () => {
    expect(zernioDedupeKey("ws", { id: "evt_1" })).toBe("zernio:ws:evt_1");
  });

  it("keeps a successful webhook job marked done", async () => {
    const first = await runWithDedupe("zernio:ws:evt_1", true, async () => "sent");
    const second = await runWithDedupe("zernio:ws:evt_1", true, async () => "again");
    expect(first).toEqual({ status: "ok", value: "sent" });
    expect(second).toEqual({ status: "done" });
    expect(redis.store.get("qstash:dm:zernio:ws:evt_1")).toBe("done");
  });

  it("releases the lock when processing throws so a retry can run", async () => {
    await expect(
      runWithDedupe("job:comment_1", true, async () => {
        throw new Error("rate limit");
      })
    ).rejects.toThrow("rate limit");
    const retry = await runWithDedupe("job:comment_1", true, async () => "sent");
    expect(retry).toEqual({ status: "ok", value: "sent" });
  });

  it("does not retry an unrecoverable send", async () => {
    await expect(
      runWithDedupe("zernio:ws:evt_2", true, async () => {
        throw new UnrecoverableError("delivery uncertain");
      })
    ).rejects.toThrow("delivery uncertain");
    const again = await runWithDedupe("zernio:ws:evt_2", true, async () => "sent");
    expect(again).toEqual({ status: "done" });
  });

  it("lets a reconciler sweep enqueue the same comment again", async () => {
    const key = processDedupeKey({
      name: "process-comment",
      data: { commentId: "c1" },
    });
    expect(key.durable).toBe(false);
    await runWithDedupe(key.key, key.durable, async () => undefined);
    const next = await runWithDedupe(key.key, key.durable, async () => "second");
    expect(next).toEqual({ status: "ok", value: "second" });
  });

  it("reports an in-flight duplicate instead of running twice", async () => {
    let release: () => void = () => {};
    const gate = new Promise<void>((resolve) => {
      release = resolve;
    });
    const first = runWithDedupe("job:held", true, () => gate);
    await vi.waitFor(() => {
      expect(redis.store.get("qstash:dm:job:held")).toBe("inflight");
    });
    const second = await runWithDedupe("job:held", true, async () => "nope");
    expect(second).toEqual({ status: "inflight" });
    release();
    await expect(first).resolves.toEqual({ status: "ok", value: undefined });
  });
});
