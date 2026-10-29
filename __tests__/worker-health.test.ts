import { beforeEach, describe, expect, it, vi } from "vitest";

const { mockGet } = vi.hoisted(() => ({
  mockGet: vi.fn(),
}));

vi.mock("@/lib/queue/client", () => ({
  getRedisConnection: () => ({
    get: mockGet,
  }),
}));

import {
  getWorkerHealth,
  WORKER_HEARTBEAT_TTL_SECONDS,
} from "../lib/ops/worker-health";

const HEARTBEAT_INTERVAL_MS = 60_000;

function heartbeatAt(ageMs: number) {
  return JSON.stringify({
    status: "running",
    worker: "dm",
    pid: 1,
    checkedAt: new Date(Date.now() - ageMs).toISOString(),
  });
}

beforeEach(() => {
  mockGet.mockReset();
});

describe("worker health window", () => {
  it("keeps the Redis TTL and the healthy cutoff on one 180s window", () => {
    expect(WORKER_HEARTBEAT_TTL_SECONDS).toBe(180);
    expect(WORKER_HEARTBEAT_TTL_SECONDS * 1000).toBeGreaterThan(
      HEARTBEAT_INTERVAL_MS * 2
    );
  });

  it("stays healthy after one missed 60s heartbeat", async () => {
    mockGet.mockResolvedValue(heartbeatAt(HEARTBEAT_INTERVAL_MS * 2));

    const health = await getWorkerHealth();

    expect(health.healthy).toBe(true);
    expect(health.ageMs).toBeGreaterThan(HEARTBEAT_INTERVAL_MS);
    expect(health.ageMs).toBeLessThanOrEqual(
      WORKER_HEARTBEAT_TTL_SECONDS * 1000
    );
  });

  it("reads unhealthy once the heartbeat is older than the TTL", async () => {
    mockGet.mockResolvedValue(
      heartbeatAt(WORKER_HEARTBEAT_TTL_SECONDS * 1000 + 1)
    );

    await expect(getWorkerHealth()).resolves.toMatchObject({
      healthy: false,
    });
  });

  it("reads unhealthy when the heartbeat key has expired", async () => {
    mockGet.mockResolvedValue(null);

    await expect(getWorkerHealth()).resolves.toEqual({
      healthy: false,
      heartbeat: null,
      ageMs: null,
    });
  });
});
