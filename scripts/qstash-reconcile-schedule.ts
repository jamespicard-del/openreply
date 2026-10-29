/**
 * Create or update the 5-minute comment reconcile schedule.
 *
 * Not run by tests, CI, or deploy. Run once at cutover:
 *   npx tsx scripts/qstash-reconcile-schedule.ts
 *
 * Requires QSTASH_TOKEN and NEXTAUTH_URL. The route no-ops while
 * QUEUE_BACKEND is still bullmq, so the schedule can be created first.
 */
import { Client } from "@upstash/qstash";
import {
  QSTASH_RETRIES,
  QSTASH_RETRY_DELAY,
  RECONCILE_CRON,
  RECONCILE_MAX_DURATION_SECONDS,
  RECONCILE_SCHEDULE_ID,
  reconcileUrl,
} from "../lib/queue/qstash";

async function main() {
  const token = process.env.QSTASH_TOKEN;
  if (!token) {
    console.error("QSTASH_TOKEN is required");
    process.exit(1);
  }

  const client = new Client({
    token,
    baseUrl: process.env.QSTASH_URL,
    devMode: false,
  });
  const destination = reconcileUrl();
  const result = await client.schedules.create({
    destination,
    cron: RECONCILE_CRON,
    scheduleId: RECONCILE_SCHEDULE_ID,
    retries: QSTASH_RETRIES,
    retryDelay: QSTASH_RETRY_DELAY,
    timeout: RECONCILE_MAX_DURATION_SECONDS,
    method: "POST",
    label: "openreply-reconcile",
  });
  console.log(`schedule ${result.scheduleId} -> ${destination}`);
}

main().catch((error: unknown) => {
  const token = process.env.QSTASH_TOKEN ?? "";
  const message = error instanceof Error ? error.message : "schedule create failed";
  console.error(token ? message.replaceAll(token, "[redacted]") : message);
  process.exit(1);
});
