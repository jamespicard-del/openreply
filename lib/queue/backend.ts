export type QueueBackend = "bullmq" | "qstash";

/**
 * `bullmq` is the default so a deploy of this code does not change production
 * until QUEUE_BACKEND is set. Any other value, including unset, stays on BullMQ.
 */
export function getQueueBackend(): QueueBackend {
  const value = process.env.QUEUE_BACKEND?.trim().toLowerCase();
  return value === "qstash" ? "qstash" : "bullmq";
}
