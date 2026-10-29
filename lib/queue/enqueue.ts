import { getDMQueue, type DmQueueJob } from "@/lib/queue/client";
import { getQueueBackend } from "@/lib/queue/backend";

export type EnqueueDmOptions = {
  jobId?: string;
  delay?: number;
};

export async function enqueueDmJob(
  name: string,
  data: DmQueueJob,
  options?: EnqueueDmOptions
): Promise<void> {
  if (getQueueBackend() === "qstash") {
    const { publishDmJob } = await import("@/lib/queue/qstash");
    await publishDmJob({
      name,
      data,
      jobId: options?.jobId,
      delayMs: options?.delay,
    });
    return;
  }

  if (options) {
    await getDMQueue().add(name, data, options);
    return;
  }
  await getDMQueue().add(name, data);
}
