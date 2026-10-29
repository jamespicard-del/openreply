// Keep aligned with `attempts: 3` on the BullMQ queue in lib/queue/client.ts.
// Attempt 1 is immediate. The two retries wait 5 minutes, then 15. The third
// slot is what a fourth try would use, and what QStash uses if retries is raised.
export const DM_JOB_ATTEMPTS = 3;
export const DM_JOB_BACKOFF_MS = [
  5 * 60 * 1000,
  15 * 60 * 1000,
  45 * 60 * 1000,
] as const;

export const QSTASH_RETRIES = DM_JOB_ATTEMPTS - 1;

// `retried` starts at 0. 300000 * 3^n is 5 min, 15 min, 45 min.
export const QSTASH_RETRY_DELAY = `${DM_JOB_BACKOFF_MS[0]} * pow(3, retried)`;

// One Zernio event is a few provider calls. 60s matches Vercel Hobby's cap so
// a deploy is not rejected, and it is the timeout we hand QStash as well.
export const DM_JOB_MAX_DURATION_SECONDS = 60;

// The reconcile sweep walks every active campaign. Same Hobby cap; raise this
// on a plan that allows it if a sweep is killed mid-way. The sweep is safe to rerun.
export const RECONCILE_MAX_DURATION_SECONDS = 60;
