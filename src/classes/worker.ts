import { EventEmitter } from "node:events";
import { Job } from "./job";
import type { RedisClient, WorkerOptions } from "../interfaces";
import {
  createRedisClient,
  delay,
  getQueueKeys,
  type QueueKeys,
} from "../utils";
import { LuaScripts } from "../scripts/lua";
import { randomUUID } from "node:crypto";

export type Processor<
  TData = any,
  TResult = any,
  TName extends string = string,
> = (job: Job<TData, TResult, TName>) => Promise<TResult>;

export class Worker<
  TData = any,
  TResult = any,
  TName extends string = string,
> extends EventEmitter {
  readonly name: string;
  readonly client: RedisClient;
  readonly bClient: RedisClient; // Blocking client
  readonly opts: WorkerOptions;
  readonly keys: QueueKeys;
  private scripts: LuaScripts;
  private processor: Processor<TData, TResult, TName>;
  private prefix: string;

  private running = false;
  private closing: Promise<void> | null = null;
  private paused = false;
  private jobsInFlight = new Map<string, Job<TData, TResult, TName>>(); // Track active jobs
  private lockRenewTimers = new Map<string, NodeJS.Timeout>();
  private mainLoopPromise: Promise<void> | null = null;
  private workerId = randomUUID(); // Unique ID for this worker instance

  constructor(
    name: string,
    processor: Processor<TData, TResult, TName>,
    opts: WorkerOptions,
  ) {
    super();
    this.name = name;
    this.opts = {
      concurrency: 1,
      lockDuration: 30000,
      lockRenewTime: 15000,
      ...opts,
    };
    this.prefix = opts.prefix ?? "smq";
    this.keys = getQueueKeys(this.prefix, this.name);

    this.client = createRedisClient(opts.connection);
    this.bClient = this.client.duplicate();

    this.scripts = new LuaScripts(this.client); // Scripts use the normal client
    this.processor = processor;

    this.client.on("error", (err) => this.emit("error", err));
    this.bClient.on("error", (err) => this.emit("error", err)); // Listen to blocking client errors too

    this.run(); // Autorun by default (can be made optional)
  }

  private async run(): Promise<void> {
    if (this.running || this.closing) {
      return;
    }
    this.running = true;
    this.mainLoopPromise = this.mainLoop();
  }

  private async mainLoop(): Promise<void> {
    while (this.running && !this.closing && !this.paused) {
      if (this.jobsInFlight.size < (this.opts.concurrency ?? 1)) {
        try {
          const job = await this.acquireNextJob();
          if (job) {
            // Process concurrently but don't await here directly
            this.processJob(job).catch((err) => {
              console.error(`Unhandled error processing job ${job.id}:`, err);
              // Ensure cleanup even on unhandled promise rejection
              this.cleanupJob(job.id, err);
            });
          } else {
            // No job found, wait a bit before retrying to avoid busy-looping
            await delay(1000); // Configurable delay?
          }
        } catch (err) {
          if (
            !this.closing &&
            (err as Error).message !== "Connection is closed."
          ) {
            this.emit("error", err as Error);
            // Wait before retrying after an error
            await delay(5000);
          }
        }
      } else {
        // At concurrency limit, wait a bit
        await delay(200); // Small delay to yield
      }
    }
    this.running = false;
    // Ensure cleanup if loop exits unexpectedly
    await this.waitForJobsToComplete();
  }

  private async acquireNextJob(): Promise<Job<TData, TResult, TName> | null> {
    // TODO: Incorporate blocking B(L|Z)MOVE or BRPOPLPUSH equivalent via Lua
    // This simple version uses polling for delayed jobs and non-blocking moves.
    // A production version NEEDS blocking for efficiency.

    // 1. Check delayed jobs
    const now = Date.now();
    const movedFromDelayed = await this.scripts.moveDelayedToWait(
      this.keys,
      now,
    );
    if (movedFromDelayed > 0) {
      this.emit("movedDelayed", movedFromDelayed); // Maybe a local event?
    }

    // 2. Attempt to move from wait to active (needs Lua script)
    const lockToken = randomUUID();
    const lockDuration = this.opts.lockDuration!;
    const result = await this.scripts.moveToActive(
      this.keys,
      lockToken,
      lockDuration,
    );

    if (result) {
      const [jobId, rawJobData] = result;
      if (jobId && rawJobData) {
        // Got a job!
        // Construct Job from raw data (needs parsing logic in Job class)
        const job = Job.fromRedisHash<TData, TResult, TName>(
          // Need a Queue-like object or pass necessary info
          { client: this.client, keys: this.keys, name: this.name } as any,
          rawJobData,
        );
        job.lockToken = lockToken;
        job.lockedUntil = Date.now() + lockDuration;

        this.jobsInFlight.set(job.id, job);
        this.startLockRenewal(job);
        this.emit("active", job);
        return job;
      }
    }

    // No job found in wait
    return null;
  }

  private async processJob(job: Job<TData, TResult, TName>): Promise<void> {
    try {
      const result = await this.processor(job);
      await this.scripts.moveToCompleted(
        this.keys,
        job,
        result,
        this.opts.removeOnComplete ?? false,
      );
      this.emit("completed", job, result);
      this.cleanupJob(job.id);
    } catch (err) {
      this.handleJobError(job, err as Error);
    }
  }

  private async handleJobError(
    job: Job<TData, TResult, TName>,
    err: Error,
  ): Promise<void> {
    job.attemptsMade += 1;
    const opts = job.opts;
    const maxAttempts = opts.attempts ?? 1;

    console.error(`Job ${job.id} failed with error: ${err.message}`);
    this.emit("failed", job, err);

    if (job.attemptsMade < maxAttempts) {
      // Calculate backoff
      let backoffDelay = 0;
      if (opts.backoff) {
        if (typeof opts.backoff === "number") {
          backoffDelay = opts.backoff;
        } else if (opts.backoff.type === "fixed") {
          backoffDelay = opts.backoff.delay;
        } else if (opts.backoff.type === "exponential") {
          backoffDelay = Math.round(
            opts.backoff.delay * Math.pow(2, job.attemptsMade - 1),
          );
        }
      }
      // Retry: Move to delayed
      await this.scripts.retryJob(this.keys, job, backoffDelay, err);
      this.emit("retrying", job, err);
    } else {
      // Final failure: Move to failed
      await this.scripts.moveToFailed(
        this.keys,
        job,
        err,
        this.opts.removeOnFail ?? false,
      );
    }
    this.cleanupJob(job.id, err); // Pass error for context
  }

  private startLockRenewal(job: Job<TData, TResult, TName>): void {
    const renewTime = this.opts.lockRenewTime ?? this.opts.lockDuration! / 2;
    const timer = setTimeout(async () => {
      if (this.jobsInFlight.has(job.id) && !this.closing && !this.paused) {
        try {
          const newLockedUntil = await this.scripts.extendLock(
            this.keys,
            job.id,
            job.lockToken!,
            this.opts.lockDuration!,
          );
          if (newLockedUntil > 0) {
            job.lockedUntil = newLockedUntil;
            this.lockRenewTimers.delete(job.id); // Clear old timer before setting new one
            this.startLockRenewal(job); // Reschedule renewal
          } else {
            // Lock lost or job gone
            console.warn(
              `Failed to renew lock for job ${job.id}. It might have been stalled.`,
            );
            this.cleanupJob(job.id); // Stop processing this job
          }
        } catch (err) {
          console.error(`Error renewing lock for job ${job.id}:`, err);
          // If renewal fails, we might lose the lock, stop processing
          this.cleanupJob(job.id);
        }
      }
    }, renewTime);
    this.lockRenewTimers.set(job.id, timer);
  }

  private cleanupJob(jobId: string, error?: Error): void {
    const timer = this.lockRenewTimers.get(jobId);
    if (timer) {
      clearTimeout(timer);
      this.lockRenewTimers.delete(jobId);
    }
    this.jobsInFlight.delete(jobId);
    // Potentially emit a specific 'jobCleaned' or 'jobFinishedProcessing' event
  }

  private async waitForJobsToComplete(): Promise<void> {
    const finishingPromises: Promise<any>[] = [];
    // NOTE: This is a simplified wait. In reality, you might need
    // a more robust mechanism, perhaps tracking promises directly.
    // This assumes that when `processJob` completes (or errors),
    // the job is eventually removed from jobsInFlight.
    while (this.jobsInFlight.size > 0) {
      console.log(`Waiting for ${this.jobsInFlight.size} jobs to complete...`);
      await delay(500); // Check periodically
    }
  }

  async close(force = false): Promise<void> {
    if (!this.closing) {
      this.closing = (async () => {
        this.emit("closing");
        this.running = false; // Signal loops to stop

        // Wait for the main loop to potentially finish its current iteration
        if (this.mainLoopPromise) {
          try {
            await this.mainLoopPromise;
          } catch (e) {
            /* Ignore errors during close */
          }
        }

        if (!force) {
          await this.waitForJobsToComplete();
        }

        // Clear all pending lock renewals
        this.lockRenewTimers.forEach((timer) => clearTimeout(timer));
        this.lockRenewTimers.clear();

        try {
          await Promise.all([
            this.client.quit(),
            this.bClient.quit(), // Close blocking client too
          ]);
        } catch (err) {
          if ((err as Error).message !== "Connection is closed.") {
            console.error("Error during Redis quit:", err);
            // Force disconnect if quit fails
            this.client.disconnect();
            this.bClient.disconnect();
          }
        }
        this.emit("closed");
      })();
    }
    return this.closing;
  }
}
