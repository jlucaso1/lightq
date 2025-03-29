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

      // *** FIX START: Check moveToCompleted result ***
      // Check if the job is still considered active by this worker before moving
      // This helps prevent attempting to complete a job whose lock was lost (and cleaned up)
      if (!this.jobsInFlight.has(job.id)) {
        console.warn(
          `Job ${job.id} processing finished, but it was no longer tracked as active (lock likely lost). Skipping move to completed.`,
        );
        return; // Exit early, cleanup already happened or will happen
      }

      const moveResult = await this.scripts.moveToCompleted(
        this.keys,
        job,
        result,
        this.opts.removeOnComplete ?? false,
      );
      if (moveResult === 0) {
        // Successfully moved
        this.emit("completed", job, result);
        this.cleanupJob(job.id); // Cleanup only after successful move
      } else {
        // Failed to move (e.g., lock mismatch - code -1, or not in active list - code -2)
        console.error(
          `Failed to move job ${job.id} to completed state (Redis script code: ${moveResult}). Lock may have been lost or job state inconsistent.`,
        );
        // Even if move fails, we need to clean up the job locally
        this.cleanupJob(job.id);
        // Optionally emit a different event like 'error' or 'stalled' here?
        this.emit(
          "error",
          new Error(
            `Failed to move job ${job.id} to completed (Script Code: ${moveResult})`,
          ),
          job,
        );
      }
    } catch (err) {
      // Check if job is still valid before handling error
      if (!this.jobsInFlight.has(job.id)) {
        console.warn(
          `Job ${job.id} processing failed, but it was no longer tracked as active (lock likely lost). Skipping move to failed/retry.`,
        );
        return; // Exit early
      }
      this.handleJobError(job, err as Error); // handleJobError will call cleanupJob
    }
  }

  private async handleJobError(
    job: Job<TData, TResult, TName>,
    err: Error,
  ): Promise<void> {
    job.attemptsMade += 1;
    const opts = job.opts;
    const maxAttempts = opts.attempts ?? 1;

    // Emit failed *before* attempting state change, so listeners know about the processor error

    this.emit("failed", job, err);

    let moveSuccessful = false; // Track if Redis move succeeds

    try {
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
        const retryResult = await this.scripts.retryJob(
          this.keys,
          job,
          backoffDelay,
          err,
        );
        if (retryResult === 0) {
          this.emit("retrying", job, err);
          moveSuccessful = true;
        } else {
          console.error(
            `Failed to move job ${job.id} for retry (Redis script code: ${retryResult}).`,
          );
          // Don't emit retrying if move failed
        }
      } else {
        // Final failure: Move to failed
        const failedResult = await this.scripts.moveToFailed(
          this.keys,
          job,
          err,
          this.opts.removeOnFail ?? false,
        );
        if (failedResult === 0) {
          // Optionally emit a 'finallyFailed' event here if needed?
          moveSuccessful = true;
        } else {
          console.error(
            `Failed to move job ${job.id} to final failed state (Redis script code: ${failedResult}).`,
          );
        }
      }
    } catch (redisError) {
      console.error(
        `Redis error during job failure handling for job ${job.id}:`,
        redisError,
      );
      // Emit worker error?
      this.emit("error", redisError as Error, job);
    } finally {
      this.cleanupJob(job.id, err); // Pass error for context
    }
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
            // Give the main loop a brief moment to exit if it's in a delay/wait
            await Promise.race([this.mainLoopPromise, delay(force ? 50 : 500)]);
          } catch (e) {
            /* Ignore errors during close */
            console.warn("Error ignored during main loop shutdown:", e);
          }
        }
        this.mainLoopPromise = null; // Ensure we don't await it again

        if (!force) {
          await this.waitForJobsToComplete();
        } else {
          console.log(
            `Force closing: Skipping wait for ${this.jobsInFlight.size} jobs.`,
          );
          // Optionally try to release locks for jobsInFlight? Risky during force close.
        }

        // Clear all pending lock renewals
        this.lockRenewTimers.forEach((timer) => clearTimeout(timer));
        this.lockRenewTimers.clear();

        try {
          // *** FIX START: Use disconnect() on force close ***
          if (force) {
            console.log("Force closing Redis connections using disconnect().");
            // Disconnect immediately without waiting for server ACK
            this.client.disconnect();
            this.bClient.disconnect();
          } else {
            // Attempt graceful shutdown
            await Promise.all([
              this.client.quit(),
              this.bClient.quit(), // Close blocking client too
            ]);
          }
          // *** FIX END ***
        } catch (err) {
          // Ignore "Connection is closed" error which is expected if already closed/disconnected
          if (
            (err as Error).message &&
            !(err as Error).message.includes("Connection is closed")
          ) {
            console.error("Error during Redis connection close:", err);
            // Force disconnect if quit/disconnect failed unexpectedly
            try {
              this.client.disconnect();
            } catch {
              /* ignore */
            }
            try {
              this.bClient.disconnect();
            } catch {
              /* ignore */
            }
          }
        }
        this.jobsInFlight.clear(); // Ensure jobsInFlight is cleared on close
        this.emit("closed");
      })();
    }
    return this.closing;
  }
}
