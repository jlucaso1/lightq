import { Job } from "./job";
import type { RedisClient, WorkerOptions } from "../interfaces";
import { delay, getQueueKeys, type QueueKeys } from "../utils";
import { LuaScripts } from "../scripts/lua";
import { randomUUID } from "node:crypto";
import { RedisService } from "./base-service";

export type Processor<
  TData = any,
  TResult = any,
  TName extends string = string,
> = (job: Job<TData, TResult, TName>) => Promise<TResult>;

export class Worker<
  TData = any,
  TResult = any,
  TName extends string = string,
> extends RedisService<WorkerOptions> {
  readonly name: string;
  readonly bClient: RedisClient; // Blocking client
  readonly keys: QueueKeys;
  private scripts: LuaScripts;
  private processor: Processor<TData, TResult, TName>;

  private running = false;
  private paused = false;
  private jobsInFlight = new Map<string, Job<TData, TResult, TName>>(); // Track active jobs
  private lockRenewTimers = new Map<string, NodeJS.Timeout>();
  private mainLoopPromise: Promise<void> | null = null;
  private workerId = randomUUID(); // Unique ID for this worker instance
  private activeJobPromises = new Set<Promise<any>>();

  constructor(
    name: string,
    processor: Processor<TData, TResult, TName>,
    opts: WorkerOptions,
  ) {
    super(opts);
    this.name = name;
    this.processor = processor;
    this.keys = getQueueKeys(this.prefix, this.name);

    this.bClient = this.client.duplicate();
    this.bClient.on("error", (err) => this.emit("error", err)); // Listen to blocking client errors too

    this.scripts = new LuaScripts(this.client);

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
            const processingPromise = this.processJob(job)
              .catch((err) => {
                console.error(`Unhandled error processing job ${job.id}:`, err);
              })
              .finally(() => {
                this.activeJobPromises.delete(processingPromise);
              });

            this.activeJobPromises.add(processingPromise);
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
            if (!this.closing) await delay(5000);
          }
        }
      } else {
        // At concurrency limit, wait a bit
        if (!this.closing) await delay(200);
      }
    }
    this.running = false;
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

      const removeOpt = job.opts.removeOnComplete ??
        this.opts.removeOnComplete ??
        (this as any)._queue?.opts?.defaultJobOptions?.removeOnComplete ??
        false;

      const moveResult = await this.scripts.moveToCompleted(
        this.keys,
        job,
        result,
        removeOpt ?? false,
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
        const removeOnFail = job.opts.removeOnFail ??
          this.opts.removeOnFail ??
          (this as any)._queue?.opts?.defaultJobOptions?.removeOnFail ??
          false;
        // Final failure: Move to failed
        const failedResult = await this.scripts.moveToFailed(
          this.keys,
          job,
          err,
          removeOnFail ?? false,
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

  protected async _internalClose(force = false): Promise<void> {
    this.running = false; // Signal loops to stop

    // Stop accepting new jobs immediately by setting paused?
    this.paused = true;

    // Wait for the main loop promise to potentially finish its current cycle
    if (this.mainLoopPromise) {
      try {
        await Promise.race([this.mainLoopPromise, delay(force ? 50 : 500)]);
      } catch (e) {
        /* Ignore errors during close */
      }
    }
    this.mainLoopPromise = null;

    if (!force) {
      const promisesToWaitFor = [...this.activeJobPromises];
      if (promisesToWaitFor.length > 0) {
        console.log(
          `Waiting for ${promisesToWaitFor.length} active job promises to settle...`,
        );
        await Promise.allSettled(promisesToWaitFor);
        console.log("Active job promises settled.");
      }
    } else {
      console.log(
        `Force closing worker: Skipping wait for ${this.jobsInFlight.size} jobs.`,
      );

      this.activeJobPromises.clear();
    }

    // Clear timers and tracking maps
    this.lockRenewTimers.forEach((timer) => clearTimeout(timer));
    this.lockRenewTimers.clear();
    this.jobsInFlight.clear();
    this.activeJobPromises.clear();

    // Close the blocking client connection
    try {
      if (force) {
        this.bClient.disconnect();
      } else if (["connect", "ready"].includes(this.bClient.status)) {
        await this.bClient.quit();
      } else {
        this.bClient.disconnect();
      }
    } catch (err) {
      if (
        (err as Error).message &&
        !(err as Error).message.includes("Connection is closed")
      ) {
        console.error("Error closing blocking Redis connection:", err);
        this.bClient.disconnect(); // Force disconnect on error
      }
    }
  }
}
