import { Job } from "./job";
import type {
  RedisClient,
  WorkerOptions,
  JobContext,
  ProgressUpdater,
} from "../interfaces";
import { delay, getQueueKeys, type QueueKeys } from "../utils";
import { LuaScripts } from "../scripts/lua";
import { randomUUID } from "node:crypto";
import { RedisService } from "./base-service";
import { JobProgressUpdater } from "./progress-updater";

export type Processor<
  TData = unknown,
  TResult = unknown,
  TName extends string = string
> = (job: Job<TData, TResult, TName>) => Promise<TResult>;

export class Worker<
  TData = unknown,
  TResult = unknown,
  TName extends string = string
> extends RedisService<WorkerOptions> {
  readonly name: string;
  readonly bClient: RedisClient;
  readonly keys: QueueKeys;
  private scripts: LuaScripts;
  private processor: Processor<TData, TResult, TName>;
  private progressUpdater: ProgressUpdater;

  private running = false;
  private paused = false;
  private jobsInFlight = new Map<string, Job<TData, TResult, TName>>();
  private lockRenewTimers = new Map<string, NodeJS.Timeout>();
  private mainLoopPromise: Promise<void> | null = null;
  private activeJobPromises = new Set<Promise<void>>();

  constructor(
    name: string,
    processor: Processor<TData, TResult, TName>,
    opts: WorkerOptions
  ) {
    super(opts);
    this.name = name;
    this.processor = processor;
    this.keys = getQueueKeys(this.prefix, this.name);

    const jobContext: JobContext = {
      client: this.client,
      keys: this.keys,
    };
    this.progressUpdater = new JobProgressUpdater(jobContext);

    this.bClient = this.client.duplicate();
    this.bClient.on("error", (err) => this.emit("error", err));

    this.scripts = new LuaScripts(this.client);

    this.run();
  }

  private run(): void {
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
          const job = await this.acquireNextJobBlocking();
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
            await delay(100);
          }
        } catch (err) {
          if (
            !this.closing &&
            (err as Error).message !== "Connection is closed."
          ) {
            this.emit("error", err as Error);
            if (!this.closing) await delay(5000);
          }
        }
      } else {
        if (!this.closing) await delay(200);
      }
    }
    this.running = false;
  }

  private async acquireNextJobBlocking(): Promise<Job<
    TData,
    TResult,
    TName
  > | null> {
    const blockTimeout = 5;
    try {
      const jobId = await this.bClient.brpoplpush(
        this.keys.wait,
        this.keys.active,
        blockTimeout
      );

      if (jobId) {
        const lockToken = randomUUID();
        const lockDuration = this.opts.lockDuration!;

        const result = await this.scripts.moveSpecificJobToActive(
          this.keys,
          jobId,
          lockToken,
          lockDuration
        );

        if (result) {
          const rawJobData = result;
          if (rawJobData) {
            const job = Job.fromRedisHash<TData, TResult, TName>(
              rawJobData,
              this.progressUpdater
            );
            job.lockToken = lockToken;
            job.lockedUntil = Date.now() + lockDuration;

            this.jobsInFlight.set(job.id, job);
            this.startLockRenewal(job);
            this.emit("active", job);
            return job;
          }
        } else {
          await this.client.lrem(this.keys.active, 1, jobId);
          console.warn(
            `Could not acquire lock for job ${jobId}; removed from active list to prevent stall.`
          );
          return null;
        }
      }

      return null;
    } catch (err: unknown) {
      const message = err instanceof Error ? err.message : String(err);
      if (this.closing || message?.includes("Connection is closed")) {
        return null;
      }
      throw err;
    }
  }

  private async processJob(job: Job<TData, TResult, TName>): Promise<void> {
    try {
      const result = await this.processor(job);

      if (!this.jobsInFlight.has(job.id)) {
        console.warn(
          `Job ${job.id} processing finished, but it was no longer tracked as active (lock likely lost). Skipping move to completed.`
        );
        return;
      }

      const removeOpt =
        job.opts.removeOnComplete ?? this.opts.removeOnComplete ?? false;

      const moveResult = await this.scripts.moveToCompleted(
        this.keys,
        job,
        result,
        removeOpt ?? false
      );
      if (moveResult === 0) {
        this.emit("completed", job, result);
        this.cleanupJob(job.id);
      } else {
        console.error(
          `Failed to move job ${job.id} to completed state (Redis script code: ${moveResult}). Lock may have been lost or job state inconsistent.`
        );
        this.cleanupJob(job.id);
        this.emit(
          "error",
          new Error(
            `Failed to move job ${job.id} to completed (Script Code: ${moveResult})`
          ),
          job
        );
      }
    } catch (err) {
      if (!this.jobsInFlight.has(job.id)) {
        console.warn(
          `Job ${job.id} processing failed, but it was no longer tracked as active (lock likely lost). Skipping move to failed/retry.`
        );
        return;
      }
      this.handleJobError(job, err as Error);
    }
  }

  private async handleJobError(
    job: Job<TData, TResult, TName>,
    err: Error
  ): Promise<void> {
    job.attemptsMade += 1;
    const opts = job.opts;
    const maxAttempts = opts.attempts ?? 1;

    this.emit("failed", job, err);

    try {
      if (job.attemptsMade < maxAttempts) {
        let backoffDelay = 0;
        if (opts.backoff) {
          if (typeof opts.backoff === "number") {
            backoffDelay = opts.backoff;
          } else if (opts.backoff.type === "fixed") {
            backoffDelay = opts.backoff.delay;
          } else if (opts.backoff.type === "exponential") {
            backoffDelay = Math.round(
              opts.backoff.delay * 2 ** (job.attemptsMade - 1)
            );
          }
        }
        const retryResult = await this.scripts.retryJob(
          this.keys,
          job,
          backoffDelay,
          err
        );
        if (retryResult === 0) {
          this.emit("retrying", job, err);
        } else {
          console.error(
            `Failed to move job ${job.id} for retry (Redis script code: ${retryResult}).`
          );
        }
      } else {
        const removeOnFail =
          job.opts.removeOnFail ?? this.opts.removeOnFail ?? false;
        const failedResult = await this.scripts.moveToFailed(
          this.keys,
          job,
          err,
          removeOnFail ?? false
        );
        if (failedResult === 0) {
        } else {
          console.error(
            `Failed to move job ${job.id} to final failed state (Redis script code: ${failedResult}).`
          );
        }
      }
    } catch (redisError) {
      console.error(
        `Redis error during job failure handling for job ${job.id}:`,
        redisError
      );
      this.emit("error", redisError as Error, job);
    } finally {
      this.cleanupJob(job.id, err);
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
            this.opts.lockDuration!
          );
          if (newLockedUntil > 0) {
            job.lockedUntil = newLockedUntil;
            this.lockRenewTimers.delete(job.id);
            this.startLockRenewal(job);
          } else {
            console.warn(
              `Failed to renew lock for job ${job.id}. It might have been stalled.`
            );
            this.cleanupJob(job.id);
          }
        } catch (err) {
          console.error(`Error renewing lock for job ${job.id}:`, err);
          this.cleanupJob(job.id);
        }
      }
    }, renewTime);
    this.lockRenewTimers.set(job.id, timer);
  }

  private cleanupJob(jobId: string, _error?: Error): void {
    const timer = this.lockRenewTimers.get(jobId);
    if (timer) {
      clearTimeout(timer);
      this.lockRenewTimers.delete(jobId);
    }
    this.jobsInFlight.delete(jobId);
  }

  protected async _internalClose(force = false): Promise<void> {
    this.running = false;

    this.paused = true;

    if (this.mainLoopPromise) {
      try {
        await Promise.race([this.mainLoopPromise, delay(force ? 50 : 500)]);
      } catch (_e) {}
    }
    this.mainLoopPromise = null;

    if (!force) {
      const promisesToWaitFor = [...this.activeJobPromises];
      if (promisesToWaitFor.length > 0) {
        console.log(
          `Waiting for ${promisesToWaitFor.length} active job promises to settle...`
        );
        await Promise.allSettled(promisesToWaitFor);
        console.log("Active job promises settled.");
      }
    } else {
      console.log(
        `Force closing worker: Skipping wait for ${this.jobsInFlight.size} jobs.`
      );

      this.activeJobPromises.clear();
    }

    this.lockRenewTimers.forEach((timer) => clearTimeout(timer));
    this.lockRenewTimers.clear();
    this.jobsInFlight.clear();
    this.activeJobPromises.clear();

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
        this.bClient.disconnect();
      }
    }
  }
}
