import { EventEmitter } from "node:events";
import type {
  JobData,
  JobOptions,
  JobSchedulerOptions,
  JobTemplate,
  QueueOptions,
  RedisClient,
  SchedulerRepeatOptions,
} from "../interfaces";
import { createRedisClient, getQueueKeys, type QueueKeys } from "../utils";
import { LuaScripts } from "../scripts/lua";
import { randomUUID } from "node:crypto";
import { Job } from "./job";
import { Pipeline } from "ioredis";
import { Buffer } from "node:buffer";
import { JobScheduler } from "./scheduler";

export class Queue<
  TData = any,
  TResult = any,
  TName extends string = string,
> extends EventEmitter {
  readonly name: string;
  readonly client: RedisClient;
  readonly opts: QueueOptions;
  readonly keys: QueueKeys;
  private scripts: LuaScripts;
  private closing: Promise<void> | null = null;
  private prefix: string;
  private scheduler: JobScheduler | null = null;
  private schedulerOpts: JobSchedulerOptions | null = null;

  constructor(name: string, opts: QueueOptions) {
    super();
    this.name = name;
    this.opts = opts;
    this.prefix = opts.prefix ?? "lightq";
    this.client = createRedisClient(opts.connection);
    this.keys = getQueueKeys(this.prefix, this.name);
    this.scripts = new LuaScripts(this.client);

    this.schedulerOpts = {
      connection: this.client, // Share connection by default
      prefix: this.prefix,
      defaultJobOptions: this.opts.defaultJobOptions,
      // Make scheduler prefix configurable if needed, defaults to queue prefix
      schedulerPrefix: opts.prefix ?? "lightq",
      // Allow overriding checkInterval via QueueOptions potentially?
      // checkInterval: (opts as any).schedulerCheckInterval ?? 5000,
    };

    this.client.on("error", (err) => this.emit("error", err));
    this.client.on("ready", () => this.emit("ready"));
  }

  async add(
    name: TName,
    data: TData,
    opts?: JobOptions,
  ): Promise<Job<TData, TResult, TName>> {
    if (this.closing) {
      throw new Error("Queue is closing");
    }

    const mergedOpts: JobOptions = {
      attempts: 1,
      delay: 0,
      ...this.opts.defaultJobOptions,
      ...opts,
    };

    if (mergedOpts?.jobId?.startsWith("scheduler:")) {
      console.warn(
        `Attempting to manually add a job with a reserved scheduler ID prefix: ${mergedOpts.jobId}. Schedulers manage their own job IDs.`,
      );
      // Decide whether to throw an error or just remove the conflicting ID
      // mergedOpts.jobId = undefined; // Option: Silently fix
      throw new Error(
        `Cannot manually add job with reserved scheduler ID: ${mergedOpts.jobId}`,
      );
    }

    const jobId = mergedOpts.jobId ?? randomUUID();
    const timestamp = Date.now();

    const jobData: JobData<TData> = {
      id: jobId,
      name,
      data,
      opts: mergedOpts,
      timestamp,
      delay: mergedOpts.delay ?? 0,
      attemptsMade: 0,
    };

    await this.scripts.addJob(this.keys, jobData);

    const job = Job.fromData<TData, TResult, TName>(this, jobData);
    this.emit("waiting", job);
    return job;
  }

  async addBulk(
    jobs: { name: TName; data: TData; opts?: JobOptions }[],
  ): Promise<Job<TData, TResult, TName>[]> {
    if (this.closing) {
      throw new Error("Queue is closing");
    }

    const jobInstances: Job<TData, TResult, TName>[] = [];
    const pipeline = this.client.pipeline();
    if (!(pipeline instanceof Pipeline)) {
      throw new Error("Pipeline is not an instance of Pipeline");
    }
    const timestamp = Date.now();

    for (const jobDef of jobs) {
      const mergedOpts: JobOptions = {
        attempts: 1,
        delay: 0,
        ...this.opts.defaultJobOptions,
        ...jobDef.opts,
      };
      if (mergedOpts?.jobId?.startsWith("scheduler:")) {
        console.warn(
          `Attempting to bulk add a job with a reserved scheduler ID prefix: ${mergedOpts.jobId}. Skipping this specific job.`,
        );
        continue;
      }
      const jobId = mergedOpts.jobId ?? randomUUID();

      const jobData: JobData<TData> = {
        id: jobId,
        name: jobDef.name,
        data: jobDef.data,
        opts: mergedOpts,
        timestamp,
        delay: mergedOpts.delay ?? 0,
        attemptsMade: 0,
      };

      this.scripts.addJob(this.keys, jobData, pipeline);
      jobInstances.push(Job.fromData<TData, TResult, TName>(this, jobData));
    }

    await pipeline.exec();

    jobInstances.forEach((job) => this.emit("waiting", job));
    return jobInstances;
  }

  async getJob(jobId: string): Promise<Job<TData, TResult, TName> | null> {
    const jobKey = `${this.keys.jobs}:${jobId}`;
    const data = await this.client.hgetall(jobKey);
    if (Object.keys(data).length === 0) {
      return null;
    }
    return Job.fromRedisHash<TData, TResult, TName>(this, data);
  }

  async getJobCounts(): Promise<{
    wait: number;
    active: number;
    delayed: number;
    completed: number;
    failed: number;
  }> {
    const keys = this.keys;
    const multi = this.client.multi();
    multi.llen(keys.wait);
    multi.llen(keys.active);
    multi.zcard(keys.delayed);
    multi.zcard(keys.completed);
    multi.zcard(keys.failed);

    const results = await multi.exec();
    const counts = results?.map((res) => res[1] as number) ?? [0, 0, 0, 0, 0];

    return {
      wait: counts[0]!,
      active: counts[1]!,
      delayed: counts[2]!,
      completed: counts[3]!,
      failed: counts[4]!,
    };
  }

  async close(): Promise<void> {
    if (!this.closing) {
      this.closing = (async () => {
        // --- Close scheduler first ---
        if (this.scheduler) {
          await this.scheduler.close();
        }
        // --- End Add ---
        try {
          // Now close the queue's client (potentially shared)
          // Check if client is still connected before quitting
          if (
            this.client &&
            ["connect", "ready"].includes(this.client.status)
          ) {
            await this.client.quit();
          } else {
            // If not connected or already closing/closed, just disconnect locally
            this.client?.disconnect();
          }
        } catch (err) {
          if ((err as Error).message !== "Connection is closed.") {
            console.error("Error during Redis quit:", err);
            this.client.disconnect(); // Force disconnect on error
          }
        }
      })();
    }
    return this.closing;
  }

  async executeScript(
    scriptName: string,
    keys: string[],
    args: (string | number | Buffer)[],
    pipeline?: Pipeline,
  ): Promise<any> {
    const command = pipeline || this.client;

    return (command as any)[scriptName]?.(...keys, ...args);
  }

  private getScheduler(): JobScheduler {
    if (!this.scheduler) {
      if (!this.schedulerOpts) {
        // This case shouldn't happen with the constructor logic
        throw new Error("Scheduler options not initialized.");
      }
      this.scheduler = new JobScheduler(this, this.schedulerOpts);
      // Forward scheduler events if needed
      this.scheduler.on("error", (err) => this.emit("scheduler_error", err));
      this.scheduler.on(
        "job_added",
        (schedulerId, job) =>
          this.emit("scheduler_job_added", schedulerId, job),
      );
      // ... forward other events like upsert, remove, start, stop ...
      this.scheduler.start(); // Start the scheduler when first accessed
    }
    return this.scheduler;
  }

  /**
   * Creates or updates a job scheduler associated with this queue.
   * Starts the scheduler polling process if it wasn't running.
   * @param schedulerId A unique identifier for this scheduler.
   * @param repeat The repeat options (cron pattern or interval).
   * @param template The template for jobs to be generated.
   */
  async upsertJobScheduler<TJobData = any, TJobName extends string = string>(
    schedulerId: string,
    repeat: SchedulerRepeatOptions,
    template: JobTemplate<TJobData, TJobName>,
  ): Promise<void> {
    return this.getScheduler().upsertJobScheduler(
      schedulerId,
      repeat,
      template,
    );
  }

  /**
   * Removes a job scheduler associated with this queue.
   * @param schedulerId The ID of the scheduler to remove.
   */
  async removeJobScheduler(schedulerId: string): Promise<boolean> {
    // Only try to remove if the scheduler has potentially been initialized
    if (this.scheduler) {
      return this.scheduler.removeJobScheduler(schedulerId);
    }
    // If scheduler never started, try removing directly via a temporary instance or static method (less ideal)
    // For simplicity, assume remove is called after scheduler might have been used.
    // Or, instantiate it just to remove (might start polling unnecessarily):
    // return this.getScheduler().removeJobScheduler(schedulerId);
    // Best: If scheduler is not active, deletion via direct redis command is okay if needed
    console.warn(
      "Attempted to remove scheduler before scheduler process was started for this queue.",
    );
    return false; // Indicate not removed as scheduler wasn't active
  }
}
