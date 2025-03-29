import { EventEmitter } from "node:events";
import type {
  JobData,
  JobOptions,
  QueueOptions,
  RedisClient,
} from "../interfaces";
import { createRedisClient, getQueueKeys, type QueueKeys } from "../utils";
import { LuaScripts } from "../scripts/lua";
import { randomUUID } from "node:crypto";
import { Job } from "./job";
import { Pipeline } from "ioredis";
import { Buffer } from "node:buffer";

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

  constructor(name: string, opts: QueueOptions) {
    super();
    this.name = name;
    this.opts = opts;
    this.prefix = opts.prefix ?? "smq";
    this.client = createRedisClient(opts.connection);
    this.keys = getQueueKeys(this.prefix, this.name);
    this.scripts = new LuaScripts(this.client);

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
        try {
          await this.client.quit();
        } catch (err) {
          if ((err as Error).message !== "Connection is closed.") {
            console.error("Error during Redis quit:", err);
            this.client.disconnect();
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
}
