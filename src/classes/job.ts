import type { JobData, JobOptions } from "../interfaces";
import type { QueueKeys } from "../utils";
import type { Queue } from "./queue";

export class Job<TData = any, TResult = any, TName extends string = string> {
  id: string;
  name: TName;
  data: TData;
  opts: JobOptions;
  timestamp: number;
  delay: number;
  attemptsMade: number;
  processedOn?: number;
  finishedOn?: number;
  returnValue?: TResult;
  failedReason?: string;
  stacktrace?: string[];
  lockedUntil?: number;
  lockToken?: string;

  private queue: Queue;
  private queueKeys: QueueKeys;

  constructor(queue: Queue, jobData: JobData<TData>) {
    this.queue = queue;
    this.queueKeys = queue.keys;

    this.id = jobData.id;
    this.name = jobData.name as TName;
    this.data = jobData.data;
    this.opts = jobData.opts;
    this.timestamp = jobData.timestamp;
    this.delay = jobData.delay;
    this.attemptsMade = jobData.attemptsMade;
    this.processedOn = jobData.processedOn;
    this.finishedOn = jobData.finishedOn;
    this.returnValue = jobData.returnValue;
    this.failedReason = jobData.failedReason;
    this.stacktrace = jobData.stacktrace;
    this.lockedUntil = jobData.lockedUntil;
    this.lockToken = jobData.lockToken;
  }

  async updateProgress(progress: number | object): Promise<void> {
    // NOTE: Progress updates are complex with atomicity.
    // For simplicity, maybe store progress directly in the job hash?
    // This isn't ideal for real-time updates but simplifies the core.
    // Or implement a specific script like BullMQ.
    console.warn("updateProgress not fully implemented in this simple version");
    const jobKey = `${this.queueKeys.jobs}:${this.id}`;
    const currentData = (await this.queue.client.hgetall(jobKey)) as any; // TODO: Improve typing
    if (currentData) {
      currentData.progress = JSON.stringify(progress);
      await this.queue.client.hset(
        jobKey,
        "progress",
        JSON.stringify(progress),
      );
      // Maybe emit local event?
    } else {
      throw new Error(`Job ${this.id} not found for progress update.`);
    }
  }

  toData(): JobData<TData> {
    return {
      id: this.id,
      name: this.name,
      data: this.data,
      opts: this.opts,
      timestamp: this.timestamp,
      delay: this.delay,
      attemptsMade: this.attemptsMade,
      processedOn: this.processedOn,
      finishedOn: this.finishedOn,
      returnValue: this.returnValue,
      failedReason: this.failedReason,
      stacktrace: this.stacktrace,
      lockedUntil: this.lockedUntil,
      lockToken: this.lockToken,
    };
  }

  static fromData<TData = any, TResult = any, TName extends string = string>(
    queue: Queue,
    jobData: JobData<TData>,
  ): Job<TData, TResult, TName> {
    return new Job<TData, TResult, TName>(queue, jobData);
  }

  static fromRedisHash<
    TData = any,
    TResult = any,
    TName extends string = string,
  >(
    queue: Queue,
    hashData: Record<string, string>,
  ): Job<TData, TResult, TName> {
    const jobData: Partial<JobData<TData>> = {};
    jobData.id = hashData.id;
    jobData.name = hashData.name;
    jobData.data = JSON.parse(hashData.data || "{}");
    jobData.opts = JSON.parse(hashData.opts || "{}");
    jobData.timestamp = parseInt(hashData.timestamp || "0", 10);
    jobData.delay = parseInt(hashData.delay || "0", 10);
    jobData.attemptsMade = parseInt(hashData.attemptsMade || "0", 10);
    jobData.processedOn = hashData.processedOn
      ? parseInt(hashData.processedOn, 10)
      : undefined;
    jobData.finishedOn = hashData.finishedOn
      ? parseInt(hashData.finishedOn, 10)
      : undefined;
    jobData.returnValue = hashData.returnValue
      ? JSON.parse(hashData.returnValue)
      : undefined;
    jobData.failedReason = hashData.failedReason;
    jobData.stacktrace = hashData.stacktrace
      ? JSON.parse(hashData.stacktrace)
      : [];
    jobData.lockedUntil = hashData.lockedUntil
      ? parseInt(hashData.lockedUntil, 10)
      : undefined;
    jobData.lockToken = hashData.lockToken;

    // TODO: Add progress parsing if implemented

    return new Job<TData, TResult, TName>(queue, jobData as JobData<TData>);
  }
}
