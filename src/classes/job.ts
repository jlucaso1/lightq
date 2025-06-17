import type { JobData, JobOptions, ProgressUpdater } from "../interfaces";

export class Job<
  TData = unknown,
  TResult = unknown,
  TName extends string = string
> {
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

  private progressUpdater?: ProgressUpdater;

  constructor(jobData: JobData<TData>, progressUpdater?: ProgressUpdater) {
    this.id = jobData.id;
    this.name = jobData.name as TName;
    this.data = jobData.data;
    this.opts = jobData.opts;
    this.timestamp = jobData.timestamp;
    this.delay = jobData.delay;
    this.attemptsMade = jobData.attemptsMade;
    this.processedOn = jobData.processedOn;
    this.finishedOn = jobData.finishedOn;
    this.returnValue = jobData.returnValue as TResult | undefined;
    this.failedReason = jobData.failedReason;
    this.stacktrace = jobData.stacktrace;
    this.lockedUntil = jobData.lockedUntil;
    this.lockToken = jobData.lockToken;
    this.progressUpdater = progressUpdater;
  }

  async updateProgress(progress: number | object): Promise<void> {
    if (!this.progressUpdater) {
      throw new Error(
        "Progress updater not available. Job was created without progress update capability."
      );
    }
    await this.progressUpdater.updateProgress(this.id, progress);
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

  static fromData<
    TData = unknown,
    TResult = unknown,
    TName extends string = string
  >(
    jobData: JobData<TData>,
    progressUpdater?: ProgressUpdater
  ): Job<TData, TResult, TName> {
    return new Job<TData, TResult, TName>(jobData, progressUpdater);
  }

  static fromRedisHash<
    TData = unknown,
    TResult = unknown,
    TName extends string = string
  >(
    hashData: Record<string, string>,
    progressUpdater?: ProgressUpdater
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

    return new Job<TData, TResult, TName>(
      jobData as JobData<TData>,
      progressUpdater
    );
  }
}
