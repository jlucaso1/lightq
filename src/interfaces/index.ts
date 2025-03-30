import { Cluster, Redis, type RedisOptions } from "ioredis";

export type RedisClient = Redis | Cluster;

export interface QueueOptions {
  connection: RedisOptions | RedisClient;
  prefix?: string;
  defaultJobOptions?: JobOptions;
}

export interface WorkerOptions extends QueueOptions {
  concurrency?: number;
  lockDuration?: number;
  lockRenewTime?: number;
  removeOnComplete?: boolean | number;
  removeOnFail?: boolean | number;
}

export interface JobOptions {
  jobId?: string;
  delay?: number;
  attempts?: number;
  backoff?: number | { type: "fixed" | "exponential"; delay: number };
  removeOnComplete?: boolean | number;
  removeOnFail?: boolean | number;
}

export interface RedisJobOptions extends JobOptions {}

export interface JobData<T = any> {
  id: string;
  name: string;
  data: T;
  opts: RedisJobOptions;
  timestamp: number;
  delay: number;
  attemptsMade: number;
  // State tracking fields managed internally
  processedOn?: number;
  finishedOn?: number;
  returnValue?: any;
  failedReason?: string;
  stacktrace?: string[];
  // Lock info
  lockedUntil?: number;
  lockToken?: string;
}

export type SchedulerRepeatOptions =
  | {
    /** Cron pattern (e.g., '0 * * * *') */
    pattern: string;
    /** Optional timezone for cron pattern */
    tz?: string;
    /** Not used with pattern */
    every?: never;
  }
  | {
    /** Repeat interval in milliseconds */
    every: number;
    /** Not used with every */
    pattern?: never;
    tz?: never;
  };

/** Template for jobs created by a scheduler */
export interface JobTemplate<TData = any, TName extends string = string> {
  name: TName;
  data?: TData;
  opts?: Omit<JobOptions, "jobId" | "delay">; // Cannot set jobId or delay on scheduled jobs
}

/** Internal representation of scheduler data stored in Redis */
export interface SchedulerData extends JobTemplate {
  id: string;
  type: "cron" | "every";
  value: string | number; // Cron pattern or 'every' ms
  tz?: string;
  nextRun: number; // Timestamp (ms)
  lastRun?: number; // Timestamp (ms)
  // lockUntil?: number; // Potential future addition for distributed locking
}

/** Options for the JobScheduler instance */
export interface JobSchedulerOptions extends QueueOptions {
  // Reuse QueueOptions for connection
  /** How often the scheduler checks for due jobs (milliseconds) */
  checkInterval?: number;
  /** Prefix for scheduler-specific keys */
  schedulerPrefix?: string;
}
