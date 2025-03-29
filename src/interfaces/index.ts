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
