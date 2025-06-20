import type { Cluster, Redis, RedisOptions } from "ioredis";

export type RedisClient = Redis | Cluster;

export interface BaseServiceOptions {
	/** Redis connection options or an existing ioredis instance */
	connection: RedisOptions | RedisClient;
	/** Prefix for Redis keys (default: 'lightq') */
	prefix?: string;
}

export interface QueueOptions extends BaseServiceOptions {
	defaultJobOptions?: JobOptions;
}

export interface WorkerOptions extends BaseServiceOptions {
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

export interface JobData<T = unknown> {
	id: string;
	name: string;
	data: T;
	opts: RedisJobOptions;
	timestamp: number;
	delay: number;
	attemptsMade: number;
	processedOn?: number;
	finishedOn?: number;
	returnValue?: unknown;
	failedReason?: string;
	stacktrace?: string[];
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
export interface JobTemplate<TData = unknown, TName extends string = string> {
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
	nextRun: number;
	lastRun?: number;
	failureCount?: number; // Track consecutive failures for poison pill mechanism
}

/** Options for the JobScheduler instance */
export interface JobSchedulerOptions extends BaseServiceOptions {
	/** How often the scheduler checks for due jobs (milliseconds) */
	checkInterval?: number;
	/** Prefix for scheduler-specific keys */
	schedulerPrefix?: string;
	defaultJobOptions?: JobOptions;
	/** Duration in seconds for distributed locking when processing schedulers (default: 10) */
	lockDuration?: number;
	/** Maximum consecutive failures before disabling a scheduler (default: 5) */
	maxFailureCount?: number;
}

// Export job context interfaces
export * from "./job-context";
