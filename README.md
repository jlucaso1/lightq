# LightQ ✨

[![codecov](https://codecov.io/gh/jlucaso1/lightq/graph/badge.svg)](https://codecov.io/gh/jlucaso1/lightq)
[![NPM Version](https://img.shields.io/npm/v/@jlucaso%2Flightq.svg)](https://www.npmjs.com/package/@jlucaso/lightq)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

A simple, lightweight, Redis-backed job queue library designed for TypeScript
applications. LightQ provides essential queueing features with a focus on
reliability and ease of use, leveraging atomic Redis operations via Lua scripts.

## Features

- ✅ **Simple API:** Easy-to-understand interface for adding and processing
  jobs.
- 🚀 **Redis Backend:** Uses Redis for persistence, ensuring jobs are not lost.
- 🔒 **Atomic Operations:** Leverages Lua scripts for safe, atomic state
  transitions in Redis.
- 🔷 **TypeScript Native:** Written entirely in TypeScript with types included.
- 🕒 **Delayed Jobs:** Schedule jobs to run after a specific delay.
- 🗓️ **Scheduled Jobs:** Define recurring jobs using cron patterns or fixed
  intervals via the integrated Job Scheduler.
- 🔄 **Automatic Retries:** Failed jobs can be automatically retried with
  configurable backoff strategies (fixed, exponential).
- ⚖️ **Concurrency Control:** Limit the number of jobs a worker processes
  concurrently.
- 🧹 **Job Lifecycle Events:** Emit events for `active`, `completed`, `failed`,
  `retrying`, etc. (including scheduler-specific events).
- 🗑️ **Automatic Cleanup:** Optionally remove job data automatically upon
  completion or failure.

## Installation

```bash
# Using npm
npm install @jlucaso/lightq

# Using yarn
yarn add @jlucaso/lightq

# Using Bun
bun add @jlucaso/lightq
```

**Prerequisite:** Requires a running Redis server (version 4.0 or later
recommended due to Lua script usage). `ioredis` is used as the Redis client.

## Basic Usage

Check the `example/index.ts` file for a comprehensive demonstration, including:

- Creating Queues and Workers.
- Adding immediate, delayed, and failing jobs.
- **Setting up and removing scheduled jobs.**
- Handling various job lifecycle events.

## API Overview

### `Queue<TData, TResult, TName>`

- **`constructor(name: string, opts: QueueOptions)`**: Creates a new queue
  instance.
- **`add(name: TName, data: TData, opts?: JobOptions): Promise<Job<TData, TResult, TName>>`**:
  Adds a single job to the queue.
- **`addBulk(jobs: { name: TName; data: TData; opts?: JobOptions }[]): Promise<Job<TData, TResult, TName>[]>`**:
  Adds multiple jobs efficiently.
- **`upsertJobScheduler(schedulerId: string, repeat: SchedulerRepeatOptions, template: JobTemplate): Promise<void>`**:
  Creates or updates a recurring job schedule associated with this queue. Starts
  the scheduler polling process if needed.
- **`removeJobScheduler(schedulerId: string): Promise<boolean>`**: Removes a
  specific job schedule associated with this queue.
- **`getJob(jobId: string): Promise<Job<TData, TResult, TName> | null>`**:
  Retrieves job details by ID.
- **`getJobCounts(): Promise<{ wait: number; active: number; ... }>`**: Gets the
  number of jobs in different states.
- **`close(): Promise<void>`**: Closes the Redis connection used by the queue
  (and its associated scheduler, if active).
- **Events**: `error`, `ready`, `waiting`, `closing`, `closed`,
  `scheduler_error`, `scheduler_job_added`.

### `Worker<TData, TResult, TName>`

- **`constructor(name: string, processor: Processor<TData, TResult, TName>, opts: WorkerOptions)`**:
  Creates a worker instance to process jobs from the specified queue.
  - The `processor` function takes a `Job` object and should return a `Promise`
    resolving with the job's result or throwing an error if it fails.
- **`close(force?: boolean): Promise<void>`**: Closes the worker. By default, it
  waits for active jobs to complete. `force = true` attempts a faster shutdown.
- **Events**:
  - `active`: Job started processing. `(job: Job) => void`
  - `completed`: Job finished successfully.
    `(job: Job, result: TResult) => void`
  - `failed`: Job failed (possibly after retries).
    `(job: Job | undefined, error: Error) => void`
  - `error`: An error occurred within the worker itself (e.g., Redis connection
    issue, lock renewal failure). `(error: Error, job?: Job) => void`
  - `retrying`: Job failed but will be retried.
    `(job: Job, error: Error) => void`
  - `movedDelayed`: Delayed jobs were moved to the wait list.
    `(count: number) => void`
  - `ready`: Worker connected to Redis. `() => void`
  - `closing`: Worker is starting the closing process. `() => void`
  - `closed`: Worker has finished closing. `() => void`

### `Job<TData, TResult, TName>`

- Represents a single job instance. Contains properties like:
  - `id`: Unique job ID.
  - `name`: Job name provided during `add`.
  - `data`: The payload provided during `add`.
  - `opts`: Job-specific options.
  - `attemptsMade`: How many times this job has been attempted.
  - `progress`: (Note: `updateProgress` is currently not fully implemented).
  - `returnValue`: The result returned by the processor upon completion.
  - `failedReason`: The error message if the job failed permanently.
  - `stacktrace`: Stack trace if the job failed.

### `JobScheduler`

- While not typically instantiated directly, the `JobScheduler` class manages
  recurring job schedules. It's accessed via the `Queue`'s `upsertJobScheduler`
  and `removeJobScheduler` methods. It runs a background process to check for
  due schedules and adds corresponding jobs to the queue.
- **Events (Forwarded via Queue)**: `error`, `job_added`, `upsert`, `remove`,
  `start`, `stop`, `closing`, `closed`.

## Configuration

### QueueOptions

- `connection`: IORedis connection options or an existing `IORedis` instance.
- `prefix`: Redis key prefix (default: `lightq`).
- `defaultJobOptions`: Default `JobOptions` applied to all jobs added to this
  queue.
- _(Implicitly used by Scheduler)_: `prefix` is used as the base for scheduler
  keys unless `schedulerPrefix` is provided in `JobSchedulerOptions`.

### WorkerOptions

- Extends `QueueOptions`.
- `concurrency`: Max number of jobs to process concurrently (default: `1`).
- `lockDuration`: Time (ms) a job is locked during processing (default:
  `30000`).
- `lockRenewTime`: Interval (ms) before lock expiration to attempt renewal
  (default: `lockDuration / 2`).
- `removeOnComplete`: `true` to delete job data on success, or a `number` to
  keep only the latest N completed jobs' data (Note: Trimming by count requires
  uncommenting Lua script logic). Default: `false`.
- `removeOnFail`: `true` to delete job data on final failure, or a `number` to
  keep only the latest N failed jobs' data (Note: Trimming by count requires
  uncommenting Lua script logic). Default: `false`.

### JobOptions

- `jobId`: Assign a custom job ID (cannot start with `scheduler:`).
- `delay`: Delay (ms) before the job should be processed.
- `attempts`: Max number of times to attempt the job (default: `1`).
- `backoff`: Backoff strategy for retries: `number` (fixed delay ms) or
  `{ type: 'fixed' | 'exponential', delay: number }`.
- `removeOnComplete`: Overrides worker/queue default.
- `removeOnFail`: Overrides worker/queue default.

### Scheduler Configuration

- **`JobSchedulerOptions` (extends `QueueOptions`)**: Options for the scheduler
  instance (usually configured internally by the Queue).
  - `checkInterval`: How often (ms) the scheduler checks for due jobs (default:
    `5000`).
  - `schedulerPrefix`: Specific Redis key prefix for scheduler data (defaults to
    `prefix` from `QueueOptions`).
- **`SchedulerRepeatOptions`**: Defines when scheduled jobs run.
  - `{ pattern: string, tz?: string }`: A cron pattern string and optional
    timezone (e.g., `pattern: '0 * * * *'` for hourly).
  - `{ every: number }`: A repeat interval in milliseconds (e.g., `every: 60000`
    for every minute).
- **`JobTemplate`**: Defines the job created by the scheduler.
  - `name`: The name of the job to be added to the queue.
  - `data?`: The data payload for the job.
  - `opts?`: `JobOptions` for the created job (cannot include `jobId` or
    `delay`).

## Security Considerations

- **Job Data Sanitization**: While LightQ safely handles job data using parameterized Lua scripts to prevent injection attacks, be aware that job data is stored and retrieved as-is from Redis. If you plan to display job data in logs, UIs, or web interfaces, ensure you properly sanitize the data to prevent Cross-Site Scripting (XSS) vulnerabilities.

- **Redis Security**: Ensure your Redis instance is properly secured with authentication, network restrictions, and encryption if handling sensitive data.

## Contributing

Contributions are welcome! Please feel free to open an issue or submit a pull
request.

## License

Licensed under the [MIT License](LICENSE).
