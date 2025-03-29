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
- 🔄 **Automatic Retries:** Failed jobs can be automatically retried with
  configurable backoff strategies (fixed, exponential).
- ⚖️ **Concurrency Control:** Limit the number of jobs a worker processes
  concurrently.
- 🧹 **Job Lifecycle Events:** Emit events for `active`, `completed`, `failed`,
  `retrying`, etc.
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

Here's a quick example demonstrating how to add jobs and process them with a
worker:

```typescript
import { Job, Queue, Worker } from "lightq"; // Assuming 'lightq' is your package name

// 1. Configure Redis Connection
const redisConnectionOpts = {
  host: "localhost",
  port: 6379,
  maxRetriesPerRequest: null,
};
const queueName = "email-tasks";

// --- Define Job Data and Result Types (Optional but recommended) ---
interface EmailJobData {
  to: string;
  subject: string;
  body: string;
}

interface EmailJobResult {
  success: boolean;
  messageId?: string;
}

// 2. Create a Queue Instance
const emailQueue = new Queue<EmailJobData, EmailJobResult>(queueName, {
  connection: redisConnectionOpts,
  defaultJobOptions: {
    attempts: 3, // Default attempts for jobs in this queue
    backoff: { type: "exponential", delay: 1000 }, // Default backoff strategy
  },
});

// 3. Add Jobs to the Queue
async function addEmails() {
  console.log("Adding jobs...");
  await emailQueue.add("send-welcome", {
    to: "user1@example.com",
    subject: "Welcome!",
    body: "Thanks for signing up.",
  });

  await emailQueue.add(
    "send-promo",
    {
      to: "user2@example.com",
      subject: "Special Offer!",
      body: "Check out our new deals.",
    },
    { delay: 5000 }, // Send this job after a 5-second delay
  );

  console.log("Jobs added.");
  // Close the queue connection when no longer needed for adding jobs
  // await emailQueue.close(); // Or keep it open if adding more jobs later
}

// 4. Create a Worker to Process Jobs
const emailWorker = new Worker<EmailJobData, EmailJobResult>(
  queueName,
  async (job: Job<EmailJobData, EmailJobResult>) => {
    console.log(`Processing job ${job.id} (${job.name}) for ${job.data.to}`);
    console.log(`Subject: ${job.data.subject}`);

    // Simulate sending the email
    await new Promise((resolve) => setTimeout(resolve, 1500));

    // Example: Simulate failure for a specific email
    if (job.data.to === "fail@example.com") {
      throw new Error("Simulated email sending failure");
    }

    console.log(`Finished job ${job.id}`);
    // Return the result
    return { success: true, messageId: `msg_${job.id}` };
  },
  {
    connection: redisConnectionOpts,
    concurrency: 5, // Process up to 5 emails concurrently
  },
);

// 5. Listen to Worker Events (Optional)
emailWorker.on("completed", (job, result) => {
  console.log(
    `Job ${job.id} completed successfully! Result:`,
    result,
  );
});

emailWorker.on("failed", (job, error) => {
  console.error(
    `Job ${job?.id} failed after ${job?.attemptsMade} attempts with error: ${error.message}`,
  );
});

emailWorker.on("error", (error) => {
  console.error("Worker encountered an error:", error);
});

emailWorker.on("ready", () => {
  console.log("Worker is ready and connected to Redis.");
});

emailWorker.on("closing", () => {
  console.log("Worker is closing...");
});

emailWorker.on("closed", () => {
  console.log("Worker has closed.");
});

// --- Start the process ---
addEmails().catch(console.error);
console.log("Worker started...");

// --- Graceful Shutdown Handling ---
async function shutdown() {
  console.log("Received signal to shut down.");
  console.log("Closing worker...");
  await emailWorker.close(); // Wait for active jobs to finish
  console.log("Closing queue connection (if still open)...");
  await emailQueue.close(); // Close queue connection used for adding
  console.log("Shutdown complete.");
  process.exit(0);
}

process.on("SIGINT", shutdown); // Handle Ctrl+C
process.on("SIGTERM", shutdown); // Handle kill commands
```

## API Overview

### `Queue<TData, TResult, TName>`

- **`constructor(name: string, opts: QueueOptions)`**: Creates a new queue
  instance.
- **`add(name: TName, data: TData, opts?: JobOptions): Promise<Job<TData, TResult, TName>>`**:
  Adds a single job to the queue.
- **`addBulk(jobs: { name: TName; data: TData; opts?: JobOptions }[]): Promise<Job<TData, TResult, TName>[]>`**:
  Adds multiple jobs efficiently.
- **`getJob(jobId: string): Promise<Job<TData, TResult, TName> | null>`**:
  Retrieves job details by ID.
- **`getJobCounts(): Promise<{ wait: number; active: number; ... }>`**: Gets the
  number of jobs in different states.
- **`close(): Promise<void>`**: Closes the Redis connection used by the queue.
- **Events**: `error`, `ready`, `waiting` (when a job is added/ready),
  `closing`, `closed`.

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

## Configuration

### QueueOptions

- `connection`: IORedis connection options or an existing `IORedis` instance.
- `prefix`: Redis key prefix (default: `smq`).
- `defaultJobOptions`: Default `JobOptions` applied to all jobs added to this
  queue.

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

- `jobId`: Assign a custom job ID.
- `delay`: Delay (ms) before the job should be processed.
- `attempts`: Max number of times to attempt the job (default: `1`).
- `backoff`: Backoff strategy for retries: `number` (fixed delay ms) or
  `{ type: 'fixed' | 'exponential', delay: number }`.
- `removeOnComplete`: Overrides worker/queue default.
- `removeOnFail`: Overrides worker/queue default.

## Contributing

Contributions are welcome! Please feel free to open an issue or submit a pull
request.

## License

Licensed under the [MIT License](LICENSE).
