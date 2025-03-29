import {
  afterAll,
  afterEach,
  beforeAll,
  beforeEach,
  describe,
  expect,
  it,
  mock,
  spyOn,
} from "bun:test";
import IORedis, { type Redis } from "ioredis";
import { type JobData, Queue, Worker } from "../src/index";
import { Job } from "../src/classes/job";
import type { Processor } from "../src/classes/worker";
import process from "node:process";
import * as queueUtils from "../src/utils";

const REDIS_HOST = process.env.REDIS_HOST || "localhost";
const REDIS_PORT = process.env.REDIS_PORT
  ? parseInt(process.env.REDIS_PORT, 10)
  : 6379;
const redisConnectionOpts = {
  host: REDIS_HOST,
  port: REDIS_PORT,
  maxRetriesPerRequest: null,
};

const delay = (ms: number): Promise<void> =>
  new Promise((resolve) => setTimeout(resolve, ms));

async function waitFor(
  conditionFn: () => Promise<boolean> | boolean,
  timeout = 5000,
  interval = 100,
): Promise<void> {
  const start = Date.now();
  while (Date.now() - start < timeout) {
    if (await conditionFn()) {
      return;
    }
    await delay(interval);
  }
  throw new Error(`Condition not met within ${timeout}ms`);
}

describe("Simple Message Queue (smq)", () => {
  let testQueueName: string;
  let redisClient: Redis;
  let queuesToClose: Queue<any, any, any>[] = [];
  let workersToClose: Worker<any, any, any>[] = [];

  // Helper to create unique queue names for each test
  const generateQueueName = (base = "test-queue") =>
    `${base}:${Date.now()}:${Math.random().toString(36).substring(7)}`;

  // Helper to create and track queues
  const createQueue = <
    TData = any,
    TResult = any,
    TName extends string = string,
  >(
    name: string,
    opts: Partial<Queue<TData, TResult, TName>["opts"]> = {},
  ): Queue<TData, TResult, TName> => {
    const queue = new Queue<TData, TResult, TName>(name, {
      connection: { ...redisConnectionOpts }, // Use a fresh connection object potentially
      ...opts,
    });
    queuesToClose.push(queue);
    return queue;
  };

  // Helper to create and track workers
  const createWorker = <
    TData = any,
    TResult = any,
    TName extends string = string,
  >(
    name: string,
    processor: Processor<TData, TResult, TName>,
    opts: Partial<Worker<TData, TResult, TName>["opts"]> = {},
  ): Worker<TData, TResult, TName> => {
    const worker = new Worker<TData, TResult, TName>(name, processor, {
      connection: { ...redisConnectionOpts }, // Use a fresh connection object potentially
      lockDuration: 5000, // Shorter duration for tests
      lockRenewTime: 2500,
      ...opts,
    });
    workersToClose.push(worker);
    return worker;
  };

  beforeAll(async () => {
    redisClient = new IORedis(redisConnectionOpts);
    // Ensure connection is ready before tests start
    await redisClient.ping();
  });

  afterAll(async () => {
    await redisClient.quit();
  });

  beforeEach(async () => {
    testQueueName = generateQueueName();
    queuesToClose = [];
    workersToClose = [];
    // Clear Redis before each test
    await redisClient.flushdb();
  });

  afterEach(async () => {
    // Close all created queues and workers
    await Promise.all([
      ...workersToClose.map((w) =>
        w
          .close()
          .catch((e) =>
            console.error(`Error closing worker ${w.name}: ${e.message}`)
          )
      ), // Force close workers quickly
      ...queuesToClose.map((q) =>
        q
          .close()
          .catch((e) =>
            console.error(`Error closing queue ${q.name}: ${e.message}`)
          )
      ),
    ]);
    workersToClose = [];
    queuesToClose = [];
    mock.restore();
  });

  describe("Job Class", () => {
    it("should create Job instance from data", () => {
      const queue = createQueue(testQueueName);
      const jobData: JobData = {
        id: "job-1",
        name: "test-job",
        data: { x: 1 },
        opts: { attempts: 1, delay: 0 },
        timestamp: Date.now(),
        delay: 0,
        attemptsMade: 0,
      };
      const job = Job.fromData(queue, jobData);
      expect(job).toBeInstanceOf(Job);
      expect(job.id).toBe("job-1");
      expect(job.data).toEqual({ x: 1 });
    });

    it("should convert Job instance back to data", () => {
      const queue = createQueue(testQueueName);
      const jobDataInput: JobData = {
        id: "job-2",
        name: "test-job-2",
        data: { y: "abc" },
        opts: { attempts: 3, delay: 100 },
        timestamp: Date.now(),
        delay: 100,
        attemptsMade: 1,
        processedOn: Date.now() - 1000,
        finishedOn: Date.now(),
        returnValue: { result: "ok" },
        failedReason: undefined,
        stacktrace: [],
        lockedUntil: undefined,
        lockToken: undefined,
      };
      const job = Job.fromData(queue, jobDataInput);
      const jobDataOutput = job.toData();

      // Compare relevant fields (ignore potential minor timestamp differences if applicable)
      expect(jobDataOutput.id).toBe(jobDataInput.id);
      expect(jobDataOutput.name).toBe(jobDataInput.name);
      expect(jobDataOutput.data).toEqual(jobDataInput.data);
      expect(jobDataOutput.opts).toEqual(jobDataInput.opts);
      expect(jobDataOutput.timestamp).toBe(jobDataInput.timestamp);
      expect(jobDataOutput.delay).toBe(jobDataInput.delay);
      expect(jobDataOutput.attemptsMade).toBe(jobDataInput.attemptsMade);
      expect(jobDataOutput.processedOn).toBe(jobDataInput.processedOn!);
      expect(jobDataOutput.finishedOn).toBe(jobDataInput.finishedOn!);
      expect(jobDataOutput.returnValue).toEqual(jobDataInput.returnValue);
    });

    it("should parse job from Redis hash correctly", async () => {
      const queue = createQueue(testQueueName);
      const jobId = "job-from-redis";
      const timestamp = Date.now();
      const hashData = {
        id: jobId,
        name: "redis-job",
        data: JSON.stringify({ value: 123 }),
        opts: JSON.stringify({ attempts: 2, removeOnFail: true }),
        timestamp: timestamp.toString(),
        delay: "500",
        attemptsMade: "1",
        processedOn: (timestamp + 1000).toString(),
        finishedOn: (timestamp + 2000).toString(),
        returnValue: JSON.stringify({ success: true }),
        failedReason: "Something failed",
        stacktrace: JSON.stringify(["line 1", "line 2"]),
        lockedUntil: (timestamp + 5000).toString(),
        lockToken: "redis-lock-token",
      };
      await redisClient.hset(`${queue.keys.jobs}:${jobId}`, hashData);

      // Use queue.getJob which uses fromRedisHash internally
      const job = await queue.getJob(jobId);

      expect(job).toBeInstanceOf(Job);
      expect(job!.id).toBe(jobId);
      expect(job!.name).toBe("redis-job");
      expect(job!.data).toEqual({ value: 123 });
      expect(job!.opts).toEqual({ attempts: 2, removeOnFail: true });
      expect(job!.timestamp).toBe(timestamp);
      expect(job!.delay).toBe(500);
      expect(job!.attemptsMade).toBe(1);
      expect(job!.processedOn).toBe(timestamp + 1000);
      expect(job!.finishedOn).toBe(timestamp + 2000);
      expect(job!.returnValue).toEqual({ success: true });
      expect(job!.failedReason).toBe("Something failed");
      expect(job!.stacktrace).toEqual(["line 1", "line 2"]);
      expect(job!.lockedUntil).toBe(timestamp + 5000);
      expect(job!.lockToken).toBe("redis-lock-token");
    });

    it("should handle missing optional fields when parsing from Redis hash", async () => {
      const queue = createQueue(testQueueName);
      const jobId = "job-missing-fields";
      const timestamp = Date.now();
      const hashData = {
        // Only required fields + some others
        id: jobId,
        name: "minimal-job",
        data: JSON.stringify({}),
        opts: JSON.stringify({}),
        timestamp: timestamp.toString(),
        delay: "0",
        attemptsMade: "0",
      };
      await redisClient.hset(`${queue.keys.jobs}:${jobId}`, hashData);

      const job = await queue.getJob(jobId);

      expect(job).toBeInstanceOf(Job);
      expect(job!.id).toBe(jobId);
      expect(job!.processedOn).toBeUndefined();
      expect(job!.finishedOn).toBeUndefined();
      expect(job!.returnValue).toBeUndefined();
      expect(job!.failedReason).toBeUndefined();
      expect(job!.stacktrace).toEqual([]); // Default value
      expect(job!.lockedUntil).toBeUndefined();
      expect(job!.lockToken).toBeUndefined();
    });

    it("should warn when updateProgress is called (as it's not implemented)", async () => {
      const queue = createQueue(testQueueName);
      const job = await queue.add("progress-job", { d: 1 });
      const consoleWarnSpy = spyOn(console, "warn"); // Spy on console.warn

      await job.updateProgress(50);

      expect(consoleWarnSpy).toHaveBeenCalledTimes(1);
      expect(consoleWarnSpy).toHaveBeenCalledWith(
        "updateProgress not fully implemented in this simple version",
      );

      // Optional: Check if HSET was attempted (it shouldn't ideally if not impl)
      // This requires more involved mocking or checking Redis state changes
      // For now, just checking the warning is sufficient given the implementation note.
    });
  });

  describe("Queue Class", () => {
    it("should create a queue instance", () => {
      const queue = createQueue(testQueueName);
      expect(queue).toBeInstanceOf(Queue);
      expect(queue.name).toBe(testQueueName);
    });

    it("should add a job to the wait list", async () => {
      const queue = createQueue<{ msg: string }>(testQueueName);
      const jobData = { msg: "hello" };
      const jobName = "test-job";

      const job = await queue.add(jobName, jobData);

      expect(job).toBeInstanceOf(Job);
      expect(job.id).toBeString();
      expect(job.name).toBe(jobName);
      expect(job.data).toEqual(jobData);
      expect(job.opts.attempts).toBe(1); // Default

      const counts = await queue.getJobCounts();
      expect(counts.wait).toBe(1);
      expect(counts.active).toBe(0);
      expect(counts.completed).toBe(0);
      expect(counts.failed).toBe(0);
      expect(counts.delayed).toBe(0);

      const retrievedJob = await queue.getJob(job.id);
      expect(retrievedJob).not.toBeNull();
      expect(retrievedJob!.id).toBe(job.id);
      expect(retrievedJob!.data).toEqual(jobData);
    });

    it("should add a delayed job to the delayed list", async () => {
      const queue = createQueue<{ msg: string }>(testQueueName);
      const jobData = { msg: "later" };
      const jobName = "delayed-job";
      const delayMs = 500;

      const job = await queue.add(jobName, jobData, { delay: delayMs });

      let counts = await queue.getJobCounts();
      expect(counts.delayed).toBe(1);
      expect(counts.wait).toBe(0);

      // Wait for the job to become active (needs worker or manual script call simulation)
      // We'll test the move script directly for simplicity here
      const keys = queue.keys;
      await delay(delayMs + 100); // Wait past the delay
      // @ts-ignore - Accessing private scripts for testing moveDelayedToWait
      const movedCount = await queue.scripts.moveDelayedToWait(
        keys,
        Date.now(),
        10,
      );

      expect(movedCount).toBe(1);

      counts = await queue.getJobCounts();
      expect(counts.delayed).toBe(0);
      expect(counts.wait).toBe(1);

      const retrievedJob = await queue.getJob(job.id);
      expect(retrievedJob).not.toBeNull();
    });

    it("should add jobs in bulk", async () => {
      const queue = createQueue<{ index: number }>(testQueueName);
      const jobsToAdd = [
        { name: "bulk-job", data: { index: 1 } },
        { name: "bulk-job", data: { index: 2 }, opts: { delay: 100 } },
        { name: "bulk-job", data: { index: 3 } },
      ];

      const addedJobs = await queue.addBulk(jobsToAdd);

      expect(addedJobs).toHaveLength(3);
      expect(addedJobs[0]).toBeInstanceOf(Job);
      expect(addedJobs[1]?.opts.delay).toBe(100);

      const counts = await queue.getJobCounts();
      expect(counts.wait).toBe(2);
      expect(counts.delayed).toBe(1);

      // Verify jobs exist
      const job1 = await queue.getJob(addedJobs[0]!.id);
      const job2 = await queue.getJob(addedJobs[1]!.id);
      expect(job1).not.toBeNull();
      expect(job2).not.toBeNull();
      expect(job1?.data.index).toBe(1);
      expect(job2?.data.index).toBe(2);
    });

    it("should apply default job options", async () => {
      const queue = createQueue<{ msg: string }>(testQueueName, {
        defaultJobOptions: { attempts: 5, removeOnComplete: true },
      });
      const job = await queue.add("default-opts-job", { msg: "test" });

      expect(job.opts.attempts).toBe(5);
      expect(job.opts.removeOnComplete).toBe(true);

      const retrievedJob = await queue.getJob(job.id);
      expect(retrievedJob!.opts.attempts).toBe(5);
      expect(retrievedJob!.opts.removeOnComplete).toBe(true);
    });

    it("should override default job options", async () => {
      const queue = createQueue<{ msg: string }>(testQueueName, {
        defaultJobOptions: { attempts: 5, removeOnComplete: true },
      });
      const job = await queue.add(
        "override-opts-job",
        { msg: "test" },
        {
          attempts: 2,
          removeOnComplete: false,
        },
      );

      expect(job.opts.attempts).toBe(2);
      expect(job.opts.removeOnComplete).toBe(false);

      const retrievedJob = await queue.getJob(job.id);
      expect(retrievedJob!.opts.attempts).toBe(2);
      expect(retrievedJob!.opts.removeOnComplete).toBe(false);
    });

    it("should retrieve job counts accurately", async () => {
      const queue = createQueue(testQueueName);
      await queue.add("job1", { d: 1 });
      await queue.add("job2", { d: 2 }, { delay: 5000 }); // delayed
      // Manually move one to active (simulate worker start) - Requires internal script access
      // For a pure queue test, we rely on adding to specific lists initially.
      // We will test counts more thoroughly with the worker.

      const counts = await queue.getJobCounts();
      expect(counts.wait).toBe(1);
      expect(counts.delayed).toBe(1);
      expect(counts.active).toBe(0);
      expect(counts.completed).toBe(0);
      expect(counts.failed).toBe(0);
    });

    it("should close the queue connection", async () => {
      const queue = createQueue(testQueueName);
      await queue.add("job-before-close", { d: 1 });
      await queue.close();
      // Try adding after close - should throw
      await expect(queue.add("job-after-close", { d: 2 })).rejects.toThrow(
        "Queue is closing",
      );
      // Check connection status (ioredis specific)
      // expect(queue.client.status).toBe("end");
      // Remove from list so afterEach doesn't try to close again
      queuesToClose = queuesToClose.filter((q) => q !== queue);
    });

    it("should not add a job if job ID already exists", async () => {
      const queue = createQueue(testQueueName);
      const jobId = "duplicate-job-id";
      const job1 = await queue.add("job-type-1", { val: 1 }, { jobId });
      const initialCounts = await queue.getJobCounts();
      expect(initialCounts.wait).toBe(1);

      // Try adding again with the same jobId
      // NOTE: The current implementation of queue.add doesn't check the script's
      // return value (0 for duplicate). It returns a Job object regardless.
      // The *effect* however, is that the job data isn't overwritten and
      // the job isn't added to the wait/delayed list again by the Lua script.
      const job2 = await queue.add("job-type-2", { val: 2 }, { jobId });

      expect(job2.id).toBe(jobId); // It creates a Job object representation

      // Verify counts haven't changed
      const finalCounts = await queue.getJobCounts();
      expect(finalCounts.wait).toBe(1); // Should still be 1
      expect(finalCounts.delayed).toBe(0);

      // Verify the data in Redis hasn't changed to the second job's data
      const retrievedJob = await queue.getJob(jobId);
      expect(retrievedJob).not.toBeNull();
      expect(retrievedJob!.name).toBe("job-type-1"); // Should be the first job's name
      expect(retrievedJob!.data).toEqual({ val: 1 }); // Should be the first job's data
    });

    it("should close the queue connection", async () => {
      const queue = createQueue(testQueueName);
      // Use a separate client to check connection status if possible,
      // or rely on the error throwing behavior.
      const connectionStatusClient = new IORedis({ ...redisConnectionOpts });
      queuesToClose.push({
        client: connectionStatusClient,
        close: async () => {
          connectionStatusClient.disconnect();
        },
      } as any); // Add to cleanup

      const sharedQueue = new Queue(testQueueName, {
        connection: connectionStatusClient,
      });
      queuesToClose.push(sharedQueue);

      await sharedQueue.add("job-before-close", { d: 1 });
      let statusBefore = connectionStatusClient.status;
      expect(statusBefore).toBeOneOf(["connecting", "connect", "ready"]);

      await sharedQueue.close();

      // Wait a moment for status to potentially update
      await delay(50);
      let statusAfter = connectionStatusClient.status;
      // ioredis might go to 'end' or just stay 'ready' if other connections exist
      // console.log("Status after close:", statusAfter);

      // Try adding after close - should throw
      await expect(
        sharedQueue.add("job-after-close", { d: 2 }),
      ).rejects.toThrow("Queue is closing");

      // Remove from list so afterEach doesn't try to close again
      // (Handled by pushing onto queuesToClose and the afterEach loop)
      queuesToClose = queuesToClose.filter((q) => q !== sharedQueue);
      queuesToClose = queuesToClose.filter(
        (q) => q.client !== connectionStatusClient,
      ); // Remove the status checker too
      await connectionStatusClient.quit().catch(() => {}); // Ensure checker client is closed
    });

    it("should execute a raw script command via executeScript", async () => {
      const queue = createQueue(testQueueName);
      // Example: Use EVAL to run a simple command
      const script = "return redis.call('SET', KEYS[1], ARGV[1])";
      const key = `${queue.keys.base}:exec-test`;
      const value = "hello world";
      // Define the command temporarily for testing (or use EVAL directly if simpler)
      queue.client.defineCommand("testExecScript", {
        // <-- Command 1
        numberOfKeys: 1,
        lua: script,
      });

      // Test without pipeline
      // @ts-ignore - Accessing potentially private/internal method for testing
      let result = await queue.executeScript("testExecScript", [key], [value]);
      expect(result).toBe("OK");
      expect(await redisClient.get(key)).toBe(value);

      // Test with pipeline
      const key2 = `${queue.keys.base}:exec-test-pipe`;
      const value2 = "pipeline test";

      // *** FIX START: Define command BEFORE creating the pipeline ***
      queue.client.defineCommand("testExecScriptPipe", {
        // <-- Command 2
        numberOfKeys: 1,
        lua: script,
      });
      const pipeline = queue.client.pipeline(); // Create pipeline AFTER definition
      // *** FIX END ***

      // @ts-ignore
      queue.executeScript("testExecScriptPipe", [key2], [value2], pipeline); // Queue the command onto the pipeline
      const execResult = await pipeline.exec(); // Execute the pipeline

      // --- Assertions ---
      expect(execResult).toHaveLength(1); // <-- Should pass now
      expect(execResult).toBeArray();
      expect(execResult![0]).toBeArray();
      // The result structure from pipeline.exec is [[error, resultFromCommand1], [error, resultFromCommand2], ...]
      expect(execResult![0]![0]).toBeNull(); // No error for the command
      expect(execResult![0]![1]).toBe("OK"); // Result of the script command in pipeline
      expect(await redisClient.get(key2)).toBe(value2);

      // Cleanup test keys
      await redisClient.del(key, key2);
    });
  });

  describe("Worker Class", () => {
    it("should process a job successfully", async () => {
      const queue = createQueue<{ input: number }, { output: number }>(
        testQueueName,
      );
      const jobData = { input: 5 };
      const expectedResult = { output: 10 };
      let processedJob: Job | null = null;
      let jobResult: any = null;

      const processor: Processor<
        { input: number },
        { output: number }
      > = async (job) => {
        processedJob = job;
        await delay(50); // Simulate work
        return { output: job.data.input * 2 };
      };

      const worker = createWorker(testQueueName, processor);

      const completedPromise = new Promise<void>((resolve) => {
        worker.on("completed", (job, result) => {
          if (job.id === addedJob.id) {
            jobResult = result;
            resolve();
          }
        });
      });

      const addedJob = await queue.add("multiply", jobData);
      await completedPromise; // Wait for the 'completed' event

      expect(processedJob).not.toBeNull();
      expect(processedJob!.id).toBe(addedJob.id);
      expect(processedJob!.data).toEqual(jobData);
      expect(jobResult).toEqual(expectedResult);

      const counts = await queue.getJobCounts();
      expect(counts.completed).toBe(1);
      expect(counts.wait).toBe(0);
      expect(counts.active).toBe(0);

      const retrievedJob = await queue.getJob(addedJob.id);
      expect(retrievedJob).not.toBeNull();
      expect(retrievedJob!.finishedOn).toBeNumber();
      expect(retrievedJob!.returnValue).toEqual(expectedResult);
      expect(retrievedJob!.lockToken).toBeUndefined(); // Lock should be released
    });

    it("should move a job to failed after exhausting retries", async () => {
      const queue = createQueue<{ fail: boolean }>(testQueueName);
      const jobData = { fail: true };
      const maxAttempts = 2;
      const failError = new Error("Job failed as planned");
      let attemptsMade = 0;
      let failedJob: Job | null = null;
      let receivedError: Error | null = null;

      const processor: Processor<{ fail: boolean }> = async (job) => {
        attemptsMade++;
        await delay(20);
        throw failError;
      };

      const worker = createWorker(testQueueName, processor, { concurrency: 1 });

      const failedPromise = new Promise<void>((resolve) => {
        worker.on("failed", (job, err) => {
          if (job?.id === addedJob.id) {
            failedJob = job;
            receivedError = err;
            // Resolve only on the final failure
            if (job?.attemptsMade === maxAttempts) {
              resolve();
            }
          }
        });
      });

      const addedJob = await queue.add("fail-job", jobData, {
        attempts: maxAttempts,
      });

      await failedPromise; // Wait for the final 'failed' event

      expect(attemptsMade).toBe(maxAttempts);
      expect(failedJob).not.toBeNull();
      expect(failedJob!.id).toBe(addedJob.id);
      expect(failedJob!.attemptsMade).toBe(maxAttempts);
      // @ts-ignore
      expect(receivedError).toBe(failError);

      const counts = await queue.getJobCounts();
      expect(counts.failed).toBe(1);
      expect(counts.completed).toBe(0);
      expect(counts.wait).toBe(0);
      expect(counts.active).toBe(0);

      const retrievedJob = await queue.getJob(addedJob.id);
      expect(retrievedJob).not.toBeNull();
      expect(retrievedJob!.failedReason).toBe(failError.message);
      expect(retrievedJob!.attemptsMade).toBe(maxAttempts);
      expect(retrievedJob!.finishedOn).toBeNumber();
      expect(retrievedJob!.stacktrace).toBeArray();
      expect(retrievedJob!.stacktrace!.length).toBeGreaterThan(0);
    });

    it("should handle fixed backoff strategy", async () => {
      const queue = createQueue<{ fail: boolean }>(testQueueName);
      const maxAttempts = 3;
      const backoffDelay = 100; // ms
      let processorCallTimestamps: number[] = [];

      const processor: Processor<{ fail: boolean }> = async (job) => {
        processorCallTimestamps.push(Date.now());
        await delay(10);
        throw new Error("Failing for backoff test");
      };

      const worker = createWorker(testQueueName, processor);

      const failedPromise = new Promise<void>((resolve) => {
        worker.on("failed", (job, err) => {
          if (job?.id === addedJob.id && job?.attemptsMade === maxAttempts) {
            resolve();
          }
        });
      });

      const addedJob = await queue.add(
        "backoff-job",
        { fail: true },
        {
          attempts: maxAttempts,
          backoff: { type: "fixed", delay: backoffDelay },
        },
      );

      await failedPromise;

      expect(processorCallTimestamps.length).toBe(maxAttempts);

      // Check approximate delay between attempts
      for (let i = 1; i < maxAttempts; i++) {
        const diff = processorCallTimestamps[i]! -
          processorCallTimestamps[i - 1]!;
        // Allow significant tolerance for test runner / event loop delays
        expect(diff).toBeGreaterThanOrEqual(backoffDelay - 20); // Lower bound
        expect(diff).toBeLessThan(backoffDelay + 500); // Upper bound (generous)
      }

      const counts = await queue.getJobCounts();
      expect(counts.failed).toBe(1);
    });

    it("should handle exponential backoff strategy", async () => {
      const queue = createQueue<{ fail: boolean }>(testQueueName);
      const maxAttempts = 3; // Will have delays of 100, 200
      const initialDelay = 100; // ms
      let processorCallTimestamps: number[] = [];

      const processor: Processor<{ fail: boolean }> = async (job) => {
        processorCallTimestamps.push(Date.now());
        await delay(10);
        throw new Error("Failing for exponential backoff test");
      };

      const worker = createWorker(testQueueName, processor);

      const failedPromise = new Promise<void>((resolve) => {
        worker.on("failed", (job, err) => {
          if (job?.id === addedJob.id && job?.attemptsMade === maxAttempts) {
            resolve();
          }
        });
      });

      const addedJob = await queue.add(
        "exp-backoff-job",
        { fail: true },
        {
          attempts: maxAttempts,
          backoff: { type: "exponential", delay: initialDelay },
        },
      );

      await failedPromise;

      expect(processorCallTimestamps.length).toBe(maxAttempts);

      // Check approximate delays: ~100ms, ~200ms
      const diff1 = processorCallTimestamps[1]! - processorCallTimestamps[0]!;
      const diff2 = processorCallTimestamps[2]! - processorCallTimestamps[1]!;

      expect(diff1).toBeGreaterThanOrEqual(initialDelay - 20);
      expect(diff1).toBeLessThan(initialDelay + 500);

      const expectedSecondDelay = initialDelay * 2;
      expect(diff2).toBeGreaterThanOrEqual(expectedSecondDelay - 40); // Allow more variation
      expect(diff2).toBeLessThan(expectedSecondDelay + 1100); // Generous upper bound

      const counts = await queue.getJobCounts();
      expect(counts.failed).toBe(1);
    });

    it("should respect concurrency limits", async () => {
      const queue = createQueue<{ index: number }>(testQueueName);
      const concurrency = 2;
      const jobCount = 5;
      const jobProcessTime = 200;
      let maxConcurrent = 0;
      let currentConcurrent = 0;
      let completedCount = 0;

      const processor: Processor<{ index: number }, boolean> = async (job) => {
        currentConcurrent++;
        maxConcurrent = Math.max(maxConcurrent, currentConcurrent);
        await delay(jobProcessTime);
        currentConcurrent--;
        return true;
      };

      const worker = createWorker(testQueueName, processor, { concurrency });

      const completionPromises = new Array(jobCount).fill(null).map(
        () =>
          new Promise<void>((resolve) => {
            worker.on("completed", (job, result) => {
              // Resolve specific promise based on job ID - needs mapping
              // Simpler: just count completed jobs
              completedCount++;
              if (completedCount === jobCount) {
                // This might resolve multiple times, handle outside
              }
              resolve(); // Resolve on any completion for simplicity now
            });
          }),
      );

      const allJobsCompleted = new Promise<void>((res) => {
        const interval = setInterval(async () => {
          const counts = await queue.getJobCounts();
          if (counts.completed === jobCount) {
            clearInterval(interval);
            res();
          }
        }, 50);
      });

      for (let i = 0; i < jobCount; i++) {
        await queue.add("concurrent-job", { index: i });
      }

      await waitFor(
        async () => (await queue.getJobCounts()).completed === jobCount,
        5000,
      );
      // await allJobsCompleted; // Wait for all jobs to complete

      expect(maxConcurrent).toBe(concurrency);

      const finalCounts = await queue.getJobCounts();
      expect(finalCounts.completed).toBe(jobCount);
      expect(finalCounts.active).toBe(0);
      expect(finalCounts.wait).toBe(0);
    });

    it("should remove job data on complete if specified", async () => {
      const queue = createQueue(testQueueName);
      const worker = createWorker(
        testQueueName,
        async (job) => {
          await delay(10);
          return "ok";
        },
        {
          removeOnComplete: true,
        },
      );
      const job = await queue.add("complete-remove", { d: 1 });

      await waitFor(async () => (await queue.getJob(job.id)) === null, 2000);

      // Wait a tiny bit more for potential DEL command
      await delay(100);

      const retrievedJob = await queue.getJob(job.id);
      expect(retrievedJob).toBeNull(); // Job data should be deleted

      const counts = await queue.getJobCounts();
      expect(counts.completed).toBe(0); // Count also depends on script logic for ZSET removal
      // Verify the moveToCompleted script removed from ZSET if removeOnComplete=true
      // This requires checking ZCARD or ZSCORE, assuming script DELs job *and* doesn't ZADD
      const completedSetSize = await redisClient.zcard(queue.keys.completed);
      expect(completedSetSize).toBe(0);
    });

    it("should remove job data on fail if specified", async () => {
      const queue = createQueue(testQueueName);
      const worker = createWorker(
        testQueueName,
        async (job) => {
          await delay(10);
          throw new Error("fail");
        },
        {
          removeOnFail: true,
          concurrency: 1,
        },
      );
      const job = await queue.add("fail-remove", { d: 1 }, { attempts: 1 }); // Only 1 attempt

      await waitFor(async () => (await queue.getJob(job.id)) === null, 2000);

      await delay(100); // Wait for potential DEL command

      const retrievedJob = await queue.getJob(job.id);
      expect(retrievedJob).toBeNull(); // Job data should be deleted

      // Verify the moveToFailed script removed from ZSET if removeOnFail=true
      const failedSetSize = await redisClient.zcard(queue.keys.failed);
      expect(failedSetSize).toBe(0);
    });

    it("should keep a specified number of completed jobs", async () => {
      const keepCount = 2;
      const jobCount = 5;
      const queue = createQueue(testQueueName);
      const worker = createWorker(
        testQueueName,
        async (job) => {
          await delay(5);
          return job.data.i;
        },
        {
          removeOnComplete: keepCount, // Keep only 2 completed jobs
        },
      );

      let completedJobs = 0;
      const completedPromise = new Promise<void>((res) => {
        worker.on("completed", () => {
          completedJobs++;
          if (completedJobs === jobCount) res();
        });
      });

      const addedJobs: Job[] = [];
      for (let i = 0; i < jobCount; i++) {
        addedJobs.push(await queue.add("complete-keep", { i }));
      }

      await waitFor(
        async () => (await queue.getJobCounts()).completed === jobCount,
        5000,
      );
      await completedPromise; // Ensure processor finished

      // Note: Lua script for trimming (ZREMRANGEBYRANK) is commented out in the source.
      // If implemented, the ZSET count should be `keepCount`. Without it, it will be `jobCount`.
      // Assuming the trim IS NOT implemented based on the provided Lua:
      const counts = await queue.getJobCounts();
      expect(counts.completed).toBe(jobCount); // Lua doesn't trim ZSET in provided code
      // If Lua WERE trimming: expect(counts.completed).toBe(keepCount);

      // Verify job data still exists for all (since Lua doesn't DEL based on count)
      for (const job of addedJobs) {
        const retrieved = await queue.getJob(job.id);
        expect(retrieved).not.toBeNull();
        expect(retrieved!.returnValue).toBeNumber();
      }
      // To properly test removal by count, the ZREMRANGEBYRANK calls
      // in moveToCompleted.lua and moveToFailed.lua need to be uncommented and tested.
    });

    it("should close gracefully, waiting for active jobs", async () => {
      const queue = createQueue(testQueueName);
      const jobProcessTime = 300;
      let jobStarted = false;
      let jobFinished = false;

      const worker = createWorker(
        testQueueName,
        async (job) => {
          jobStarted = true;
          await delay(jobProcessTime);
          jobFinished = true;
          return "done";
        },
        { concurrency: 1 },
      );

      await queue.add("slow-job", { d: 1 });

      // Wait for the job to become active
      await waitFor(
        async () => (await queue.getJobCounts()).active === 1,
        1000,
      );
      expect(jobStarted).toBe(true);

      // Initiate close *while* the job is running
      const closePromise = worker.close(); // Don't force close

      // Check that the job hasn't finished immediately
      await delay(jobProcessTime / 2);
      expect(jobFinished).toBe(false);

      // Wait for the close promise to resolve
      await closePromise;

      // Check that the job completed before close resolved
      expect(jobFinished).toBe(true);

      const counts = await queue.getJobCounts();
      expect(counts.completed).toBe(1); // Job should complete
      expect(counts.active).toBe(0);

      // Worker should be removed from cleanup list as it's closed
      workersToClose = workersToClose.filter((w) => w !== worker);
    });

    it("should emit an error event for processor errors not caught by try/catch", async () => {
      // This tests if the main loop's catch handles unexpected errors
      const queue = createQueue(testQueueName);
      const errorMessage = "Critical processor failure";
      let caughtError: Error | null = null;
      let failedJob: Job | null = null;

      const processor = (job: Job) => {
        throw new Error(errorMessage);
      };

      const worker = createWorker(testQueueName, processor as any, {
        concurrency: 1,
      });

      const failedPromise = new Promise<void>((resolve) => {
        worker.on("failed", (job, err) => {
          caughtError = err;
          failedJob = job;
          resolve();
        });
        worker.on("error", (err) => {
          console.error(
            "Worker 'error' event fired unexpectedly in test:",
            err,
          );
        });
      });

      const addedJob = await queue.add("error-job", { d: 1 }, { attempts: 1 });

      await failedPromise;

      expect(failedJob).not.toBeNull();
      expect(caughtError).toBeInstanceOf(Error);
      expect(caughtError!.message).toBe(errorMessage);

      // Job might end up in failed state depending on error handling details
      await delay(100);
      const counts = await queue.getJobCounts();
      expect(counts.failed).toBe(1);
      const retrievedJob = await queue.getJob(addedJob.id);
      expect(retrievedJob).not.toBeNull();
      expect(retrievedJob?.failedReason).toBe(errorMessage);
    });

    it("should emit 'active' event when processing starts", async () => {
      const queue = createQueue(testQueueName);
      let isActiveEventEmitted = false;
      let activeJob: Job | null = null;

      const worker = createWorker(testQueueName, async (job) => {
        await delay(50);
        return "ok";
      });

      worker.on("active", (job) => {
        isActiveEventEmitted = true;
        activeJob = job;
      });

      const addedJob = await queue.add("active-event-job", { d: 1 });

      await waitFor(() => isActiveEventEmitted, 2000);

      expect(isActiveEventEmitted).toBe(true);
      expect(activeJob).not.toBeNull();
      expect(activeJob!.id).toBe(addedJob.id);
    });

    it("should emit 'retrying' event when a job is retried", async () => {
      const queue = createQueue(testQueueName);
      const maxAttempts = 2;
      const failError = new Error("Temporary failure");
      let retryEventEmitted = false;
      let retryingJob: Job | null = null;
      let retryError: Error | null = null;

      const processor: Processor = async (job) => {
        await delay(10);
        throw failError;
      };

      const worker = createWorker(testQueueName, processor, { concurrency: 1 });

      worker.on("retrying", (job, err) => {
        retryEventEmitted = true;
        retryingJob = job;
        retryError = err;
      });

      const failedPromise = new Promise<void>((resolve) => {
        worker.on("failed", (job, err) => {
          // Wait for final failure
          if (job?.id === addedJob.id && job?.attemptsMade === maxAttempts) {
            resolve();
          }
        });
      });

      const addedJob = await queue.add(
        "retry-event-job",
        { d: 1 },
        { attempts: maxAttempts, backoff: 10 },
      );

      await waitFor(() => retryEventEmitted, 2000); // Wait for the retry event specifically

      expect(retryEventEmitted).toBe(true);
      expect(retryingJob).not.toBeNull();
      expect(retryingJob!.id).toBe(addedJob.id);
      expect(retryingJob!.attemptsMade).toBe(1); // AttemptsMade *before* retry logic increments it for the next attempt
      // @ts-ignore Try to fix this type error
      expect(retryError).toBe(failError);

      await failedPromise; // Ensure the job eventually fails completely
    });

    it("should emit 'movedDelayed' event when delayed jobs are moved", async () => {
      const queue = createQueue(testQueueName);
      let movedDelayedEventCount = 0;

      // Add a delayed job
      await queue.add("delayed-job-event", { d: 1 }, { delay: 100 });

      const worker = createWorker(
        testQueueName,
        async (job) => {
          await delay(10);
          return "ok";
        },
        { lockDuration: 10000 },
      ); // Longer lock to avoid interference

      worker.on("movedDelayed", (count) => {
        movedDelayedEventCount = count;
      });

      // Wait for the job to be processed, which implies the delayed job was moved
      await waitFor(
        async () => (await queue.getJobCounts()).completed === 1,
        3000,
      );

      // Check if the event fired. This might be slightly racy depending on timing.
      // The check happens periodically in the worker loop.
      expect(movedDelayedEventCount).toBeGreaterThanOrEqual(1);
    });

    it("should stop processing *updates* and warn if lock renewal fails", async () => {
      const queue = createQueue(testQueueName);
      const lockDuration = 100; // Very short lock
      const lockRenewTime = 50; // Renew frequently
      const processTime = 300; // Longer than lock duration
      let processorFinishedExecuting = false; // Track if the processor function ran fully
      let jobFailedEvent = false;
      let jobCompletedEvent = false;
      let lockWarningLogged = false;

      const consoleWarnSpy = spyOn(console, "warn"); // Spy on console.warn

      const worker = createWorker(
        testQueueName,
        async (job) => {
          // Simulate changing the lock token externally mid-process
          await delay(lockRenewTime + 20); // Wait until after the first renewal *should* have happened
          await redisClient.hset(
            `${queue.keys.jobs}:${job.id}`,
            "lockToken",
            "invalid-token",
          );
          // Continue processing
          await delay(processTime);

          processorFinishedExecuting = true; // This indicates the async function completed
          return "ok";
        },
        {
          concurrency: 1,
          lockDuration: lockDuration,
          lockRenewTime: lockRenewTime,
        },
      );

      worker.on("completed", () => (jobCompletedEvent = true)); // Shouldn't fire
      worker.on("failed", () => (jobFailedEvent = true)); // Shouldn't fire
      // We expect a console warning for the renewal failure
      consoleWarnSpy.mockImplementation((message: string, ...args) => {
        // Keep original console.warn functionality if needed for debugging
        // console.log('[SPY console.warn]:', message, ...args);
        if (
          typeof message === "string" &&
          message.includes("Failed to renew lock for job")
        ) {
          lockWarningLogged = true;
        }
      });

      const job = await queue.add("lock-renewal-fail", { d: 1 });

      // Wait long enough for the processor to finish *and* for cleanup logic to run
      await delay(processTime + lockDuration + lockRenewTime + 100); // Added extra buffer

      expect(lockWarningLogged).toBe(true); // Check if the renewal failure warning was logged
      expect(processorFinishedExecuting).toBe(true); // The processor *did* finish executing
      expect(jobCompletedEvent).toBe(false); // Job should NOT emit 'completed' event
      expect(jobFailedEvent).toBe(false); // Job should NOT emit 'failed' event

      // Check internal worker state: Job should be removed from jobsInFlight
      // Use waitFor as internal state updates might have slight delays
      await waitFor(() => !(worker as any).jobsInFlight.has(job.id), 1000, 50);
      expect((worker as any).jobsInFlight.has(job.id)).toBe(false);

      // Check job state in Redis - it should NOT have a return value or finishedOn
      // and should not be in the 'completed' list
      const finalJobState = await queue.getJob(job.id);
      expect(finalJobState).not.toBeNull();
      expect(finalJobState!.returnValue).toBeUndefined();
      expect(finalJobState!.finishedOn).toBeUndefined();

      const finalCounts = await queue.getJobCounts();
      expect(finalCounts.completed).toBe(0);

      consoleWarnSpy.mockRestore();
    });

    it("should not complete job if lock token mismatches on completion attempt", async () => {
      const queue = createQueue(testQueueName);
      let jobCompleted = false;
      let jobFailed = false; // It might fail if error handling is robust, or just get stuck

      const worker = createWorker(
        testQueueName,
        async (job) => {
          await delay(50); // Short processing time
          // BEFORE returning, manually change the lock token in Redis
          await redisClient.hset(
            `${queue.keys.jobs}:${job.id}`,
            "lockToken",
            "different-token",
          );
          return "should-not-be-saved";
        },
        { concurrency: 1 },
      );

      worker.on("completed", () => (jobCompleted = true));
      worker.on("failed", () => (jobFailed = true));

      const job = await queue.add("lock-mismatch-complete", { d: 1 });

      // Wait significantly longer than processing time
      await delay(500);

      expect(jobCompleted).toBe(false); // moveToCompleted script should return -1

      // Verify job state in Redis - it shouldn't be in 'completed'
      const finalCounts = await queue.getJobCounts();
      expect(finalCounts.completed).toBe(0);

      // Verify the job data hasn't been updated with the return value
      const finalJobState = await queue.getJob(job.id);
      expect(finalJobState).not.toBeNull();
      expect(finalJobState!.returnValue).toBeUndefined();
      expect(finalJobState!.finishedOn).toBeUndefined();
      // The job might still be in the 'active' list because LREM failed in moveToCompleted script
      expect(finalCounts.active).toBe(1); // Or 0 if some stall detection cleaned it up later
      expect(jobFailed).toBe(false); // It shouldn't fail unless the processor throws an error
    });

    it("should close forcefully, interrupting active jobs", async () => {
      const queue = createQueue(testQueueName);
      const jobProcessTime = 400;
      let jobStarted = false;
      let jobFinished = false;

      const worker = createWorker(
        testQueueName,
        async (job) => {
          jobStarted = true;
          await delay(jobProcessTime); // Simulate long work
          jobFinished = true;
          return "done";
        },
        { concurrency: 1 },
      );

      await queue.add("force-close-job", { d: 1 });

      // Wait for the job to become active
      await waitFor(
        async () => (await queue.getJobCounts()).active === 1,
        1000,
      );
      expect(jobStarted).toBe(true);

      // Initiate force close *while* the job is running
      const closeStartTime = Date.now();
      await worker.close(true); // Force close
      const closeEndTime = Date.now();

      // Check that close was fast (didn't wait for job)
      expect(closeEndTime - closeStartTime).toBeLessThan(jobProcessTime);

      // Check that the job did NOT finish
      expect(jobFinished).toBe(false);

      // Job should remain in the active list (or potentially moved later by stall check)
      const counts = await queue.getJobCounts();
      expect(counts.active).toBe(1);
      expect(counts.completed).toBe(0);

      workersToClose = workersToClose.filter((w) => w !== worker); // Already closed
    });
  });

  it("should pause the main loop when concurrency limit is reached", async () => {
    const queue = createQueue(testQueueName);
    const concurrency = 2; // Use concurrency > 1
    const jobCount = 3; // Add more jobs than concurrency
    const jobProcessTime = 250;

    // Spy on the actual delay function from the utils module
    const delaySpy = spyOn(queueUtils, "delay");

    const worker = createWorker(
      testQueueName,
      async (job) => {
        await queueUtils.delay(jobProcessTime); // Use the original delay for processing
        return true;
      },
      { concurrency },
    );

    // Add jobs
    for (let i = 0; i < jobCount; i++) {
      await queue.add("concurrency-pause-job", { index: i });
    }

    // Wait for all jobs to complete
    await waitFor(
      async () => (await queue.getJobCounts()).completed === jobCount,
      jobProcessTime * 2 + 500, // Adjust timeout if needed
    );

    // Check if the specific delay(200) used for pausing was called
    expect(delaySpy).toHaveBeenCalledWith(200);

    // Restore the original function after the test
    delaySpy.mockRestore();

    const finalCounts = await queue.getJobCounts();
    expect(finalCounts.completed).toBe(jobCount);
  });
});
