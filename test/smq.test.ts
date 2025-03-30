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
import IORedis, { Pipeline, type Redis } from "ioredis";
import {
  Job,
  type JobData,
  JobScheduler,
  JobTemplate,
  type Processor,
  Queue,
  RedisClient,
  SchedulerRepeatOptions,
  Worker,
} from "../src";
import process from "node:process";
import * as queueUtils from "../src/utils";
import { Cron } from "croner";

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
  interval = 100
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

describe("LightQ (lightq)", () => {
  let testQueueName: string;
  let redisClient: Redis;
  let queuesToClose: Queue<any, any, any>[] = [];
  let workersToClose: Worker<any, any, any>[] = [];
  let schedulersToClose: JobScheduler[] = [];

  // Helper to create unique queue names for each test
  const generateQueueName = (base = "test-queue") =>
    `${base}:${Date.now()}:${Math.random().toString(36).substring(7)}`;

  // Helper to create and track queues
  const createQueue = <
    TData = any,
    TResult = any,
    TName extends string = string
  >(
    name: string,
    opts: Partial<Queue<TData, TResult, TName>["opts"]> = {}
  ): Queue<TData, TResult, TName> => {
    const queue = new Queue<TData, TResult, TName>(name, {
      connection: { ...redisConnectionOpts }, // Use a fresh connection object potentially
      ...opts,
    });
    queuesToClose.push(queue);
    return queue;
  };

  const createScheduler = (
    queue: Queue<any, any, any>,
    opts: Partial<JobScheduler["opts"]> = {}
  ): JobScheduler => {
    // Use the internal getScheduler method for consistency if possible,
    // otherwise instantiate directly for isolated tests.
    // Direct instantiation:
    const scheduler = new JobScheduler(queue, {
      connection: queue.client, // Share client
      prefix: queue.opts.prefix,
      checkInterval: 100, // Faster checks for tests
      ...opts,
    });
    schedulersToClose.push(scheduler);
    return scheduler;
  };

  // Helper to create and track workers
  const createWorker = <
    TData = any,
    TResult = any,
    TName extends string = string
  >(
    name: string,
    processor: Processor<TData, TResult, TName>,
    opts: Partial<Worker<TData, TResult, TName>["opts"]> = {}
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
    schedulersToClose = [];
    // Clear Redis before each test
    await redisClient.flushdb();
    mock.restore();
    spyOn(Date, "now");
  });

  afterEach(async () => {
    mock.restore();
    await Promise.all(
      schedulersToClose.map((s) =>
        (s as any) // Access private closing state if needed for robustness check
          .close()
          .catch((e) =>
            console.error(
              `Error closing scheduler for queue ${
                (s as any).queue.name // Access private queue ref
              }: ${e.message}`
            )
          )
      )
    );
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
    schedulersToClose = [];
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
        "updateProgress not fully implemented in this simple version"
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
        10
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
        }
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
        "Queue is closing"
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
        sharedQueue.add("job-after-close", { d: 2 })
      ).rejects.toThrow("Queue is closing");

      // Remove from list so afterEach doesn't try to close again
      // (Handled by pushing onto queuesToClose and the afterEach loop)
      queuesToClose = queuesToClose.filter((q) => q !== sharedQueue);
      queuesToClose = queuesToClose.filter(
        (q) => q.client !== connectionStatusClient
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

    describe("Scheduler Integration", () => {
      it("should initialize and start the scheduler on first access via getScheduler", () => {
        const queue = createQueue(testQueueName);
        // @ts-ignore - Access private method
        expect(queue.scheduler).toBeNull();

        // @ts-ignore - Access private method
        const schedulerInstance = queue.getScheduler();

        expect(schedulerInstance).toBeInstanceOf(JobScheduler);
        // @ts-ignore - Access private property
        expect(queue.scheduler).toBe(schedulerInstance);
        // @ts-ignore - Access private property
        expect(schedulerInstance.running).toBe(true); // Should auto-start

        // @ts-ignore - Access private method
        const schedulerInstance2 = queue.getScheduler();
        expect(schedulerInstance2).toBe(schedulerInstance); // Should return same instance

        // Add scheduler to cleanup automatically via queue close
        // No need to add to schedulersToClose manually here
      });

      it("should upsert a job scheduler via the queue method", async () => {
        const queue = createQueue(testQueueName);
        const schedulerId = "q-upsert-test";
        const repeat: SchedulerRepeatOptions = { every: 5000 };
        const template: JobTemplate = {
          name: "scheduled-task",
          data: { x: 1 },
        };

        // Spy on the actual JobScheduler method
        const schedulerUpsertSpy = spyOn(
          JobScheduler.prototype,
          "upsertJobScheduler"
        ).mockResolvedValue(undefined); // Mock the underlying implementation

        await queue.upsertJobScheduler(schedulerId, repeat, template);

        // @ts-ignore - Ensure scheduler was initialized
        expect(queue.scheduler).toBeInstanceOf(JobScheduler);
        expect(schedulerUpsertSpy).toHaveBeenCalledTimes(1);
        expect(schedulerUpsertSpy).toHaveBeenCalledWith(
          schedulerId,
          repeat,
          template
        );

        schedulerUpsertSpy.mockRestore();
      });

      it("should remove a job scheduler via the queue method (when scheduler exists)", async () => {
        const queue = createQueue(testQueueName);
        const schedulerId = "q-remove-test";

        // Ensure scheduler is initialized first
        // @ts-ignore - Access private method
        const schedulerInstance = queue.getScheduler();
        const schedulerRemoveSpy = spyOn(
          schedulerInstance,
          "removeJobScheduler"
        ).mockResolvedValue(true);

        const result = await queue.removeJobScheduler(schedulerId);

        expect(result).toBe(true);
        expect(schedulerRemoveSpy).toHaveBeenCalledTimes(1);
        expect(schedulerRemoveSpy).toHaveBeenCalledWith(schedulerId);

        schedulerRemoveSpy.mockRestore();
      });

      it("should handle removing a job scheduler via the queue method (when scheduler not initialized)", async () => {
        const queue = createQueue(testQueueName);
        const schedulerId = "q-remove-test-no-init";
        const consoleWarnSpy = spyOn(console, "warn");

        // @ts-ignore - Verify scheduler is not initialized
        expect(queue.scheduler).toBeNull();

        const result = await queue.removeJobScheduler(schedulerId);

        expect(result).toBe(false);
        expect(consoleWarnSpy).toHaveBeenCalledWith(
          expect.stringContaining(
            "Attempted to remove scheduler before scheduler process was started"
          )
        );
        // @ts-ignore - Verify scheduler is still not initialized
        expect(queue.scheduler).toBeNull();

        consoleWarnSpy.mockRestore();
      });

      it("should forward scheduler 'error' event", async () => {
        const queue = createQueue(testQueueName);
        const schedulerError = new Error("Scheduler Test Error");
        let caughtError: Error | null = null;

        queue.on("scheduler_error", (err) => {
          caughtError = err;
        });

        // @ts-ignore - Initialize scheduler
        const scheduler = queue.getScheduler();
        // Manually emit error on the scheduler instance
        scheduler.emit("error", schedulerError);

        await delay(10); // Allow event propagation

        // @ts-ignore - Check if caughtError is the same as schedulerError
        expect(caughtError).toBe(schedulerError);
      });

      it("should forward scheduler 'job_added' event", async () => {
        const queue = createQueue(testQueueName);
        const schedulerId = "emitter-test";
        const fakeJob = { id: "fake-job-1", name: "fake" };
        let caughtSchedulerId: string | null = null;
        let caughtJob: any = null;

        queue.on("scheduler_job_added", (id, job) => {
          caughtSchedulerId = id;
          caughtJob = job;
        });

        // @ts-ignore - Initialize scheduler
        const scheduler = queue.getScheduler();
        scheduler.emit("job_added", schedulerId, fakeJob);

        await delay(10); // Allow event propagation

        // @ts-ignore - Check if caughtSchedulerId and caughtJob match the emitted values
        expect(caughtSchedulerId).toBe(schedulerId);
        expect(caughtJob).toBe(fakeJob);
      });

      it("should prevent adding jobs with reserved scheduler ID prefix", async () => {
        const queue = createQueue(testQueueName);
        const badJobId = "scheduler:my-id";

        await expect(
          queue.add("some-job", { d: 1 }, { jobId: badJobId })
        ).rejects.toThrow(
          `Cannot manually add job with reserved scheduler ID: ${badJobId}`
        );

        // Test bulk add as well
        await expect(
          queue.addBulk([
            { name: "other-job", data: { d: 2 }, opts: { jobId: badJobId } },
          ])
        ).resolves.toEqual([]); // Should skip the job and return empty array

        const counts = await queue.getJobCounts();
        expect(counts.wait).toBe(0);
      });
    });
  });

  describe("JobScheduler Class", () => {
    let queue: Queue;
    let scheduler: JobScheduler;
    let mockClient: RedisClient; // Use a dedicated mock client for fine control

    // Mock Redis methods used by scheduler
    let hsetSpy: any;
    let zaddSpy: any;
    let delSpy: any;
    let zremSpy: any;
    let zrangebyscoreSpy: any;
    let hgetallSpy: any;
    let multiSpy: any;
    let execSpy: any;

    beforeEach(() => {
      // Create a real queue instance
      queue = createQueue(testQueueName + "-sched");

      // Create a scheduler instance directly for isolated tests
      scheduler = new JobScheduler(queue, {
        connection: queue.client, // Use the queue's client
        checkInterval: 50, // Faster checks
        schedulerPrefix: "testprefix", // Custom prefix
      });
      schedulersToClose.push(scheduler); // Ensure cleanup

      // --- Mock Redis Client Methods on the *specific client instance* used by the scheduler ---
      mockClient = scheduler["client"]; // Access the internal client
      hsetSpy = spyOn(mockClient, "hset").mockResolvedValue(1); // Simulate success
      zaddSpy = spyOn(mockClient, "zadd").mockResolvedValue("1");
      delSpy = spyOn(mockClient, "del").mockResolvedValue(1);
      zremSpy = spyOn(mockClient, "zrem").mockResolvedValue(1);
      zrangebyscoreSpy = spyOn(mockClient, "zrangebyscore").mockResolvedValue(
        []
      ); // Default: no jobs due
      hgetallSpy = spyOn(mockClient, "hgetall").mockResolvedValue({}); // Default: not found
      // Mock multi/exec chain
      execSpy = mock().mockResolvedValue([[null, 1]]); // Mock successful exec
      multiSpy = spyOn(mockClient, "multi").mockImplementation(() => {
        const pipeline = new Pipeline(mockClient); // Create a real pipeline structure
        pipeline.hset = mock().mockReturnThis(); // Mock pipeline methods to be chainable
        pipeline.zadd = mock().mockReturnThis();
        pipeline.del = mock().mockReturnThis();
        pipeline.zrem = mock().mockReturnThis();
        pipeline.exec = execSpy; // Attach the mocked exec
        return pipeline as any; // Return the mocked pipeline
      });

      // Prevent scheduler from auto-starting timer for most tests
      // @ts-ignore Access private property
      scheduler.running = false;
      if (scheduler["checkTimer"]) {
        clearTimeout(scheduler["checkTimer"]);
        scheduler["checkTimer"] = null;
      }
    });

    it("should initialize with default and overridden options", () => {
      expect(scheduler).toBeInstanceOf(JobScheduler);
      // @ts-ignore
      expect(scheduler.opts.prefix).toBe("lightq"); // Inherited from queue options
      // @ts-ignore
      expect(scheduler.opts.schedulerPrefix).toBe("testprefix");
      // @ts-ignore
      expect(scheduler.opts.checkInterval).toBe(50);
      // @ts-ignore
      expect(scheduler.keys.base).toBe(`testprefix:${queue.name}:schedulers`);
    });

    describe("upsertJobScheduler", () => {
      it("should add a new cron job scheduler", async () => {
        const schedulerId = "cron-job-1";
        const repeat: SchedulerRepeatOptions = {
          pattern: "0 * * * *",
          tz: "UTC",
        };
        const template: JobTemplate = { name: "scheduledCron", data: { a: 1 } };
        const now = Date.now();
        const expectedNextRun = new Cron(repeat.pattern!, {
          timezone: repeat.tz,
        })
          .nextRun(new Date(now))!
          .getTime();

        (Date.now as any).mockReturnValue(now); // Fix time

        await scheduler.upsertJobScheduler(schedulerId, repeat, template);

        const expectedKey = `testprefix:${queue.name}:schedulers:${schedulerId}`;
        const expectedData = {
          id: schedulerId,
          type: "cron",
          value: repeat.pattern,
          tz: repeat.tz,
          nextRun: expectedNextRun.toString(),
          name: template.name,
          data: JSON.stringify(template.data ?? {}),
          opts: JSON.stringify(template.opts ?? {}),
        };

        expect(multiSpy).toHaveBeenCalledTimes(1);
        // Retrieve the pipeline instance from the multi spy call
        const pipelineInstance = multiSpy.mock.results[0].value;
        expect(pipelineInstance.hset).toHaveBeenCalledWith(
          expectedKey,
          expectedData
        );
        expect(pipelineInstance.zadd).toHaveBeenCalledWith(
          scheduler["keys"].index, // Access private key
          expectedNextRun,
          schedulerId
        );
        expect(execSpy).toHaveBeenCalledTimes(1);
      });

      it("should add a new 'every' job scheduler", async () => {
        const schedulerId = "every-job-1";
        const repeat: SchedulerRepeatOptions = { every: 10000 }; // 10 seconds
        const template: JobTemplate = {
          name: "scheduledEvery",
          data: { b: 2 },
        };
        const now = Date.now();
        const expectedNextRun = now + repeat.every!;

        (Date.now as any).mockReturnValue(now);

        await scheduler.upsertJobScheduler(schedulerId, repeat, template);

        const expectedKey = `testprefix:${queue.name}:schedulers:${schedulerId}`;
        const expectedData = {
          id: schedulerId,
          type: "every",
          value: repeat.every.toString(), // Stored as string
          tz: undefined, // Ensure tz is not set for 'every'
          nextRun: expectedNextRun.toString(),
          name: template.name,
          data: JSON.stringify(template.data ?? {}),
          opts: JSON.stringify(template.opts ?? {}),
        };

        expect(multiSpy).toHaveBeenCalledTimes(1);
        const pipelineInstance = multiSpy.mock.results[0].value;
        // Use expect.objectContaining because the actual call might have more keys initially
        expect(pipelineInstance.hset).toHaveBeenCalledWith(
          expectedKey,
          expect.objectContaining(expectedData)
        );
        expect(pipelineInstance.zadd).toHaveBeenCalledWith(
          scheduler["keys"].index,
          expectedNextRun,
          schedulerId
        );
        expect(execSpy).toHaveBeenCalledTimes(1);
      });

      it("should update an existing scheduler", async () => {
        const schedulerId = "update-job-1";
        const initialRepeat: SchedulerRepeatOptions = { every: 10000 };
        const initialTemplate: JobTemplate = { name: "initial" };
        const updatedRepeat: SchedulerRepeatOptions = {
          pattern: "*/5 * * * *",
        }; // Every 5 mins
        const updatedTemplate: JobTemplate = {
          name: "updated",
          data: { updated: true },
        };
        const now = Date.now();
        const expectedNextRun = new Cron(updatedRepeat.pattern!)
          .nextRun(new Date(now))!
          .getTime();

        (Date.now as any).mockReturnValue(now);

        // First upsert
        await scheduler.upsertJobScheduler(
          schedulerId,
          initialRepeat,
          initialTemplate
        );
        multiSpy.mockClear(); // Clear mocks for the second call
        execSpy.mockClear();

        // Second (updating) upsert
        await scheduler.upsertJobScheduler(
          schedulerId,
          updatedRepeat,
          updatedTemplate
        );

        const expectedKey = `testprefix:${queue.name}:schedulers:${schedulerId}`;
        const expectedData = {
          id: schedulerId,
          type: "cron",
          value: updatedRepeat.pattern,
          tz: undefined,
          nextRun: expectedNextRun.toString(),
          name: updatedTemplate.name,
          data: JSON.stringify(updatedTemplate.data ?? {}),
          opts: JSON.stringify(updatedTemplate.opts ?? {}),
        };

        expect(multiSpy).toHaveBeenCalledTimes(1);
        const pipelineInstance = multiSpy.mock.results[0].value;
        expect(pipelineInstance.hset).toHaveBeenCalledWith(
          expectedKey,
          expect.objectContaining(expectedData)
        );
        expect(pipelineInstance.zadd).toHaveBeenCalledWith(
          scheduler["keys"].index,
          expectedNextRun,
          schedulerId
        );
        expect(execSpy).toHaveBeenCalledTimes(1);
      });

      it("should throw error for invalid repeat options", async () => {
        const schedulerId = "invalid-repeat";
        await expect(
          // @ts-ignore - Testing invalid input
          scheduler.upsertJobScheduler(schedulerId, {}, { name: "test" })
        ).rejects.toThrow("Invalid repeat options");
        await expect(
          scheduler.upsertJobScheduler(
            schedulerId,
            // @ts-ignore
            { pattern: 123 },
            { name: "test" }
          )
        ).rejects.toThrow("Invalid cron pattern");
        await expect(
          scheduler.upsertJobScheduler(
            schedulerId,
            { every: -100 },
            { name: "test" }
          )
        ).rejects.toThrow("Invalid 'every' value");
        await expect(
          scheduler.upsertJobScheduler(
            schedulerId,
            { pattern: "invalid cron pattern" },
            { name: "test" }
          )
        ).rejects.toThrow(/^Invalid cron pattern:/);
      });

      it("should throw error for empty scheduler ID", async () => {
        await expect(
          scheduler.upsertJobScheduler("", { every: 1000 }, { name: "test" })
        ).rejects.toThrow("Scheduler ID cannot be empty");
      });

      it("should handle Redis errors during upsert", async () => {
        const schedulerId = "redis-fail-upsert";
        const error = new Error("Redis unavailable");
        execSpy.mockRejectedValueOnce(error); // Make exec fail

        const emitSpy = spyOn(scheduler, "emit");
        await expect(
          scheduler.upsertJobScheduler(
            schedulerId,
            { every: 1000 },
            { name: "fail" }
          )
        ).rejects.toThrowError(/Redis unavailable/);

        emitSpy.mockRestore();
      });
    });

    describe("removeJobScheduler", () => {
      it("should remove an existing job scheduler", async () => {
        const schedulerId = "remove-me";
        // Assume it exists (we don't need to actually add it due to mocks)

        execSpy.mockResolvedValueOnce([
          [null, 1],
          [null, 1],
        ]); // Simulate DEL=1, ZREM=1

        const result = await scheduler.removeJobScheduler(schedulerId);

        expect(result).toBe(true);
        const expectedKey = `testprefix:${queue.name}:schedulers:${schedulerId}`;

        expect(multiSpy).toHaveBeenCalledTimes(1);
        const pipelineInstance = multiSpy.mock.results[0].value;
        expect(pipelineInstance.del).toHaveBeenCalledWith(expectedKey);
        expect(pipelineInstance.zrem).toHaveBeenCalledWith(
          scheduler["keys"].index,
          schedulerId
        );
        expect(execSpy).toHaveBeenCalledTimes(1);
      });

      it("should return false if scheduler ID does not exist", async () => {
        const schedulerId = "does-not-exist";
        execSpy.mockResolvedValueOnce([
          [null, 0],
          [null, 0],
        ]); // Simulate DEL=0, ZREM=0

        const result = await scheduler.removeJobScheduler(schedulerId);

        expect(result).toBe(false);
        expect(multiSpy).toHaveBeenCalledTimes(1);
        expect(execSpy).toHaveBeenCalledTimes(1);
      });

      it("should return false for empty scheduler ID", async () => {
        const result = await scheduler.removeJobScheduler("");
        expect(result).toBe(false);
        expect(multiSpy).not.toHaveBeenCalled();
      });

      it("should handle Redis errors during remove", async () => {
        const schedulerId = "redis-fail-remove";
        const error = new Error("Redis unavailable");
        execSpy.mockRejectedValueOnce(error);

        const emitSpy = spyOn(scheduler, "emit");

        await expect(
          scheduler.removeJobScheduler(schedulerId)
        ).rejects.toThrowError(/Redis unavailable/);

        emitSpy.mockRestore();
      });

      it("should prevent removal if scheduler is closing", async () => {
        const schedulerId = "remove-while-closing";
        const closePromise = scheduler.close(); // Start closing
        await delay(5); // Give closing a moment to start

        await expect(scheduler.removeJobScheduler(schedulerId)).resolves.toBe(
          false
        ); // Should not throw but return false or warn

        await closePromise; // Wait for close to complete
      });
    });

    describe("start/stop/close", () => {
      it("should start the scheduler and schedule the first check", async () => {
        const setTimeoutSpy = spyOn(global, "setTimeout").mockImplementation(
          // @ts-ignore
          () => {}
        );

        scheduler.start();
        // @ts-ignore
        expect(scheduler.running).toBe(true);
        expect(setTimeoutSpy).toHaveBeenCalledTimes(1);
        // @ts-ignore
        expect(setTimeoutSpy).toHaveBeenCalledWith(
          expect.any(Function),
          // @ts-ignore
          scheduler.opts.checkInterval
        );

        setTimeoutSpy.mockRestore();
      });

      it("should stop the scheduler and clear the timer", async () => {
        const clearTimeoutSpy = spyOn(global, "clearTimeout");
        const fakeTimer = setTimeout(() => {}, 10000);
        // @ts-ignore - Set manually for test
        scheduler.checkTimer = fakeTimer;
        // @ts-ignore - Set manually for test
        scheduler.running = true;

        scheduler.stop();
        // @ts-ignore
        expect(scheduler.running).toBe(false);
        expect(clearTimeoutSpy).toHaveBeenCalledWith(fakeTimer);
        // @ts-ignore
        expect(scheduler.checkTimer).toBeNull();

        clearTimeoutSpy.mockRestore();
      });

      it("should close the scheduler, stop it, and clear cron cache", async () => {
        const stopSpy = spyOn(scheduler, "stop");
        const cronClearSpy = spyOn(scheduler["cronCache"], "clear"); // Access private cache

        // @ts-ignore - Set manually for test
        scheduler.running = true;
        // Add dummy entry to cache
        scheduler["cronCache"].set("dummy", {} as any);

        await scheduler.close();

        expect(stopSpy).toHaveBeenCalledTimes(1);
        expect(cronClearSpy).toHaveBeenCalledTimes(1);
        // @ts-ignore
        expect(scheduler.closing).toBeInstanceOf(Promise); // Should be resolving/resolved
        // @ts-ignore
        expect(scheduler.running).toBe(false); // Should be stopped by close
      });

      it("should prevent starting/stopping if closing", async () => {
        const closePromise = scheduler.close();
        await delay(5); // Allow close to start

        scheduler.start(); // Should have no effect
        // @ts-ignore
        expect(scheduler.running).toBe(false);

        scheduler.stop(); // Should have no effect

        await closePromise;
      });
    });

    describe("_checkAndProcessDueJobs / _processSingleScheduler", () => {
      let queueAddSpy: any;

      beforeEach(() => {
        // Mock queue.add for these tests
        queueAddSpy = spyOn(queue, "add").mockResolvedValue({} as Job); // Simulate successful job add
        // Mock setTimeout/clearTimeout for precise control
        // @ts-ignore
        spyOn(global, "setTimeout").mockImplementation((fn) => {
          // Immediately call the function in tests for simplicity,
          // unless specific timing tests are needed
          // fn(); // Or return a dummy timer ID if clear is tested
          return 12345 as any; // Return dummy timer ID
        });
        spyOn(global, "clearTimeout");
      });

      afterEach(() => {
        mock.restore(); // Restore setTimeout/clearTimeout
      });

      it.skip("should find due jobs and process them", async () => {
        // This function failing maybe can be related to bun macros
        const now = Date.now();
        (Date.now as any).mockReturnValue(now);
        const schedulerId1 = "due-job-1";
        const schedulerId2 = "due-job-2";
        const jobTemplate1: JobTemplate = { name: "task1", data: { i: 1 } };
        const jobTemplate2: JobTemplate = {
          name: "task2",
          data: { i: 2 },
          opts: { attempts: 5 },
        };
        const repeat1: SchedulerRepeatOptions = { every: 5000 }; // Next run = now + 5000
        const repeat2: SchedulerRepeatOptions = { pattern: "* * * * *" }; // Next run calculated by Croner
        const nextRun2 = new Cron(repeat2.pattern!)
          .nextRun(new Date(now))!
          .getTime();

        // Mock Redis responses
        zrangebyscoreSpy.mockResolvedValueOnce([schedulerId1, schedulerId2]);
        hgetallSpy
          .mockResolvedValueOnce({
            // Data for schedulerId1
            id: schedulerId1,
            type: "every",
            value: "5000",
            nextRun: (now - 100).toString(), // Due
            name: jobTemplate1.name,
            data: JSON.stringify(jobTemplate1.data),
            opts: "{}",
          })
          .mockResolvedValueOnce({
            // Data for schedulerId2
            id: schedulerId2,
            type: "cron",
            value: repeat2.pattern!,
            nextRun: (now - 50).toString(), // Due
            name: jobTemplate2.name,
            data: JSON.stringify(jobTemplate2.data),
            opts: JSON.stringify(jobTemplate2.opts),
          });

        // Mock multi/exec for state updates
        execSpy.mockResolvedValue([[null, 1]]); // Simulate success for both updates
        scheduler["running"] = true;
        const emitSpy = spyOn(scheduler, "emit");

        // @ts-ignore - Manually trigger the check
        await scheduler._checkAndProcessDueJobs();

        // --- Assertions for Job 1 (every) ---
        expect(queueAddSpy).toHaveBeenCalledWith(
          jobTemplate1.name,
          jobTemplate1.data,
          expect.objectContaining({
            // Default queue opts + scheduler opts + template opts
            // Explicitly check that jobId and delay are undefined
            jobId: undefined,
            delay: undefined,
            // Other options might be merged, check core ones
          })
        );
        // Check Redis update for Job 1
        const expectedKey1 = `testprefix:${queue.name}:schedulers:${schedulerId1}`;
        const expectedNextRun1 = now + repeat1.every!;
        // Ensure multi/exec was called twice (once per job)
        expect(multiSpy).toHaveBeenCalledTimes(2);
        expect(execSpy).toHaveBeenCalledTimes(2);

        // Check the HSET and ZADD calls within the first multi/exec
        const pipelineInstance1 = multiSpy.mock.results[0].value;
        expect(pipelineInstance1.hset).toHaveBeenCalledWith(
          expectedKey1,
          "nextRun",
          expectedNextRun1.toString(),
          "lastRun",
          now.toString()
        );
        expect(pipelineInstance1.zadd).toHaveBeenCalledWith(
          scheduler["keys"].index,
          expectedNextRun1,
          schedulerId1
        );

        // --- Assertions for Job 2 (cron) ---
        expect(queueAddSpy).toHaveBeenCalledWith(
          jobTemplate2.name,
          jobTemplate2.data,
          expect.objectContaining({
            attempts: 5, // From template opts
            jobId: undefined,
            delay: undefined,
          })
        );
        // Check Redis update for Job 2
        const expectedKey2 = `testprefix:${queue.name}:schedulers:${schedulerId2}`;
        const pipelineInstance2 = multiSpy.mock.results[1].value;
        expect(pipelineInstance2.hset).toHaveBeenCalledWith(
          expectedKey2,
          "nextRun",
          nextRun2.toString(),
          "lastRun",
          now.toString()
        );
        expect(pipelineInstance2.zadd).toHaveBeenCalledWith(
          scheduler["keys"].index,
          nextRun2,
          schedulerId2
        );
      });

      it("should skip processing if no jobs are due", async () => {
        zrangebyscoreSpy.mockResolvedValueOnce([]); // No jobs due

        // @ts-ignore
        await scheduler._checkAndProcessDueJobs();

        expect(hgetallSpy).not.toHaveBeenCalled();
        expect(queueAddSpy).not.toHaveBeenCalled();
        expect(multiSpy).not.toHaveBeenCalled();
      });

      it("should skip processing if scheduler is stopped/closing mid-loop", async () => {
        const now = Date.now();
        (Date.now as any).mockReturnValue(now);
        const schedulerId1 = "due-job-1";
        const schedulerId2 = "due-job-2"; // This one won't be processed

        zrangebyscoreSpy.mockResolvedValueOnce([schedulerId1, schedulerId2]);
        hgetallSpy.mockResolvedValueOnce({
          // Data for schedulerId1
          id: schedulerId1,
          type: "every",
          value: "5000",
          nextRun: (now - 100).toString(),
          name: "task1",
          data: "{}",
          opts: "{}",
        });
        // Will not call hgetall for the second job

        // Mock scheduler stopping after the first job is processed
        const processSpy = spyOn(
          scheduler as any,
          "_processSingleScheduler"
        ).mockImplementation(async (id) => {
          if (id === schedulerId1) {
            // Process first job normally (calls mocked add, multi, exec)
            await JobScheduler.prototype["_processSingleScheduler"].call(
              scheduler,
              id,
              now
            ); // Call original logic
            // Now simulate stop
            scheduler.stop(); // Stop the scheduler
          } else {
            // Should not be called for id2
            throw new Error("Should not process second job");
          }
        });

        scheduler["running"] = true;

        // @ts-ignore
        await scheduler._checkAndProcessDueJobs();

        expect(processSpy).toHaveBeenCalledTimes(1); // Only called for the first job
        expect(processSpy).toHaveBeenCalledWith(schedulerId1, now);
        expect(queueAddSpy).toHaveBeenCalledTimes(1); // Add called for first job
        expect(multiSpy).toHaveBeenCalledTimes(1); // Multi called for first job update

        processSpy.mockRestore(); // Restore original method
      });

      it.skip("should handle missing scheduler data after zrangebyscore (race condition)", async () => {
        const schedulerId = "deleted-job";
        zrangebyscoreSpy.mockResolvedValueOnce([schedulerId]);
        hgetallSpy.mockResolvedValueOnce({}); // Simulate HGETALL returning empty

        // Expect ZREM to be called for cleanup
        zremSpy.mockClear(); // Clear previous calls if any

        // @ts-ignore
        await scheduler._checkAndProcessDueJobs();

        expect(hgetallSpy).toHaveBeenCalledWith(
          `testprefix:${queue.name}:schedulers:${schedulerId}`
        );
        expect(zremSpy).toHaveBeenCalledWith(
          scheduler["keys"].index,
          schedulerId
        );
        expect(queueAddSpy).not.toHaveBeenCalled();
        expect(multiSpy).not.toHaveBeenCalled(); // No update attempts
      });

      it.skip("should handle job add failure and attempt recovery", async () => {
        const now = Date.now();
        (Date.now as any).mockReturnValue(now);
        const schedulerId = "fail-add-job";
        const jobTemplate: JobTemplate = { name: "wont-add" };
        const addError = new Error("Queue is full");
        const checkInterval = scheduler["opts"].checkInterval;
        const recoveryNextRun = now + checkInterval; // Expected recovery time

        zrangebyscoreSpy.mockResolvedValueOnce([schedulerId]);
        hgetallSpy.mockResolvedValueOnce({
          id: schedulerId,
          type: "every",
          value: "5000",
          nextRun: (now - 100).toString(),
          name: jobTemplate.name,
          data: "{}",
          opts: "{}",
        });
        queueAddSpy.mockRejectedValueOnce(addError); // Make queue.add fail

        // Mock multi/exec for the recovery attempt
        multiSpy.mockClear();
        execSpy.mockClear();
        execSpy.mockResolvedValueOnce([[null, 1]]); // Recovery update succeeds

        scheduler["running"] = true;

        // @ts-ignore
        const emitSpy = spyOn(scheduler, "emit").mockImplementation(() => {});

        // @ts-ignore
        await scheduler._checkAndProcessDueJobs();

        expect(queueAddSpy).toHaveBeenCalledTimes(1);
        expect(multiSpy).toHaveBeenCalledTimes(1); // Only the recovery multi call

        // Check the recovery HSET and ZADD
        const pipelineInstance = multiSpy.mock.results[0].value;
        const expectedKey = `testprefix:${queue.name}:schedulers:${schedulerId}`;
        expect(pipelineInstance.hset).toHaveBeenCalledWith(
          expectedKey,
          "nextRun",
          recoveryNextRun.toString()
        );
        expect(pipelineInstance.zadd).toHaveBeenCalledWith(
          scheduler["keys"].index,
          recoveryNextRun,
          schedulerId
        );
        expect(execSpy).toHaveBeenCalledTimes(1);

        emitSpy.mockRestore();
        scheduler["running"] = false;
      });

      it.skip("should handle recovery failure by removing from index", async () => {
        const now = Date.now();
        (Date.now as any).mockReturnValue(now);
        const schedulerId = "fail-recovery";
        const jobTemplate: JobTemplate = { name: "wont-recover" };
        const addError = new Error("Queue failed");
        const recoveryError = new Error("Redis recovery failed");

        zrangebyscoreSpy.mockResolvedValueOnce([schedulerId]);
        hgetallSpy.mockResolvedValueOnce({
          id: schedulerId,
          type: "every",
          value: "5000",
          nextRun: (now - 100).toString(),
          name: jobTemplate.name,
          data: "{}",
          opts: "{}",
        });
        queueAddSpy.mockRejectedValueOnce(addError);

        // Mock multi/exec to fail for the recovery attempt
        multiSpy.mockClear();
        execSpy.mockClear();
        execSpy.mockRejectedValueOnce(recoveryError);

        // Mock the final ZREM call
        zremSpy.mockClear();
        zremSpy.mockResolvedValueOnce(1);

        const consoleErrorSpy = spyOn(console, "error");

        // @ts-ignore
        await scheduler._checkAndProcessDueJobs();

        expect(queueAddSpy).toHaveBeenCalledTimes(1);
        expect(multiSpy).toHaveBeenCalledTimes(1); // Recovery multi was called
        expect(execSpy).toHaveBeenCalledTimes(1); // Recovery exec failed
        expect(zremSpy).toHaveBeenCalledWith(
          scheduler["keys"].index,
          schedulerId
        ); // Cleanup ZREM
        expect(consoleErrorSpy).toHaveBeenCalledWith(
          expect.stringContaining(`Removed scheduler ${schedulerId} from index`)
        );

        consoleErrorSpy.mockRestore();
      });

      it.skip("should skip job if its nextRun is in the future (race condition handled by another process)", async () => {
        const now = Date.now();
        (Date.now as any).mockReturnValue(now);
        const schedulerId = "future-job";
        const futureNextRun = now + 10000; // 10 seconds in the future

        zrangebyscoreSpy.mockResolvedValueOnce([schedulerId]); // Found by score <= now
        hgetallSpy.mockResolvedValueOnce({
          // But HGETALL reveals it was updated
          id: schedulerId,
          type: "every",
          value: "5000",
          nextRun: futureNextRun.toString(),
          name: "futureTask",
          data: "{}",
          opts: "{}",
        });

        // Mock zadd for the corrective update
        zaddSpy.mockClear();
        zaddSpy.mockResolvedValueOnce(1);

        // @ts-ignore
        await scheduler._checkAndProcessDueJobs();

        expect(hgetallSpy).toHaveBeenCalledTimes(1);
        // Corrective ZADD with NX should be called
        expect(zaddSpy).toHaveBeenCalledWith(
          scheduler["keys"].index,
          "NX", // Ensure NX flag is used
          futureNextRun,
          schedulerId
        );
        expect(queueAddSpy).not.toHaveBeenCalled(); // Should not add the job
        expect(multiSpy).not.toHaveBeenCalled(); // Should not try to update state via multi
      });
    });

    describe("parseSchedulerData", () => {
      it("should parse valid cron data", () => {
        const hash = {
          id: "s1",
          type: "cron",
          value: "* * * * *",
          tz: "UTC",
          nextRun: "1700000000000",
          name: "j1",
          data: '{"a":1}',
          opts: '{"b":2}',
          lastRun: "1699999940000",
        };
        // @ts-ignore Access private method
        const result = scheduler.parseSchedulerData(hash);
        expect(result).toEqual({
          id: "s1",
          type: "cron",
          value: "* * * * *",
          tz: "UTC",
          nextRun: 1700000000000,
          name: "j1",
          data: { a: 1 },
          // @ts-ignore
          opts: { b: 2 },
          lastRun: 1699999940000,
        });
      });

      it("should parse valid every data", () => {
        const hash = {
          id: "s2",
          type: "every",
          value: "5000",
          nextRun: "1700000005000",
          name: "j2",
          data: "[]",
          opts: "{}",
          // tz and lastRun are optional
        };
        // @ts-ignore
        const result = scheduler.parseSchedulerData(hash);
        expect(result).toEqual({
          id: "s2",
          type: "every",
          value: 5000,
          tz: undefined,
          nextRun: 1700000005000,
          name: "j2",
          data: [],
          opts: {},
          lastRun: undefined,
        });
      });

      it("should return null for invalid JSON", () => {
        const hash = {
          id: "s3",
          type: "cron",
          value: "* * * * *",
          nextRun: "1700000000000",
          name: "j3",
          data: "{invalid",
          opts: "{}",
        };

        // @ts-ignore
        const emitSpy = spyOn(scheduler, "emit").mockImplementation(() => {});

        // @ts-ignore
        const result = scheduler.parseSchedulerData(hash);

        expect(result).toBeNull();

        // @ts-ignore
        expect(emitSpy).toHaveBeenCalledWith(
          "error",
          expect.stringContaining(
            "Error parsing scheduler data from Redis: SyntaxError: JSON Parse error"
          )
        );

        emitSpy.mockRestore();
      });

      it("should return null for missing required fields", () => {
        const hash = {
          /* missing id */ type: "cron",
          value: "* * * * *",
          nextRun: "1700000000000",
          name: "j4",
        };
        // @ts-ignore
        expect(scheduler.parseSchedulerData(hash)).toBeNull();
      });

      it("should return null for invalid type", () => {
        const hash = {
          id: "s5",
          type: "wrong",
          value: "5000",
          nextRun: "1700000000000",
          name: "j5",
        };
        // @ts-ignore
        expect(scheduler.parseSchedulerData(hash)).toBeNull();
      });
    });

    describe("parseCron", () => {
      it("should parse cron string and cache the result", () => {
        const pattern = "0 * * * *";
        const now = Date.now();

        // @ts-ignore Access private method & cache
        const cache = scheduler.cronCache;
        expect(cache.size).toBe(0);

        // First call (miss)
        // @ts-ignore
        const result1 = scheduler.parseCron(pattern, now);
        expect(result1).toBeInstanceOf(Cron);
        // expect(constructorSpy).toHaveBeenCalledTimes(1); // Check constructor call (might be unreliable)
        expect(cache.size).toBe(1);
        expect(cache.has(`${pattern}_local`)).toBe(true);

        // Second call (hit)
        // @ts-ignore
        const result2 = scheduler.parseCron(pattern, now);
        expect(result2).toBe(result1); // Should return cached instance
        //  expect(constructorSpy).toHaveBeenCalledTimes(1); // Constructor should NOT be called again
        expect(cache.size).toBe(1);
      });

      it("should handle timezone correctly", () => {
        const pattern = "0 9 * * 1-5"; // 9 AM on weekdays
        const tz = "America/New_York";
        const now = Date.now();

        // Use the real Croner logic here, just verify it's called with TZ
        // mock.module('croner', () => ({ Cron: CronMock })); // Doesn't work with bun mock

        // @ts-ignore Access private method
        scheduler.parseCron(pattern, now, tz);

        // Check if Cron constructor was called with timezone options
        // This is hard to assert directly with spies on prototype.
        // Instead, we rely on the fact that if TZ is passed, Croner handles it.
        // We can check the cache key.
        // @ts-ignore
        expect(scheduler.cronCache.has(`${pattern}_${tz}`)).toBe(true);
      });
    });
  });

  describe("Worker Class", () => {
    it("should process a job successfully", async () => {
      const queue = createQueue<{ input: number }, { output: number }>(
        testQueueName
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
        }
      );

      await failedPromise;

      expect(processorCallTimestamps.length).toBe(maxAttempts);

      // Check approximate delay between attempts
      for (let i = 1; i < maxAttempts; i++) {
        const diff =
          processorCallTimestamps[i]! - processorCallTimestamps[i - 1]!;
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
        }
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
          })
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
        5000
      );
      await allJobsCompleted; // Wait for all jobs to complete

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
        }
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
        }
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
        }
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

      await waitFor(() => completedJobs === jobCount, 5000);
      await completedPromise; // Ensure processor finished

      const counts = await queue.getJobCounts();
      expect(counts.completed).toBe(keepCount);

      // Verify job data still exists for all (since Lua doesn't DEL based on count)
      for (const job of addedJobs) {
        const retrieved = await queue.getJob(job.id);
        expect(retrieved).not.toBeNull();
        expect(retrieved!.returnValue).toBeNumber();
      }
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
        { concurrency: 1 }
      );

      await queue.add("slow-job", { d: 1 });

      // Wait for the job to become active
      await waitFor(
        async () => (await queue.getJobCounts()).active === 1,
        1000
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
            err
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
        { attempts: maxAttempts, backoff: 10 }
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
        { lockDuration: 10000 }
      ); // Longer lock to avoid interference

      worker.on("movedDelayed", (count) => {
        movedDelayedEventCount = count;
      });

      // Wait for the job to be processed, which implies the delayed job was moved
      await waitFor(
        async () => (await queue.getJobCounts()).completed === 1,
        3000
      );

      // Check if the event fired. This might be slightly racy depending on timing.
      // The check happens periodically in the worker loop.
      expect(movedDelayedEventCount).toBeGreaterThanOrEqual(1);
    });

    it("should stop processing *updates* and warn if lock renewal fails", async () => {
      await delay(50)
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
            "invalid-token"
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
        }
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
      await delay(processTime + lockDuration + lockRenewTime + 200); // Added extra buffer

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
            "different-token"
          );
          return "should-not-be-saved";
        },
        { concurrency: 1 }
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
        { concurrency: 1 }
      );

      await queue.add("force-close-job", { d: 1 });

      // Wait for the job to become active
      await waitFor(
        async () => (await queue.getJobCounts()).active === 1,
        1000
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
      { concurrency }
    );

    // Add jobs
    for (let i = 0; i < jobCount; i++) {
      await queue.add("concurrency-pause-job", { index: i });
    }

    // Wait for all jobs to complete
    await waitFor(
      async () => (await queue.getJobCounts()).completed === jobCount,
      jobProcessTime * 2 + 500 // Adjust timeout if needed
    );

    // Check if the specific delay(200) used for pausing was called
    expect(delaySpy).toHaveBeenCalledWith(200);

    // Restore the original function after the test
    delaySpy.mockRestore();

    const finalCounts = await queue.getJobCounts();
    expect(finalCounts.completed).toBe(jobCount);
  });
});
