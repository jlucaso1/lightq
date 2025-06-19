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
import {
  Job,
  JobScheduler,
  type JobTemplate,
  Queue,
  type SchedulerRepeatOptions,
  type Worker,
} from "../src";
import { testConnectionOpts } from "./test.utils";
import { delay } from "../src/utils";

describe("LightQ (lightq)", () => {
  let testQueueName: string;
  let redisClient: Redis;
  let queuesToClose: Queue<any, any, any>[] = [];
  let workersToClose: Worker<any, any, any>[] = [];
  let schedulersToClose: JobScheduler[] = [];

  const generateQueueName = (base = "test-queue") =>
    `${base}:${Date.now()}:${Math.random().toString(36).substring(7)}`;

  const createQueue = <
    TData = any,
    TResult = any,
    TName extends string = string
  >(
    name: string,
    opts: Partial<Queue<TData, TResult, TName>["opts"]> = {}
  ): Queue<TData, TResult, TName> => {
    const queue = new Queue<TData, TResult, TName>(name, {
      connection: { ...testConnectionOpts },
      ...opts,
    });
    queuesToClose.push(queue);
    return queue;
  };

  beforeAll(async () => {
    redisClient = new IORedis(testConnectionOpts);
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
    await redisClient.flushdb();
    mock.restore();
    spyOn(Date, "now");
  });

  afterEach(async () => {
    mock.restore();
    await Promise.all(
      schedulersToClose.map((s) =>
        (s as any)
          .close()
          .catch((e) =>
            console.error(
              `Error closing scheduler for queue ${(s as any).queue.name}: ${
                e.message
              }`
            )
          )
      )
    );
    await Promise.all([
      ...workersToClose.map((w) =>
        w
          .close()
          .catch((e) =>
            console.error(`Error closing worker ${w.name}: ${e.message}`)
          )
      ),
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
      expect(job.opts.attempts).toBe(1);

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

      const keys = queue.keys;
      await delay(delayMs + 100);
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
      await queue.add("job2", { d: 2 }, { delay: 5000 });

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
      await expect(queue.add("job-after-close", { d: 2 })).rejects.toThrow(
        "Queue is closing"
      );

      queuesToClose = queuesToClose.filter((q) => q !== queue);
    });

    it("should not add a job if job ID already exists", async () => {
      const queue = createQueue(testQueueName);
      const jobId = "duplicate-job-id";
      const job1 = await queue.add("job-type-1", { val: 1 }, { jobId });
      const initialCounts = await queue.getJobCounts();
      expect(initialCounts.wait).toBe(1);

      const job2 = await queue.add("job-type-2", { val: 2 }, { jobId });

      expect(job2.id).toBe(jobId);

      const finalCounts = await queue.getJobCounts();
      expect(finalCounts.wait).toBe(1);
      expect(finalCounts.delayed).toBe(0);

      const retrievedJob = await queue.getJob(jobId);
      expect(retrievedJob).not.toBeNull();
      expect(retrievedJob!.name).toBe("job-type-1");
      expect(retrievedJob!.data).toEqual({ val: 1 });
    });

    it("should close the queue connection", async () => {
      const connectionStatusClient = new IORedis({ ...testConnectionOpts });
      queuesToClose.push({
        client: connectionStatusClient,
        close: async () => {
          connectionStatusClient.disconnect();
        },
      } as any);

      const sharedQueue = new Queue(testQueueName, {
        connection: connectionStatusClient,
      });
      queuesToClose.push(sharedQueue);

      await sharedQueue.add("job-before-close", { d: 1 });
      const statusBefore = connectionStatusClient.status;
      expect(statusBefore).toBeOneOf(["connecting", "connect", "ready"]);

      await sharedQueue.close();

      await delay(50);

      await expect(
        sharedQueue.add("job-after-close", { d: 2 })
      ).rejects.toThrow("Queue is closing");

      queuesToClose = queuesToClose.filter((q) => q !== sharedQueue);
      queuesToClose = queuesToClose.filter(
        (q) => q.client !== connectionStatusClient
      );
      await connectionStatusClient.quit().catch(() => {});
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
        expect(schedulerInstance.running).toBe(true);

        // @ts-ignore - Access private method
        const schedulerInstance2 = queue.getScheduler();
        expect(schedulerInstance2).toBe(schedulerInstance);
      });

      it("should upsert a job scheduler via the queue method", async () => {
        const queue = createQueue(testQueueName);
        const schedulerId = "q-upsert-test";
        const repeat: SchedulerRepeatOptions = { every: 5000 };
        const template: JobTemplate = {
          name: "scheduled-task",
          data: { x: 1 },
        };

        const schedulerUpsertSpy = spyOn(
          JobScheduler.prototype,
          "upsertJobScheduler"
        ).mockResolvedValue(undefined);

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

        await delay(10);

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

        await expect(
          queue.addBulk([
            { name: "other-job", data: { d: 2 }, opts: { jobId: badJobId } },
          ])
        ).resolves.toEqual([]);

        const counts = await queue.getJobCounts();
        expect(counts.wait).toBe(0);
      });
    });
  });
});
