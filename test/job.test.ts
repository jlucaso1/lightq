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
import { Job, type JobData, type JobScheduler, Queue, type Worker } from "../src";
import { testConnectionOpts } from "./test.utils";

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
        (s as any).close().catch((e) =>
          console.error(
            `Error closing scheduler for queue ${
              (s as any).queue.name // Access private queue ref
            }: ${e.message}`
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
      const job = Job.fromData(jobData);
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
      const job = Job.fromData(jobDataInput);
      const jobDataOutput = job.toData();

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
      expect(job!.stacktrace).toEqual([]);
      expect(job!.lockedUntil).toBeUndefined();
      expect(job!.lockToken).toBeUndefined();
    });

    it("should update progress and publish to progress channel", async () => {
      const queue = createQueue(testQueueName);
      const job = await queue.add("progress-job", { d: 1 });

      const hsetSpy = spyOn(queue.client, "hset").mockResolvedValue(1);
      const publishSpy = spyOn(queue.client, "publish").mockResolvedValue(1);

      await job.updateProgress(50);

      expect(hsetSpy).toHaveBeenCalledWith(
        `${queue.keys.jobs}:${job.id}`,
        "progress",
        JSON.stringify(50)
      );
      expect(publishSpy).toHaveBeenCalledWith(
        `${queue.keys.base}:progress`,
        expect.stringContaining(`"jobId":"${job.id}"`)
      );
    });
  });
});
