// bench/benchmark.ts
import { Queue as LightQQueue } from "@jlucaso/lightq";
import type { JobOptions as LightQJobOptions } from "@jlucaso/lightq";
import { Queue as BullMQQueue, Worker as BullMQWorker } from "bullmq";
import Redis, { type RedisOptions } from "ioredis";
import { bench, run, summary } from "mitata";
import { heapStats } from "bun:jsc";
import { randomUUID } from "node:crypto";

// --- Configuration ---
const redisConnectionOpts: RedisOptions = {
  host: process.env.REDIS_HOST || "127.0.0.1",
  port: process.env.REDIS_PORT ? parseInt(process.env.REDIS_PORT, 10) : 6379,
  maxRetriesPerRequest: null,
  enableReadyCheck: false,
};

const LIGHTQ_QUEUE_NAME = "lightq-benchmark-queue";
const BULLMQ_QUEUE_NAME = "bullmq-benchmark-queue";
const CONCURRENCY = 10; // Lower concurrency might be better for memory tests to reduce overlap
const BULK_JOB_COUNT = 50; // Lower bulk count might also help isolate memory per-op

console.log("--- Benchmark Setup ---");
console.log(
  `Redis Host: ${redisConnectionOpts.host}:${redisConnectionOpts.port}`
);
console.log(`Concurrency per bench op: ${CONCURRENCY}`);
console.log(`Bulk job count: ${BULK_JOB_COUNT}`);
console.log(
  "Ensure Redis is running and you have cleared the DB (e.g., bun run clean-redis) before starting."
);
console.log("--- MEMORY BENCHMARK MODE ---");
console.log(
  "Using .gc('inner') - ops/sec will be lower due to forced GC. Focus on memory stats in mitata output."
);
console.log("-----------------------\n");

// --- Shared Job Data ---
interface BenchmarkJobData {
  message: string;
  timestamp: number;
  id: string;
}

// --- Helper function for readable bytes ---
function formatBytes(bytes: number, decimals = 2): string {
  if (bytes === 0) return "0 Bytes";
  const k = 1024;
  const dm = decimals < 0 ? 0 : decimals;
  const sizes = ["Bytes", "KB", "MB", "GB", "TB", "PB", "EB", "ZB", "YB"];
  const i = Math.floor(Math.log(bytes) / Math.log(k));
  return parseFloat((bytes / Math.pow(k, i)).toFixed(dm)) + " " + sizes[i];
}

// --- LightQ Setup ---
// Initialize outside the run block to capture setup memory cost in initial stats
const lightqRedisClient = new Redis(redisConnectionOpts);
const lightqQueue = new LightQQueue<BenchmarkJobData, void>(LIGHTQ_QUEUE_NAME, {
  connection: lightqRedisClient,
  defaultJobOptions: {
    removeOnComplete: true,
    removeOnFail: true,
    attempts: 1,
  },
});

// --- BullMQ Setup ---
const bullmqRedisClient = new Redis(redisConnectionOpts);
const bullmqQueue = new BullMQQueue<BenchmarkJobData, void>(BULLMQ_QUEUE_NAME, {
  connection: bullmqRedisClient,
  defaultJobOptions: {
    removeOnComplete: true,
    removeOnFail: true,
    attempts: 1,
  },
});

// --- Define Benchmarks within Summary Blocks ---

summary(() => {
  bench("[LightQ] add single job", async () => {
    const promises: Promise<any>[] = [];
    for (let i = 0; i < CONCURRENCY; i++) {
      promises.push(
        lightqQueue.add("single-job", {
          message: `LightQ Job ${i}`,
          timestamp: Date.now(),
          id: randomUUID(),
        })
      );
    }
    await Promise.all(promises);
  }).gc("inner"); // Force GC before each inner iteration

  bench("[BullMQ] add single job", async () => {
    const promises: Promise<any>[] = [];
    for (let i = 0; i < CONCURRENCY; i++) {
      promises.push(
        bullmqQueue.add("single-job", {
          message: `BullMQ Job ${i}`,
          timestamp: Date.now(),
          id: randomUUID(),
        })
      );
    }
    await Promise.all(promises);
  }).gc("inner"); // Force GC before each inner iteration
});

summary(() => {
  bench(`[LightQ] add ${BULK_JOB_COUNT} bulk jobs`, async () => {
    const jobs = [];
    for (let i = 0; i < BULK_JOB_COUNT; i++) {
      jobs.push({
        name: "bulk-job" as const,
        data: {
          message: `LightQ Bulk Job ${i}`,
          timestamp: Date.now(),
          id: randomUUID(),
        },
        opts: {} as LightQJobOptions,
      });
    }
    await lightqQueue.addBulk(jobs);
  }).gc("inner"); // Force GC before each inner iteration

  bench(`[BullMQ] add ${BULK_JOB_COUNT} bulk jobs`, async () => {
    const jobs = [];
    for (let i = 0; i < BULK_JOB_COUNT; i++) {
      jobs.push({
        name: "bulk-job",
        data: {
          message: `BullMQ Bulk Job ${i}`,
          timestamp: Date.now(),
          id: randomUUID(),
        },
        opts: {},
      });
    }
    await bullmqQueue.addBulk(jobs);
  }).gc("inner"); // Force GC before each inner iteration
});

// --- Run Benchmarks ---
(async () => {
  let initialMemory: ReturnType<typeof heapStats> | null = null;
  let finalMemory: ReturnType<typeof heapStats> | null = null;

  try {
    console.log("Waiting for queues to be ready...");
    await Promise.all([
      new Promise<void>((resolve) => lightqQueue.once("ready", resolve)),
      new Promise<void>((resolve) =>
        // @ts-ignore
        bullmqQueue.waitUntilReady().then(resolve)
      ),
    ]);
    console.log("Queues are ready.");

    console.log("Running initial GC...");
    Bun.gc(true); // Force GC before initial measurement
    initialMemory = heapStats();
    console.log(
      `Initial Heap Stats: Size=${formatBytes(
        initialMemory.heapSize
      )}, Capacity=${formatBytes(
        initialMemory.heapCapacity
      )}, Extra=${formatBytes(initialMemory.extraMemorySize)}`
    );

    console.log("\nStarting benchmarks (with inner GC enabled)...\n");

    await run({
      // No need to force GC here in run options, .gc('inner') handles it per bench
      colors: true,
    });

    console.log("\nRunning final GC...");
    Bun.gc(true); // Force GC before final measurement
    finalMemory = heapStats();
    console.log(
      `Final Heap Stats:   Size=${formatBytes(
        finalMemory.heapSize
      )}, Capacity=${formatBytes(
        finalMemory.heapCapacity
      )}, Extra=${formatBytes(finalMemory.extraMemorySize)}`
    );

    if (initialMemory && finalMemory) {
      const heapSizeDiff = finalMemory.heapSize - initialMemory.heapSize;
      const heapCapacityDiff =
        finalMemory.heapCapacity - initialMemory.heapCapacity;
      const extraMemoryDiff =
        finalMemory.extraMemorySize - initialMemory.extraMemorySize;
      console.log("--- Overall Process Heap Diff ---");
      console.log(`Heap Size Change:      ${formatBytes(heapSizeDiff)}`);
      console.log(`Heap Capacity Change:  ${formatBytes(heapCapacityDiff)}`);
      console.log(`Extra Memory Change:   ${formatBytes(extraMemoryDiff)}`);
      console.log("---------------------------------");
    }
  } catch (error) {
    console.error("Benchmark run failed:", error);
  } finally {
    // --- Cleanup ---
    console.log("\nClosing connections...");
    try {
      await Promise.allSettled([
        lightqQueue.close(),
        bullmqQueue.close(),
        lightqRedisClient.quit(),
        bullmqRedisClient.quit(),
      ]);
      console.log("Connections closed.");
    } catch (closeError) {
      console.error("Error during cleanup:", closeError);
    }
    // Disconnect forcefully if quit hangs or fails
    lightqRedisClient.disconnect();
    bullmqRedisClient.disconnect();
  }
})();
