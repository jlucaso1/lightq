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
import { type Job, JobScheduler, type Processor, Queue, Worker } from "../src";
import { delay } from "../src/utils";
import { testConnectionOpts } from "./test.utils";

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
		TName extends string = string,
	>(
		name: string,
		opts: Partial<Queue<TData, TResult, TName>["opts"]> = {},
	): Queue<TData, TResult, TName> => {
		const queue = new Queue<TData, TResult, TName>(name, {
			connection: { ...testConnectionOpts },
			...opts,
		});
		queuesToClose.push(queue);
		return queue;
	};

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
			connection: { ...testConnectionOpts },
			lockDuration: 5000,
			lockRenewTime: 2500,
			...opts,
		});
		workersToClose.push(worker);
		return worker;
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
						}: ${e.message}`,
					),
				),
			),
		);
		await Promise.all([
			...workersToClose.map((w) =>
				w
					.close()
					.catch((e) =>
						console.error(`Error closing worker ${w.name}: ${e.message}`),
					),
			),
			...queuesToClose.map((q) =>
				q
					.close()
					.catch((e) =>
						console.error(`Error closing queue ${q.name}: ${e.message}`),
					),
			),
		]);
		workersToClose = [];
		queuesToClose = [];
		schedulersToClose = [];
		mock.restore();
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
				await delay(50);
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
			await completedPromise;

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
			expect(retrievedJob!.lockToken).toBeUndefined();
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
						if (job?.attemptsMade === maxAttempts) {
							resolve();
						}
					}
				});
			});

			const addedJob = await queue.add("fail-job", jobData, {
				attempts: maxAttempts,
			});

			await failedPromise;

			expect(attemptsMade).toBe(maxAttempts);
			expect(failedJob).not.toBeNull();
			expect(failedJob!.id).toBe(addedJob.id);
			expect(failedJob!.attemptsMade).toBe(maxAttempts);
			expect(receivedError).toBe(failError as any);

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
			const backoffDelay = 100;
			const processorCallTimestamps: number[] = [];

			// Start scheduler to handle delayed jobs (required for backoff retries)
			const scheduler = new JobScheduler(queue, {
				connection: { ...testConnectionOpts },
				checkInterval: 50,
			});
			schedulersToClose.push(scheduler);
			scheduler.start();

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

			for (let i = 1; i < maxAttempts; i++) {
				const diff =
					processorCallTimestamps[i]! - processorCallTimestamps[i - 1]!;
				expect(diff).toBeGreaterThanOrEqual(backoffDelay - 20);
				expect(diff).toBeLessThan(backoffDelay + 500);
			}

			// Replace the count check with this waitFor block
			await waitFor(
				async () => (await queue.getJobCounts()).failed === 1,
				2000,
			);
			const counts = await queue.getJobCounts();
			expect(counts.failed).toBe(1);
		});

		it("should handle exponential backoff strategy", async () => {
			const queue = createQueue<{ fail: boolean }>(testQueueName);
			const maxAttempts = 3;
			const initialDelay = 100;
			const processorCallTimestamps: number[] = [];

			// Start scheduler to handle delayed jobs (required for backoff retries)
			const scheduler = new JobScheduler(queue, {
				connection: { ...testConnectionOpts },
				checkInterval: 50,
			});
			schedulersToClose.push(scheduler);
			scheduler.start();

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

			const diff1 = processorCallTimestamps[1]! - processorCallTimestamps[0]!;
			const diff2 = processorCallTimestamps[2]! - processorCallTimestamps[1]!;

			expect(diff1).toBeGreaterThanOrEqual(initialDelay - 20);
			expect(diff1).toBeLessThan(initialDelay + 500);

			const expectedSecondDelay = initialDelay * 2;
			expect(diff2).toBeGreaterThanOrEqual(expectedSecondDelay - 40);
			expect(diff2).toBeLessThan(expectedSecondDelay + 1100);

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
							completedCount++;
							if (completedCount === jobCount) {
							}
							resolve();
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
			await allJobsCompleted;

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

			await delay(100);

			const retrievedJob = await queue.getJob(job.id);
			expect(retrievedJob).toBeNull();

			const counts = await queue.getJobCounts();
			expect(counts.completed).toBe(0);
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
			const job = await queue.add("fail-remove", { d: 1 }, { attempts: 1 });

			await waitFor(async () => (await queue.getJob(job.id)) === null, 2000);

			await delay(100);

			const retrievedJob = await queue.getJob(job.id);
			expect(retrievedJob).toBeNull();

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
					removeOnComplete: keepCount,
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

			await waitFor(() => completedJobs === jobCount, 5000);
			await completedPromise;

			const counts = await queue.getJobCounts();
			expect(counts.completed).toBe(keepCount);

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
				{ concurrency: 1 },
			);

			await queue.add("slow-job", { d: 1 });

			const closePromise = worker.close();

			await delay(jobProcessTime / 2);
			expect(jobFinished).toBe(false);

			await closePromise;

			expect(jobFinished).toBe(true);

			const counts = await queue.getJobCounts();
			expect(counts.completed).toBe(1);
			expect(counts.active).toBe(0);

			workersToClose = workersToClose.filter((w) => w !== worker);
		});

		it("should emit an error event for processor errors not caught by try/catch", async () => {
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

			// Start scheduler to handle delayed jobs (required for backoff retries)
			const scheduler = new JobScheduler(queue, {
				connection: { ...testConnectionOpts },
				checkInterval: 50,
			});
			schedulersToClose.push(scheduler);
			scheduler.start();

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

			await waitFor(() => retryEventEmitted, 2000);

			expect(retryEventEmitted).toBe(true);
			expect(retryingJob).not.toBeNull();
			expect(retryingJob!.id).toBe(addedJob.id);
			expect(retryingJob!.attemptsMade).toBe(1);
			expect(retryError).toBe(failError as any);

			await failedPromise;
		});

		it("should emit 'movedDelayed' event when delayed jobs are moved", async () => {
			const queue = createQueue(testQueueName);
			let movedDelayedEventCount = 0;

			await queue.add("delayed-job-event", { d: 1 }, { delay: 100 });

			const movedDelayedPromise = new Promise<void>((resolve) => {
				const worker = createWorker(
					testQueueName,
					async (job) => {
						await delay(10);
						return "ok";
					},
					{ lockDuration: 10000 },
				);

				// Start scheduler to handle delayed jobs
				const scheduler = new JobScheduler(queue, {
					connection: { ...testConnectionOpts },
					checkInterval: 50,
				});
				schedulersToClose.push(scheduler);
				scheduler.start();

				// Listen for movedDelayed event on the queue (emitted by scheduler)
				queue.on("movedDelayed", (count) => {
					movedDelayedEventCount = count;
					resolve();
				});
			});

			await movedDelayedPromise;
			expect(movedDelayedEventCount).toBeGreaterThanOrEqual(1);

			await waitFor(
				async () => (await queue.getJobCounts()).completed === 1,
				3000,
			);
		});

		it("should stop processing *updates* and warn if lock renewal fails", async () => {
			await delay(50);
			const queue = createQueue(testQueueName);
			const lockDuration = 100;
			const lockRenewTime = 50;
			const processTime = 300;
			let processorFinishedExecuting = false;
			let jobFailedEvent = false;
			let jobCompletedEvent = false;
			let lockWarningLogged = false;

			const consoleWarnSpy = spyOn(console, "warn");

			const worker = createWorker(
				testQueueName,
				async (job) => {
					await delay(lockRenewTime + 20);
					await redisClient.hset(
						`${queue.keys.jobs}:${job.id}`,
						"lockToken",
						"invalid-token",
					);
					await delay(processTime);

					processorFinishedExecuting = true;
					return "ok";
				},
				{
					concurrency: 1,
					lockDuration: lockDuration,
					lockRenewTime: lockRenewTime,
				},
			);

			worker.on("completed", () => {
				jobCompletedEvent = true;
			});
			worker.on("failed", () => {
				jobFailedEvent = true;
			});
			consoleWarnSpy.mockImplementation((message: string, ...args) => {
				if (
					typeof message === "string" &&
					message.includes("Failed to renew lock for job")
				) {
					lockWarningLogged = true;
				}
			});

			const job = await queue.add("lock-renewal-fail", { d: 1 });

			await delay(processTime + lockDuration + lockRenewTime + 200);

			expect(lockWarningLogged).toBe(true);
			expect(processorFinishedExecuting).toBe(true);
			expect(jobCompletedEvent).toBe(false);
			expect(jobFailedEvent).toBe(false);

			await waitFor(() => !(worker as any).jobsInFlight.has(job.id), 1000, 50);
			expect((worker as any).jobsInFlight.has(job.id)).toBe(false);

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
			let jobFailed = false;

			const worker = createWorker(
				testQueueName,
				async (job) => {
					await delay(50);
					await redisClient.hset(
						`${queue.keys.jobs}:${job.id}`,
						"lockToken",
						"different-token",
					);
					return "should-not-be-saved";
				},
				{ concurrency: 1 },
			);

			worker.on("completed", () => {
				jobCompleted = true;
			});
			worker.on("failed", () => {
				jobFailed = true;
			});

			const job = await queue.add("lock-mismatch-complete", { d: 1 });

			await delay(500);

			expect(jobCompleted).toBe(false);

			const finalCounts = await queue.getJobCounts();
			expect(finalCounts.completed).toBe(0);

			const finalJobState = await queue.getJob(job.id);
			expect(finalJobState).not.toBeNull();
			expect(finalJobState!.returnValue).toBeUndefined();
			expect(finalJobState!.finishedOn).toBeUndefined();
			expect(finalCounts.active).toBe(1);
			expect(jobFailed).toBe(false);
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
					await delay(jobProcessTime);
					jobFinished = true;
					return "done";
				},
				{ concurrency: 1 },
			);

			await queue.add("force-close-job", { d: 1 });

			await waitFor(
				async () => (await queue.getJobCounts()).active === 1,
				1000,
			);

			await waitFor(async () => jobStarted === true, 1000);
			expect(jobStarted).toBe(true);

			const closeStartTime = Date.now();
			await worker.close(true);
			const closeEndTime = Date.now();

			expect(closeEndTime - closeStartTime).toBeLessThan(jobProcessTime);

			expect(jobFinished).toBe(false);

			const counts = await queue.getJobCounts();
			expect(counts.active).toBe(1);
			expect(counts.completed).toBe(0);

			workersToClose = workersToClose.filter((w) => w !== worker);
		});
	});
});
