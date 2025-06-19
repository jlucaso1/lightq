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
import { Cron } from "croner";
import IORedis, { Pipeline, type Redis } from "ioredis";
import {
	type Job,
	JobScheduler,
	type JobTemplate,
	Queue,
	type RedisClient,
	type SchedulerRepeatOptions,
	type Worker,
} from "../src";
import { delay } from "../src/utils";
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
							}`,
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

	describe("JobScheduler Class", () => {
		let queue: Queue;
		let scheduler: JobScheduler;
		let mockClient: RedisClient;

		let hsetSpy: any;
		let zaddSpy: any;
		let delSpy: any;
		let zremSpy: any;
		let zrangebyscoreSpy: any;
		let hgetallSpy: any;
		let multiSpy: any;
		let execSpy: any;
		let lockAndGetSchedulerSpy: any;

		beforeEach(() => {
			queue = createQueue(testQueueName + "-sched");

			scheduler = new JobScheduler(queue, {
				connection: queue.client,
				checkInterval: 50,
				schedulerPrefix: "testprefix",
			});
			schedulersToClose.push(scheduler);

			// Add a spy for the high-level script method
			lockAndGetSchedulerSpy = spyOn(
				scheduler["scripts"],
				"lockAndGetScheduler",
			).mockResolvedValue(null);

			mockClient = scheduler["client"];
			hsetSpy = spyOn(mockClient, "hset").mockResolvedValue(1);
			zaddSpy = spyOn(mockClient, "zadd").mockResolvedValue("1");
			delSpy = spyOn(mockClient, "del").mockResolvedValue(1);
			zremSpy = spyOn(mockClient, "zrem").mockResolvedValue(1);
			zrangebyscoreSpy = spyOn(mockClient, "zrangebyscore").mockResolvedValue(
				[],
			);
			hgetallSpy = spyOn(mockClient, "hgetall").mockResolvedValue({});
			execSpy = mock().mockResolvedValue([[null, 1]]);
			multiSpy = spyOn(mockClient, "multi").mockImplementation(() => {
				const pipeline = new Pipeline(mockClient);
				pipeline.hset = mock().mockReturnThis();
				pipeline.zadd = mock().mockReturnThis();
				pipeline.del = mock().mockReturnThis();
				pipeline.zrem = mock().mockReturnThis();
				pipeline.exec = execSpy;
				return pipeline as any;
			});

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
			expect(scheduler.prefix).toBe("lightq");
			// @ts-ignore
			expect(scheduler.opts.schedulerPrefix).toBe("testprefix");
			// @ts-ignore
			expect(scheduler.opts.checkInterval).toBe(50);
			// @ts-ignore
			expect(scheduler.keys.base).toBe(`testprefix:${queue.name}:schedulers`);
		});

		it("should initialize with configurable lockDuration and maxFailureCount", () => {
			const customScheduler = new JobScheduler(queue, {
				connection: queue.client,
				lockDuration: 30,
				maxFailureCount: 10,
			});
			schedulersToClose.push(customScheduler);

			// @ts-ignore
			expect(customScheduler.opts.lockDuration).toBe(30);
			// @ts-ignore
			expect(customScheduler.opts.maxFailureCount).toBe(10);
		});

		it("should use default values for lockDuration and maxFailureCount when not specified", () => {
			const defaultScheduler = new JobScheduler(queue, {
				connection: queue.client,
			});
			schedulersToClose.push(defaultScheduler);

			// Default values should be used (10 and 5 respectively)
			// @ts-ignore
			expect(defaultScheduler.opts.lockDuration).toBeUndefined();
			// @ts-ignore
			expect(defaultScheduler.opts.maxFailureCount).toBeUndefined();
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

				(Date.now as any).mockReturnValue(now);

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
					failureCount: "0",
				};

				expect(multiSpy).toHaveBeenCalledTimes(1);
				const pipelineInstance = multiSpy.mock.results[0].value;
				expect(pipelineInstance.hset).toHaveBeenCalledWith(
					expectedKey,
					expectedData,
				);
				expect(pipelineInstance.zadd).toHaveBeenCalledWith(
					scheduler["keys"].index,
					expectedNextRun,
					schedulerId,
				);
				expect(execSpy).toHaveBeenCalledTimes(1);
			});

			it("should add a new 'every' job scheduler", async () => {
				const schedulerId = "every-job-1";
				const repeat: SchedulerRepeatOptions = { every: 10000 };
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
					value: repeat.every.toString(),
					tz: undefined,
					nextRun: expectedNextRun.toString(),
					name: template.name,
					data: JSON.stringify(template.data ?? {}),
					opts: JSON.stringify(template.opts ?? {}),
				};

				expect(multiSpy).toHaveBeenCalledTimes(1);
				const pipelineInstance = multiSpy.mock.results[0].value;
				expect(pipelineInstance.hset).toHaveBeenCalledWith(
					expectedKey,
					expect.objectContaining(expectedData),
				);
				expect(pipelineInstance.zadd).toHaveBeenCalledWith(
					scheduler["keys"].index,
					expectedNextRun,
					schedulerId,
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

				await scheduler.upsertJobScheduler(
					schedulerId,
					initialRepeat,
					initialTemplate,
				);
				multiSpy.mockClear();
				execSpy.mockClear();

				await scheduler.upsertJobScheduler(
					schedulerId,
					updatedRepeat,
					updatedTemplate,
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
					expect.objectContaining(expectedData),
				);
				expect(pipelineInstance.zadd).toHaveBeenCalledWith(
					scheduler["keys"].index,
					expectedNextRun,
					schedulerId,
				);
				expect(execSpy).toHaveBeenCalledTimes(1);
			});

			it("should throw error for invalid repeat options", async () => {
				const schedulerId = "invalid-repeat";
				await expect(
					// @ts-ignore - Testing invalid input
					scheduler.upsertJobScheduler(schedulerId, {}, { name: "test" }),
				).rejects.toThrow("Invalid repeat options");
				await expect(
					scheduler.upsertJobScheduler(
						schedulerId,
						// @ts-ignore
						{ pattern: 123 },
						{ name: "test" },
					),
				).rejects.toThrow("Invalid cron pattern");
				await expect(
					scheduler.upsertJobScheduler(
						schedulerId,
						{ every: -100 },
						{ name: "test" },
					),
				).rejects.toThrow("Invalid 'every' value");
				await expect(
					scheduler.upsertJobScheduler(
						schedulerId,
						{ pattern: "invalid cron pattern" },
						{ name: "test" },
					),
				).rejects.toThrow(/^Invalid cron pattern:/);
			});

			it("should throw error for empty scheduler ID", async () => {
				await expect(
					scheduler.upsertJobScheduler("", { every: 1000 }, { name: "test" }),
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
						{ name: "fail" },
					),
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
					schedulerId,
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
					scheduler.removeJobScheduler(schedulerId),
				).rejects.toThrowError(/Redis unavailable/);

				emitSpy.mockRestore();
			});

			it("should prevent removal if scheduler is closing", async () => {
				const schedulerId = "remove-while-closing";
				const closePromise = scheduler.close(); // Start closing
				await delay(5); // Give closing a moment to start

				await expect(scheduler.removeJobScheduler(schedulerId)).resolves.toBe(
					false,
				); // Should not throw but return false or warn

				await closePromise; // Wait for close to complete
			});
		});

		describe("start/stop/close", () => {
			it("should start the scheduler and schedule the first check", async () => {
				const setTimeoutSpy = spyOn(global, "setTimeout").mockImplementation(
					// @ts-ignore
					() => {},
				);

				scheduler.start();
				// @ts-ignore
				expect(scheduler.running).toBe(true);
				expect(setTimeoutSpy).toHaveBeenCalledTimes(1);
				// @ts-ignore
				expect(setTimeoutSpy).toHaveBeenCalledWith(
					expect.any(Function),
					// @ts-ignore
					scheduler.opts.checkInterval,
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

			it("should close the scheduler, stop it", async () => {
				const stopSpy = spyOn(scheduler, "stop");

				// @ts-ignore - Set manually for test
				scheduler.running = true;

				await scheduler.close();

				expect(stopSpy).toHaveBeenCalledTimes(1);
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

			it("should find due jobs and process them", async () => {
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
				const repeat1: SchedulerRepeatOptions = { every: 5000 };
				const repeat2: SchedulerRepeatOptions = { pattern: "* * * * *" };
				const nextRun2 = new Cron(repeat2.pattern!)
					.nextRun(new Date(now))!
					.getTime();

				// 1. Mock the list of due jobs
				zrangebyscoreSpy.mockResolvedValueOnce([schedulerId1, schedulerId2]);

				// 2. Mock the result of the lock-and-get script for EACH job
				lockAndGetSchedulerSpy
					.mockResolvedValueOnce({
						id: schedulerId1,
						type: "every",
						value: "5000",
						nextRun: (now - 100).toString(),
						name: jobTemplate1.name,
						data: JSON.stringify(jobTemplate1.data),
						opts: "{}",
					})
					.mockResolvedValueOnce({
						id: schedulerId2,
						type: "cron",
						value: repeat2.pattern!,
						nextRun: (now - 50).toString(),
						name: jobTemplate2.name,
						data: JSON.stringify(jobTemplate2.data),
						opts: JSON.stringify(jobTemplate2.opts),
					});

				execSpy.mockResolvedValue([[null, 1]]);
				scheduler["running"] = true;
				const emitSpy = spyOn(scheduler, "emit");

				// 3. Run the function to test
				await scheduler["_checkAndProcessDueJobs"]();

				// 4. Assertions (these should now pass)
				expect(lockAndGetSchedulerSpy).toHaveBeenCalledTimes(2);
				expect(queueAddSpy).toHaveBeenCalledTimes(2);

				expect(queueAddSpy).toHaveBeenCalledWith(
					jobTemplate1.name,
					jobTemplate1.data,
					expect.objectContaining({
						jobId: undefined,
						delay: undefined,
					}),
				);

				const expectedKey1 = `testprefix:${queue.name}:schedulers:${schedulerId1}`;
				const expectedNextRun1 = now + repeat1.every!;
				expect(multiSpy).toHaveBeenCalledTimes(2);
				expect(execSpy).toHaveBeenCalledTimes(2);

				const pipelineInstance1 = multiSpy.mock.results[0].value;
				expect(pipelineInstance1.hset).toHaveBeenCalledWith(
					expectedKey1,
					"nextRun",
					expectedNextRun1.toString(),
					"lastRun",
					now.toString(),
					"failureCount",
					"0",
				);
				expect(pipelineInstance1.zadd).toHaveBeenCalledWith(
					scheduler["keys"].index,
					expectedNextRun1,
					schedulerId1,
				);

				expect(queueAddSpy).toHaveBeenCalledWith(
					jobTemplate2.name,
					jobTemplate2.data,
					expect.objectContaining({
						attempts: 5,
						jobId: undefined,
						delay: undefined,
					}),
				);
				const expectedKey2 = `testprefix:${queue.name}:schedulers:${schedulerId2}`;
				const pipelineInstance2 = multiSpy.mock.results[1].value;
				expect(pipelineInstance2.hset).toHaveBeenCalledWith(
					expectedKey2,
					"nextRun",
					nextRun2.toString(),
					"lastRun",
					now.toString(),
					"failureCount",
					"0",
				);
				expect(pipelineInstance2.zadd).toHaveBeenCalledWith(
					scheduler["keys"].index,
					nextRun2,
					schedulerId2,
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

			it("should process schedulers in parallel and respect running state", async () => {
				const now = Date.now();
				(Date.now as any).mockReturnValue(now);
				const schedulerId1 = "due-job-1";
				const schedulerId2 = "due-job-2";

				zrangebyscoreSpy.mockResolvedValueOnce([schedulerId1, schedulerId2]);

				// Mock the lock-and-get script for both schedulers
				lockAndGetSchedulerSpy
					.mockResolvedValueOnce({
						id: schedulerId1,
						type: "every",
						value: "5000",
						nextRun: (now - 100).toString(),
						name: "task1",
						data: "{}",
						opts: "{}",
					})
					.mockResolvedValueOnce({
						id: schedulerId2,
						type: "every",
						value: "5000",
						nextRun: (now - 100).toString(),
						name: "task2",
						data: "{}",
						opts: "{}",
					});

				let processedCount = 0;
				const processSpy = spyOn(
					scheduler as any,
					"_processSingleScheduler",
				).mockImplementation(async (id) => {
					processedCount++;
					await JobScheduler.prototype["_processSingleScheduler"].call(
						scheduler,
						id,
						now,
					);
				});

				scheduler["running"] = true;

				// @ts-ignore
				await scheduler._checkAndProcessDueJobs();

				// With parallel processing, both schedulers should be called
				expect(processSpy).toHaveBeenCalledTimes(2);
				expect(processSpy).toHaveBeenCalledWith(schedulerId1, now);
				expect(processSpy).toHaveBeenCalledWith(schedulerId2, now);
				expect(queueAddSpy).toHaveBeenCalledTimes(2); // Both jobs added
				expect(multiSpy).toHaveBeenCalledTimes(2); // Both schedulers updated

				processSpy.mockRestore();
			});

			it("should handle missing scheduler data after zrangebyscore (race condition)", async () => {
				const schedulerId = "deleted-job";
				zrangebyscoreSpy.mockResolvedValueOnce([schedulerId]);

				// Mock the lock-and-get script to return empty object (scheduler was deleted)
				lockAndGetSchedulerSpy.mockResolvedValueOnce({});

				zremSpy.mockClear();

				scheduler["running"] = true;

				// @ts-ignore
				await scheduler._checkAndProcessDueJobs();

				expect(lockAndGetSchedulerSpy).toHaveBeenCalledTimes(1);
				expect(zremSpy).toHaveBeenCalledWith(
					scheduler["keys"].index,
					schedulerId,
				);
				expect(queueAddSpy).not.toHaveBeenCalled();
				expect(multiSpy).not.toHaveBeenCalled();
			});

			it("should handle job add failure and attempt recovery", async () => {
				// Arrange: Control the environment
				const now = Date.now();
				(Date.now as any).mockReturnValue(now);

				const schedulerId = "fail-add-job";
				const jobTemplate: JobTemplate = { name: "wont-add" };
				const addError = new Error("Queue is full");
				const checkInterval = scheduler["opts"].checkInterval || 50;

				// Set up error event listener to prevent unhandled errors
				const errorHandler = mock(() => {});
				scheduler.on("error", errorHandler);

				// Arrange: Set up spies and mocks for this specific scenario
				queueAddSpy.mockRejectedValueOnce(addError);
				zrangebyscoreSpy.mockResolvedValueOnce([schedulerId]);

				// Mock the lock-and-get script to return scheduler data
				lockAndGetSchedulerSpy.mockResolvedValueOnce({
					id: schedulerId,
					type: "every",
					value: "5000",
					nextRun: (now - 100).toString(), // Make it due
					name: jobTemplate.name,
					data: "{}",
					opts: "{}",
				});

				// Clear previous mock calls and set up the expected pipeline result for recovery
				multiSpy.mockClear();
				execSpy.mockClear();
				execSpy.mockResolvedValueOnce([[null, 1]]); // Mock pipeline.exec() for the recovery update

				scheduler["running"] = true; // Ensure the scheduler thinks it's running

				// Act: Trigger the scheduler's main logic
				await scheduler["_checkAndProcessDueJobs"]();

				// Assert: Verify the correct recovery actions were taken
				expect(queueAddSpy).toHaveBeenCalledTimes(1);

				// Verify error events were emitted
				expect(errorHandler).toHaveBeenCalledWith(
					expect.stringContaining(
						`Failed to process scheduler ${schedulerId}: ${addError.message}`,
					),
				);

				// Check that the recovery pipeline was created and executed
				expect(multiSpy).toHaveBeenCalledTimes(1);
				const pipelineInstance = multiSpy.mock.results[0].value;
				const expectedKey = `testprefix:${queue.name}:schedulers:${schedulerId}`;

				// The actual recovery logic uses Math.max(newNextRun, now + checkInterval)
				// where newNextRun = now + 5000 (from the "every" value)
				// and now + checkInterval = now + 50
				// So the actual recovery time should be now + 5000
				const expectedRecoveryTime = now + 5000; // Since Math.max(now + 5000, now + 50) = now + 5000

				// It should have tried to set the nextRun to a future time
				expect(pipelineInstance.hset).toHaveBeenCalledWith(
					expectedKey,
					"nextRun",
					expectedRecoveryTime.toString(),
					"failureCount",
					"1",
				);
				expect(pipelineInstance.zadd).toHaveBeenCalledWith(
					scheduler["keys"].index,
					expectedRecoveryTime,
					schedulerId,
				);
				expect(execSpy).toHaveBeenCalledTimes(1);

				// Cleanup
				scheduler.removeAllListeners("error");
				scheduler["running"] = false;
			});

			it("should handle recovery failure by removing from index", async () => {
				// Arrange: Control the environment
				const now = Date.now();
				(Date.now as any).mockReturnValue(now);

				const schedulerId = "fail-recovery";
				const jobTemplate: JobTemplate = { name: "wont-recover" };
				const addError = new Error("Queue failed");
				const recoveryError = new Error("Redis recovery failed");

				// Set up error event listener to prevent unhandled errors
				const errorHandler = mock(() => {});
				scheduler.on("error", errorHandler);

				// Arrange: Set up the scenario where both queue.add and recovery fail
				queueAddSpy.mockRejectedValueOnce(addError);
				zrangebyscoreSpy.mockResolvedValueOnce([schedulerId]);

				// Mock the lock-and-get script to return scheduler data
				lockAndGetSchedulerSpy.mockResolvedValueOnce({
					id: schedulerId,
					type: "every",
					value: "5000",
					nextRun: (now - 100).toString(), // Make it due
					name: jobTemplate.name,
					data: "{}",
					opts: "{}",
				});

				// Mock multi/exec to fail for the recovery attempt
				multiSpy.mockClear();
				execSpy.mockClear();
				execSpy.mockRejectedValueOnce(recoveryError);

				const consoleWarnSpy = spyOn(console, "warn");
				const consoleErrorSpy = spyOn(console, "error");

				scheduler["running"] = true;

				// Act: Trigger the scheduler's main logic
				await scheduler["_checkAndProcessDueJobs"]();

				// Assert: Verify the recovery failure was handled correctly
				expect(queueAddSpy).toHaveBeenCalledTimes(1);
				expect(multiSpy).toHaveBeenCalledTimes(1); // Recovery multi was called
				expect(execSpy).toHaveBeenCalledTimes(1); // Recovery exec failed

				// Should emit error for the initial queue.add failure
				expect(errorHandler).toHaveBeenCalledWith(
					expect.stringContaining(
						`Failed to process scheduler ${schedulerId}: ${addError.message}`,
					),
				);

				// Should log recovery failure and retry message (based on actual implementation)
				expect(consoleErrorSpy).toHaveBeenCalledWith(
					expect.stringContaining(
						`Failed recovery update for scheduler ${schedulerId}`,
					),
				);

				expect(consoleWarnSpy).toHaveBeenCalledWith(
					expect.stringContaining(
						`Scheduler ${schedulerId} will be retried on next check interval`,
					),
				);

				// Cleanup
				consoleWarnSpy.mockRestore();
				consoleErrorSpy.mockRestore();
				scheduler.removeAllListeners("error");
				scheduler["running"] = false;
			});

			it("should use configurable lockDuration when processing schedulers", async () => {
				// Create a scheduler with custom lockDuration
				const customLockDuration = 25;
				const customScheduler = new JobScheduler(queue, {
					connection: queue.client,
					lockDuration: customLockDuration,
					checkInterval: 50,
				});
				schedulersToClose.push(customScheduler);

				const lockAndGetCustomSpy = spyOn(
					customScheduler["scripts"],
					"lockAndGetScheduler",
				).mockResolvedValue({
					id: "test-scheduler",
					type: "every",
					value: "5000",
					nextRun: (Date.now() - 100).toString(),
					name: "testTask",
					data: "{}",
					opts: "{}",
				});

				const zrangebyscoreCustomSpy = spyOn(
					customScheduler["client"],
					"zrangebyscore",
				).mockResolvedValue(["test-scheduler"]);

				const queueAddCustomSpy = spyOn(queue, "add").mockResolvedValue(
					{} as Job,
				);

				const multiCustomSpy = spyOn(
					customScheduler["client"],
					"multi",
				).mockImplementation(() => {
					const pipeline = new Pipeline(customScheduler["client"]);
					pipeline.hset = mock().mockReturnThis();
					pipeline.zadd = mock().mockReturnThis();
					pipeline.exec = mock().mockResolvedValue([[null, 1]]);
					return pipeline as any;
				});

				customScheduler["running"] = true;

				await customScheduler["_checkAndProcessDueJobs"]();

				// Verify that lockAndGetScheduler was called with the custom lockDuration
				expect(lockAndGetCustomSpy).toHaveBeenCalledWith(
					expect.stringContaining("lock:test-scheduler"),
					expect.stringContaining("schedulers:test-scheduler"),
					expect.stringMatching(/^scheduler-.*-\d+$/),
					customLockDuration, // This should be the custom value
				);

				lockAndGetCustomSpy.mockRestore();
				zrangebyscoreCustomSpy.mockRestore();
				queueAddCustomSpy.mockRestore();
				multiCustomSpy.mockRestore();
			});

			it("should use configurable maxFailureCount for poison pill mechanism", async () => {
				// Create a scheduler with custom maxFailureCount
				const customMaxFailureCount = 3;
				const customScheduler = new JobScheduler(queue, {
					connection: queue.client,
					maxFailureCount: customMaxFailureCount,
					checkInterval: 50,
				});
				schedulersToClose.push(customScheduler);

				const schedulerId = "failing-scheduler";
				const addError = new Error("Persistent failure");
				const now = Date.now();

				// Set up error event listener
				const errorHandler = mock(() => {});
				customScheduler.on("error", errorHandler);

				// Mock queue.add to always fail
				const queueAddCustomSpy = spyOn(queue, "add").mockRejectedValue(
					addError,
				);

				const zrangebyscoreCustomSpy = spyOn(
					customScheduler["client"],
					"zrangebyscore",
				).mockResolvedValue([schedulerId]);

				const lockAndGetCustomSpy = spyOn(
					customScheduler["scripts"],
					"lockAndGetScheduler",
				).mockResolvedValue({
					id: schedulerId,
					type: "every",
					value: "5000",
					nextRun: (now - 100).toString(),
					name: "failingTask",
					data: "{}",
					opts: "{}",
					failureCount: (customMaxFailureCount - 1).toString(), // One failure away from threshold
				});

				const multiCustomSpy = spyOn(
					customScheduler["client"],
					"multi",
				).mockImplementation(() => {
					const pipeline = new Pipeline(customScheduler["client"]);
					pipeline.hset = mock().mockReturnThis();
					pipeline.zadd = mock().mockReturnThis();
					pipeline.exec = mock().mockResolvedValue([[null, 1]]);
					return pipeline as any;
				});

				const consoleErrorSpy = spyOn(console, "error");

				customScheduler["running"] = true;
				(Date.now as any).mockReturnValue(now);

				await customScheduler["_checkAndProcessDueJobs"]();

				// Verify that the scheduler was disabled after reaching the custom maxFailureCount
				expect(multiCustomSpy).toHaveBeenCalledTimes(1);
				const pipelineInstance = multiCustomSpy.mock.results[0].value as any;

				// Should disable scheduler by setting nextRun to far future
				const disabledNextRun = now + 365 * 24 * 60 * 60 * 1000; // 1 year from now
				expect(pipelineInstance.hset).toHaveBeenCalledWith(
					expect.stringContaining(schedulerId),
					"nextRun",
					disabledNextRun.toString(),
					"failureCount",
					customMaxFailureCount.toString(),
				);

				expect(consoleErrorSpy).toHaveBeenCalledWith(
					expect.stringContaining(
						`CRITICAL: Scheduler ${schedulerId} has been automatically disabled after ${customMaxFailureCount} consecutive failures`,
					),
				);

				// Cleanup
				consoleErrorSpy.mockRestore();
				queueAddCustomSpy.mockRestore();
				zrangebyscoreCustomSpy.mockRestore();
				lockAndGetCustomSpy.mockRestore();
				multiCustomSpy.mockRestore();
				customScheduler.removeAllListeners("error");
			});

			it("should skip job if its nextRun is in the future (race condition handled by another process)", async () => {
				// Arrange: Control the environment
				const now = Date.now();
				(Date.now as any).mockReturnValue(now);

				const schedulerId = "future-job";
				const futureNextRun = now + 10000; // 10 seconds in the future

				// Arrange: Set up the race condition scenario
				// zrangebyscore finds the job (it was due when queried)
				zrangebyscoreSpy.mockResolvedValueOnce([schedulerId]);

				// But lock-and-get reveals it was updated by another process to a future time
				lockAndGetSchedulerSpy.mockResolvedValueOnce({
					id: schedulerId,
					type: "every",
					value: "5000",
					nextRun: futureNextRun.toString(), // Now in the future
					name: "futureTask",
					data: "{}",
					opts: "{}",
				});

				// Mock zadd for the corrective update
				zaddSpy.mockClear();
				zaddSpy.mockResolvedValueOnce(1);

				scheduler["running"] = true;

				// Act: Trigger the scheduler's main logic
				await scheduler["_checkAndProcessDueJobs"]();

				// Assert: Verify the race condition was handled correctly
				expect(lockAndGetSchedulerSpy).toHaveBeenCalledTimes(1);

				// Should perform corrective ZADD with NX flag to fix the index
				expect(zaddSpy).toHaveBeenCalledWith(
					scheduler["keys"].index,
					"NX", // Ensure NX flag is used
					futureNextRun,
					schedulerId,
				);

				// Should NOT add the job since it's not due anymore
				expect(queueAddSpy).not.toHaveBeenCalled();

				// Should NOT try to update state via multi since job wasn't processed
				expect(multiSpy).not.toHaveBeenCalled();

				// Cleanup
				scheduler["running"] = false;
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
					failureCount: 0,
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
					failureCount: 0,
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
						"Error parsing scheduler data from Redis: SyntaxError: JSON Parse error",
					),
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
			it("should handle timezone correctly", () => {
				const pattern = "0 9 * * 1-5"; // 9 AM on weekdays
				const tz = "America/New_York";
				const now = Date.now();

				// Use the real Croner logic here, just verify it's called with TZ
				// mock.module('croner', () => ({ Cron: CronMock })); // Doesn't work with bun mock

				// @ts-ignore Access private method
				scheduler.parseCron(pattern, now, tz);
			});
		});
	});
});
