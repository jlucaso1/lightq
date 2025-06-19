import { randomUUID } from "node:crypto";
import type {
	JobContext,
	JobData,
	JobOptions,
	JobSchedulerOptions,
	JobTemplate,
	ProgressUpdater,
	QueueOptions,
	SchedulerRepeatOptions,
} from "../interfaces";
import { LuaScripts } from "../scripts/lua";
import { getQueueKeys, type QueueKeys } from "../utils";
import { RedisService } from "./base-service";
import { Job } from "./job";
import { JobProgressUpdater } from "./progress-updater";
import { JobScheduler } from "./scheduler";

export class Queue<
	TData = unknown,
	TResult = unknown,
	TName extends string = string,
> extends RedisService<QueueOptions> {
	readonly name: string;
	readonly keys: QueueKeys;
	private scripts: LuaScripts;
	private scheduler: JobScheduler<TData, TResult, TName> | null = null;
	private schedulerOpts: JobSchedulerOptions | null = null;
	private progressUpdater: ProgressUpdater;

	constructor(name: string, opts: QueueOptions) {
		super(opts);
		this.name = name;
		this.keys = getQueueKeys(this.prefix, this.name);
		this.scripts = new LuaScripts(this.client);

		const jobContext: JobContext = {
			client: this.client,
			keys: this.keys,
		};
		this.progressUpdater = new JobProgressUpdater(jobContext);

		this.schedulerOpts = {
			connection: this.client,
			prefix: this.prefix,
			defaultJobOptions: this.opts.defaultJobOptions,
			schedulerPrefix: opts.prefix ?? "lightq",
		};
	}

	async add(
		name: TName,
		data: TData,
		opts?: JobOptions,
	): Promise<Job<TData, TResult, TName>> {
		if (this.closing) {
			throw new Error("Queue is closing");
		}

		const mergedOpts: JobOptions = {
			attempts: 1,
			delay: 0,
			...this.opts.defaultJobOptions,
			...opts,
		};

		if (mergedOpts?.jobId?.startsWith("scheduler:")) {
			console.warn(
				`Attempting to manually add a job with a reserved scheduler ID prefix: ${mergedOpts.jobId}. Schedulers manage their own job IDs.`,
			);
			throw new Error(
				`Cannot manually add job with reserved scheduler ID: ${mergedOpts.jobId}`,
			);
		}

		const jobId = mergedOpts.jobId ?? randomUUID();
		const timestamp = Date.now();

		const jobData: JobData<TData> = {
			id: jobId,
			name,
			data,
			opts: mergedOpts,
			timestamp,
			delay: mergedOpts.delay ?? 0,
			attemptsMade: 0,
		};

		await this.scripts.addJob(this.keys, jobData);

		const job = Job.fromData<TData, TResult, TName>(
			jobData,
			this.progressUpdater,
		);
		this.emit("waiting", job);
		return job;
	}

	async addBulk(
		jobs: { name: TName; data: TData; opts?: JobOptions }[],
	): Promise<Job<TData, TResult, TName>[]> {
		if (this.closing) {
			throw new Error("Queue is closing");
		}

		const jobInstances: Job<TData, TResult, TName>[] = [];
		const pipeline = this.client.pipeline();

		const timestamp = Date.now();

		for (const jobDef of jobs) {
			const mergedOpts: JobOptions = {
				attempts: 1,
				delay: 0,
				...this.opts.defaultJobOptions,
				...jobDef.opts,
			};
			if (mergedOpts?.jobId?.startsWith("scheduler:")) {
				console.warn(
					`Attempting to bulk add a job with a reserved scheduler ID prefix: ${mergedOpts.jobId}. Skipping this specific job.`,
				);
				continue;
			}
			const jobId = mergedOpts.jobId ?? randomUUID();

			const jobData: JobData<TData> = {
				id: jobId,
				name: jobDef.name,
				data: jobDef.data,
				opts: mergedOpts,
				timestamp,
				delay: mergedOpts.delay ?? 0,
				attemptsMade: 0,
			};

			this.scripts.addJob(this.keys, jobData, pipeline);
			jobInstances.push(
				Job.fromData<TData, TResult, TName>(jobData, this.progressUpdater),
			);
		}

		await pipeline.exec();

		jobInstances.forEach((job) => this.emit("waiting", job));
		return jobInstances;
	}

	async getJob(jobId: string): Promise<Job<TData, TResult, TName> | null> {
		const jobKey = `${this.keys.jobs}:${jobId}`;
		const data = await this.client.hgetall(jobKey);
		if (Object.keys(data).length === 0) {
			return null;
		}
		return Job.fromRedisHash<TData, TResult, TName>(data, this.progressUpdater);
	}

	async getJobCounts(): Promise<{
		wait: number;
		active: number;
		delayed: number;
		completed: number;
		failed: number;
	}> {
		const keys = this.keys;
		const multi = this.client.multi();
		multi.llen(keys.wait);
		multi.llen(keys.active);
		multi.zcard(keys.delayed);
		multi.zcard(keys.completed);
		multi.zcard(keys.failed);

		const results = await multi.exec();
		const counts = results?.map((res) => res[1] as number) ?? [0, 0, 0, 0, 0];

		return {
			wait: counts[0]!,
			active: counts[1]!,
			delayed: counts[2]!,
			completed: counts[3]!,
			failed: counts[4]!,
		};
	}

	protected async _internalClose(force = false): Promise<void> {
		if (this.scheduler) {
			await this.scheduler.close(force);
		}
	}

	private getScheduler(): JobScheduler<TData, TResult, TName> {
		if (!this.scheduler) {
			if (!this.schedulerOpts) {
				throw new Error("Scheduler options not initialized.");
			}
			this.scheduler = new JobScheduler<TData, TResult, TName>(
				this,
				this.schedulerOpts,
			);
			this.scheduler.on("error", (err) => this.emit("scheduler_error", err));
			this.scheduler.on("job_added", (schedulerId, job) =>
				this.emit("scheduler_job_added", schedulerId, job),
			);
			this.scheduler.start();
		}
		return this.scheduler;
	}

	/**
	 * Creates or updates a job scheduler associated with this queue.
	 * Starts the scheduler polling process if it wasn't running.
	 * @param schedulerId A unique identifier for this scheduler.
	 * @param repeat The repeat options (cron pattern or interval).
	 * @param template The template for jobs to be generated.
	 */
	async upsertJobScheduler<
		TJobData = unknown,
		TJobName extends string = string,
	>(
		schedulerId: string,
		repeat: SchedulerRepeatOptions,
		template: JobTemplate<TJobData, TJobName>,
	): Promise<void> {
		return this.getScheduler().upsertJobScheduler(
			schedulerId,
			repeat,
			template,
		);
	}

	/**
	 * Removes a job scheduler associated with this queue.
	 * @param schedulerId The ID of the scheduler to remove.
	 */
	async removeJobScheduler(schedulerId: string): Promise<boolean> {
		if (this.scheduler) {
			return this.scheduler.removeJobScheduler(schedulerId);
		}
		console.warn(
			"Attempted to remove scheduler before scheduler process was started for this queue.",
		);
		return false;
	}
}
