import type {
  JobSchedulerOptions,
  JobTemplate,
  SchedulerData,
  SchedulerRepeatOptions,
} from "../interfaces";
import type { Queue } from "./queue";
import { Cron } from "croner";
import { RedisService } from "./base-service";
import { LuaScripts } from "../scripts/lua";

function getSchedulerKeys(prefix: string, queueName: string) {
  const base = `${prefix}:${queueName}:schedulers`;
  return {
    base,
    hashPrefix: `${base}:`,
    index: `${base}:index`,
  };
}

type SchedulerKeys = ReturnType<typeof getSchedulerKeys>;

export class JobScheduler<
  TData = unknown,
  TResult = unknown,
  TName extends string = string
> extends RedisService<JobSchedulerOptions> {
  private queue: Queue<TData, TResult, TName>;
  private keys: SchedulerKeys;
  private scripts: LuaScripts;
  private checkTimer: NodeJS.Timeout | null = null;
  private running: boolean = false;
  private schedulerPrefix: string;
  private workerId: string;

  constructor(queue: Queue<TData, TResult, TName>, opts: JobSchedulerOptions) {
    super(opts);
    this.queue = queue;

    this.schedulerPrefix = opts.schedulerPrefix ?? this.prefix;
    this.workerId = `scheduler-${Math.random().toString(36).substring(2, 15)}`;

    this.keys = getSchedulerKeys(this.schedulerPrefix, queue.name);
    this.scripts = new LuaScripts(this.client);
  }

  /**
   * Creates or updates a job scheduler.
   * @param schedulerId A unique identifier for this scheduler.
   * @param repeat The repeat options (cron pattern or interval).
   * @param template The template for jobs to be generated.
   */
  async upsertJobScheduler<TData = unknown, TName extends string = string>(
    schedulerId: string,
    repeat: SchedulerRepeatOptions,
    template: JobTemplate<TData, TName>
  ): Promise<void> {
    if (this.closing) throw new Error("Scheduler is closing");
    if (!schedulerId) throw new Error("Scheduler ID cannot be empty");

    const now = Date.now();
    let type: "cron" | "every";
    let value: string | number;
    let tz: string | undefined;
    let nextRun: number | undefined;

    if (repeat.pattern) {
      if (typeof repeat.pattern !== "string") {
        throw new Error("Invalid cron pattern");
      }
      type = "cron";
      value = repeat.pattern;
      tz = repeat.tz;
      try {
        const interval = this.parseCron(value, now, tz);
        nextRun = interval.nextRun()?.getTime();
      } catch (err: unknown) {
        const message = err instanceof Error ? err.message : String(err);
        throw new Error(`Invalid cron pattern: ${message}`);
      }
    } else if (repeat.every) {
      if (typeof repeat.every !== "number" || repeat.every <= 0) {
        throw new Error(
          "Invalid 'every' value: must be a positive number (milliseconds)"
        );
      }
      type = "every";
      value = repeat.every;
      nextRun = now + value;
    } else {
      throw new Error(
        "Invalid repeat options: must provide 'pattern' or 'every'"
      );
    }

    if (!nextRun) {
      console.warn(
        `Scheduler ${schedulerId} has no nextRun calculated. Skipping upsert.`
      );
      return;
    }

    const schedulerKey = `${this.keys.hashPrefix}${schedulerId}`;
    const schedulerData: Omit<SchedulerData, "lastRun"> = {
      id: schedulerId,
      type,
      value,
      tz,
      nextRun,
      name: template.name,
      data: template.data,
      opts: template.opts,
    };

    try {
      const multi = this.client.multi();
      multi.hset(schedulerKey, {
        ...schedulerData,
        data: JSON.stringify(schedulerData.data ?? {}),
        opts: JSON.stringify(schedulerData.opts ?? {}),
        nextRun: nextRun.toString(),
        value: value.toString(),
        failureCount: "0",
      });

      multi.zadd(this.keys.index, nextRun, schedulerId);
      await multi.exec();
      this.emit("upsert", schedulerId, schedulerData);
    } catch (err: unknown) {
      const message = err instanceof Error ? err.message : String(err);
      this.emit(
        "error",
        `Failed to upsert scheduler ${schedulerId}: ${message}`
      );
      throw err;
    }
  }

  /**
   * Removes a job scheduler.
   * @param schedulerId The ID of the scheduler to remove.
   */
  async removeJobScheduler(schedulerId: string): Promise<boolean> {
    if (this.closing) {
      console.warn("Cannot remove scheduler, scheduler is closing.");
      return false;
    }
    if (!schedulerId) return false;

    const schedulerKey = `${this.keys.hashPrefix}${schedulerId}`;

    try {
      const multi = this.client.multi();
      multi.del(schedulerKey);
      multi.zrem(this.keys.index, schedulerId);
      const results = await multi.exec();

      const deleted = !!results && results[0]?.[1] === 1;
      if (deleted) {
        this.emit("remove", schedulerId);
      }
      return deleted;
    } catch (err: unknown) {
      const message = err instanceof Error ? err.message : String(err);
      this.emit(
        "error",
        `Failed to remove scheduler ${schedulerId}: ${message}`
      );
      throw err;
    }
  }

  /** Starts the background polling process */
  start(): void {
    if (this.running || this.closing) {
      return;
    }
    this.running = true;
    this.emit("start");
    this._scheduleNextCheck();
    console.log(
      `JobScheduler for queue "${this.queue.name}" started. Checking every ${this.opts.checkInterval}ms.`
    );
  }

  /** Stops the background polling process */
  stop(): void {
    if (!this.running || this.closing) {
      return;
    }
    this.running = false;
    if (this.checkTimer) {
      clearTimeout(this.checkTimer);
      this.checkTimer = null;
    }
    this.emit("stop");
    console.log(`JobScheduler for queue "${this.queue.name}" stopped.`);
  }

  protected async _internalClose(): Promise<void> {
    this.stop();
  }

  private _scheduleNextCheck(): void {
    if (!this.running || this.closing) return;

    this.checkTimer = setTimeout(() => {
      this._checkAndProcessDueJobs()
        .catch((err) => {
          this.emit("error", `Error during scheduler check: ${err.message}`);
        })
        .finally(() => {
          this._scheduleNextCheck();
        });
    }, this.opts.checkInterval);
  }

  private async _checkAndProcessDueJobs(): Promise<void> {
    if (!this.running || this.closing) return;

    const now = Date.now();
    const limit = 50;

    const checkDelayedJobs = async () => {
      try {
        const movedCount = await this.scripts.moveDelayedToWait(
          this.queue.keys,
          now,
          limit
        );
        if (movedCount > 0) {
          this.queue.emit("movedDelayed", movedCount);
        }
      } catch (err: unknown) {
        const message = err instanceof Error ? err.message : String(err);
        if (message !== "Connection is closed.") {
          this.emit(
            "error",
            `Scheduler failed to check delayed jobs: ${message}`
          );
        }
      }
    };

    const checkScheduledJobs = async () => {
      try {
        const results = await this.client.zrangebyscore(
          this.keys.index,
          "-inf",
          now,
          "LIMIT",
          "0",
          limit.toString()
        );

        if (results.length === 0) {
          return;
        }

        const processingPromises = results.map((schedulerId) => {
          if (!this.running || this.closing) {
            return Promise.resolve();
          }
          return this._processSingleScheduler(schedulerId, now);
        });

        await Promise.all(processingPromises);
      } catch (err: unknown) {
        const message = err instanceof Error ? err.message : String(err);
        if (message !== "Connection is closed.") {
          this.emit("error", `Scheduler check failed: ${message}`);
        }
      }
    };

    await Promise.all([checkDelayedJobs(), checkScheduledJobs()]);
  }

  private async _processSingleScheduler(
    schedulerId: string,
    now: number
  ): Promise<void> {
    if (!this.running || this.closing) {
      return;
    }

    const schedulerKey = `${this.keys.hashPrefix}${schedulerId}`;
    let schedulerData: Record<string, string> | null = null;
    let parsedData: SchedulerData | null = null;
    let newNextRun: number | null = null;

    const lockKey = `${this.keys.base}:lock:${schedulerId}`;
    const lockValue = `${this.workerId}-${Date.now()}`;
    const lockDuration = this.opts.lockDuration ?? 10;

    try {
      schedulerData = await this.scripts.lockAndGetScheduler(
        lockKey,
        schedulerKey,
        lockValue,
        lockDuration
      );

      if (!schedulerData || Object.keys(schedulerData).length === 0) {
        if (schedulerData === null) {
          return;
        }
        await this.client.zrem(this.keys.index, schedulerId);
        console.warn(
          `Scheduler data for ${schedulerId} not found, removing from index.`
        );
        return;
      }

      const currentNextRun = parseInt(schedulerData.nextRun || "0", 10);
      if (currentNextRun > now) {
        await this.client.zadd(
          this.keys.index,
          "NX",
          currentNextRun,
          schedulerId
        );
        return;
      }

      parsedData = this.parseSchedulerData(schedulerData);
      if (!parsedData) {
        throw new Error("Failed to parse scheduler data from Redis.");
      }

      const lastRun = now;
      if (parsedData.type === "cron") {
        try {
          const interval = this.parseCron(
            parsedData.value as string,
            lastRun,
            parsedData.tz
          );
          newNextRun = interval.nextRun()?.getTime() || null;
        } catch (err: unknown) {
          const message = err instanceof Error ? err.message : String(err);
          throw new Error(
            `Invalid cron pattern (${parsedData.value}): ${message}`
          );
        }
      } else {
        newNextRun = lastRun + (parsedData.value as number);
      }

      if (!newNextRun) {
        console.warn(
          `Scheduler ${schedulerId} has no new nextRun calculated. Skipping job addition.`
        );
        return;
      }

      const jobName = parsedData.name as TName;
      const jobData = parsedData.data as TData;
      const jobOpts = {
        ...this.queue.opts.defaultJobOptions,
        ...this.opts.defaultJobOptions,
        ...parsedData.opts,
        jobId: undefined,
        delay: undefined,
      };

      const addedJob = await this.queue.add(jobName, jobData, jobOpts);
      this.emit("job_added", schedulerId, addedJob);

      const multi = this.client.multi();
      multi.hset(
        schedulerKey,
        "nextRun",
        newNextRun.toString(),
        "lastRun",
        lastRun.toString(),
        "failureCount",
        "0"
      );
      multi.zadd(this.keys.index, newNextRun, schedulerId);
      await multi.exec();
    } catch (err: unknown) {
      const message = err instanceof Error ? err.message : String(err);
      this.emit(
        "error",
        `Failed to process scheduler ${schedulerId}: ${message}`
      );

      const currentFailureCount = (parsedData?.failureCount || 0) + 1;
      const maxFailureCount = this.opts.maxFailureCount ?? 5;

      if (currentFailureCount >= maxFailureCount) {
        const disabledNextRun = now + 365 * 24 * 60 * 60 * 1000;
        try {
          await this.client
            .multi()
            .hset(
              schedulerKey,
              "nextRun",
              disabledNextRun.toString(),
              "failureCount",
              currentFailureCount.toString()
            )
            .zadd(this.keys.index, disabledNextRun, schedulerId)
            .exec();
          this.emit(
            "error",
            `CRITICAL: Scheduler ${schedulerId} disabled after ${maxFailureCount} consecutive failures. Manual intervention required.`
          );
          console.error(
            `CRITICAL: Scheduler ${schedulerId} has been automatically disabled after ${maxFailureCount} consecutive failures. ` +
              `It has been scheduled to run again in 1 year. Manual intervention is required.`
          );
        } catch (disableErr: unknown) {
          const message =
            disableErr instanceof Error
              ? disableErr.message
              : String(disableErr);
          console.error(
            `CRITICAL: Failed to disable problematic scheduler ${schedulerId}: ${message}`
          );
        }
      } else {
        if (newNextRun && parsedData) {
          try {
            const recoveryNextRun = Math.max(
              newNextRun,
              now + (this.opts.checkInterval || 5000)
            );
            await this.client
              .multi()
              .hset(
                schedulerKey,
                "nextRun",
                recoveryNextRun.toString(),
                "failureCount",
                currentFailureCount.toString()
              )
              .zadd(this.keys.index, recoveryNextRun, schedulerId)
              .exec();
            console.warn(
              `Scheduler ${schedulerId} processing error (failure ${currentFailureCount}/${maxFailureCount}), pushed nextRun forward.`
            );
          } catch (recoveryErr: unknown) {
            const message =
              recoveryErr instanceof Error
                ? recoveryErr.message
                : String(recoveryErr);
            console.error(
              `Failed recovery update for scheduler ${schedulerId}: ${message}`
            );
            console.warn(
              `Scheduler ${schedulerId} will be retried on next check interval.`
            );
          }
        }
      }
    } finally {
      try {
        const currentLockValue = await this.client.get(lockKey);
        if (currentLockValue === lockValue) {
          await this.client.del(lockKey);
        }
      } catch (lockErr: unknown) {
        const message =
          lockErr instanceof Error ? lockErr.message : String(lockErr);
        console.warn(
          `Failed to release lock for scheduler ${schedulerId}: ${message}`
        );
      }
    }
  }

  private parseSchedulerData(
    hashData: Record<string, string>
  ): SchedulerData | null {
    try {
      const data: Partial<SchedulerData> = {};
      data.id = hashData.id;
      data.type = hashData.type as "cron" | "every";
      data.tz = hashData.tz;
      data.nextRun = parseInt(hashData.nextRun || "0", 10);
      data.lastRun = hashData.lastRun
        ? parseInt(hashData.lastRun, 10)
        : undefined;
      data.failureCount = hashData.failureCount
        ? parseInt(hashData.failureCount, 10)
        : 0;
      data.name = hashData.name;
      data.data = JSON.parse(hashData.data || "{}");
      data.opts = JSON.parse(hashData.opts || "{}");

      if (data.type === "cron") {
        data.value = hashData.value;
      } else if (data.type === "every") {
        data.value = parseInt(hashData.value || "0", 10);
      } else {
        return null;
      }

      if (
        !data.id ||
        !data.type ||
        !data.value ||
        !data.name ||
        isNaN(data.nextRun)
      ) {
        return null;
      }

      return data as SchedulerData;
    } catch (e) {
      this.emit("error", `Error parsing scheduler data from Redis: ${e}`);
      return null;
    }
  }

  private parseCron(
    pattern: string,
    currentDate: number | Date,
    tz?: string
  ): Cron {
    const interval = new Cron(pattern, { timezone: tz });

    return interval;
  }
}
