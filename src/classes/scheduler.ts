import type {
  JobSchedulerOptions,
  JobTemplate,
  SchedulerData,
  SchedulerRepeatOptions,
} from "../interfaces";
import type { Queue } from "./queue";
import { Cron } from "croner";
import { RedisService } from "./base-service";

// Helper to generate scheduler-specific keys
function getSchedulerKeys(prefix: string, queueName: string) {
  const base = `${prefix}:${queueName}:schedulers`;
  return {
    base,
    hashPrefix: `${base}:`, // Prefix for individual scheduler hashes HASH (`base:<schedulerId>`)
    index: `${base}:index`, // ZSET (score: nextRun, member: schedulerId)
  };
}

type SchedulerKeys = ReturnType<typeof getSchedulerKeys>;

export class JobScheduler extends RedisService<JobSchedulerOptions> {
  private queue: Queue<any, any, any>;
  private keys: SchedulerKeys;
  private checkTimer: NodeJS.Timeout | null = null;
  private running: boolean = false;
  private schedulerPrefix: string;

  // Cache for parsed cron expressions to avoid re-parsing
  private cronCache = new Map<string, Cron>();

  constructor(queue: Queue<any, any, any>, opts: JobSchedulerOptions) {
    super(opts);
    this.queue = queue;

    this.schedulerPrefix = opts.schedulerPrefix ?? this.prefix;

    this.keys = getSchedulerKeys(this.schedulerPrefix, queue.name);
  }

  /**
   * Creates or updates a job scheduler.
   * @param schedulerId A unique identifier for this scheduler.
   * @param repeat The repeat options (cron pattern or interval).
   * @param template The template for jobs to be generated.
   */
  async upsertJobScheduler<TData = any, TName extends string = string>(
    schedulerId: string,
    repeat: SchedulerRepeatOptions,
    template: JobTemplate<TData, TName>,
  ): Promise<void> {
    if (this.closing) throw new Error("Scheduler is closing");
    if (!schedulerId) throw new Error("Scheduler ID cannot be empty");

    const now = Date.now();
    let type: "cron" | "every";
    let value: string | number;
    let tz: string | undefined;
    let nextRun: number | undefined;

    // Validate and parse repeat options
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
      } catch (err: any) {
        throw new Error(`Invalid cron pattern: ${err.message}`);
      }
    } else if (repeat.every) {
      if (typeof repeat.every !== "number" || repeat.every <= 0) {
        throw new Error(
          "Invalid 'every' value: must be a positive number (milliseconds)",
        );
      }
      type = "every";
      value = repeat.every;
      // First run is 'every' ms from now
      nextRun = now + value;
    } else {
      throw new Error(
        "Invalid repeat options: must provide 'pattern' or 'every'",
      );
    }

    if (!nextRun) {
      console.warn(
        `Scheduler ${schedulerId} has no nextRun calculated. Skipping upsert.`,
      );
      return;
    }

    // Prepare data for Redis Hash
    const schedulerKey = `${this.keys.hashPrefix}${schedulerId}`;
    const schedulerData: Omit<SchedulerData, "lastRun"> = {
      id: schedulerId,
      type,
      value,
      tz,
      nextRun,
      name: template.name,
      data: template.data, // Store as provided, will be stringified below
      opts: template.opts, // Store as provided, will be stringified below
    };

    try {
      const multi = this.client.multi();
      // Use HMSET/HSET compatible with older Redis versions if needed
      // Modern ioredis handles object mapping for HSET
      multi.hset(schedulerKey, {
        ...schedulerData,
        data: JSON.stringify(schedulerData.data ?? {}), // Ensure data is stored as JSON string
        opts: JSON.stringify(schedulerData.opts ?? {}), // Ensure opts are stored as JSON string
        nextRun: nextRun.toString(), // Store numbers as strings
        value: value.toString(), // Store value as string
      });

      multi.zadd(this.keys.index, nextRun, schedulerId);
      await multi.exec();
      this.emit("upsert", schedulerId, schedulerData);

      // Potentially wake up the poller if the new nextRun is sooner than the current check interval
      // For simplicity, we rely on the regular checkInterval for now.
    } catch (err: any) {
      this.emit(
        "error",
        `Failed to upsert scheduler ${schedulerId}: ${err.message}`,
      );
      throw err; // Re-throw
    }
  }

  /**
   * Removes a job scheduler.
   * @param schedulerId The ID of the scheduler to remove.
   */
  async removeJobScheduler(schedulerId: string): Promise<boolean> {
    if (this.closing) {
      console.warn("Cannot remove scheduler, scheduler is closing.");
      return false; // Or throw error
    }
    if (!schedulerId) return false;

    const schedulerKey = `${this.keys.hashPrefix}${schedulerId}`;

    try {
      const multi = this.client.multi();
      multi.del(schedulerKey);
      multi.zrem(this.keys.index, schedulerId);
      const results = await multi.exec();

      // Check results: [[null, delCount], [null, zremCount]]
      const deleted = !!results && results[0]?.[1] === 1;
      if (deleted) {
        this.emit("remove", schedulerId);
      }
      return deleted;
    } catch (err: any) {
      this.emit(
        "error",
        `Failed to remove scheduler ${schedulerId}: ${err.message}`,
      );
      throw err; // Re-throw
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
      `JobScheduler for queue "${this.queue.name}" started. Checking every ${this.opts.checkInterval}ms.`,
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

  protected async _internalClose(force = false): Promise<void> {
    this.stop(); // Stops polling and clears the timer
    this.cronCache.clear();
    // No other async cleanup needed before client disconnects
  }

  private _scheduleNextCheck(): void {
    if (!this.running || this.closing) return;

    this.checkTimer = setTimeout(() => {
      // Run check and then schedule the next one
      this._checkAndProcessDueJobs()
        .catch((err) => {
          this.emit("error", `Error during scheduler check: ${err.message}`);
        })
        .finally(() => {
          this._scheduleNextCheck(); // Always reschedule after completion or error
        });
    }, this.opts.checkInterval);
  }

  private async _checkAndProcessDueJobs(): Promise<void> {
    if (!this.running || this.closing) return;

    const now = Date.now();
    const limit = 50; // Process up to 50 due schedulers per check cycle

    try {
      // Find schedulers due to run (nextRun <= now)
      const results = await this.client.zrangebyscore(
        this.keys.index,
        "-inf", // Min score
        now, // Max score (current time)
        "LIMIT",
        "0",
        limit.toString(),
      );

      if (results.length === 0) {
        return; // Nothing to do
      }

      // --- Simple processing loop (potential for race conditions in distributed env) ---
      // A Lua script would be needed for true atomicity if multiple schedulers run
      for (const schedulerId of results) {
        if (!this.running || this.closing) break; // Stop if scheduler stopped mid-loop
        await this._processSingleScheduler(schedulerId, now);
      }
      // --- End Simple processing loop ---
    } catch (err: any) {
      // Handle Redis errors during the check
      if (err.message !== "Connection is closed.") {
        this.emit("error", `Scheduler check failed: ${err.message}`);
      }
    }
  }

  // Processes a single scheduler identified by ID
  private async _processSingleScheduler(
    schedulerId: string,
    now: number,
  ): Promise<void> {
    const schedulerKey = `${this.keys.hashPrefix}${schedulerId}`;
    let schedulerData: Record<string, string> | null = null;
    let parsedData: SchedulerData | null = null;
    let newNextRun: number | null = null;

    try {
      // 1. Get Scheduler Data
      schedulerData = await this.client.hgetall(schedulerKey);
      if (!schedulerData || Object.keys(schedulerData).length === 0) {
        // Scheduler was likely removed between ZRANGE and HGETALL
        await this.client.zrem(this.keys.index, schedulerId); // Clean up index just in case
        console.warn(
          `Scheduler data for ${schedulerId} not found, removing from index.`,
        );
        return;
      }

      // Basic check to prevent processing too early if checkInterval is very short
      const currentNextRun = parseInt(schedulerData.nextRun || "0", 10);
      if (currentNextRun > now) {
        // This scheduler isn't actually due yet (e.g., updated by another process)
        // Ensure ZSET score is correct, then skip
        await this.client.zadd(
          this.keys.index,
          "NX",
          currentNextRun,
          schedulerId,
        ); // Update score if it doesn't exist
        return;
      }

      // Parse the data from Redis strings
      parsedData = this.parseSchedulerData(schedulerData);
      if (!parsedData) {
        throw new Error("Failed to parse scheduler data from Redis.");
      }

      // 2. Calculate Next Run Time BEFORE adding the job
      const lastRun = now; // Use current time as the effective last run
      if (parsedData.type === "cron") {
        try {
          const interval = this.parseCron(
            parsedData.value as string,
            lastRun,
            parsedData.tz,
          );
          newNextRun = interval.nextRun()?.getTime() || null;
        } catch (err: any) {
          throw new Error(
            `Invalid cron pattern (${parsedData.value}): ${err.message}`,
          );
        }
      } else {
        // type === 'every'
        newNextRun = lastRun + (parsedData.value as number);
      }

      if (!newNextRun) {
        console.warn(
          `Scheduler ${schedulerId} has no new nextRun calculated. Skipping job addition.`,
        );
        return;
      }

      // 3. Add Job to Queue
      const jobName = parsedData.name;
      const jobData = parsedData.data;
      // Combine default opts with template opts, then with queue defaults
      const jobOpts = {
        ...this.queue.opts.defaultJobOptions, // Queue defaults
        ...this.opts.defaultJobOptions, // Scheduler defaults (if any)
        ...parsedData.opts, // Template specific opts
        // Force override crucial options
        jobId: undefined, // Scheduled jobs cannot have custom IDs managed externally
        delay: undefined, // Job is added to run NOW, not delayed further
      };

      // Add the job (fire and forget for the scheduler's perspective, worker handles it)
      // Important: No 'await' here if we want to update scheduler state quickly,
      // but awaiting ensures the job was accepted by Redis before updating the schedule.
      // Let's await for robustness.
      const addedJob = await this.queue.add(jobName, jobData, jobOpts);
      this.emit("job_added", schedulerId, addedJob);

      // 4. Update Scheduler State in Redis (Hash and ZSet)
      const multi = this.client.multi();
      multi.hset(
        schedulerKey,
        "nextRun",
        newNextRun.toString(),
        "lastRun",
        lastRun.toString(),
      );
      multi.zadd(this.keys.index, newNextRun, schedulerId); // Update score in ZSET
      await multi.exec();
    } catch (err: any) {
      this.emit(
        "error",
        `Failed to process scheduler ${schedulerId}: ${err.message}`,
      );
      // Decide how to handle failures:
      // - Remove the scheduler?
      // - Mark it as failed?
      // - Retry later? (Requires more complex state)
      // For now, we log the error. The scheduler might be picked up again if its
      // nextRun time doesn't get updated, potentially causing duplicate jobs if the
      // job add succeeded but the state update failed. Atomicity (Lua) is key here.
      // If nextRun *was* calculated, try to update just that to avoid tight loop.
      if (newNextRun && parsedData) {
        try {
          const recoveryNextRun = Math.max(
            newNextRun,
            now + (this.opts.checkInterval || 5000),
          ); // Push it at least one interval out
          await this.client
            .multi()
            .hset(schedulerKey, "nextRun", recoveryNextRun.toString())
            .zadd(this.keys.index, recoveryNextRun, schedulerId)
            .exec();
          console.warn(
            `Scheduler ${schedulerId} processing error, pushed nextRun forward.`,
          );
        } catch (recoveryErr: any) {
          console.error(
            `Failed recovery update for scheduler ${schedulerId}: ${recoveryErr.message}`,
          );
          // Consider removing from index if recovery fails to prevent infinite loop
          await this.client.zrem(this.keys.index, schedulerId);
          console.error(
            `Removed scheduler ${schedulerId} from index due to recovery failure.`,
          );
        }
      }
    }
  }

  // Helper to parse scheduler data stored as strings in Redis hash
  private parseSchedulerData(
    hashData: Record<string, string>,
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
      data.name = hashData.name;
      data.data = JSON.parse(hashData.data || "{}");
      data.opts = JSON.parse(hashData.opts || "{}");

      if (data.type === "cron") {
        data.value = hashData.value;
      } else if (data.type === "every") {
        data.value = parseInt(hashData.value || "0", 10);
      } else {
        return null; // Invalid type
      }

      if (
        !data.id ||
        !data.type ||
        !data.value ||
        !data.name ||
        isNaN(data.nextRun)
      ) {
        return null; // Missing essential fields
      }

      return data as SchedulerData;
    } catch (e) {
      this.emit("error", `Error parsing scheduler data from Redis: ${e}`);
      return null;
    }
  }

  // Helper to parse cron expression, using cache
  private parseCron(
    pattern: string,
    currentDate: number | Date,
    tz?: string,
  ): Cron {
    const cacheKey = `${pattern}_${tz || "local"}`;
    if (this.cronCache.has(cacheKey)) {
      const expr = this.cronCache.get(cacheKey)!;
      // Important: Reset the iterator state for the cached expression
      // expr.reset(new Date(currentDate));
      return expr;
    }

    const interval = new Cron(pattern, { timezone: tz });
    this.cronCache.set(cacheKey, interval); // Cache it

    return interval;
  }
}
