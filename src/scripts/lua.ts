import type { JobData, RedisClient } from "../interfaces";
import type { ChainableCommander } from "ioredis";
import type { Job } from "../classes/job";
import type { QueueKeys } from "../utils";
import { loadLuaScriptContent } from "../macros/loadLuaScript.ts" with { type: "macro" };

export class LuaScripts {
  private client: RedisClient;

  constructor(client: RedisClient) {
    this.client = client;
    this.loadScripts();
  }

  private loadScripts() {
    this.client.defineCommand("addJob", {
      numberOfKeys: 3,
      lua: loadLuaScriptContent("addJob"),
    });

    this.client.defineCommand("moveToCompleted", {
      numberOfKeys: 3,
      lua: loadLuaScriptContent("moveToCompleted"),
    });

    this.client.defineCommand("moveToFailed", {
      numberOfKeys: 3,
      lua: loadLuaScriptContent("moveToFailed"),
    });

    this.client.defineCommand("retryJob", {
      numberOfKeys: 4,
      lua: loadLuaScriptContent("retryJob"),
    });

    this.client.defineCommand("moveDelayedToWait", {
      numberOfKeys: 2,
      lua: loadLuaScriptContent("moveDelayedToWait"),
    });

    this.client.defineCommand("extendLock", {
      numberOfKeys: 1,
      lua: loadLuaScriptContent("extendLock"),
    });

    this.client.defineCommand("moveSpecificJobToActive", {
      numberOfKeys: 2,
      lua: loadLuaScriptContent("moveSpecificJobToActive"),
    });

    this.client.defineCommand("lockAndGetScheduler", {
      numberOfKeys: 2,
      lua: loadLuaScriptContent("lockAndGetScheduler"),
    });
  }

  async addJob<TData>(
    keys: QueueKeys,
    jobData: JobData<TData>,
    pipeline?: ChainableCommander,
  ): Promise<string> {
    const command = pipeline || this.client;
    const optsJson = JSON.stringify(jobData.opts);
    const dataJson = JSON.stringify(jobData.data);

    const args = [
      jobData.id,
      jobData.name,
      dataJson,
      optsJson,
      jobData.timestamp.toString(),
      jobData.delay.toString(),
      jobData.attemptsMade.toString(),
    ];
    // @ts-ignore
    return command.addJob(keys.jobs, keys.wait, keys.delayed, ...args);
  }


  async moveToCompleted<TData, TResult, TName extends string>(
    keys: QueueKeys,
    job: Job<TData, TResult, TName>,
    returnValue: TResult,
    removeOnComplete: boolean | number,
  ): Promise<number> {
    const now = Date.now();
    const removeOption = typeof removeOnComplete === "number"
      ? removeOnComplete.toString()
      : String(removeOnComplete);
    const rvJson = JSON.stringify(returnValue ?? null);
    const args = [job.id, rvJson, removeOption, now.toString(), job.lockToken];
    // @ts-ignore
    return this.client.moveToCompleted(
      keys.active,
      keys.completed,
      keys.jobs,
      ...args,
    );
  }

  async moveToFailed<TData, TResult, TName extends string>(
    keys: QueueKeys,
    job: Job<TData, TResult, TName>,
    error: Error,
    removeOnFail: boolean | number,
  ): Promise<number> {
    const now = Date.now();
    const removeOption = typeof removeOnFail === "number"
      ? removeOnFail.toString()
      : String(removeOnFail);
    const failedReason = error.message || "Unknown error";
    const stacktrace = JSON.stringify(
      error.stack?.split("\n").slice(0, 20) ?? [],
    );
    const finalAttemptsMade = job.attemptsMade;
    const args = [
      job.id,
      failedReason,
      stacktrace,
      removeOption,
      now.toString(),
      job.lockToken,
      finalAttemptsMade.toString(),
    ];
    // @ts-ignore
    return this.client.moveToFailed(
      keys.active,
      keys.failed,
      keys.jobs,
      ...args,
    );
  }

  async retryJob<TData, TResult, TName extends string>(
    keys: QueueKeys,
    job: Job<TData, TResult, TName>,
    delay: number,
    error: Error,
  ): Promise<number> {
    const now = Date.now();
    const failedReason = error.message || "Retry Error";
    const stacktrace = JSON.stringify(
      error.stack?.split("\n").slice(0, 20) ?? [],
    );
    const args = [
      job.id,
      delay.toString(),
      now.toString(),
      failedReason,
      stacktrace,
    ];
    // @ts-ignore
    return this.client.retryJob(
      keys.active,
      keys.delayed,
      keys.wait,
      keys.jobs,
      ...args,
    );
  }

  async moveDelayedToWait(
    keys: QueueKeys,
    timestamp: number,
    limit: number = 50,
  ): Promise<number> {
    const args = [timestamp.toString(), limit.toString()];
    // @ts-ignore
    return this.client.moveDelayedToWait(keys.delayed, keys.wait, ...args);
  }

  async extendLock(
    keys: QueueKeys,
    jobId: string,
    token: string,
    duration: number,
  ): Promise<number> {
    const now = Date.now();
    const args = [jobId, token, duration.toString(), now.toString()];
    // @ts-ignore
    return this.client.extendLock(keys.jobs, ...args);
  }

  async moveSpecificJobToActive(
    keys: QueueKeys,
    jobId: string,
    lockToken: string,
    lockDuration: number,
  ): Promise<Record<string, string> | null> {
    const now = Date.now();
    const args = [jobId, lockToken, lockDuration.toString(), now.toString()];
    // @ts-ignore
    const result = await this.client.moveSpecificJobToActive(
      keys.active,
      keys.jobs,
      ...args,
    );
    if (result) {
      const jobDataMap: Record<string, string> = {};
      for (let i = 0; i < result.length; i += 2) {
        jobDataMap[result[i]] = result[i + 1];
      }
      return jobDataMap;
    }
    return null;
  }

  async lockAndGetScheduler(
    lockKey: string,
    schedulerKey: string,
    lockValue: string,
    lockDuration: number,
  ): Promise<Record<string, string> | null> {
    const args = [lockValue, lockDuration.toString()];
    // @ts-ignore
    const result = await this.client.lockAndGetScheduler(
      lockKey,
      schedulerKey,
      ...args,
    );
    
    if (result && Array.isArray(result) && result.length > 0) {
      const schedulerDataMap: Record<string, string> = {};
      for (let i = 0; i < result.length; i += 2) {
        schedulerDataMap[result[i]] = result[i + 1];
      }
      return schedulerDataMap;
    }
    return null;
  }
}
