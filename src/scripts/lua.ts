import type { JobData, RedisClient } from "../interfaces";
import * as fs from "node:fs";
import * as path from "node:path";
import { Pipeline } from "ioredis";
import { Job } from "../classes/job";
import type { QueueKeys } from "../utils";

function loadScriptContent(scriptName: string): string {
  const scriptPath = path.join(import.meta.dirname, `${scriptName}.lua`);
  try {
    return fs.readFileSync(scriptPath, "utf-8");
  } catch (err) {
    console.error(`Error loading Lua script ${scriptName}:`, err);
    throw new Error(`Could not load script ${scriptName}`);
  }
}

export class LuaScripts {
  private client: RedisClient;
  static readonly commands = [
    "addJob",
    "moveToActive",
    "moveToCompleted",
    "moveToFailed",
    "retryJob",
    "moveDelayedToWait",
    "extendLock",
  ] as const;

  constructor(client: RedisClient) {
    this.client = client;
    this.loadScripts();
  }

  private loadScripts() {
    LuaScripts.commands.forEach((name) => {
      const scriptContent = loadScriptContent(name);
      let numberOfKeys: number;
      switch (name) {
        case "addJob":
          numberOfKeys = 3;
          break;
        case "moveToActive":
          numberOfKeys = 3;
          break;
        case "moveToCompleted":
          numberOfKeys = 3;
          break;
        case "moveToFailed":
          numberOfKeys = 3;
          break;
        case "retryJob":
          numberOfKeys = 4;
          break;
        case "moveDelayedToWait":
          numberOfKeys = 2;
          break;
        case "extendLock":
          numberOfKeys = 1;
          break;
        default:
          throw new Error(`Unknown script or missing key count: ${name}`);
      }
      this.client.defineCommand(name, { numberOfKeys, lua: scriptContent });
    });
  }

  async addJob(
    keys: QueueKeys,
    jobData: JobData<any>,
    pipeline?: Pipeline
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

  async moveToActive(
    keys: QueueKeys,
    lockToken: string,
    lockDuration: number
  ): Promise<[string, Record<string, string>] | null> {
    const now = Date.now();
    const args = [lockToken, lockDuration.toString(), now.toString()];
    // @ts-ignore
    const result = await this.client.moveToActive(
      keys.wait,
      keys.active,
      keys.jobs,
      ...args
    );
    if (result) {
      const jobId = result[0];
      const jobDataArr = result[1];
      const jobDataMap: Record<string, string> = {};
      for (let i = 0; i < jobDataArr.length; i += 2) {
        jobDataMap[jobDataArr[i]] = jobDataArr[i + 1];
      }
      return [jobId, jobDataMap];
    }
    return null;
  }

  async moveToCompleted(
    keys: QueueKeys,
    job: Job<any, any, any>,
    returnValue: any,
    removeOnComplete: boolean | number
  ): Promise<number> {
    const now = Date.now();
    const removeOption =
      typeof removeOnComplete === "number"
        ? removeOnComplete.toString()
        : String(removeOnComplete);
    const rvJson = JSON.stringify(returnValue ?? null);
    const args = [job.id, rvJson, removeOption, now.toString(), job.lockToken!];
    // @ts-ignore
    return this.client.moveToCompleted(
      keys.active,
      keys.completed,
      keys.jobs,
      ...args
    );
  }

  async moveToFailed(
    keys: QueueKeys,
    job: Job<any, any, any>,
    error: Error,
    removeOnFail: boolean | number
  ): Promise<number> {
    const now = Date.now();
    const removeOption =
      typeof removeOnFail === "number"
        ? removeOnFail.toString()
        : String(removeOnFail);
    const failedReason = error.message || "Unknown error";
    const stacktrace = JSON.stringify(
      error.stack?.split("\n").slice(0, 20) ?? []
    );
    const finalAttemptsMade = job.attemptsMade;
    const args = [
      job.id,
      failedReason,
      stacktrace,
      removeOption,
      now.toString(),
      job.lockToken!,
      finalAttemptsMade.toString(),
    ];
    // @ts-ignore
    return this.client.moveToFailed(
      keys.active,
      keys.failed,
      keys.jobs,
      ...args
    );
  }

  async retryJob(
    keys: QueueKeys,
    job: Job<any, any, any>,
    delay: number,
    error: Error
  ): Promise<number> {
    const now = Date.now();
    const failedReason = error.message || "Retry Error";
    const stacktrace = JSON.stringify(
      error.stack?.split("\n").slice(0, 20) ?? []
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
      ...args
    );
  }

  async moveDelayedToWait(
    keys: QueueKeys,
    timestamp: number,
    limit: number = 50
  ): Promise<number> {
    const args = [timestamp.toString(), limit.toString()];
    // @ts-ignore
    return this.client.moveDelayedToWait(keys.delayed, keys.wait, ...args);
  }

  async extendLock(
    keys: QueueKeys,
    jobId: string,
    token: string,
    duration: number
  ): Promise<number> {
    // jobsPrefix is not needed directly, jobKey is constructed in Lua
    // const jobKey = `${keys.jobs}:${jobId}`; // Constructing jobKey here is not needed for the call
    const now = Date.now();
    const args = [jobId, token, duration.toString(), now.toString()];
    // @ts-ignore
    return this.client.extendLock(keys.jobs, ...args);
  }
}
