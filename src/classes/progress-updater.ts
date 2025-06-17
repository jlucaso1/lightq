import type { JobContext, ProgressUpdater } from "../interfaces/job-context";

export class JobProgressUpdater implements ProgressUpdater {
  constructor(private context: JobContext) {}

  async updateProgress(
    jobId: string,
    progress: number | object
  ): Promise<void> {
    const jobKey = `${this.context.keys.jobs}:${jobId}`;
    const progressValue = JSON.stringify(progress);

    await this.context.client.hset(jobKey, "progress", progressValue);

    const progressChannel = `${this.context.keys.base}:progress`;
    await this.context.client.publish(
      progressChannel,
      JSON.stringify({
        jobId: jobId,
        progress: progress,
        timestamp: Date.now(),
      })
    );
  }
}
