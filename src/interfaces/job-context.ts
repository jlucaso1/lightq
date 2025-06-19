import type { QueueKeys } from "../utils";
import type { RedisClient } from "./index";

/**
 * Context interface that provides the necessary dependencies for Job operations
 * without creating a circular dependency with Queue
 */
export interface JobContext {
	/** Redis client for database operations */
	client: RedisClient;
	/** Queue keys for Redis operations */
	keys: QueueKeys;
}

/**
 * Interface for progress update operations
 */
export interface ProgressUpdater {
	updateProgress(jobId: string, progress: number | object): Promise<void>;
}
