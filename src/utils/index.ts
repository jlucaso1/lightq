import IORedis, { type RedisOptions } from "ioredis";
import type { RedisClient } from "../interfaces";

export function createRedisClient(
	connection: RedisOptions | RedisClient,
): RedisClient {
	if (connection instanceof IORedis || connection instanceof IORedis.Cluster) {
		// TODO: Consider implications of sharing/duplicating connections
		return connection;
	}
	return new IORedis(connection as RedisOptions);
}

export function getQueueKeys(prefix: string, queueName: string) {
	const base = `${prefix}:${queueName}`;
	return {
		base,
		wait: `${base}:wait`, // LIST - New jobs
		active: `${base}:active`, // LIST - Processing jobs
		delayed: `${base}:delayed`, // ZSET - Jobs scheduled for later
		completed: `${base}:completed`, // ZSET - Successfully finished jobs
		failed: `${base}:failed`, // ZSET - Jobs that exhausted retries
		locks: `${base}:locks`, // HASH or SET - JobId -> LockToken/Timestamp
		jobs: `${base}:jobs`, // HASH - JobId -> Job Data
		meta: `${base}:meta`, // HASH - Queue metadata (paused flag, etc.)
		// Potentially others: events (simple pub/sub?), priority (if added)
	};
}

export type QueueKeys = ReturnType<typeof getQueueKeys>;

export function delay(ms: number): Promise<void> {
	return new Promise((resolve) => setTimeout(resolve, ms));
}
