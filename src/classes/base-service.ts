import { EventEmitter } from "node:events";
import type { BaseServiceOptions, RedisClient } from "../interfaces";
import { createRedisClient } from "../utils";

/**
 * Abstract base class for services that interact with Redis.
 * Handles common client setup, prefix management, and closing logic.
 */
export abstract class RedisService<
	T extends BaseServiceOptions,
> extends EventEmitter {
	/** The ioredis client instance */
	readonly client: RedisClient;
	/** The resolved prefix for Redis keys */
	readonly prefix: string;
	/** The options passed to the constructor */
	readonly opts: T;

	/** Promise indicating the service is closing */
	protected closing: Promise<void> | null = null;

	constructor(opts: T) {
		super();
		this.opts = opts;
		this.prefix = opts.prefix ?? "lightq";
		this.client = createRedisClient(opts.connection);

		// Basic error handling - specific services might add more handlers
		this.client.on("error", (err) => {
			// Prevent emitting errors after closing has started or finished
			if (!this.closing) {
				this.emit("error", err);
			}
		});

		this.client.on("ready", () => {
			// Prevent emitting ready after closing has started or finished
			if (!this.closing) {
				this.emit("ready");
			}
		});
	}

	/**
	 * Closes the service and its Redis connection(s).
	 * Waits for internal cleanup defined in `_internalClose`.
	 * @param force - If true, attempts a faster shutdown, potentially abandoning ongoing tasks.
	 */
	public async close(force = false): Promise<void> {
		if (!this.closing) {
			this.closing = (async () => {
				this.emit("closing");
				try {
					// Perform service-specific cleanup first
					await this._internalClose(force);
				} catch (err) {
					console.error(
						`Error during internal close for ${this.constructor.name}:`,
						err,
					);
					// Optionally emit an error event here
					this.emit(
						"error",
						new Error(
							`Internal close failed for ${this.constructor.name}: ${
								(err as Error).message
							}`,
						),
					);
				}

				// Close the main Redis client
				try {
					if (force) {
						this.client.disconnect();
					} else if (["connect", "ready"].includes(this.client.status)) {
						await this.client.quit();
					} else {
						// If not connected or already closing/closed, just disconnect locally
						this.client.disconnect();
					}
				} catch (err) {
					// Ignore "Connection is closed" error which is expected sometimes
					if (
						(err as Error).message &&
						!(err as Error).message.includes("Connection is closed")
					) {
						console.error(
							`Error closing main Redis connection for ${this.constructor.name}:`,
							err,
						);
						this.client.disconnect(); // Force disconnect on unexpected error
						this.emit(
							"error",
							new Error(
								`Redis quit failed for ${this.constructor.name}: ${
									(err as Error).message
								}`,
							),
						);
					}
				}

				this.emit("closed");
			})();
		}
		return this.closing;
	}

	/**
	 * Abstract method for service-specific cleanup logic before the Redis connection is closed.
	 * This is where derived classes should stop loops, clear timers, wait for jobs, etc.
	 * @param force - Indicates if the close should be forced.
	 */
	protected abstract _internalClose(force?: boolean): Promise<void>;
}
