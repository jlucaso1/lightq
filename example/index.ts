import { Job, Queue, Worker } from "../src"; // Use '@jlucaso/lightq' if installed as a package
import process from "node:process";

// --- 1. Configure Redis Connection ---
const redisConnectionOpts = {
  host: process.env.REDIS_HOST || "localhost",
  port: process.env.REDIS_PORT ? parseInt(process.env.REDIS_PORT, 10) : 6379,
  maxRetriesPerRequest: null, // Avoid default ioredis retry noise in demo
};

const QUEUE_NAME = "demo-tasks";

// --- 2. Define Job Data Interfaces (Best Practice) ---
interface EmailJobData {
  to: string;
  subject: string;
  body: string;
}
interface EmailJobResult {
  status: "success" | "failed";
  messageId?: string;
  error?: string;
}

interface ReportJobData {
  type: "daily" | "weekly";
  recipient: string;
}
interface ReportJobResult {
  status: "success" | "failed";
  reportUrl?: string;
  error?: string;
}

// --- 3. Create a Queue Instance ---
console.log(`[Queue] Creating queue "${QUEUE_NAME}"...`);
// Use a Union Type for the queue to handle multiple job types
const tasksQueue = new Queue<
  EmailJobData | ReportJobData,
  EmailJobResult | ReportJobResult
>(QUEUE_NAME, {
  connection: { ...redisConnectionOpts }, // Use a copy for potential modification needs
  // Default options applied to ALL jobs unless overridden
  defaultJobOptions: {
    attempts: 2,
    backoff: { type: "fixed", delay: 500 }, // Modest backoff for demo
    removeOnComplete: 50, // Keep details of last 50 completed jobs in the 'completed' set
    removeOnFail: 100, // Keep details of last 100 failed jobs in the 'failed' set
  },
});

tasksQueue.on(
  "ready",
  () => console.log("[Queue] Connected to Redis and ready."),
);
tasksQueue.on(
  "error",
  (error) => console.error("[Queue] Error:", error.message),
);

// --- 4. Define Functions to Add Different Job Types ---

async function addImmediateEmail() {
  console.log("[Queue] Adding immediate welcome email job...");
  const job = await tasksQueue.add("email:send-welcome", {
    to: "new-user@example.com",
    subject: "Welcome to LightQ!",
    body: "We're glad you're here.",
  });
  console.log(`[Queue] Added Job ID: ${job.id} (email:send-welcome)`);
}

async function addDelayedEmail() {
  const delayMs = 5000; // 5 seconds
  console.log(
    `[Queue] Adding delayed promo email job (Delay: ${delayMs}ms)...`,
  );
  const job = await tasksQueue.add("email:send-promo", {
    to: "subscriber@example.com",
    subject: "Special Offer Just For You!",
    body: "Check out these amazing deals.",
  }, {
    delay: delayMs, // Job-specific option
    attempts: 1, // Override default attempts
  });
  console.log(`[Queue] Added Job ID: ${job.id} (email:send-promo)`);
}

async function addFailingEmail() {
  console.log("[Queue] Adding email job designed to fail...");
  const job = await tasksQueue.add("email:send-invalid", {
    to: "fail@example.com", // Worker logic will cause this to fail
    subject: "This Should Fail",
    body: "Testing failure and retries.",
  }, {
    // Uses default attempts (2) and backoff (500ms fixed) from queue options
    removeOnFail: 5, // Keep only the last 5 records for this specific failing type
  });
  console.log(`[Queue] Added Job ID: ${job.id} (email:send-invalid)`);
}

// --- 5. Define and Add a Scheduled Job ---

async function setupScheduledReport() {
  const schedulerId = "report:weekly-summary";
  const repeatOptions = {
    // pattern: '0 0 * * MON', // Every Monday at midnight
    every: 10000, // For demo: Run every 10 seconds
    // tz: 'America/New_York' // Optional timezone for cron
  };
  const jobTemplate = {
    name: "report:generate-summary", // Job name for instances created by scheduler
    data: { type: "weekly", recipient: "admin@example.com" },
    opts: {
      // Specific options for generated report jobs
      removeOnComplete: 5, // Keep only 5 successful report job records
      attempts: 1,
    },
  };

  console.log(
    `[Scheduler] Upserting job scheduler "${schedulerId}" (runs every ${
      repeatOptions.every / 1000
    }s)...`,
  );
  try {
    await tasksQueue.upsertJobScheduler(
      schedulerId,
      repeatOptions,
      jobTemplate,
    );
    console.log(
      `[Scheduler] Scheduler "${schedulerId}" upserted successfully.`,
    );
  } catch (error) {
    console.error(
      `[Scheduler] Failed to upsert scheduler "${schedulerId}":`,
      error,
    );
  }

  // Optional: Demonstrate removing the scheduler after some time
  const REMOVAL_DELAY = 35 * 1000; // 35 seconds
  console.log(
    `[Scheduler] Will attempt to remove scheduler "${schedulerId}" in ${
      REMOVAL_DELAY / 1000
    } seconds.`,
  );
  setTimeout(async () => {
    console.log(
      `[Scheduler] Attempting to remove scheduler "${schedulerId}"...`,
    );
    try {
      const removed = await tasksQueue.removeJobScheduler(schedulerId);
      console.log(
        `[Scheduler] Scheduler "${schedulerId}" removal status:`,
        removed,
      );
    } catch (err) {
      console.error(
        `[Scheduler] Error removing scheduler "${schedulerId}":`,
        err,
      );
    }
  }, REMOVAL_DELAY);
}

tasksQueue.on("scheduler_error", (err) => {
  console.error("[Scheduler] Error:", err);
});
tasksQueue.on("scheduler_job_added", (schedulerId, job) => {
  console.log(
    `[Scheduler] Scheduler [${schedulerId}] added job: ${job.id} (${job.name})`,
  );
});

// --- 6. Create a Worker to Process Jobs ---

console.log(`[Worker] Creating worker for queue "${QUEUE_NAME}"...`);
// Use a Union Type for the worker matching the queue
const taskWorker = new Worker<
  EmailJobData | ReportJobData,
  EmailJobResult | ReportJobResult
>(
  QUEUE_NAME,
  // --- Processor Function ---
  async (
    job: Job<EmailJobData | ReportJobData, EmailJobResult | ReportJobResult>,
  ) => {
    console.log(
      `[Worker] Processing job ${job.id} (Name: ${job.name}, Attempt: ${
        job.attemptsMade + 1
      }/${job.opts.attempts ?? tasksQueue.opts.defaultJobOptions?.attempts})`,
    );

    // Handle different job types based on name
    if (job.name.startsWith("email:")) {
      const data = job.data as EmailJobData; // Type assertion
      console.log(
        `   -> Sending email to: ${data.to}, Subject: ${data.subject}`,
      );

      await delay(1000); // Simulate network latency

      if (data.to === "fail@example.com") {
        throw new Error(`Simulated email failure for ${data.to}`);
      }

      console.log(`   -> Email sent successfully for job ${job.id}`);
      return { status: "success", messageId: `fake-id-${job.id}` }; // Return EmailJobResult
    } else if (job.name.startsWith("report:")) {
      const data = job.data as ReportJobData; // Type assertion
      console.log(
        `   -> Generating ${data.type} report for: ${data.recipient}`,
      );

      await delay(1500); // Simulate report generation time

      console.log(`   -> Report generated successfully for job ${job.id}`);
      return {
        status: "success",
        reportUrl: `/reports/summary-${Date.now()}.pdf`,
      }; // Return ReportJobResult
    } else {
      console.warn(`[Worker] Unknown job name: ${job.name}`);
      throw new Error(`Unknown job name: ${job.name}`);
    }
  },
  // --- Worker Options ---
  {
    connection: { ...redisConnectionOpts }, // Worker needs its own connection options
    concurrency: 3, // Process up to 3 jobs concurrently
    lockDuration: 10000, // Lock jobs for 10 seconds while processing
    // removeOnComplete/Fail defaults can also be set here, overriding Queue defaults
    // removeOnComplete: 100,
  },
);

// --- 7. Listen to Worker Events (Optional but Recommended) ---

taskWorker.on("completed", (job, result) => {
  console.log(`[Worker] Job ${job.id} (${job.name}) COMPLETED successfully.`);
  // console.log(`   -> Result:`, result);
});

taskWorker.on("failed", (job, error) => {
  // Note: Job object might be undefined if the error occurred before a job was fully fetched/locked
  if (job) {
    console.error(
      `[Worker] Job ${job.id} (${job.name}) FAILED after ${job.attemptsMade} attempts.`,
    );
    console.error(`   -> Error: ${error.message}`);
  } else {
    console.error(
      `[Worker] A job failed with error, but job details are unavailable. Error: ${error.message}`,
    );
  }
});

taskWorker.on("error", (error, job) => {
  // Errors not related to a specific job's processing (e.g., connection, lock renewal)
  if (job) {
    console.error(`[Worker] Error related to job ${job.id}: ${error.message}`);
  } else {
    console.error(`[Worker] Generic worker error: ${error.message}`);
  }
});

taskWorker.on("active", (job) => {
  // console.log(`[Worker] Job ${job.id} (${job.name}) is now ACTIVE.`);
});

taskWorker.on("retrying", (job, error) => {
  console.warn(
    `[Worker] Job ${job.id} (${job.name}) failed, will RETRY. Error: ${error.message}`,
  );
});

taskWorker.on("ready", () => {
  console.log("[Worker] Worker is ready and connected to Redis.");
});

taskWorker.on("closing", () => {
  console.log("[Worker] Worker is closing...");
});

taskWorker.on("closed", () => {
  console.log("[Worker] Worker has closed.");
});

// --- 8. Start Adding Jobs and Running the Demo ---

async function runDemo() {
  console.log("--- Adding initial jobs ---");
  await addImmediateEmail();
  await addDelayedEmail();
  await addFailingEmail();

  console.log("--- Setting up scheduled jobs ---");
  await setupScheduledReport(); // Set up the recurring job

  console.log(
    "--- Worker started, processing jobs... (Press Ctrl+C to exit) ---",
  );
  // Worker starts processing automatically upon creation if not paused
}

runDemo().catch((error) => {
  console.error("Demo initialization failed:", error);
  process.exit(1);
});

// --- 9. Graceful Shutdown Handling ---

let isShuttingDown = false;
async function shutdown() {
  if (isShuttingDown) return;
  isShuttingDown = true;
  console.log("\n--- Initiating graceful shutdown ---");

  console.log("[Main] Closing worker (waiting for active jobs)...");
  await taskWorker.close(); // Wait for active jobs to finish

  console.log("[Main] Closing queue connection...");
  await tasksQueue.close(); // Close queue & scheduler connection

  console.log("--- Shutdown complete ---");
  process.exit(0);
}

process.on("SIGINT", shutdown); // Handle Ctrl+C
process.on("SIGTERM", shutdown); // Handle kill commands

function delay(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}
