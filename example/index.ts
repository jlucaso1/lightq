import { Job, Queue, Worker } from "../src";

// 1. Configure Redis Connection
const redisConnectionOpts = {
  host: "localhost",
  port: 6379,
  maxRetriesPerRequest: null,
};
const queueName = "email-tasks";

// --- Define Job Data and Result Types (Optional but recommended) ---
interface EmailJobData {
  to: string;
  subject: string;
  body: string;
}

interface EmailJobResult {
  success: boolean;
  messageId?: string;
}

// 2. Create a Queue Instance
const emailQueue = new Queue<EmailJobData, EmailJobResult>(queueName, {
  connection: redisConnectionOpts,
  defaultJobOptions: {
    attempts: 3, // Default attempts for jobs in this queue
    backoff: { type: "exponential", delay: 1000 }, // Default backoff strategy
  },
});

// 3. Add Jobs to the Queue
async function addEmails() {
  console.log("Adding jobs...");
  await emailQueue.add("send-welcome", {
    to: "user1@example.com",
    subject: "Welcome!",
    body: "Thanks for signing up.",
  });

  await emailQueue.add(
    "send-promo",
    {
      to: "user2@example.com",
      subject: "Special Offer!",
      body: "Check out our new deals.",
    },
    { delay: 5000 } // Send this job after a 5-second delay
  );

  console.log("Jobs added.");
  // Close the queue connection when no longer needed for adding jobs
  // await emailQueue.close(); // Or keep it open if adding more jobs later
}

// 4. Create a Worker to Process Jobs
const emailWorker = new Worker<EmailJobData, EmailJobResult>(
  queueName,
  async (job: Job<EmailJobData, EmailJobResult>) => {
    console.log(`Processing job ${job.id} (${job.name}) for ${job.data.to}`);
    console.log(`Subject: ${job.data.subject}`);

    // Simulate sending the email
    await new Promise((resolve) => setTimeout(resolve, 1500));

    // Example: Simulate failure for a specific email
    if (job.data.to === "fail@example.com") {
      throw new Error("Simulated email sending failure");
    }

    console.log(`Finished job ${job.id}`);
    // Return the result
    return { success: true, messageId: `msg_${job.id}` };
  },
  {
    connection: redisConnectionOpts,
    concurrency: 5, // Process up to 5 emails concurrently
  }
);

// 5. Listen to Worker Events (Optional)
emailWorker.on("completed", (job, result) => {
  console.log(`Job ${job.id} completed successfully! Result:`, result);
});

emailWorker.on("failed", (job, error) => {
  console.error(
    `Job ${job?.id} failed after ${job?.attemptsMade} attempts with error: ${error.message}`
  );
});

emailWorker.on("error", (error) => {
  console.error("Worker encountered an error:", error);
});

emailWorker.on("ready", () => {
  console.log("Worker is ready and connected to Redis.");
});

emailWorker.on("closing", () => {
  console.log("Worker is closing...");
});

emailWorker.on("closed", () => {
  console.log("Worker has closed.");
});

// --- Start the process ---
addEmails().catch(console.error);
console.log("Worker started...");

// --- Graceful Shutdown Handling ---
async function shutdown() {
  console.log("Received signal to shut down.");
  console.log("Closing worker...");
  await emailWorker.close(); // Wait for active jobs to finish
  console.log("Closing queue connection (if still open)...");
  await emailQueue.close(); // Close queue connection used for adding
  console.log("Shutdown complete.");
  process.exit(0);
}

process.on("SIGINT", shutdown); // Handle Ctrl+C
process.on("SIGTERM", shutdown); // Handle kill commands
