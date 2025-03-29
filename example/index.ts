import { Queue, Worker } from "../src";
import process from "node:process";

const connectionOpts = { host: "localhost", port: 6379 };

const myQueue = new Queue<
  { email: string; subject: string },
  { success: boolean }
>("emails", {
  connection: connectionOpts,
  defaultJobOptions: {
    attempts: 3,
    backoff: { type: "exponential", delay: 1000 },
    removeOnComplete: 100,
    removeOnFail: 500,
  },
});

async function addEmails() {
  await myQueue.add("send-welcome", {
    email: "test1@example.com",
    subject: "Welcome!",
  });
  await myQueue.add(
    "send-promo",
    {
      email: "test2@example.com",
      subject: "Special Offer!",
    },
    { delay: 5000 },
  );

  console.log("Jobs added");
  await myQueue.close();
}

const worker = new Worker<
  { email: string; subject: string },
  { success: boolean }
>(
  "emails",
  async (job) => {
    console.log(`Processing job ${job.id} (${job.name}) for ${job.data.email}`);
    await new Promise((resolve) => setTimeout(resolve, 1000));

    if (job.data.email === "fail@example.com") {
      throw new Error("Invalid email address");
    }

    console.log(`Finished job ${job.id}`);
    return { success: true };
  },
  {
    connection: connectionOpts,
    concurrency: 5,
  },
);

worker.on("completed", (job, result) => {
  console.log(`Job ${job.id} completed! Result:`, result);
});

worker.on("failed", (job, err) => {
  console.error(`Job ${job?.id} failed with ${err.message}`);
});

worker.on("error", (err) => {
  console.error("Worker error:", err);
});

addEmails().catch(console.error);

console.log("Worker started...");

process.on("SIGINT", async () => {
  console.log("Closing worker...");
  await worker.close();
  console.log("Worker closed.");
  process.exit(0);
});
