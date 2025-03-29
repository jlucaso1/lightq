--[[
  Moves a job from failed set back to wait or delayed (for backoff).

  Input:
    KEYS[1] failedKey
    KEYS[2] delayedKey
    KEYS[3] waitKey
    KEYS[4] jobsPrefix

    ARGV[1] jobId
    ARGV[2] delayMs (backoff delay)
    ARGV[3] now (timestamp ms)
    ARGV[4] newFailedReason (optional: update reason on retry?)
    ARGV[5] newStacktraceJson (optional: update stacktrace on retry?)

  Output:
     0: Success
    -1: Job not found in failed set
    -2: Job hash data missing (consistency issue)
]]
local failedKey = KEYS[1]
local delayedKey = KEYS[2]
local waitKey = KEYS[3]
local jobsPrefix = KEYS[4]

local jobId = ARGV[1]
local delayMs = tonumber(ARGV[2])
local now = tonumber(ARGV[3])
local newFailedReason = ARGV[4] -- Potentially store retry reason? Keep original for now.
local newStacktraceJson = ARGV[5] -- Potentially store retry stack? Keep original for now.

local jobKey = jobsPrefix .. ':' .. jobId

-- 1. Remove from Failed set
local removedCount = redis.call("ZREM", failedKey, jobId)
if removedCount == 0 then
  return -1 -- Job not in failed set
end

-- 2. Check if job data exists
if redis.call("EXISTS", jobKey) == 0 then
    return -2 -- Job data is missing, cannot retry
end

-- 3. Update Job Data: Increment attempts, clear finish/fail state
redis.call("HINCRBY", jobKey, "attemptsMade", 1)
redis.call("HDEL", jobKey, "finishedOn", "failedReason", "stacktrace", "processedOn")
-- Optionally update reason/stacktrace if ARGV[4]/[5] are provided

-- 4. Move to Wait or Delayed
if delayMs > 0 then
  local delayedTimestamp = now + delayMs
  redis.call("ZADD", delayedKey, delayedTimestamp, jobId)
  -- TODO: Add delayed marker if needed
else
  redis.call("LPUSH", waitKey, jobId) -- Add to head for FIFO processing
  -- TODO: Add wait marker if needed
end

-- TODO: Emit 'retrying' or 'waiting' event via Pub/Sub if needed

return 0 -- Success