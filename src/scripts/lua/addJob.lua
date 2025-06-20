--[[
  Adds a job to the wait or delayed list and creates its data hash.

  Input:
    KEYS[1] jobsPrefix (e.g., lightq:myqueue:jobs)
    KEYS[2] waitKey    (e.g., lightq:myqueue:wait)
    KEYS[3] delayedKey (e.g., lightq:myqueue:delayed)

    ARGV[1] jobId
    ARGV[2] name
    ARGV[3] dataJson
    ARGV[4] optsJson
    ARGV[5] timestamp (creation time as string)
    ARGV[6] delayMs   (initial delay as string)
    ARGV[7] attemptsMade (as string, should be "0" initially)

  Output:
    jobId if successful
    0 if job already exists
]]
local jobsPrefix = KEYS[1]
local waitKey = KEYS[2]
local delayedKey = KEYS[3]

local jobId = ARGV[1]
local name = ARGV[2]
local dataJson = ARGV[3]
local optsJson = ARGV[4]
local timestamp_str = ARGV[5]
local delayMs_str = ARGV[6]
local attemptsMade_str = ARGV[7] -- Keep as string

local timestamp = tonumber(timestamp_str) -- Convert for calculation
local delayMs = tonumber(delayMs_str)     -- Convert for calculation and check

local jobKey = jobsPrefix .. ':' .. jobId

-- Atomically claim the job ID by setting the 'id' field only if the hash is new.
if redis.call("HSETNX", jobKey, "id", jobId) == 0 then
  return 0 -- Job already exists
end

-- At this point, the job is claimed. Now set the rest of the data.
redis.call("HMSET", jobKey,
  "name", name,
  "data", dataJson,
  "opts", optsJson,
  "timestamp", timestamp_str, -- Use original string
  "delay", delayMs_str,     -- Use original string
  "attemptsMade", attemptsMade_str -- Use original string
  -- Note: initial progress, returnvalue, failedReason, stacktrace, locks are omitted
)

if delayMs > 0 then
  local delayedTimestamp = timestamp + delayMs
  -- Pass score as string
  redis.call("ZADD", delayedKey, tostring(delayedTimestamp), jobId)
  -- TODO: Add marker for delayed jobs if implementing efficient blocking
else
  redis.call("LPUSH", waitKey, jobId) -- Add to head for FIFO processing with RPOPLPUSH
  -- TODO: Add marker for waiting jobs if implementing efficient blocking
end

return jobId