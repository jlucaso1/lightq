--[[
  Moves a job from active to failed, releases lock, stores reason/stack.

  Input:
    KEYS[1] activeKey
    KEYS[2] failedKey (ZSET, score=timestamp)
    KEYS[3] jobsPrefix

    ARGV[1] jobId
    ARGV[2] failedReason
    ARGV[3] stacktraceJson
    ARGV[4] removeOptionString ('true', 'false', or max count number as string)
    ARGV[5] now (timestamp ms)
    ARGV[6] lockToken

  Output:
     0: Success
    -1: Lock mismatch or job hash missing
    -2: Job not found in active list
]]
local activeKey = KEYS[1]
local failedKey = KEYS[2]
local jobsPrefix = KEYS[3]

local jobId = ARGV[1]
local failedReason = ARGV[2]
local stacktraceJson = ARGV[3]
local removeOption = ARGV[4]
local now = tonumber(ARGV[5])
local lockToken = ARGV[6]

local jobKey = jobsPrefix .. ':' .. jobId

-- 1. Verify Lock
local currentLockToken = redis.call("HGET", jobKey, "lockToken")
if not currentLockToken or currentLockToken ~= lockToken then
  return -1 -- Lock mismatch or job missing
end

-- 2. Remove from Active list
local removedCount = redis.call("LREM", activeKey, -1, jobId)
if removedCount == 0 then
  return -2 -- Job not in active list
end

-- 3. Handle removal or move to Failed set
if removeOption == 'true' then
  -- Remove job data completely
  redis.call("DEL", jobKey)
else
  -- Update job data: add failure info, finish time, remove lock info
  redis.call("HMSET", jobKey,
    "failedReason", failedReason,
    "stacktrace", stacktraceJson,
    "finishedOn", now
    -- Keep attemptsMade
  )
  redis.call("HDEL", jobKey, "lockToken", "lockedUntil", "processedOn")

  -- Add to failed set, score is finish timestamp
  redis.call("ZADD", failedKey, now, jobId)

  -- Optional: Trim failed set (implement if needed)
  -- local keepCount = tonumber(removeOption)
  -- if keepCount and keepCount > 0 then
  --   redis.call("ZREMRANGEBYRANK", failedKey, 0, -(keepCount + 1))
  -- end
end

-- TODO: Emit 'failed' event via Pub/Sub if needed

return 0 -- Success