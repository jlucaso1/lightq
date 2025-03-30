--[[
  Moves a job from active to completed, releases lock, stores result.

  Input:
    KEYS[1] activeKey
    KEYS[2] completedKey (ZSET, score=timestamp)
    KEYS[3] jobsPrefix

    ARGV[1] jobId
    ARGV[2] returnValueJson
    ARGV[3] removeOptionString ('true', 'false', or max count number as string)
    ARGV[4] now (timestamp ms)
    ARGV[5] lockToken

  Output:
     0: Success
    -1: Lock mismatch or job hash missing
    -2: Job not found in active list
]]
local activeKey = KEYS[1]
local completedKey = KEYS[2]
local jobsPrefix = KEYS[3]

local jobId = ARGV[1]
local returnValueJson = ARGV[2]
local removeOption = ARGV[3] -- Keep as string for comparison
local now = tonumber(ARGV[4])
local lockToken = ARGV[5]

local jobKey = jobsPrefix .. ':' .. jobId

-- 1. Verify Lock
local currentLockToken = redis.call("HGET", jobKey, "lockToken")
if not currentLockToken or currentLockToken ~= lockToken then
  return -1 -- Lock mismatch or job missing
end

-- 2. Remove from Active list
-- LREM count: 0=all, >0=from head, <0=from tail. Use -1 to remove one from tail (matches RPOPLPUSH)
local removedCount = redis.call("LREM", activeKey, -1, jobId)
if removedCount == 0 then
  return -2 -- Job not in active list (or already moved)
end

-- 3. Handle removal or move to Completed set
if removeOption == 'true' then
  -- Remove job data completely
  redis.call("DEL", jobKey)
else
  -- Update job data: add result, finish time, remove lock info
  redis.call("HMSET", jobKey,
    "returnValue", returnValueJson,
    "finishedOn", now
  )
  redis.call("HDEL", jobKey, "lockToken", "lockedUntil", "processedOn")

  -- Add to completed set, score is finish timestamp
  redis.call("ZADD", completedKey, now, jobId)

  -- Optional: Trim completed set (implement if needed)
  local keepCount = tonumber(removeOption)

  if keepCount and keepCount > 0 then
    redis.call("ZREMRANGEBYRANK", completedKey, 0, -(keepCount + 1))
  end
end

-- TODO: Emit 'completed' event via Pub/Sub if needed

return 0 -- Success