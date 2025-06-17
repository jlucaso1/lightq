--[[
  Atomically locks a specific job in the active list and returns its data.
  This is used after a BRPOPLPUSH operation.

  Input:
    KEYS[1] activeKey (Note: This key is no longer used for LPUSH)
    KEYS[2] jobsPrefix

    ARGV[1] jobId (the specific job ID to lock)
    ARGV[2] lockToken (unique token for this worker+job)
    ARGV[3] lockDuration (milliseconds)
    ARGV[4] now (current timestamp ms)

  Output:
    jobDataArray (flat array from HGETALL) if successful
    nil if job doesn't exist or is already locked
]]
local activeKey = KEYS[1]
local jobsPrefix = KEYS[2]

local jobId = ARGV[1]
local lockToken = ARGV[2]
local lockDuration = tonumber(ARGV[3])
local now = tonumber(ARGV[4])

local jobKey = jobsPrefix .. ':' .. jobId
local lockedUntil = now + lockDuration

local lockInfo = redis.call("HMGET", jobKey, "lockToken", "lockedUntil")
local currentLockToken = lockInfo[1]
local currentLockedUntil = lockInfo[2]

if not currentLockToken and not currentLockedUntil then
    if redis.call("EXISTS", jobKey) == 0 then
        return nil
    end
end

if currentLockToken and currentLockedUntil then
    local lockedUntilNum = tonumber(currentLockedUntil)
    if lockedUntilNum and lockedUntilNum > now then
        -- Job is still locked by another worker
        return nil
    end
end

redis.call("HMSET", jobKey,
  "lockToken", lockToken,
  "lockedUntil", lockedUntil,
  "processedOn", now
)

-- Return all job data
local jobData = redis.call("HGETALL", jobKey)
return jobData