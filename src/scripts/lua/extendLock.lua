--[[
  Extends the lock duration for an active job if the token matches.

  Input:
    KEYS[1] jobsPrefix

    ARGV[1] jobId
    ARGV[2] lockToken
    ARGV[3] lockDuration (milliseconds)
    ARGV[4] now (timestamp ms)

  Output:
    newLockedUntil timestamp (ms) if successful
    0 if lock mismatch or job does not exist
]]
local jobsPrefix = KEYS[1]

local jobId = ARGV[1]
local lockToken = ARGV[2]
local lockDuration = tonumber(ARGV[3])
local now = tonumber(ARGV[4])

local jobKey = jobsPrefix .. ':' .. jobId

local currentLockToken = redis.call("HGET", jobKey, "lockToken")

-- Check if job exists and token matches
if currentLockToken and currentLockToken == lockToken then
  local newLockedUntil = now + lockDuration
  redis.call("HSET", jobKey, "lockedUntil", newLockedUntil)
  return newLockedUntil
else
  -- Lock mismatch, job doesn't exist, or lock field missing
  return 0
end