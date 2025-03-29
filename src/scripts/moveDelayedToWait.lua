--[[
  Moves jobs from the delayed set to the wait list if their time has come.

  Input:
    KEYS[1] delayedKey
    KEYS[2] waitKey

    ARGV[1] now (timestamp ms as string)
    ARGV[2] limit (max jobs to move per call as string)

  Output:
    Number of jobs moved
]]
local delayedKey = KEYS[1]
local waitKey = KEYS[2]

local now_str = ARGV[1]
local limit_str = ARGV[2]
-- No need to convert limit to number unless used for complex looping logic later
-- local limit = tonumber(limit_str)

-- Get jobs ready to be moved (score <= now)
-- Pass score/limit arguments as strings
local jobIds = redis.call("ZRANGEBYSCORE", delayedKey, "-inf", now_str, "LIMIT", "0", limit_str) -- Use "-inf" for min, pass args as strings

local numJobIds = #jobIds
if numJobIds > 0 then
  -- Remove them from the delayed set (jobId is already a string)
  for i = 1, numJobIds do
      local jobId = jobIds[i]
      redis.call("ZREM", delayedKey, jobId) -- jobId from ZRANGEBYSCORE is string
  end

  -- Add them to the wait list (jobId is already a string)
  for i = 1, numJobIds do
      local jobId = jobIds[i]
      redis.call("LPUSH", waitKey, jobId) -- jobId from ZRANGEBYSCORE is string
  end

  -- TODO: Update delay field in job hashes to 0? (Optional optimization)
  -- TODO: Emit 'waiting' events via Pub/Sub?
  -- TODO: Add wait marker?
end

return numJobIds -- Return the count