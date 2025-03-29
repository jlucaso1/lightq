--[[
  Atomically moves a job from wait to active, locks it, and returns job data.

  Input:
    KEYS[1] waitKey
    KEYS[2] activeKey
    KEYS[3] jobsPrefix

    ARGV[1] lockToken (unique token for this worker+job)
    ARGV[2] lockDuration (milliseconds)
    ARGV[3] now (current timestamp ms)

  Output:
    {jobId, jobDataArray} if successful (jobDataArray is flat array from HGETALL)
    nil if no job is available in the wait queue
]]
local waitKey = KEYS[1]
local activeKey = KEYS[2]
local jobsPrefix = KEYS[3]

local lockToken = ARGV[1]
local lockDuration = tonumber(ARGV[2])
local now = tonumber(ARGV[3])

-- RPOP from wait, LPUSH to active (maintains FIFO within active for easier check)
local jobId = redis.call("RPOPLPUSH", waitKey, activeKey)

if jobId then
  local jobKey = jobsPrefix .. ':' .. jobId
  local lockedUntil = now + lockDuration

  -- Check if job hash exists (safety check)
  if redis.call("EXISTS", jobKey) == 0 then
      -- Job data missing, remove from active and return nil
      redis.call("LREM", activeKey, -1, jobId) -- Remove the instance we just pushed
      return nil
  end

  -- Set lock info and processing time directly in the job hash
  redis.call("HMSET", jobKey,
    "lockToken", lockToken,
    "lockedUntil", lockedUntil,
    "processedOn", now
    -- Increment attemptsMade only when moving to failed/completed later?
    -- Or maybe increment 'attemptsStarted' here? Simpler to do later.
  )

  -- Return jobId and all job data
  local jobData = redis.call("HGETALL", jobKey)
  return {jobId, jobData}
end

return nil