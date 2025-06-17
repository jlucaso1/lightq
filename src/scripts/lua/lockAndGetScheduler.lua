-- Lock and Get Scheduler Lua Script
-- Atomically acquire a distributed lock and fetch scheduler data
--
-- KEYS[1] = lockKey (e.g., lightq:my-queue:schedulers:lock:my-job)
-- KEYS[2] = schedulerKey (e.g., lightq:my-queue:schedulers:my-job)
-- ARGV[1] = lockValue
-- ARGV[2] = lockDuration (seconds)

local lockAcquired = redis.call('SET', KEYS[1], ARGV[1], 'EX', ARGV[2], 'NX')

if lockAcquired then
  return redis.call('HGETALL', KEYS[2])
else
  return nil
end