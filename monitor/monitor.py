import redis
import json
import time
import os
from redis_client_singleton import RedisClientSingleton

# Configuration from environment variables
MONITOR_INTERVAL = int(os.getenv('MONITOR_INTERVAL', 30))

# Redis key names
MAIN_QUEUE_ZSET = os.getenv('MAIN_QUEUE_ZSET', 'my_priority_queue')
JOB_PAYLOADS_HASH = os.getenv('JOB_PAYLOADS_HASH', 'job_payloads')
IN_PROGRESS_ZSET = os.getenv('IN_PROGRESS_ZSET', 'in_progress_jobs')
DLQ_LIST = os.getenv('DLQ_LIST', 'dead_letter_queue')

def reclamation_monitor_loop():
    """Main loop for the monitor to find and reclaim stalled jobs."""
    print(f"Reclamation Monitor service started. Check interval: {MONITOR_INTERVAL}s.")
    while True:
        redis_client = RedisClientSingleton.get_client()

        try:
            current_time = time.time()
            stalled_jobs_with_scores = redis_client.zrangebyscore(
                IN_PROGRESS_ZSET,
                min='-inf',
                max=current_time,
                withscores=True
            )

            if stalled_jobs_with_scores:
                 print(f"Monitor ({time.strftime('%H:%M:%S')}): Found {len(stalled_jobs_with_scores)} potential stalled job(s).")

            for job_id, expiry_score in stalled_jobs_with_scores:
                print(f"Monitor: Checking stalled job {job_id}. Lease expired at {time.strftime('%H:%M:%S', time.gmtime(expiry_score))}")

                if redis_client.zrem(IN_PROGRESS_ZSET, job_id) == 1:
                    print(f"Monitor: Claimed stalled job {job_id} for re-queuing.")
                    payload_json = redis_client.hget(JOB_PAYLOADS_HASH, job_id)
                    if payload_json:
                        payload = json.loads(payload_json)
                        original_priority = payload.get('original_priority')
                        if original_priority is not None:
                            redis_client.zadd(MAIN_QUEUE_ZSET, {job_id: original_priority})
                            print(f"Monitor: Re-queued job {job_id} with priority {original_priority}.")
                        else:
                            dlq_payload = {"job_id": job_id, "error": "Reclaimed, but original_priority missing."}
                            redis_client.lpush(DLQ_LIST, json.dumps(dlq_payload))
                    else:
                        dlq_payload = {"job_id": job_id, "error": "Reclaimed, but payload was missing."}
                        redis_client.lpush(DLQ_LIST, json.dumps(dlq_payload))
                else:
                    print(f"Monitor: Stalled job {job_id} was not found during claim (already handled).")
            
            time.sleep(MONITOR_INTERVAL)

        except redis.exceptions.RedisError as e:
            print(f"Monitor: Redis error: {e}. Retrying...")
            time.sleep(MONITOR_INTERVAL)
        except Exception as e:
            print(f"Monitor: Unhandled exception: {e}")
            time.sleep(MONITOR_INTERVAL)

if __name__ == "__main__":
    reclamation_monitor_loop()