import redis
import json
import time
import os
import random
import uuid
import threading
from redis_client_singleton import RedisClientSingleton

# --- Configuration ---
WORKER_ID = os.getenv('WORKER_ID', f"worker-{uuid.uuid4().hex[:6]}")
LEASE_DURATION = int(os.getenv('LEASE_DURATION', 300))
WORKER_POLL_TIMEOUT = int(os.getenv('WORKER_POLL_TIMEOUT', 5))
HEARTBEAT_INTERVAL = LEASE_DURATION / 3

# --- Redis Keys ---
MAIN_QUEUE_ZSET = os.getenv('MAIN_QUEUE_ZSET', 'my_priority_queue')
JOB_PAYLOADS_HASH = os.getenv('JOB_PAYLOADS_HASH', 'job_payloads')
IN_PROGRESS_ZSET = os.getenv('IN_PROGRESS_ZSET', 'in_progress_jobs')
DLQ_LIST = os.getenv('DLQ_LIST', 'dead_letter_queue')

def heartbeat_thread_func(redis_conn, job_id, stop_event):
    """Thread that sends heartbeats to maintain the lease."""
    while not stop_event.wait(HEARTBEAT_INTERVAL):
        try:
            new_expiry_time = time.time() + LEASE_DURATION
            result = redis_conn.zadd(IN_PROGRESS_ZSET, {job_id: new_expiry_time}, xx=True)
            if result == 1:
                print(f"Worker {WORKER_ID}: Heartbeat for job {job_id}. New lease until {time.strftime('%H:%M:%S', time.gmtime(new_expiry_time))}")
            else:
                print(f"Worker {WORKER_ID}: Job {job_id} no longer in progress. Stopping heartbeat.")
                break
        except redis.exceptions.RedisError as e:
            print(f"Worker {WORKER_ID}: Redis error during heartbeat for {job_id}: {e}. Stopping.")
            break

def process_video_encoding(job_id, job_data):
    """Handles video encoding and the heartbeat thread."""
    print(f"Worker {WORKER_ID}: Starting video encoding for job {job_id}. Source: {job_data.get('source_file')}")
    redis_client = RedisClientSingleton.get_client()
    stop_heartbeat_event = threading.Event()
    heartbeat = threading.Thread(target=heartbeat_thread_func, args=(redis_client, job_id, stop_heartbeat_event))
    
    try:
        heartbeat.start()
        # Mocking a encoding time
        encoding_time = random.uniform(10, LEASE_DURATION * 1.5)
        print(f"Worker {WORKER_ID}: Encoding {job_id} will take ~{encoding_time:.0f} seconds...")
        time.sleep(encoding_time)
        print(f"Worker {WORKER_ID}: Encoding for {job_id} finished.")
    finally:
        print(f"Worker {WORKER_ID}: Signaling heartbeat to stop for job {job_id}.")
        stop_heartbeat_event.set()
        heartbeat.join()

def worker_loop():
    """Main worker loop."""
    print(f"Worker {WORKER_ID} started. Lease duration: {LEASE_DURATION}s. Heartbeat interval: {HEARTBEAT_INTERVAL:.0f}s.")

    while True:
        current_job_id = None
        try:
            redis_client = RedisClientSingleton.get_client()
            
            popped_item = redis_client.bzpopmax(MAIN_QUEUE_ZSET, timeout=WORKER_POLL_TIMEOUT)
            if popped_item is None: continue

            _queue_name, current_job_id, job_priority_score_str = popped_item
            lease_expiry_time = time.time() + LEASE_DURATION
            
            if not redis_client.zadd(IN_PROGRESS_ZSET, {current_job_id: lease_expiry_time}):
                print(f"Worker {WORKER_ID}: WARNING - Could not acquire lease for {current_job_id}. Skipping.")
                continue
            print(f"Worker {WORKER_ID}: Acquired lease for job {current_job_id} (Priority: {job_priority_score_str})")

            payload_json = redis_client.hget(JOB_PAYLOADS_HASH, current_job_id)
            if payload_json is None:
                print(f"Worker {WORKER_ID}: ERROR - Payload for {current_job_id} not found. Releasing lease.")
                redis_client.zrem(IN_PROGRESS_ZSET, current_job_id)
                continue

            payload = json.loads(payload_json)
            
            try:
                process_video_encoding(current_job_id, payload.get('data', {}))
                
                if redis_client.zrem(IN_PROGRESS_ZSET, current_job_id) == 1:
                    redis_client.hdel(JOB_PAYLOADS_HASH, current_job_id)
                    print(f"Worker {WORKER_ID}: Successfully processed and cleaned up job {current_job_id}.")
                else:
                    print(f"Worker {WORKER_ID}: Job {current_job_id} was not in progress set upon completion (reclaimed by monitor?).")

            except Exception as e_process:
                print(f"Worker {WORKER_ID}: ERROR processing job {current_job_id}: {e_process}")
                redis_client.zrem(IN_PROGRESS_ZSET, current_job_id)
                dlq_payload = {"job_id": current_job_id, "payload": payload, "error": str(e_process), "worker_id": WORKER_ID}
                redis_client.lpush(DLQ_LIST, json.dumps(dlq_payload))
                print(f"Worker {WORKER_ID}: Moved job {current_job_id} to DLQ.")

        except redis.exceptions.RedisError as e_redis:
            print(f"Worker {WORKER_ID}: Redis error in main loop: {e_redis}. Reconnecting...")
            redis_client = None # Force reconnection
            time.sleep(5)
        except Exception as e_outer:
            print(f"Worker {WORKER_ID}: Unhandled exception for job '{current_job_id}': {e_outer}")
            time.sleep(5)

if __name__ == "__main__":
    worker_loop()