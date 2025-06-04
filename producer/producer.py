import redis
import uuid
import json
import time
import os
import random
from redis_client_singleton import RedisClientSingleton

# Redis key names
MAIN_QUEUE_ZSET = os.getenv('MAIN_QUEUE_ZSET', 'my_priority_queue')
JOB_PAYLOADS_HASH = os.getenv('JOB_PAYLOADS_HASH', 'job_payloads')

print(MAIN_QUEUE_ZSET, "ici")
print(JOB_PAYLOADS_HASH, "ici")

def enqueue_job(job_data, priority_weight):
    """Enqueues a job into the priority queue."""
    redis_client = RedisClientSingleton.get_client()

    job_id = f"{time.time():.6f}-{uuid.uuid4()}"
    
    payload = {
        'job_id': job_id,
        'data': job_data,
        'original_priority': priority_weight,
        'enqueued_at_timestamp': time.time(),
        'enqueued_at_readable': time.strftime('%Y-%m-%d %H:%M:%S UTC', time.gmtime())
    }
    serialized_payload = json.dumps(payload)
    
    try:
        pipe = redis_client.pipeline()
        pipe.zadd(MAIN_QUEUE_ZSET, {job_id: priority_weight})
        pipe.hset(JOB_PAYLOADS_HASH, job_id, serialized_payload)
        results = pipe.execute()
        
        if results[0] is not None and results[1] is not None:
            print(f"Producer: Enqueued job {job_id} with priority {priority_weight}. Video Source: {job_data.get('source_file')}")
            return job_id
        else:
            print(f"Producer: Failed to enqueue job {job_id}. Redis command results: {results}")
            return None
            
    except redis.exceptions.RedisError as e:
        print(f"Producer: Redis error while enqueueing job {job_id}: {e}")
        return None
    except Exception as e:
        print(f"Producer: An unexpected error occurred while enqueueing job {job_id}: {e}")
        return None


if __name__ == "__main__":
    print("Producer service started.")
    
    video_files = ["movie_A.mp4", "short_clip_B.mov", "long_documentary_C.mkv", "interview_D.avi"]
    job_counter = 0
    while True:
        redis_client = RedisClientSingleton.get_client()
        priority = random.choice([3, 5])
        source_file = random.choice(video_files)
        job_counter += 1
        
        task_data = {
            "task_type": "video_encoding", 
            "source_file": f"{job_counter}-{source_file}", 
            "encoding_profile": "1080p_high_quality" if priority == 5 else "720p_standard"
        }

        enqueue_job(task_data, priority)
        time.sleep(random.uniform(2, 5))