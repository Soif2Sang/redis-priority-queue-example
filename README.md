---
## Job Lifecycle Summary

The complete journey of a job illustrates their interaction well:

1.  The **producer** creates a job and adds it to `MAIN_QUEUE_ZSET` with a priority of 3 or 5.
2.  An available **worker** picks up the highest-priority job. The job is removed from `MAIN_QUEUE_ZSET`.
3.  The worker immediately adds the job to `IN_PROGRESS_ZSET` with a 5-minute **lease**.
4.  The worker processes the job (and sends **heartbeats** that update the lease in `IN_PROGRESS_ZSET`).
5.  **Success Case**: The worker finishes the job and removes it from `IN_PROGRESS_ZSET`. The job has completed its lifecycle.
6.  **Failure Case (Timeout)**: The worker crashes. The job remains in `IN_PROGRESS_ZSET`. Its lease eventually expires.
7.  The **monitor** detects the job with the expired lease.
8.  The monitor removes it from `IN_PROGRESS_ZSET` and puts it back into `MAIN_QUEUE_ZSET` for another worker to process.