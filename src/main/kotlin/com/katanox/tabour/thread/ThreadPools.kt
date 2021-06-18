package com.katanox.tabour.thread

import java.util.concurrent.*

object ThreadPools {

    fun blockingThreadPool(threads: Int, queueSize: Int, poolName: String): ThreadPoolExecutor {
        return ThreadPoolExecutor(
            threads,
            threads,
            0L,
            TimeUnit.SECONDS,
            ArrayBlockingQueue(queueSize),
            NamedThreadFactory(threadNamePrefix = poolName),
            retryPolicy()
        )
    }

    fun blockingScheduledThreadPool(
        threads: Int, poolName: String
    ): ScheduledThreadPoolExecutor {
        return ScheduledThreadPoolExecutor(
            threads, NamedThreadFactory(threadNamePrefix = poolName), retryPolicy()
        )
    }

    /**
     * Re-Queues a rejected [Runnable] into the thread pool's blocking queue, making the
     * submitting thread wait until the threadpool has capacity again.
     */
    private fun retryPolicy(): RejectedExecutionHandler {
        return RejectedExecutionHandler { r: Runnable, executor: ThreadPoolExecutor ->
            try {
                executor.queue.put(r)
            } catch (e: InterruptedException) {
                throw RuntimeException(e)
            }
        }
    }


}