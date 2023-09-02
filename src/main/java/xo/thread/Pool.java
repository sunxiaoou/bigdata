package xo.thread;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class Pool {
    private static final Logger LOG = LoggerFactory.getLogger(Pool.class);

    private final ThreadPoolExecutor executor;

    public Pool(String worker) {
        AtomicInteger nextId = new AtomicInteger(1);

        ThreadFactory myThreadFactory = task -> {
            String name = worker + "-" + nextId.getAndIncrement();
            Thread thread = new Thread(null, task, name);
            LOG.info(thread.getName() + " created");
            return thread;
        };

        executor = new ThreadPoolExecutor(
                2, // Core pool size (minimum number of threads to keep alive)
                5, // Maximum pool size (maximum number of threads to create)
                10, // Keep-alive time for excess threads
                TimeUnit.SECONDS, // Time unit for the keep-alive time
                new LinkedBlockingQueue<>(), // Work queue (where tasks are held before execution)
                myThreadFactory);
    }

    public void execute(Runnable runnable) {
        executor.execute(runnable);
    }

    public void shutdown() {
        executor.shutdown();
    }

    public static void main(String[] args) {
        Pool pool = new Pool("Worker");
        for (int i = 1; i <= 10; i ++) {
            final int taskId = i;
            pool.execute(() -> {
                LOG.info("Task " + taskId + " is running on thread " + Thread.currentThread().getName());
                try {
                    Thread.sleep(1000); // Simulate some work
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                LOG.info("Task " + taskId + " completed.");
            });
        }

        // Shutdown the thread pool when done with it
        pool.shutdown();
    }
}
