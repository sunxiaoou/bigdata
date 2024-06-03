package xo.dumper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;


public class ExportThreadManager {
    private static final Logger LOG = LoggerFactory.getLogger(ExportThreadManager.class);

    private final int threadCount;
    private final RuleState ruleState;
    private final BlockingQueue<String> exportQueue;
    private final BlockingQueue<String> restoreQueue;
    private final AtomicBoolean stopFlag;
    private final Consumer<Boolean> stopFn;
    private final CountDownLatch countDownLatch;
    private final ExecutorService pool;

    public ExportThreadManager(int threadCount,
                               RuleState ruleState,
                               BlockingQueue<String> exportQueue,
                               BlockingQueue<String> restoreQueue,
                               AtomicBoolean stopFlag,
                               Consumer<Boolean> stopFn) {
        this.threadCount = threadCount;
        this.ruleState = ruleState;
        this.exportQueue = exportQueue;
        this.restoreQueue = restoreQueue;
        this.stopFlag = stopFlag;
        this.countDownLatch = new CountDownLatch(threadCount);

        AtomicInteger nextId = new AtomicInteger(1);
        ThreadFactory threadFactory = task -> {
            String name = "exporter-" + nextId.getAndIncrement();
            Thread thread = new Thread(null, task, name);
            LOG.info(thread.getName() + " created");
            return thread;
        };
        this.pool = new ThreadPoolExecutor(
                threadCount,
                threadCount,
                0L,
                TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<>(),
                threadFactory);
        this.stopFn = stopFn;
    }

    public void start() {
        for (int i = 0; i < threadCount; i ++) {
            pool.execute(new ExportThread(ruleState, exportQueue, restoreQueue, stopFlag, stopFn, countDownLatch));
        }
    }

    public void stop() {
        exportQueue.clear();
        restoreQueue.clear();
        pool.shutdownNow();
    }

    public void waitForComplete() {
        try {
            countDownLatch.await();
            restoreQueue.put("");
        } catch (InterruptedException e) {
            LOG.error(e.getMessage(), e);
        }
        pool.shutdown();
    }
}