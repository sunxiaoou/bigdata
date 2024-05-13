package xo.netty.backup;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;


class SnapshotThread implements Runnable {
    private static final Logger LOG = LoggerFactory.getLogger(SnapshotThread.class);

    private final int threadCount;
    private final BlockingQueue<String> snapshotQueue;
    private final BlockingQueue<String> exportQueue;
    private final AtomicBoolean stopFlag;
    private final Consumer<Boolean> stopFn;

    public SnapshotThread(int threadCount,
                          BlockingQueue<String> snapshotQueue,
                          BlockingQueue<String> exportQueue,
                          AtomicBoolean stopFlag,
                          Consumer<Boolean> stopFn) {
        this.threadCount = threadCount;
        this.snapshotQueue = snapshotQueue;
        this.exportQueue = exportQueue;
        this.stopFlag = stopFlag;
        this.stopFn = stopFn;
    }

    private String snapshot(String table) throws InterruptedException {
        SimpleDateFormat sdf = new SimpleDateFormat("yyMMdd");
        String dateStr = sdf.format(new Date());
        String snapshot = table + "_" + dateStr;
        Thread.sleep(100);
        LOG.info("Generated snapshot: " + snapshot);
        return snapshot;
    }

    private boolean checkStopFlag() {
        return stopFlag.get() || Thread.currentThread().isInterrupted();
    }

    @Override
    public void run() {
        try {
            while (!checkStopFlag()) {
                String table = snapshotQueue.poll();
                if (table == null) {
                    for (int i = 0; i < threadCount; i ++) {
                        exportQueue.put("");
                    }
                    break;
                }
                String snapshot = snapshot(table);
                exportQueue.put(snapshot);
            }
        } catch (Exception e) {
            if (!stopFlag.get()) {
                LOG.error("unexpected exception in snapshot thread");
                stopFn.accept(false);
            }
        } finally {
            LOG.info("snapshot thread exited");
        }
    }
}

class ExportThread implements Runnable {
    private static final Logger LOG = LoggerFactory.getLogger(ExportThread.class);

    private final BlockingQueue<String> exportQueue;
    private final BlockingQueue<String> restoreQueue;
    private final AtomicBoolean stopFlag;
    private final Consumer<Boolean> stopFn;
    private final CountDownLatch countDownLatch;
//    private final JobStatusCounter statusCounter;
//    private final RuleState ruleState;
//    private final RuleConfig ruleConfig;

    public ExportThread(BlockingQueue<String> exportQueue,
                        BlockingQueue<String> restoreQueue,
                        AtomicBoolean stopFlag,
                        Consumer<Boolean> stopFn,
                        CountDownLatch countDownLatch) {
        this.exportQueue = exportQueue;
        this.restoreQueue = restoreQueue;
        this.stopFlag = stopFlag;
        this.stopFn = stopFn;
        this.countDownLatch = countDownLatch;
    }

    private void export(String snapshot) throws InterruptedException {
        Thread.sleep(2000);
        LOG.info("Export snapshot: " + snapshot);
    }

    private boolean checkStopFlag() {
        return stopFlag.get() || Thread.currentThread().isInterrupted();
    }

    @Override
    public void run() {
        String mine = Thread.currentThread().getName();
        try {
            while (!checkStopFlag()) {
                String snapshot = exportQueue.take();
                if ("".equals(snapshot)) {
                    break;
                }
                export(snapshot);
                restoreQueue.put(snapshot);
            }
        } catch (Exception e) {
            if (!stopFlag.get()) {
                LOG.error("unexpected exception in export thread {}", mine);
                stopFn.accept(false);
            }
        } finally {
            countDownLatch.countDown();
            LOG.info("export thread {} exited", mine);
        }
    }
}

class ExportThreadManager {
    private static final Logger LOG = LoggerFactory.getLogger(ExportThreadManager.class);

    private final int threadCount;
    private final BlockingQueue<String> exportQueue;
    private final BlockingQueue<String> restoreQueue;
//    private final RuleConfig ruleConfig;
//    private final RuleState ruleState;
//    private final JobStatusCounter jobStatusCounter;
    private final AtomicBoolean stopFlag;
    private final Consumer<Boolean> stopFn;
    private final CountDownLatch countDownLatch;
    private final ExecutorService pool;

    public ExportThreadManager(int threadCount,
                               BlockingQueue<String> exportQueue,
                               BlockingQueue<String> restoreQueue,
                               AtomicBoolean stopFlag,
                               Consumer<Boolean> stopFn) {
        this.threadCount = threadCount;
        this.exportQueue = exportQueue;
        this.restoreQueue = restoreQueue;
//        this.ruleState = ruleState;
//        this.jobStatusCounter = jobStatusCounter;
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
            pool.execute(new ExportThread(exportQueue, restoreQueue, stopFlag, stopFn, countDownLatch));
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

class RestoreThread implements Runnable {
    private static final Logger LOG = LoggerFactory.getLogger(RestoreThread.class);

    private final BlockingQueue<String> restoreQueue;
    private final AtomicBoolean stopFlag;
    private final Consumer<Boolean> stopFn;
//    private final JobStatusCounter statusCounter;
//    private final RuleState ruleState;
//    private final RuleConfig ruleConfig;

    public RestoreThread(BlockingQueue<String> restoreQueue,
                         AtomicBoolean stopFlag,
                         Consumer<Boolean> stopFn) {
        this.restoreQueue = restoreQueue;
        this.stopFlag = stopFlag;
        this.stopFn = stopFn;
    }

    private void restore(String snapshot) throws InterruptedException {
        Thread.sleep(100);
        LOG.info("restore snapshot: " + snapshot);
    }

    private boolean checkStopFlag() {
        return stopFlag.get() || Thread.currentThread().isInterrupted();
    }

    @Override
    public void run() {
        try {
            while (!checkStopFlag()) {
                String snapshot = restoreQueue.take();
                if ("".equals(snapshot)) {
                    break;
                }
                restore(snapshot);
            }
        } catch (Exception e) {
            if (!stopFlag.get()) {
                LOG.error("unexpected exception in restore thread");
                stopFn.accept(false);
            }
        } finally {
            LOG.info("restore thread exited");
        }
    }
}

public class Backup {
    private static final Logger LOG = LoggerFactory.getLogger(Backup.class);

    private final int threadCount = 3;
    private final BlockingQueue<String> snapshotQueue = new LinkedBlockingQueue<>();
    private final BlockingQueue<String> exportQueue = new LinkedBlockingQueue<>();
    private final BlockingQueue<String> restoreQueue = new LinkedBlockingQueue<>();
    private AtomicBoolean stopFlag = new AtomicBoolean(false);
    private volatile ExportThreadManager exportMgr;


    public Backup() {
        LOG.info("Backup constructor");
    }

    public void stop(Boolean userCancelled) {
        stopFlag.set(true);
        if (exportMgr != null) {
            exportMgr.stop();
        }
        exportMgr = null;
    }

    public void start() {
        List<String> tables = Arrays.asList("table1", "table2", "table3", "table4", "table5", "table6", "table7");
        for (String table: tables) {
            try {
                snapshotQueue.put(table);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        this.exportMgr = new ExportThreadManager(threadCount, exportQueue, restoreQueue, stopFlag, this::stop);
        exportMgr.start();
        SnapshotThread snapshot = new SnapshotThread(threadCount, snapshotQueue, exportQueue, stopFlag, this::stop);
        new Thread(snapshot, "generator").start();
        RestoreThread restoreThread = new RestoreThread(restoreQueue, stopFlag, this::stop);
        new Thread(restoreThread, "restorer").start();
        exportMgr.waitForComplete();
    }

    public static void main(String[] args) {
        Backup backup = new Backup();
        backup.start();
        backup.stop(true);
    }
}

