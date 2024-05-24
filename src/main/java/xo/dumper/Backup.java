package xo.dumper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
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
    private final RuleState ruleState;
    private final BlockingQueue<String> snapshotQueue;
    private final BlockingQueue<String> exportQueue;
    private final AtomicBoolean stopFlag;
    private final Consumer<Boolean> stopFn;

    public SnapshotThread(int threadCount,
                          RuleState ruleState,
                          BlockingQueue<String> snapshotQueue,
                          BlockingQueue<String> exportQueue,
                          AtomicBoolean stopFlag,
                          Consumer<Boolean> stopFn) {
        this.threadCount = threadCount;
        this.ruleState = ruleState;
        this.snapshotQueue = snapshotQueue;
        this.exportQueue = exportQueue;
        this.stopFlag = stopFlag;
        this.stopFn = stopFn;
    }

    private String snapshot(String table) throws InterruptedException, RuleException {
        SimpleDateFormat sdf = new SimpleDateFormat("yyMMdd");
        String dateStr = sdf.format(new Date());
        String snapshot = table + "_" + dateStr;
        ruleState.addRunningTable(table);
        Thread.sleep(100);
        if ("table2".equals(table)) {
            throw new RuleException(String.format("snapshot %s failed", table), 1000);
        }
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
                try {
                    String snapshot = snapshot(table);
                    exportQueue.put(snapshot);
                } catch (RuleException e) {
                    ruleState.addDoneTable(table, false);
                    LOG.error(e.getMessage());
                }
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

    private final RuleState ruleState;
    private final BlockingQueue<String> exportQueue;
    private final BlockingQueue<String> restoreQueue;
    private final AtomicBoolean stopFlag;
    private final Consumer<Boolean> stopFn;
    private final CountDownLatch countDownLatch;

    public ExportThread(RuleState ruleState,
                        BlockingQueue<String> exportQueue,
                        BlockingQueue<String> restoreQueue,
                        AtomicBoolean stopFlag,
                        Consumer<Boolean> stopFn,
                        CountDownLatch countDownLatch) {
        this.ruleState = ruleState;
        this.exportQueue = exportQueue;
        this.restoreQueue = restoreQueue;
        this.stopFlag = stopFlag;
        this.stopFn = stopFn;
        this.countDownLatch = countDownLatch;
    }

    private void export(String table, String snapshot) throws RuleException, InterruptedException {
        Thread.sleep(2000);
        if ("table3".equals(table)) {
            throw new RuleException(String.format("export %s failed", table), 1000);
        }
        LOG.info("Exported snapshot: " + snapshot);
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
                String table = snapshot.substring(0, snapshot.length() - 7);
                try {
                    export(table, snapshot);
                    restoreQueue.put(snapshot);
                } catch (RuleException e) {
                    ruleState.addDoneTable(table, false);
                    LOG.error(e.getMessage());
                }
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

class RestoreThread implements Runnable {
    private static final Logger LOG = LoggerFactory.getLogger(RestoreThread.class);

    private final RuleState ruleState;
    private final BlockingQueue<String> restoreQueue;
    private final AtomicBoolean stopFlag;
    private final Consumer<Boolean> stopFn;

    public RestoreThread(RuleState ruleState,
                         BlockingQueue<String> restoreQueue,
                         AtomicBoolean stopFlag,
                         Consumer<Boolean> stopFn) {
        this.ruleState = ruleState;
        this.restoreQueue = restoreQueue;
        this.stopFlag = stopFlag;
        this.stopFn = stopFn;
    }

    private void restore(String table, String snapshot) throws RuleException, InterruptedException {
        Thread.sleep(100);
        if ("table5".equals(table)) {
            throw new RuleException(String.format("restore %s failed", table), 1000);
        }
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
                String table = snapshot.substring(0, snapshot.length() - 7);
                try {
                    restore(table, snapshot);
                    ruleState.addDoneTable(table, true);
                } catch (RuleException e) {
                    ruleState.addDoneTable(table, false);
                    LOG.error(e.getMessage());
                }
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
    private volatile RuleState ruleState = new RuleState(null, "HBase", 0);
    private final BlockingQueue<String> snapshotQueue = new LinkedBlockingQueue<>();
    private final BlockingQueue<String> exportQueue = new LinkedBlockingQueue<>();
    private final BlockingQueue<String> restoreQueue = new LinkedBlockingQueue<>();
    private AtomicBoolean stopFlag = new AtomicBoolean(false);
    private Thread generator = null;
    private Thread restorer = null;
    private volatile ExportThreadManager exportMgr = null;


    public Backup() {
        LOG.info("Backup constructor");
    }

    public void stop(Boolean userCancelled) {
        stopFlag.set(true);
        if (exportMgr != null) {
            exportMgr.stop();
        }
        exportMgr = null;
        generator = null;
        restorer = null;
    }

    public void start() throws InterruptedException {
        ruleState.setStart(LocalDateTime.now());
        List<String> tables = Arrays.asList("table1", "table2", "table3", "table4", "table5", "table6", "table7");
        for (String table: tables) {
            try {
                snapshotQueue.put(table);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        try {
            SnapshotThread snapshot = new SnapshotThread(threadCount, ruleState, snapshotQueue, exportQueue, stopFlag,
                    this::stop);
            this.generator = new Thread(snapshot, "generator");
            generator.start();
            this.exportMgr = new ExportThreadManager(threadCount, ruleState, exportQueue, restoreQueue, stopFlag,
                    this::stop);
            exportMgr.start();
            RestoreThread restoreThread = new RestoreThread(ruleState, restoreQueue, stopFlag, this::stop);
            this.restorer = new Thread(restoreThread, "restorer");
            restorer.start();
        } finally {
            if (generator != null) {
                generator.join();
            }
            if (exportMgr != null) {
                exportMgr.waitForComplete();
            }
            if (restorer != null) {
                restorer.join();
            }
            ruleState.setEnd(LocalDateTime.now());
            ruleState.setStage1(ruleState.getErrTables().isEmpty() ? 0: 1);
            LOG.info(ruleState.toString());
        }
    }

    public static void main(String[] args) throws InterruptedException {
        Backup backup = new Backup();
        backup.start();
        backup.stop(true);
    }
}

