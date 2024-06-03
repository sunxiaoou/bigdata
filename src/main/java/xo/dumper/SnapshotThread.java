package xo.dumper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;


public class SnapshotThread implements Runnable {
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
        Thread.sleep(500);
        if (Random.getRandomBoolean(7)) {
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