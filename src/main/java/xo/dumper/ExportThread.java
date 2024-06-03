package xo.dumper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;


public class ExportThread implements Runnable {
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
        Thread.sleep(10000);
        if (Random.getRandomBoolean(6)) {
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
