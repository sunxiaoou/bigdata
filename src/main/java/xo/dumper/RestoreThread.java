package xo.dumper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;


public class RestoreThread implements Runnable {
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
        Thread.sleep(500);
        if (Random.getRandomBoolean(5)) {
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
