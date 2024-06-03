package xo.dumper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;


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

    public void resetRuleState() {
        this.ruleState =  new RuleState(null, "HBase", 0);
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
        stopFlag.set(false);
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

