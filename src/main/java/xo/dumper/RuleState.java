package xo.dumper;

import java.io.*;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public final class RuleState implements Serializable {
    private static final long serialVersionUID = 1L;

    public static final int PENDING = 0;        // wait for running
    public static final int RUNNING = 1;
    public static final int COMPLETED = 2;      // completed successfully
    public static final int CANCELLED = 3;      // cancelled because receive user stop command
    public static final int ERROR = 4;          // completed with serious errors

    private String uuid;
    private String dbType;
    private int state;
    private int stage;
    private int totalTableNumber;
    private int doneTableNumber;
    private Set<String> runTables = new TreeSet<>();
    private Set<String> doneTables = new TreeSet<>();
    private Set<String> errTables = new TreeSet<>();

    private long lastModifyTime;
    private LocalDateTime start;
    private LocalDateTime end;
    private String ruleStatisticsStr;

    /**
     * 每个编码的schema+objname开始时间
     */
    private Map<String, LocalDateTime> startEachTable = new ConcurrentHashMap<>();
    /**
     * 每个编码的schema+objname结束时间
     */
    private Map<String, LocalDateTime> endEachTable = new ConcurrentHashMap<>();

    /**
     * 异常消息
     */
    private Map<String,String> errTableInfos = new ConcurrentHashMap<>();

//    private volatile JSONObject jobStatusCounter;

    public RuleState() {

    }

    public RuleState(String uuid, String dbType, int stage) {
        this.uuid = uuid;
        this.dbType = dbType;
        this.state = 0;
        this.stage = stage;
    }

    public void init(){
    }

    public void setStage1(int stage) {
        synchronized (this) {
            this.state = (stage == ERROR) ? -1 : 0;
            this.stage = stage;
//            writeToFile(false);
        }
    }

    /**
     * 开始执行
     * @param objIdentifiers
     */
    public void setSyncRange(List<ObjIdentifier> objIdentifiers) {
        synchronized (this) {
            stage = RUNNING;
            state = 0;

            runTables.clear();
            doneTableNumber = doneTables.size() + errTables.size();
            totalTableNumber = doneTableNumber;

            lastModifyTime = System.currentTimeMillis();

            for (ObjIdentifier objIdentifier : objIdentifiers) {
                String dumpSchemaName = generateCheckSchemaName(objIdentifier);
                if (doneTables.remove(dumpSchemaName) || errTables.remove(dumpSchemaName)) {
                    doneTableNumber --;
                } else {
                    totalTableNumber ++;
                }
            }
//            writeToFile(false);
        }
    }

    public void addRunningTable(String table) {
        synchronized (this) {
            runTables.add(table);
            startEachTable.put(table, LocalDateTime.now());
        }
    }

    public void addDoneTables(List<String> tables, boolean success) {
        synchronized (this) {
            for(String table:  tables) {
                endEachTable.put(table, LocalDateTime.now());
                runTables.remove(table);
                if (success) {
                    doneTables.add(table);
                } else {
                    errTables.add(table);
                }
                doneTableNumber ++;
            }
//            writeToFile(true);
        }
    }

    public void addDoneTable(String table, boolean success) {
        addDoneTables(Arrays.asList(table), success);
    }

    public synchronized List<String> getRunTables() {
        return new ArrayList<>(this.runTables);
    }

    public synchronized List<String> getDoneTables() {
        return new ArrayList<>(this.doneTables);
    }

    public synchronized List<String> getErrTables() {
        return new ArrayList<>(this.errTables);
    }

    public boolean isErrTable(String schemaTable) {
        return errTables.contains(schemaTable);
    }

    public int getState() {
        return state;
    }

    public int getStage() {
        return stage;
    }

    public void setStage(int stage) {
        this.stage = stage;
    }

    public LocalDateTime getStart() {
        return start;
    }

    public void setStart(LocalDateTime start) {
        if (this.start == null) {
            this.start = start;
        }
    }

    public LocalDateTime getEnd() {
        return end;
    }

    public void setEnd(LocalDateTime end) {
        this.end = end;
    }

    private void updateLastModifyTime() {
        this.lastModifyTime = System.currentTimeMillis();
    }

    private long getLastModifyTime() {
        return lastModifyTime;
    }

    public Map<String, String> getErrTableInfos() {
        return errTableInfos;
    }

    public void setErrTableInfos(Map<String, String> errTableInfos) {
        this.errTableInfos = errTableInfos;
    }

    public void putErrTableInfo(ObjIdentifier objIdentifier, Exception e) {
        String checkSchemaName = generateCheckSchemaName(objIdentifier);
        errTableInfos.put(checkSchemaName, Objects.nonNull(e.getMessage()) ? e.getMessage() : "null pointer");
    }

    public String getStageDescription() {
        switch (stage) {
            case PENDING:
                return "wait";
            case RUNNING:
                return "doing";
            case COMPLETED:
                return "done";
            case CANCELLED:
                return "stop";
            case ERROR:
            default:
                return "abnormal";
        }
    }

    public int getTotalTableNumber() {
        return totalTableNumber;
    }

    public int getDoneTableNumber() {
        return doneTableNumber;
    }

    public int getProgress() {
        if (state == COMPLETED) {
            return 100;
        }

        if (totalTableNumber == 0) {
            return state == COMPLETED ? 100 : 0;
        }
        return Double.valueOf((doneTableNumber * 100.0) / totalTableNumber).intValue();
    }

    public String getRuleStatisticsStr() {
        return ruleStatisticsStr;
    }

    public void setRuleStatisticsStr(String ruleStatisticsStr) {
        this.ruleStatisticsStr = ruleStatisticsStr;
    }

    @Override
    public String toString() {
        return generateSimpleDescriptionString().toString();
    }

    private StringBuilder generateSimpleDescriptionString() {
        Duration duration = (Objects.isNull(stage)|| Objects.isNull(end)) ? null : Duration.between(start, end);
        return new StringBuilder("RuleState{" +
                "uuid='" + uuid + '\'' +
                ", state=" + state +
                ", stage=" + stage +
                ", totalTableNumber=" + totalTableNumber +
                ", doneTableNumber=" + doneTableNumber +
                ", runTables=" + runTables +
                ", errTables=" + errTables +
                ", lastModifyTime=" + lastModifyTime +
                ", start=" + start +
                ", end=" + end +
                ", duration " + (Objects.isNull(duration) ? "" : duration.toMillis()) + " millis" +
                '}');

    }

    public void setUuid(String uuid) {
        this.uuid = uuid;
    }

    public void setState(int state) {
        this.state = state;
    }

    public void setTotalTableNumber(int totalTableNumber) {
        this.totalTableNumber = totalTableNumber;
    }

    public void setDoneTableNumber(int doneTableNumber) {
        this.doneTableNumber = doneTableNumber;
    }

    public void setRunTables(Set<String> runTables) {
        this.runTables = runTables;
    }

    public void setDoneTables(Set<String> doneTables) {
        this.doneTables = doneTables;
    }

    public void setErrTables(Set<String> errTables) {
        this.errTables = errTables;
    }

    public void setLastModifyTime(long lastModifyTime) {
        this.lastModifyTime = lastModifyTime;
    }

    public String getUuid() {
        return uuid;
    }

    public String getDbType() {
        return dbType;
    }

    public void setDbType(String dbType) {
        this.dbType = dbType;
    }

    public String generateCheckSchemaName(ObjIdentifier objIdentifier) {
        return objIdentifier.getSchema() + ":" + objIdentifier.getName();
    }
}
