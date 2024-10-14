package xo.hbase;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class HBaseData implements Serializable, Cloneable {
    private final String replicationClusterId;
    private final String sourceBaseNamespaceDirPath;
    private final String sourceHFileArchiveDirPath;
    private final List<AdminProtos.WALEntry> entryProtos;
    private final List<Cell> cells;

    public HBaseData(String replicationClusterId,
                     String sourceBaseNamespaceDirPath,
                     String sourceHFileArchiveDirPath,
                     List<AdminProtos.WALEntry> entryProtos,
                     List<Cell> cells) {
        super();
        this.replicationClusterId = replicationClusterId;
        this.sourceBaseNamespaceDirPath = sourceBaseNamespaceDirPath;
        this.sourceHFileArchiveDirPath = sourceHFileArchiveDirPath;
        this.entryProtos = entryProtos;
        this.cells = cells;
    }

    public String getReplicationClusterId() {
        return replicationClusterId;
    }

    public String getSourceBaseNamespaceDirPath() {
        return sourceBaseNamespaceDirPath;
    }

    public String getSourceHFileArchiveDirPath() {
        return sourceHFileArchiveDirPath;
    }

    public List<AdminProtos.WALEntry> getEntryProtos() {
        return entryProtos;
    }

    public List<Cell> getCells() {
        return cells;
    }

    @Override
    protected Object clone() throws CloneNotSupportedException {
        List<AdminProtos.WALEntry> clonedEntryProtos = new ArrayList<>();
        for (AdminProtos.WALEntry entry : this.entryProtos) {
            clonedEntryProtos.add(entry.toBuilder().build());
        }
        List<Cell> clonedCells = new ArrayList<>();
        for (Cell cell : this.cells) {
            Cell copiedCell = CellUtil.cloneIfNecessary(cell);
            clonedCells.add(copiedCell);
        }
        return new HBaseData(
                this.replicationClusterId,
                this.sourceBaseNamespaceDirPath,
                this.sourceHFileArchiveDirPath,
                clonedEntryProtos,
                clonedCells
        );
    }
}