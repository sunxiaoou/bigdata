package xo.hbase;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.wal.WAL;
import org.apache.hadoop.hbase.wal.WALEdit;
import org.apache.hadoop.hbase.wal.WALKeyImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;

public class ChangeUtil {
    private static final Logger LOG = LoggerFactory.getLogger(ChangeUtil.class);

    private static OneRowChange.ActionType actionMapping(Cell.Type type) {
        if (type == Cell.Type.DeleteFamily) {
            return OneRowChange.ActionType.DELETE;
        } else if (type == Cell.Type.Put) {
            return OneRowChange.ActionType.INSERTONDUP;
        } else {
            LOG.info("Type {} is not supported", type);
            return null;
        }
    }

    private static byte[] extractBytes(byte[] arr, int offset, int length) {
        return Arrays.copyOfRange(arr, offset, offset + length);
    }

    public static OneRowChange entry2OneRowChange(WAL.Entry entry) {
        WALKeyImpl key = entry.getKey();
        TableName tableName = key.getTableName();
        long id = key.getSequenceId();
        WALEdit edit = entry.getEdit();
        ArrayList<Cell> cells = edit.getCells();
        Cell cell = cells.get(0);

        OneRowChange change = new OneRowChange();
        change.setSchemaName(tableName.getNamespaceAsString());
        change.setTableName(tableName.getQualifierAsString());
        change.setTableId(-1);
        change.setAction(actionMapping(cell.getType()));

        ArrayList<OneRowChange.ColumnSpec> specs = change.getColumnSpec();
        OneRowChange.ColumnSpec spec = change.new ColumnSpec();
        specs.add(spec);
        spec.setName("ROW");
        spec.setIndex(1);

        ArrayList<OneRowChange.ColumnVal> values = new ArrayList<>();
        change.getColumnValues().add(values);
        OneRowChange.ColumnVal value = change.new ColumnVal();
        values.add(value);
        String row = new String(extractBytes(cell.getRowArray(), cell.getRowOffset(), cell.getRowLength()));
        value.setValue(row);

        for (int i = 0; i < edit.size(); i ++) {
            cell = cells.get(i);
            String family = new String(extractBytes(cell.getFamilyArray(), cell.getFamilyOffset(),
                    cell.getFamilyLength()));
            String qualifier = new String(extractBytes(cell.getQualifierArray(), cell.getQualifierOffset(),
                    cell.getQualifierLength()));
            Date timestamp = new Date(cell.getTimestamp());
            byte[] bytes = extractBytes(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());
            spec = change.new ColumnSpec();
            spec.setName(family + "." + qualifier);
            spec.setIndex(i + 2);
            spec.setType(java.sql.Types.VARBINARY);
            specs.add(spec);

            value = change.new ColumnVal();
            values.add(value);
            value.setValue(bytes);
        }

        return change;
    }
}
