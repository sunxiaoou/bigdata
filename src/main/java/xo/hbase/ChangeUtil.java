package xo.hbase;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Put;
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

    public static Put toHBasePut(OneRowChange orc) {
        if (orc.getAction() != OneRowChange.ActionType.INSERTONDUP) {
            throw new IllegalArgumentException("Only INSERTONDUP supported");
        }
        // 假设第一个columnSpec为ROW，columnValues的第一个为rowkey
        ArrayList<OneRowChange.ColumnSpec> columnSpecs = orc.getColumnSpec();
        ArrayList<ArrayList<OneRowChange.ColumnVal>> columnValues = orc.getColumnValues();
        if (columnSpecs == null || columnSpecs.isEmpty() || columnValues == null || columnValues.isEmpty()) {
            throw new IllegalArgumentException("No columns or values");
        }
        ArrayList<OneRowChange.ColumnVal> rowVals = columnValues.get(0);
        byte[] rowKey = null;
        for (int i = 0; i < columnSpecs.size(); i++) {
            if ("ROW".equalsIgnoreCase(columnSpecs.get(i).getName())) {
                Object v = rowVals.get(i).getValue();
                if (v instanceof byte[]) {
                    rowKey = (byte[]) v;
                } else if (v instanceof String) {
                    rowKey = ((String) v).getBytes();
                } else if (v instanceof Number) {
                    rowKey = v.toString().getBytes();
                }
                break;
            }
        }
        if (rowKey == null)
            throw new IllegalArgumentException("No rowkey found");
        Put put = new Put(rowKey);
        for (int i = 0; i < columnSpecs.size(); i++) {
            String colName = columnSpecs.get(i).getName();
            if ("ROW".equalsIgnoreCase(colName)) continue;
            String[] parts = colName.split("\\.", 2);
            if (parts.length != 2) continue;
            String family = parts[0];
            String qualifier = parts[1];
            Object v = rowVals.get(i).getValue();
            byte[] value;
            if (v instanceof byte[]) {
                value = (byte[]) v;
            } else if (v instanceof String) {
                value = ((String) v).getBytes();
            } else if (v != null) {
                value = v.toString().getBytes();
            } else {
                value = null;
            }
            if (value != null) {
                put.addColumn(family.getBytes(), qualifier.getBytes(), value);
            }
        }
        return put;
    }
}
