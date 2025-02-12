package xo.sap.jco;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import xo.hbase.OneRowChange;

import java.io.Serializable;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.sql.Date;
import java.sql.Time;
import java.sql.Types;
import java.util.*;

public class ChangeUtil {
    private static final Logger LOG = LoggerFactory.getLogger(ChangeUtil.class);

    private static OneRowChange.ActionType actionMapping(char type) {
        if ('C' == type) {
            return OneRowChange.ActionType.INSERT;
        } else if ('U' == type) {
            return OneRowChange.ActionType.UPDATE;
        } else if ('D' == type) {
            return OneRowChange.ActionType.DELETE;
        } else {
            LOG.error("Type {} is not supported", type);
            return null;
        }
    }

    private static final Map<String, Integer> abapToSqlTypeMap = new HashMap<>();
    static {
        // Mapping ABAP data types to Java SQL types
        abapToSqlTypeMap.put("ACCP", Types.VARCHAR);
        abapToSqlTypeMap.put("CHAR", Types.VARCHAR);
        abapToSqlTypeMap.put("CLNT", Types.VARCHAR);
        abapToSqlTypeMap.put("CUKY", Types.VARCHAR);
        abapToSqlTypeMap.put("CURR", Types.DECIMAL);
        abapToSqlTypeMap.put("DF16_DEC", Types.DECIMAL);
        abapToSqlTypeMap.put("DF16_RAW", Types.DOUBLE);
        abapToSqlTypeMap.put("DF16_SCL", Types.DECIMAL);
        abapToSqlTypeMap.put("DF34_DEC", Types.DECIMAL);
        abapToSqlTypeMap.put("DF34_RAW", Types.DOUBLE);
        abapToSqlTypeMap.put("DF34_SCL", Types.DECIMAL);
        abapToSqlTypeMap.put("DATS", Types.DATE);
        abapToSqlTypeMap.put("DEC", Types.DECIMAL);
        abapToSqlTypeMap.put("FLTP", Types.DOUBLE);
        abapToSqlTypeMap.put("INT1", Types.TINYINT);
        abapToSqlTypeMap.put("INT2", Types.SMALLINT);
        abapToSqlTypeMap.put("INT4", Types.INTEGER);
        abapToSqlTypeMap.put("INT8", Types.BIGINT);
        abapToSqlTypeMap.put("LANG", Types.VARCHAR);
        abapToSqlTypeMap.put("LCHR", Types.CLOB);
        abapToSqlTypeMap.put("LRAW", Types.BLOB);
        abapToSqlTypeMap.put("NUMC", Types.VARCHAR);
        abapToSqlTypeMap.put("PREC", Types.DECIMAL);
        abapToSqlTypeMap.put("QUAN", Types.DECIMAL);
        abapToSqlTypeMap.put("RAW", Types.BINARY);
        abapToSqlTypeMap.put("RAWSTRING", Types.BLOB);
        abapToSqlTypeMap.put("SSTRING", Types.VARCHAR);
        abapToSqlTypeMap.put("STRING", Types.CLOB);
        abapToSqlTypeMap.put("TIMS", Types.TIME);
        abapToSqlTypeMap.put("UNIT", Types.VARCHAR);
        abapToSqlTypeMap.put("VARC", Types.VARCHAR);
    }

    private static int abapTypeMapping(String abapType) {
        return abapToSqlTypeMap.getOrDefault(abapType.toUpperCase(), Types.VARCHAR);
    }

    private static byte[] extractBytes(byte[] arr, int offset, int length) {
        return Arrays.copyOfRange(arr, offset, offset + length);
    }

    static int getUtf8BytesLength(byte[] data, int offset, int charCount) {
        int byteLength = 0; // 记录实际字节长度
        int charsProcessed = 0; // 已处理的 char 数量

        while (charsProcessed < charCount) {
            int currentByte = data[offset + byteLength] & 0xFF; // 当前字节的无符号值
            int charByteCount;

            // 判断当前字符的 UTF-8 字节数
            if ((currentByte & 0x80) == 0x00) { // 单字节字符
                charByteCount = 1;
            } else if ((currentByte & 0xE0) == 0xC0) { // 2 字节字符
                charByteCount = 2;
            } else if ((currentByte & 0xF0) == 0xE0) { // 3 字节字符
                charByteCount = 3;
            } else if ((currentByte & 0xF8) == 0xF0) { // 4 字节字符
                charByteCount = 4;
            } else {
                throw new IllegalArgumentException("Invalid UTF-8 encoding at byte offset " + (offset + byteLength));
            }

            // 验证续字节的合法性
            for (int i = 1; i < charByteCount; i++) {
                if ((data[offset + byteLength + i] & 0xC0) != 0x80) {
                    throw new IllegalArgumentException("Invalid UTF-8 continuation byte at offset " + (offset + byteLength + i));
                }
            }

            // 增加 char 计数
            if (charByteCount == 4) {
                charsProcessed += 2; // 补充字符对应两个 char
            } else {
                charsProcessed += 1; // 其他字符对应一个 char
            }

            // 增加字节偏移量
            byteLength += charByteCount;
        }

        return byteLength;
    }

    private static Date parseDate(String dateStr) {
        if (dateStr.length() == 8) {
            String formatted = dateStr.substring(0, 4) + "-" + dateStr.substring(4, 6) + "-" + dateStr.substring(6, 8);
            return Date.valueOf(formatted);
        }
        throw new IllegalArgumentException("Invalid date format: " + dateStr);
    }

    private static Time parseTime(String timeStr) {
        if (timeStr.length() == 6) {
            String formatted = timeStr.substring(0, 2) + ":" + timeStr.substring(2, 4) + ":" + timeStr.substring(4, 6);
            return Time.valueOf(formatted);
        }
        throw new IllegalArgumentException("Invalid time format: " + timeStr);
    }

    public static Serializable convertValue(String rawValue, int sqlType) {
        if (rawValue == null || rawValue.trim().isEmpty()) {
            return null;
        }
        rawValue = rawValue.trim(); // Trim spaces
        try {
            switch (sqlType) {
                case Types.TINYINT:
                    return Byte.parseByte(rawValue); // INT1
                case Types.SMALLINT:
                    return Short.parseShort(rawValue); // INT2
                case Types.INTEGER:
                    return Integer.parseInt(rawValue); // INT4
                case Types.BIGINT:
                    return Long.parseLong(rawValue); // INT8
                case Types.DOUBLE:
                    return Double.parseDouble(rawValue); // FLTP
                case Types.DECIMAL:
                    return new BigDecimal(rawValue); // DEC, CURR, QUAN
                case Types.DATE:
                    return parseDate(rawValue); // DATS (YYYYMMDD)
                case Types.TIME:
                    return parseTime(rawValue); // TIMS (HHMMSS)
                case Types.BINARY:
                case Types.BLOB:
                    return rawValue.getBytes(); // RAW, RAWSTRING, LRAW
                case Types.VARCHAR:
                case Types.CLOB:
                    return rawValue; // CHAR, STRING, NUMC, etc.
                default:
                    return rawValue; // Default: return as String
            }
        } catch (Exception e) {
            throw new IllegalArgumentException("Failed to parse value: " + rawValue + " for SQL Type: " + sqlType, e);
        }
    }

    public static OneRowChange odpRow2OneRowChange(
            byte[] odpRow,
            String odpContext,
            String odpName,
            List<FieldMeta> fieldMetas) {
        OneRowChange change = new OneRowChange();
        change.setSchemaName(odpContext);
        change.setTableName(odpName);
        change.setTableId(-1);

        ArrayList<OneRowChange.ColumnSpec> specs = change.getColumnSpec();
        ArrayList<OneRowChange.ColumnVal> values = new ArrayList<>();
        change.getColumnValues().add(values);
        int offset = 0;
        for (int i = 0; i < fieldMetas.size(); i ++) {
            FieldMeta fieldMeta = fieldMetas.get(i);
            int sqlType = abapTypeMapping(fieldMeta.getType());
            int length = fieldMeta.getOutputLength();
            if (Types.VARCHAR == sqlType || Types.CLOB == sqlType) {
                length = getUtf8BytesLength(odpRow, offset, length);
            }
            String rawValue = new String(odpRow, offset, length, StandardCharsets.UTF_8).trim();
            offset += length;
            Serializable serializable = convertValue(rawValue, sqlType);
            String name = fieldMeta.getName();
            if ("ODQ_CHANGEMODE".equalsIgnoreCase(name)) {
                change.setAction(actionMapping(((String) serializable).charAt(0)));
            }
            OneRowChange.ColumnSpec spec = change.new ColumnSpec();
            spec.setIndex(i + 1);
            spec.setName(name);
            spec.setType(sqlType);
            specs.add(spec);
            OneRowChange.ColumnVal value = change.new ColumnVal();
            value.setValue(serializable);
            values.add(value);
        }
        return change;
    }
}
