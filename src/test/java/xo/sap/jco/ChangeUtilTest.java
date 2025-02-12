package xo.sap.jco;

import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import xo.hbase.OneRowChange;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.List;

public class ChangeUtilTest {
    private static final Logger LOG = LoggerFactory.getLogger(ChangeUtil.class);
    private static final String context = "SLT~ODP01";
    private static final String odpName = "FRUIT2";

    private final List<FieldMeta> fieldMetas = new ArrayList<>();
    private final byte[] rowData = {
            0x20, 0x20, 0x20, 0x20, 0x20, 0x20, 0x20, 0x31,
            0x30, 0x31, 0x20, (byte) 0xF0, (byte) 0x9F, (byte) 0x8D, (byte) 0x89, 0x20,
            0x20, 0x20, 0x20, 0x20, 0x20, 0x20, 0x20, 0x20, 0x20, 0x36, 0x2E, 0x30, 0x30, 0x30, 0x30, 0x30,
            0x30, 0x30, 0x30, 0x30, 0x30, 0x30, 0x30, 0x30, 0x30, 0x30, 0x30, 0x45, 0x2B, 0x30, 0x32, 0x43,
            0x20, 0x20, 0x20, 0x20, 0x20, 0x20, 0x20, 0x20, 0x20, 0x20, 0x20, 0x20, 0x20, 0x20, 0x20, 0x20,
            0x20, 0x20, 0x31, 0x20, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
    };

    @Before
    public void setUp() {
        fieldMetas.add(new FieldMeta(
                true, 11, "ID", true, true, 10, true,
                false, 0, "INT4", true, "ID", false));
        fieldMetas.add(new FieldMeta(
                false, 10, "NAME", true, true, 10, true,
                true, 0, "CHAR", true, "NAME", false));
        fieldMetas.add(new FieldMeta(
                false, 24, "null", true, true, 16, true,
                true, 16, "FLTP", true, "PRICE", false));
        fieldMetas.add(new FieldMeta(
                false, 1, "Change Mode for a Data Record in the Delta", true,
                false, 1, false, false, 0, "CHAR", false,
                "ODQ_CHANGEMODE", false));
        fieldMetas.add(new FieldMeta(
                false, 20, "Number of Data Units (Data Records for Example)", true,
                false, 19, false, false, 0, "DEC", false,
                "ODQ_ENTITYCNTR", false));
    }

    @Test
    public void odpRow2OneRowChange() {
        OneRowChange change = ChangeUtil.odpRow2OneRowChange(rowData, context, odpName, fieldMetas);
    }
}