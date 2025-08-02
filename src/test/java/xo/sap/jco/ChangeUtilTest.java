package xo.sap.jco;

import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.List;

public class ChangeUtilTest {
    private static final Logger LOG = LoggerFactory.getLogger(ChangeUtil.class);
    private static final String context = "SLT~ODP01";
    private static final String odpName = "FRUIT2";

    @Test
    public void fruitChange() {
        byte[] rowData = {
                0x20, 0x20, 0x20, 0x20, 0x20, 0x20, 0x20, 0x31,
                0x30, 0x31, 0x20, (byte) 0xF0, (byte) 0x9F, (byte) 0x8D, (byte) 0x89, 0x20,
                0x20, 0x20, 0x20, 0x20, 0x20, 0x20, 0x20, 0x20, 0x20, 0x36, 0x2E, 0x30, 0x30, 0x30, 0x30, 0x30,
                0x30, 0x30, 0x30, 0x30, 0x30, 0x30, 0x30, 0x30, 0x30, 0x30, 0x30, 0x45, 0x2B, 0x30, 0x32, 0x43,
                0x20, 0x20, 0x20, 0x20, 0x20, 0x20, 0x20, 0x20, 0x20, 0x20, 0x20, 0x20, 0x20, 0x20, 0x20, 0x20,
                0x20, 0x20, 0x31, 0x20, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
                0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        };

        List<FieldMeta> fieldMetas = new ArrayList<>();
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

        OneRowChange change = ChangeUtil.odpRow2OneRowChange(rowData, context, odpName, fieldMetas);
        LOG.info("fruit: {}", change);
    }

    @Test
    public void myTypeChange() {
        byte[] rowData = {
                0x20, 0x20, 0x20, 0x20, 0x20, 0x20, 0x20, 0x20, 0x20, 0x31, 0x2D, 0x20, 0x20, 0x20, 0x20, 0x20,
                0x20, 0x39, 0x38, 0x37, 0x36, 0x35, 0x34, 0x33, 0x32, 0x31, 0x30, 0x39, 0x38, 0x37, 0x2D, 0x20,
                0x2D, 0x32, 0x2E, 0x37, 0x31, 0x38, 0x32, 0x38, 0x30, 0x30, 0x30, 0x30, 0x30, 0x30, 0x30, 0x30,
                0x30, 0x30, 0x30, 0x45, 0x2B, 0x30, 0x30, 0x20, 0x20, 0x20, 0x20, 0x20, 0x20, 0x20, 0x20, 0x39,
                0x38, 0x37, 0x36, 0x35, 0x2E, 0x34, 0x33, 0x2D, 0x32, 0x30, 0x32, 0x35, 0x30, 0x37, 0x33, 0x31,
                0x32, 0x33, 0x34, 0x35, 0x30, 0x31, 0x30, 0x30, 0x30, 0x30, 0x30, 0x30, 0x30, 0x30, 0x30, 0x20,
                0x20, 0x20, 0x20, 0x43, 0x20, 0x20, 0x20, 0x20, 0x20, 0x20, 0x20, 0x20, 0x20, 0x20, 0x20, 0x20,
                0x20, 0x20, 0x20, 0x20, 0x20, 0x20, 0x31, 0x20, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
                0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        };

        List<FieldMeta> fieldMetas = new ArrayList<>();
        fieldMetas.add(new FieldMeta(
                true, 11, "ID", true, true, 10, true,
                false, 0, "INT4", true, "ID", false));
        fieldMetas.add(new FieldMeta(
                false, 20, "BIG_NUMBER", true, true, 19, true,
                true, 0, "INT8", true, "BIG_NUMBER", false));
        fieldMetas.add(new FieldMeta(
                false, 24, "FLOAT_VAL", true, true, 16, true,
                true, 16, "FLTP", true, "FLOAT_VAL", false));
        fieldMetas.add(new FieldMeta(
                false, 17, "DEC_VAL", true, true, 15, true,
                true, 2, "DEC", true, "DEC_VAL", false));
        fieldMetas.add(new FieldMeta(
                false, 27, "null", true, true, 27, true,
                true, 0, "CHAR", true, "TS_VAL", false));
        fieldMetas.add(new FieldMeta(
                false, 1, "Change Mode for a Data Record in the Delta", true,
                false, 1, false, false, 0, "CHAR", false,
                "ODQ_CHANGEMODE", false));
        fieldMetas.add(new FieldMeta(
                false, 20, "Number of Data Units (Data Records for Example)", true,
                false, 19, false, false, 0, "DEC", false,
                "ODQ_ENTITYCNTR", false));
        OneRowChange change = ChangeUtil.odpRow2OneRowChange(rowData, context, odpName, fieldMetas);
        LOG.info("myType: {}", change);
    }
}