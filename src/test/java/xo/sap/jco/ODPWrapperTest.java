package xo.sap.jco;

import com.sap.conn.jco.JCoException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import xo.utility.HexDump;
import xo.utility.Triple;

import java.util.List;
import java.util.Map;

import static org.junit.Assert.*;

public class ODPWrapperTest {
    private static final Logger LOG = LoggerFactory.getLogger(ODPWrapperTest.class);
    private ODPWrapper odpWrapper;

    @Before
    public void setUp() throws Exception {
        odpWrapper = new ODPWrapper(DestinationConcept.SomeSampleDestinations.ABAP_AS1);
    }

    @After
    public void tearDown() throws Exception {
    }

    @Test
    public void getContexts() throws JCoException {
        LOG.info("{}", odpWrapper.getContexts());
    }

    @Test
    public void getODPList() throws JCoException {
        LOG.info("{}", odpWrapper.getODPList("RODPS_REPL_TEST", "SLT~ODP01", "FRUIT*"));
    }

    @Test
    public void getODPDetails() throws JCoException {
        Triple<Map<String, String>, List<Map<String, String>>, List<FieldMeta>> details =
                odpWrapper.getODPDetails("RODPS_REPL_TEST", "SLT~ODP01", "FRUIT2");
        LOG.info("exportParameters - {}", details.getFirst());
//        LOG.info("deltaModes - {}", details.getSecond());
        LOG.info("segments - {}", details.getSecond());
        LOG.info("fields - {}", details.getThird());
    }

    @Test
    public void closeExtractionSession() throws JCoException {
        odpWrapper.closeExtractionSession("20241202052351");
    }

    public void getODPCursors(String mode) throws JCoException {
//        String subscriberProcess = "F".equals(mode) ? "TestDataFlow_DoesNotExist":  "TestDeltaFlow_DoesNotExist";
        LOG.info("{}", odpWrapper.getODPCursors(
                "RODPS_REPL_TEST",
                "TestRepository_DoesNotExist",
                "SLT~ODP01",
                "FRUIT2",
                mode));
    }

    @Test
    public void getODPCursorsFull() throws JCoException {
        getODPCursors("F");
    }

    @Test
    public void getODPCursorsDelta() throws JCoException {
        getODPCursors("D");
    }

    @Test
    public void getODPCursorsRealTime() throws JCoException {
        getODPCursors("R");
    }

    @Test
    public void resetODP() throws JCoException {
        odpWrapper.resetODP(
                "RODPS_REPL_TEST",
                "TestRepository_DoesNotExist",
                "TestDataFlow_DoesNotExist",
                "SLT~ODP01",
                "FRUIT2"
        );
    }

    @Test
    public void getUtf8BytesLength() {
        byte[] data = {(byte) 0xF0, (byte) 0x9F, (byte) 0x8D, (byte) 0x89, 0x20, 0x20, 0x20, 0x20, 0x20, 0x20, 0x20,
                0x20};
        LOG.info("Bytes length is {}", ODPParser.getUtf8BytesLength(data, 0, 10));
    }

    @Test
    public void parseRow() throws Exception {
        byte[] rowData = {
                0x20, 0x20, 0x20, 0x20, 0x20, 0x20, 0x20, 0x31,
                0x30, 0x31, 0x20, (byte) 0xF0, (byte) 0x9F, (byte) 0x8D, (byte) 0x89, 0x20,
                0x20, 0x20, 0x20, 0x20, 0x20, 0x20, 0x20, 0x20, 0x20, 0x36, 0x2E, 0x30, 0x30, 0x30, 0x30, 0x30,
                0x30, 0x30, 0x30, 0x30, 0x30, 0x30, 0x30, 0x30, 0x30, 0x30, 0x30, 0x45, 0x2B, 0x30, 0x32, 0x43,
                0x20, 0x20, 0x20, 0x20, 0x20, 0x20, 0x20, 0x20, 0x20, 0x20, 0x20, 0x20, 0x20, 0x20, 0x20, 0x20,
                0x20, 0x20, 0x31, 0x20, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
                0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        };
        List<FieldMeta> fieldMetas = odpWrapper.getODPDetails(
                "RODPS_REPL_TEST",
                "SLT~ODP01",
                "FRUIT2").getThird();
        ODPParser odpParser = new ODPParser(fieldMetas);
        LOG.info("rowData - {}", odpParser.parseRow2Json(rowData));
    }

    private void fetchODP(String mode) throws Exception {
        List<FieldMeta> fieldMetas = odpWrapper.getODPDetails(
                "RODPS_REPL_TEST",
                "SLT~ODP01",
                "FRUIT2").getThird();
        ODPParser odpParser = new ODPParser(fieldMetas);
        List<byte[]> rows = odpWrapper.fetchODP(
                "RODPS_REPL_TEST",
                "TestRepository_DoesNotExist",
                "TestDataFlow_DoesNotExist",
                "SLT~ODP01",
                "FRUIT2",
                mode);
        for (byte[] rowData: rows) {
//            HexDump.hexDump(rowData);
            LOG.info("row - {}", odpParser.parseRow2Json(rowData));
        }
    }

    @Test
    public void fetchODPFull() throws Exception {
        fetchODP("F");
    }

    @Test
    public void fetchODPDelta() throws Exception {
        fetchODP("D");
    }
}