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

    private static final String subscriberType = "RODPS_REPL_TEST";
    private static final String subscriber = "TestRepository_DoesNotExist";
    private static final String subscription = "TestDataFlow_DoesNotExist";
    private static final String odpContext = "SLT~ODP01";
//    private static final String odpName = "FRUIT2";
    private static final String odpName = "VALUATION";

    private ODPWrapper odpWrapper;

    @Before
    public void setUp() throws Exception {
        odpWrapper = new ODPWrapper(DestinationConcept.SomeSampleDestinations.ABAP_AS1);
//        odpWrapper = new ODPWrapper(DestinationConcept.SomeSampleDestinations.ABAP_MS);
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
//        LOG.info("{}", odpWrapper.getODPList(subscriberType, odpContext, "FRUIT*"));
        LOG.info("{}", odpWrapper.getODPList(subscriberType, odpContext, "VAL*"));
    }

    @Test
    public void getODPDetails() throws JCoException {
        Triple<Map<String, String>, List<Map<String, String>>, List<FieldMeta>> details =
                odpWrapper.getODPDetails(subscriberType, odpContext, odpName);
        LOG.info("exportParameters - {}", details.getFirst());
//        LOG.info("deltaModes - {}", details.getSecond());
        LOG.info("segments - {}", details.getSecond());
        List<FieldMeta> fieldMetas = details.getThird();
        LOG.info("fields - [{}]", fieldMetas.size());
        fieldMetas.forEach(x -> LOG.info("{}", x));
        LOG.info("outputLength sum - {}", fieldMetas.stream().mapToInt(FieldMeta::getOutputLength).sum());
    }

    @Test
    public void closeExtractionSession() throws JCoException {
        odpWrapper.closeExtractionSession("20241202052351");
    }

    @Test
    public void registerODPCallback() throws JCoException {
        odpWrapper.registerODPCallback(
                subscriberType,
                "tester",
                "EN",
                null,
                null,
                null,
                null,
                null,
                null,
                "sap01");
    }

    @Test
    public void createODPSubscriber() throws JCoException {
        odpWrapper.createODPSubscriber(
                subscriberType,
                "ELDCLNT150",
                "EN",
                null,
                null,
                null,
                null,
                null,
                null,
                "sap01",
                "SAPABAP1");
    }

    @Test
    public void getODPSubscriptions() throws JCoException {
        odpWrapper.getODPSubscriptions(
                subscriberType,
                "",
                "",
                "",
                "");
    }

    public void getODPCursors(String mode) throws JCoException {
//        String subscriberProcess = "F".equals(mode) ? subscription:  "TestDeltaFlow_DoesNotExist";
        List<Map<String, String>> cursors = odpWrapper.getODPCursors(
                subscriberType,
                subscriber,
                odpContext,
                odpName,
                mode);
        LOG.info("cursors num({})", cursors.size());
        cursors.forEach(x -> LOG.info("{}", x));
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
    public void getODPLastModification() throws JCoException {
        odpWrapper.getLastModification(subscriberType, odpContext);
    }

    @Test
    public void resetODP() throws JCoException {
        odpWrapper.resetODP(
                subscriberType,
                subscriber,
                subscription,
                odpContext,
                odpName
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
                subscriberType,
                odpContext,
                "FRUIT2").getThird();
        ODPParser odpParser = new ODPParser("FRUIT2", fieldMetas);
        LOG.info("rowData - {}", odpParser.parseRow2Json(rowData));
    }

    private void fetchODP(String mode) throws Exception {
        List<FieldMeta> fieldMetas = odpWrapper.getODPDetails(
                subscriberType,
                odpContext,
                odpName).getThird();
        ODPParser odpParser = new ODPParser(odpName, fieldMetas);
        List<byte[]> fictions = odpWrapper.fetchODP(
                subscriberType,
                subscriber,
                subscription,
                odpContext,
                odpName,
                mode);
        LOG.info("got {} fictions(s)", fictions.size());
        List<byte[]> rows = ODPParser.mergeFragments(fictions, odpParser.getNumOfFragment());
        LOG.info("as {} row(s)", rows.size());
        for (byte[] rowData: rows) {
            if (LOG.isDebugEnabled()) {
                HexDump.hexDump(rowData);
            }
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