package xo.sap.jco;

import com.sap.conn.jco.JCoException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import xo.utility.HexDump;

import java.util.List;

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
        LOG.info("{}", odpWrapper.getODPDetails("RODPS_REPL_TEST", "SLT~ODP01", "FRUIT2"));
    }

    @Test
    public void openExtractionSession() throws JCoException {
        LOG.info("{}", odpWrapper.openExtractionSession(
                "RODPS_REPL_TEST",
                "TestRepository_DoesNotExist",
                "TestDataFlow_DoesNotExist",
                "SLT~ODP01",
                "FRUIT2",
                "F"));
    }

    public void getODPCursors(String mode) throws JCoException {
        LOG.info("{}", odpWrapper.getODPCursors(
                "RODPS_REPL_TEST",
                "TestRepository_DoesNotExist",
                "TestDataFlow_DoesNotExist",
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
    public void fullFetch() throws JCoException {
        List<byte[]> list = odpWrapper.fullFetch(
                "RODPS_REPL_TEST",
                "TestRepository_DoesNotExist",
                "TestDataFlow_DoesNotExist",
                "SLT~ODP01",
                "FRUIT2");
        for (byte[] bytes: list) {
            HexDump.hexDump(bytes);
        }
    }
}