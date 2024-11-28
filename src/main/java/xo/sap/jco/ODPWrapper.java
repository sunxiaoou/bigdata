package xo.sap.jco;

import com.sap.conn.jco.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import xo.utility.HexDump;

import java.util.*;

public class ODPWrapper {
    private static final Logger LOG = LoggerFactory.getLogger(ODPWrapper.class);
    private final JCoDestination destination;

    /**
     * Constructor to initialize JCo destination
     * @param destinationName The name of the JCo destination configuration
     */
    public ODPWrapper(String destinationName) throws JCoException {
        this.destination = JCoDestinationManager.getDestination(destinationName);
    }

    /**
     * Fetch the list of available ODP contexts.
     * @return List of contexts with their description
     * @throws JCoException if any SAP error occurs
     */
    public List<Map<String, String>> getContexts() throws JCoException {
        JCoFunction function = destination.getRepository().getFunction("RODPS_REPL_CONTEXT_GET_LIST");
        if (function == null) {
            throw new RuntimeException("Function RODPS_REPL_CONTEXT_GET_LIST not found in SAP.");
        }
        function.getImportParameterList().setValue("I_INCLUDE_HIDDEN", "X");
        function.execute(destination);
        JCoTable table = function.getTableParameterList().getTable("ET_CONTEXT");
        List<String> fields = new ArrayList<>();
        for (JCoField field : table) {
            fields.add(field.getName());
        }
        List<Map<String, String>> contexts = new ArrayList<>();
        while (table.nextRow()) {
            Map<String, String> context = new HashMap<>();
            for (String field: fields) {
                context.put(field, table.getString(field));
            }
            contexts.add(context);
        }
        return contexts;
    }

    /**
     * Fetch the list of ODPs for a given context.
     * @param context The context name
     * @return List of ODPs with their description
     * @throws JCoException if any SAP error occurs
     */
    public List<Map<String, String>> getODPList(
            String subscriberType,
            String context,
            String pattern) throws JCoException {
        JCoFunction function = destination.getRepository().getFunction("RODPS_REPL_ODP_GET_LIST");
        if (function == null) {
            throw new RuntimeException("Function RODPS_REPL_ODP_GET_LIST not found in SAP.");
        }
        function.getImportParameterList().setValue("I_SUBSCRIBER_TYPE", subscriberType);
        function.getImportParameterList().setValue("I_CONTEXT", context);
        function.getImportParameterList().setValue("I_SEARCH_PATTERN", pattern);
        function.execute(destination);

        JCoTable table = function.getTableParameterList().getTable("ET_NODES");
        List<String> fields = new ArrayList<>();
        for (JCoField field : table) {
            fields.add(field.getName());
        }
        List<Map<String, String>> odps = new ArrayList<>();
        while (table.nextRow()) {
            Map<String, String> odp = new HashMap<>();
            for (String field: fields) {
                odp.put(field, table.getString(field));
            }
            odps.add(odp);
        }
        return odps;
    }

    /**
     * Fetch the fields and metadata for a specific ODP.
     * @param context The context name
     * @param odpName The ODP name
     * @return List of fields with their metadata
     * @throws JCoException if any SAP error occurs
     */
    public List<Map<String, String>> getODPDetails(
            String subscriberType,
            String context,
            String odpName) throws JCoException {
        JCoFunction function = destination.getRepository().getFunction("RODPS_REPL_ODP_GET_DETAIL");
        if (function == null) {
            throw new RuntimeException("Function RODPS_REPL_ODP_GET_DETAIL not found in SAP.");
        }
        function.getImportParameterList().setValue("I_SUBSCRIBER_TYPE", subscriberType);
        function.getImportParameterList().setValue("I_CONTEXT", context);
        function.getImportParameterList().setValue("I_ODPNAME", odpName);
        function.execute(destination);

        JCoTable table = function.getTableParameterList().getTable("ET_FIELDS");
        List<String> fields = new ArrayList<>();
        for (JCoField field : table) {
            fields.add(field.getName());
        }
        List<Map<String, String>> odps = new ArrayList<>();
        while (table.nextRow()) {
            Map<String, String> odp = new HashMap<>();
            for (String field: fields) {
                String value = table.getString(field);
                if (!"".equals(value)) {
                    odp.put(field, value);
                }
            }
            odps.add(odp);
        }
        return odps;
    }

    /**
     * Open a data extraction session in Full or Delta mode.
     * @param context The context name
     * @param odpName The ODP name
     * @param mode Full ("F") or Delta ("D")
     * @return The extraction pointer
     * @throws JCoException if any SAP error occurs
     */
    public String openExtractionSession(
            String subscriberType,
            String subscriberName,
            String subscriberProcess,
            String context,
            String odpName,
            String mode) throws JCoException {
        JCoFunction function = destination.getRepository().getFunction("RODPS_REPL_ODP_OPEN");
        if (function == null) {
            throw new RuntimeException("Function RODPS_REPL_ODP_OPEN not found in SAP.");
        }
        function.getImportParameterList().setValue("I_SUBSCRIBER_TYPE", subscriberType);
        function.getImportParameterList().setValue("I_SUBSCRIBER_NAME", subscriberName);
        function.getImportParameterList().setValue("I_SUBSCRIBER_PROCESS", subscriberProcess);
        function.getImportParameterList().setValue("I_CONTEXT", context);
        function.getImportParameterList().setValue("I_ODPNAME", odpName);
        function.getImportParameterList().setValue("I_EXTRACTION_MODE", mode);
        function.execute(destination);
        return function.getExportParameterList().getString("E_POINTER");
    }

    /**
     * Fetch data packages for the given extraction pointer.
     * @param pointer The extraction pointer
     * @return List of data packages, each represented as a table
     * @throws JCoException if any SAP error occurs
     */
    public List<JCoTable> fetchData(String pointer, String extractPackage) throws JCoException {
        JCoFunction function = destination.getRepository().getFunction("RODPS_REPL_ODP_FETCH");
        if (function == null) {
            throw new RuntimeException("Function RODPS_REPL_ODP_FETCH not found in SAP.");
        }
        function.getImportParameterList().setValue("I_POINTER", pointer);
        function.getImportParameterList().setValue("I_PACKAGE", extractPackage);
        function.execute(destination);

        List<JCoTable> dataPackages = new ArrayList<>();
        JCoTable dataTable = function.getTableParameterList().getTable("ET_DATA");
        while (dataTable.nextRow()) {
            dataPackages.add(dataTable);
        }
        return dataPackages;
    }

    /**
     * Close an open extraction session.
     * @param pointer The extraction pointer
     * @throws JCoException if any SAP error occurs
     */
    public void closeExtractionSession(String pointer) throws JCoException {
        JCoFunction function = destination.getRepository().getFunction("RODPS_REPL_ODP_CLOSE");
        if (function == null) {
            throw new RuntimeException("Function RODPS_REPL_ODP_CLOSE not found in SAP.");
        }
        function.getImportParameterList().setValue("I_POINTER", pointer);
        function.execute(destination);
    }

    public void getMeta(JCoTable jCoTable) {
        System.out.println("Row Data:");
        JCoRecordMetaData metaData = (JCoRecordMetaData) jCoTable.getMetaData();
        for (int i = 0; i < metaData.getFieldCount(); i++) {
            String fieldName = metaData.getName(i);         // 字段名称
            String fieldType = metaData.getTypeAsString(i); // 字段类型
            String fieldValue = jCoTable.getString(fieldName); // 获取字段值（这里用字符串读取）

            System.out.println("  Field: " + fieldName);
            System.out.println("    Type: " + fieldType);
            System.out.println("    Value: " + fieldValue);
        }
    }

    public List<byte[]> fullFetch(
            String subscriberType,
            String subscriberName,
            String subscriberProcess,
            String context,
            String odpName) throws JCoException {
        String pointer =
                openExtractionSession(subscriberType, subscriberName, subscriberProcess, context, odpName, "F");
        String extractPackage = "";
        List<byte[]> list = new ArrayList<>();
        while (true) {
            JCoFunction function = destination.getRepository().getFunction("RODPS_REPL_ODP_FETCH");
            if (function == null) {
                throw new RuntimeException("Function RODPS_REPL_ODP_FETCH not found in SAP.");
            }
            function.getImportParameterList().setValue("I_POINTER", pointer);
            function.getImportParameterList().setValue("I_PACKAGE", extractPackage);
            function.execute(destination);
            JCoTable etData = function.getTableParameterList().getTable("ET_DATA");
            if (etData.isEmpty()) {
                LOG.info("ET_DATA is empty, all data fetched.");
                break;
            }
            while (etData.nextRow()) {
                for (JCoField field : etData) {
                    if ("DATA".equals(field.getName())) {
                        list.add(field.getByteArray());
                        break;
                    }
                }
            }
            extractPackage = function.getExportParameterList().getString("E_PACKAGE");
            if (extractPackage.isEmpty()) {
                LOG.info("No more extract package, all data fetched.");
                break;
            }
        }
        closeExtractionSession(pointer);
        return list;
    }

    /**
     * Print errors from a JCo table (ET_RETURN).
     * @param returnTable The ET_RETURN table from the SAP function
     */
    private void printErrors(JCoTable returnTable) {
        while (returnTable.nextRow()) {
            System.err.println("Error: " + returnTable.getString("MESSAGE"));
        }
    }
}
