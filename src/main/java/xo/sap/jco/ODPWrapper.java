package xo.sap.jco;

import com.sap.conn.jco.*;
import org.apache.commons.cli.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import xo.utility.Triple;

import java.util.*;
import java.util.stream.Collectors;

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

    public ODPWrapper(String destinationName, Properties properties) throws JCoException {
        SimpleDestinationDataProvider.register();
        SimpleDestinationDataProvider.addDestination(destinationName, properties);
        this.destination = JCoDestinationManager.getDestination(destinationName);
    }

    private Map<String, String> getExportParameters(JCoFunction function) {
        JCoParameterList exportParameters = function.getExportParameterList();
        Map<String, String> parameters = new HashMap<>();
        for (JCoField field: exportParameters) {
            String value = field.getValue().toString();
            if (!"".equals(value)) {
                parameters.put(field.getName(), value);
            }
        }
        return parameters;
    }

    private List<Map<String, String>> getTableFields(JCoFunction function, String tableName) {
        List<Map<String, String>> fields = new ArrayList<>();
        JCoTable table = function.getTableParameterList().getTable(tableName);
        if (!table.isEmpty()) {
            List<String> names = new ArrayList<>();
            for (JCoField field : table) {
                names.add(field.getName());
            }
            do {
                Map<String, String> field = new HashMap<>();
                for (String name : names) {
                    String value = table.getString(name);
                    if (!"".equals(value)) {
                        field.put(name, value);
                    }
                }
                fields.add(field);
            } while (table.nextRow());
        }
        return fields;
    }

    private void printError(JCoFunction function) {
        JCoTable etReturn = function.getTableParameterList().getTable("ET_RETURN");
        if (!etReturn.isEmpty()) {
            LOG.error(etReturn.getString("MESSAGE"));
        }
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
        return getTableFields(function, "ET_CONTEXT");
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
        printError(function);
        return getTableFields(function, "ET_NODES");
    }

    public static List<FieldMeta> parseFieldMeta(List<Map<String, String>> fieldMetaData) {
        List<FieldMeta> fieldMetaList = new ArrayList<>();
        for (Map<String, String> meta : fieldMetaData) {
            FieldMeta fieldMeta = new FieldMeta(
                    "X".equals(meta.get("KEYFLAG")),
                    Integer.parseInt(meta.getOrDefault("OUTPUTLENG", "0")),
                    meta.get("DESCRIPTION"),
                    "X".equals(meta.get("TRANSFER")),
                    "X".equals(meta.get("SEL_EQUAL")),
                    Integer.parseInt(meta.getOrDefault("LENGTH", "0")),
                    "X".equals(meta.get("SEL_BETWEEN")),
                    "X".equals(meta.get("SEL_NONDEFAULT")),
                    Integer.parseInt(meta.getOrDefault("DECIMALS", "0")),
                    meta.get("TYPE"),
                    "X".equals(meta.get("SEL_PATTERN")),
                    meta.get("NAME"),
                    "X".equals(meta.get("DELTAFIELD"))
            );
            fieldMetaList.add(fieldMeta);
        }
        return fieldMetaList;
    }

    public Triple<Map<String, String>, List<Map<String, String>>, List<FieldMeta>> getODPDetails(
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

        printError(function);
        return new Triple<>(getExportParameters(function),
//                getTableFields(function, "ET_DELTAMODES"),
                getTableFields(function, "ET_SEGMENTS"),
                parseFieldMeta(getTableFields(function, "ET_FIELDS")));
    }

    public List<Map<String, String>> getODPCursors(
            String subscriberType,
            String subscriberName,
            String context,
            String odpName,
            String mode) throws JCoException {
        JCoFunction function = destination.getRepository().getFunction("RODPS_REPL_CURSOR_GET_LIST");
        if (function == null) {
            throw new RuntimeException("Function RODPS_REPL_CURSOR_GET_LIST not found in SAP.");
        }
        function.getImportParameterList().setValue("I_SUBSCRIBER_TYPE", subscriberType);
        function.getImportParameterList().setValue("I_SUBSCRIBER_NAME", subscriberName);
        function.getImportParameterList().setValue("I_CONTEXT", context);
        function.getImportParameterList().setValue("I_ODPNAME", odpName);
        function.getImportParameterList().setValue("I_EXTRACTION_MODE", mode);
        function.execute(destination);
        List<Map<String, String>> pointers = getTableFields(function, "ET_PROCESS");
        return pointers.stream().filter(map -> map.containsKey("CLOSED")).collect(Collectors.toList());
    }

    public void resetODP(
            String subscriberType,
            String subscriberName,
            String subscriberProcess,
            String context,
            String odpName) throws JCoException {
        JCoFunction function = destination.getRepository().getFunction("RODPS_REPL_ODP_RESET");
        if (function == null) {
            throw new RuntimeException("Function RODPS_REPL_ODP_RESET not found in SAP.");
        }
        function.getImportParameterList().setValue("I_SUBSCRIBER_TYPE", subscriberType);
        function.getImportParameterList().setValue("I_SUBSCRIBER_NAME", subscriberName);
        function.getImportParameterList().setValue("I_SUBSCRIBER_PROCESS", subscriberProcess);
        function.getImportParameterList().setValue("I_CONTEXT", context);
        function.getImportParameterList().setValue("I_ODPNAME", odpName);
        function.execute(destination);
        JCoTable etReturn = function.getTableParameterList().getTable("ET_RETURN");
        if (!etReturn.isEmpty()) {
            LOG.info(etReturn.getString("MESSAGE"));
        }
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
        printError(function);
        Map<String, String> parameters = getExportParameters(function);
        LOG.info("{}", parameters);
        return parameters.get("E_POINTER");
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
        JCoTable etReturn = function.getTableParameterList().getTable("ET_RETURN");
        if (!etReturn.isEmpty()) {
            LOG.info(etReturn.getString("MESSAGE"));
        }
    }

    private void getMeta(JCoTable jCoTable) {
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

    private List<byte[]> fetchODP(String pointer, String extractPackage) throws JCoException {
        List<byte[]> rows = new ArrayList<>();
        while (true) {
            JCoFunction function = destination.getRepository().getFunction("RODPS_REPL_ODP_FETCH");
            if (function == null) {
                throw new RuntimeException("Function RODPS_REPL_ODP_FETCH not found in SAP.");
            }
            function.getImportParameterList().setValue("I_POINTER", pointer);
            function.getImportParameterList().setValue("I_PACKAGE", extractPackage);
            function.execute(destination);
            if ("X".equals(function.getExportParameterList().getString("E_NO_MORE_DATA"))) {
                break;
            }
            extractPackage = function.getExportParameterList().getString("E_PACKAGE");
            JCoTable table = function.getTableParameterList().getTable("ET_DATA");
            if (table.isEmpty()) {
                break;
            }
            do {
                for (JCoField field : table) {
                    if ("DATA".equals(field.getName())) {
                        rows.add(field.getByteArray());
                        break;
                    }
                }
            } while (table.nextRow());
        }
        return rows;
    }

    public List<byte[]> fetchODP(
            String subscriberType,
            String subscriberName,
            String subscriberProcess,
            String context,
            String odpName,
            String mode) throws JCoException {
        String pointer =
                openExtractionSession(subscriberType, subscriberName, subscriberProcess, context, odpName, mode);
        List<byte[]> rows = fetchODP(pointer, "");
        closeExtractionSession(pointer);
        return rows;
    }

    private static void run(String action, CommandLine cmd) {
        try {
            ODPWrapper odpWrapper = new ODPWrapper(DestinationConcept.SomeSampleDestinations.ABAP_AS1);
            switch (action) {
                case "ListContexts":
                    LOG.info("{}", odpWrapper.getContexts());
                    return;
                case "ListODPs":
                    assert cmd.hasOption("subscriberType") &&
                            cmd.hasOption("contextName") &&
                            cmd.hasOption("odpNamePattern");
                    LOG.info("{}", odpWrapper.getODPList(
                            cmd.getOptionValue("subscriberType"),
                            cmd.getOptionValue("contextName"),
                            cmd.getOptionValue("odpNamePattern")));
                    return;
                case "ShowODP":
                    assert cmd.hasOption("subscriberType") &&
                            cmd.hasOption("contextName") &&
                            cmd.hasOption("odpNamePattern");
                    Triple<Map<String, String>, List<Map<String, String>>, List<FieldMeta>> details =
                            odpWrapper.getODPDetails(
                                    cmd.getOptionValue("subscriberType"),
                                    cmd.getOptionValue("contextName"),
                                    cmd.getOptionValue("odpNamePattern"));
                    LOG.info("exportParameters - {}", details.getFirst());
                    LOG.info("segments - {}", details.getSecond());
                    LOG.info("fields - {}", details.getThird());
                    return;
                case "ListCursors":
                    assert cmd.hasOption("subscriberType") &&
                            cmd.hasOption("subscriberName") &&
                            cmd.hasOption("contextName") &&
                            cmd.hasOption("odpNamePattern") &&
                            cmd.hasOption("extractMode");
                    LOG.info("{}", odpWrapper.getODPCursors(
                            cmd.getOptionValue("subscriberType"),
                            cmd.getOptionValue("subscriberName"),
                            cmd.getOptionValue("contextName"),
                            cmd.getOptionValue("odpNamePattern"),
                            cmd.getOptionValue("extractMode")));
                    return;
                case "ResetODP":
                    assert cmd.hasOption("subscriberType") &&
                            cmd.hasOption("subscriberName") &&
                            cmd.hasOption("subscriberProcess") &&
                            cmd.hasOption("contextName") &&
                            cmd.hasOption("odpNamePattern");
                    odpWrapper.resetODP(
                            cmd.getOptionValue("subscriberType"),
                            cmd.getOptionValue("subscriberName"),
                            cmd.getOptionValue("subscriberProcess"),
                            cmd.getOptionValue("contextName"),
                            cmd.getOptionValue("odpNamePattern"));
                    return;
                case "FetchODP":
                    assert cmd.hasOption("subscriberType") &&
                            cmd.hasOption("subscriberName") &&
                            cmd.hasOption("subscriberProcess") &&
                            cmd.hasOption("contextName") &&
                            cmd.hasOption("odpNamePattern") &&
                            cmd.hasOption("extractMode");
                    String odpName = cmd.getOptionValue("odpNamePattern");
                    List<FieldMeta> fieldMetas = odpWrapper.getODPDetails(
                            cmd.getOptionValue("subscriberType"),
                            cmd.getOptionValue("contextName"),
                            odpName).getThird();
                    ODPParser odpParser = new ODPParser(odpName, fieldMetas);
                    List<byte[]> rows = odpWrapper.fetchODP(
                            cmd.getOptionValue("subscriberType"),
                            cmd.getOptionValue("subscriberName"),
                            cmd.getOptionValue("subscriberProcess"),
                            cmd.getOptionValue("contextName"),
                            odpName,
                            cmd.getOptionValue("extractMode"));
                    for (byte[] rowData: rows) {
//                        HexDump.hexDump(rowData);
                        LOG.info("row - {}", odpParser.parseRow2Json(rowData));
                    }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        Options options = new Options();
        // Define action argument (mandatory)
        Option action = Option.builder("a")
                .longOpt("action")
                .desc("Action to perform: ListContexts, ListODPs, ShowODP, ListCursors, ResetODP, FetchODP")
                .hasArg()
                .argName("action")
                .required()
                .build();
        options.addOption(action);

        // Define optional parameters
        options.addOption(Option.builder("st")
                .longOpt("subscriberType")
                .desc("Type of the subscriber")
                .hasArg()
                .argName("subscriberType")
                .build());
        options.addOption(Option.builder("sn")
                .longOpt("subscriberName")
                .desc("Name of the subscriber")
                .hasArg()
                .argName("subscriberName")
                .build());
        options.addOption(Option.builder("sp")
                .longOpt("subscriberProcess")
                .desc("Subscriber process name")
                .hasArg()
                .argName("subscriberProcess")
                .build());
        options.addOption(Option.builder("cn")
                .longOpt("contextName")
                .desc("Name of the context")
                .hasArg()
                .argName("contextName")
                .build());
        options.addOption(Option.builder("onp")
                .longOpt("odpNamePattern")
                .desc("Pattern for ODP names")
                .hasArg()
                .argName("odpNamePattern")
                .build());
        options.addOption(Option.builder("em")
                .longOpt("extractMode")
                .desc("Mode of extraction")
                .hasArg()
                .argName("extractMode")
                .build());
        // Parse the command-line arguments
        CommandLineParser parser = new DefaultParser();
        HelpFormatter formatter = new HelpFormatter();
        try {
            CommandLine cmd = parser.parse(options, args);
            // Retrieve the action
            String actionValue = cmd.getOptionValue("action");
            System.out.println("Action: " + actionValue);
            run(actionValue, cmd);
        } catch (ParseException e) {
            System.out.println(e.getMessage());
            formatter.printHelp("ODPWrapper", options);
            System.exit(1);
        }
    }
}