package xo.sap.jco;

import com.sap.conn.jco.*;
import org.apache.commons.cli.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import xo.utility.HexDump;
import xo.utility.Triple;

import java.util.*;
import java.util.stream.Collectors;

public class ODPWrapper {
    private static final Logger LOG = LoggerFactory.getLogger(ODPWrapper.class);
    private static final int lenOfFragment = 250;

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

    public static int getNumOfFragment(List<FieldMeta> fieldMetas) {
        int lenOfRow = fieldMetas.stream().mapToInt(FieldMeta::getOutputLength).sum();
        int n = lenOfRow / lenOfFragment;
        return lenOfRow % lenOfFragment == 0 ? n : n + 1;
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

    public void getLastModification(String subscriberType, String context) throws JCoException {
        JCoFunction function = destination.getRepository().getFunction("RODPS_REPL_ODP_GET_LAST_MODIF");
        if (function == null) {
            throw new RuntimeException("Function RODPS_REPL_ODP_GET_LAST_MODIF not found in SAP.");
        }
        JCoParameterList parameters = function.getImportParameterList();
        parameters.setValue("I_SUBSCRIBER_TYPE", subscriberType);
        parameters.setValue("I_CONTEXT", context);
        function.execute(destination);
        Map<String, String> exportParameters = getExportParameters(function);
        LOG.info("{}", exportParameters);
        printError(function);
        JCoTable itOdp = function.getTableParameterList().getTable("IT_ODP");
        if (itOdp.getNumRows() > 0) {
            LOG.info("{}", itOdp.toJSON());
        }
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
        } else {
            LOG.info("{}.{} has been reset", context, odpName);
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
        if (parameters.containsKey("E_DELTA_EXTENSION")) {
            LOG.info("{}.{} has opened with {}", context, odpName, parameters);
        } else {
            LOG.debug("{}.{} has opened with {}", context, odpName, parameters);
        }
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
        } else {
            LOG.debug("pointer {} has closed", pointer);
        }
    }

    public List<String> preFetchODP(String pointer, String odpName) throws JCoException {
        List<String> newPackages = new ArrayList<>();
        List<String> returnMessages = new ArrayList<>();
        for (int i = 0; ; i++) {
            JCoFunction function = destination.getRepository().getFunction("RODPS_REPL_ODP_PREFETCH");
            if (function == null) {
                throw new RuntimeException("Function RODPS_REPL_ODP_PREFETCH not found in SAP.");
            }
            function.getImportParameterList().setValue("I_POINTER", pointer);
            function.getImportParameterList().setValue("I_PACKAGE", String.format("%s_%04d", odpName, i));
//            function.getImportParameterList().setValue("I_MAXPACKAGESIZE", 256);
            function.execute(destination);
            JCoTable etReturn = function.getTableParameterList().getTable("ET_RETURN");
            if (!etReturn.isEmpty()) {
                for (int j = 0; j < etReturn.getNumRows(); j++) {
                    etReturn.setRow(j);
                    returnMessages.add(etReturn.getString("MESSAGE"));
                    LOG.info(etReturn.getString("MESSAGE"));
                }
            }
            boolean noMoreData = "X".equals(function.getExportParameterList().getString("E_NO_MORE_DATA"));
            if (!returnMessages.isEmpty() || noMoreData) {
                break;
            }
            String packageName = function.getExportParameterList().getString("E_PACKAGE");
            newPackages.add(packageName);
            LOG.info("E_PACKAGE={}", packageName);
        }
        return newPackages;
    }

    public void preFetchAndFetchODP(
            String subscriberType,
            String subscriberName,
            String subscriberProcess,
            String context,
            String odpName,
            String mode) throws JCoException {
        String pointer =
                openExtractionSession(subscriberType, subscriberName, subscriberProcess, context, odpName, mode);
        List<String> packages = preFetchODP(pointer, odpName);
        List<FieldMeta> fieldMetas = getODPDetails(subscriberType, context, odpName).getThird();
        int numOfFragment = getNumOfFragment(fieldMetas);
        for (String extractPackage : packages) {
            LOG.info("Fetching package: {}", extractPackage);

            JCoFunction function = destination.getRepository().getFunction("RODPS_REPL_ODP_FETCH");
            if (function == null) {
                throw new RuntimeException("Function RODPS_REPL_ODP_FETCH not found in SAP.");
            }
            function.getImportParameterList().setValue("I_POINTER", pointer);
            function.getImportParameterList().setValue("I_PACKAGE", extractPackage);
            function.getImportParameterList().setValue("I_REDO", "X");
            function.execute(destination);
            JCoTable table = function.getTableParameterList().getTable("ET_DATA");
            if (table.isEmpty()) {
                break;
            }
            List<byte[]> fragments = new ArrayList<>();
            do {
                for (JCoField field : table) {
                    if ("DATA".equals(field.getName())) {
                        fragments.add(field.getByteArray());
                        break;
                    }
                }
            } while (table.nextRow());
            if (!fragments.isEmpty()) {
                LOG.info("Got {} fragment(s) for package {}", fragments.size(), extractPackage);
                List<byte[]> rows = mergeFragments(fragments, numOfFragment);
                LOG.info("as {} row(s)", rows.size());
            } else {
                LOG.warn("No data found for package {}", extractPackage);
            }
        }
        closeExtractionSession(pointer);
    }

    private List<byte[]> fetchODP(String pointer, String extractPackage) throws JCoException {
        List<byte[]> fragments = new ArrayList<>();
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
                        fragments.add(field.getByteArray());
                        break;
                    }
                }
            } while (table.nextRow());
        }
        return fragments;
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
        List<byte[]> fragments = fetchODP(pointer, "");
        closeExtractionSession(pointer);
        return fragments;
    }

    public static List<byte[]> mergeFragments(List<byte[]> fragments, int n) {
        List<byte[]> rows = new ArrayList<>();
        int size = fragments.size();
        for (int i = 0; i < size; i += n) {
            int end = Math.min(i + n, size);
            int totalLength = 0;
            for (int j = i; j < end; j++) {
                totalLength += fragments.get(j).length;
            }
            byte[] row = new byte[totalLength];
            int offset = 0;
            for (int j = i; j < end; j ++) {
                byte[] currentArray = fragments.get(j);
                System.arraycopy(currentArray, 0, row, offset, currentArray.length);
                offset += currentArray.length;
            }
            rows.add(row);
        }
        return rows;
    }

    private static void run(String action, CommandLine cmd) {
        try {
            ODPWrapper odpWrapper = new ODPWrapper(DestinationConcept.SomeSampleDestinations.ABAP_AS1);
            List<FieldMeta> fieldMetas;
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
                    fieldMetas = details.getThird();
                    LOG.info("fields - [{}]", fieldMetas.size());
                    fieldMetas.forEach(x -> LOG.info("{}", x));
                    return;
                case "ListCursors":
                    assert cmd.hasOption("subscriberType") &&
                            cmd.hasOption("subscriberName") &&
                            cmd.hasOption("contextName") &&
                            cmd.hasOption("odpNamePattern") &&
                            cmd.hasOption("extractMode");
                    List<Map<String, String>> odpCursors = odpWrapper.getODPCursors(
                            cmd.getOptionValue("subscriberType"),
                            cmd.getOptionValue("subscriberName"),
                            cmd.getOptionValue("contextName"),
                            cmd.getOptionValue("odpNamePattern"),
                            cmd.getOptionValue("extractMode"));
                    LOG.info("cursors - {}", odpCursors.size());
                    odpCursors.forEach(x -> LOG.info("{}", x));
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
                    fieldMetas = odpWrapper.getODPDetails(
                            cmd.getOptionValue("subscriberType"),
                            cmd.getOptionValue("contextName"),
                            odpName).getThird();
                    int numOfFragment = getNumOfFragment(fieldMetas);
                    ODPParser odpParser = new ODPParser(odpName, fieldMetas);
                    List<byte[]> fragments = odpWrapper.fetchODP(
                            cmd.getOptionValue("subscriberType"),
                            cmd.getOptionValue("subscriberName"),
                            cmd.getOptionValue("subscriberProcess"),
                            cmd.getOptionValue("contextName"),
                            odpName,
                            cmd.getOptionValue("extractMode"));
                    if (!fragments.isEmpty()) {
                        LOG.info("got {} fragment(s)", fragments.size());
                        List<byte[]> rows = mergeFragments(fragments, numOfFragment);
                        LOG.info("as {} row(s)", rows.size());
                        for (byte[] row : rows) {
                            if (LOG.isDebugEnabled()) {
                                HexDump.hexDump(row);
                            }
                            LOG.info("row - {}", odpParser.parseRow2Json(row));
                        }
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