package xo.hbase;

import org.apache.commons.cli.*;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.util.Triple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

public class Fruit {
    private static final Logger LOG = LoggerFactory.getLogger(Fruit.class);

    /**
     * Convert triple of a fruit row to a pair
     * @param triple <row_number, name_value, price_value>
     * @return a Pair - (row_string, {"cf": {"name": name_string, "price": price_string}})
     */
    public static Pair<String, Map<String, Map<String, String>>> fruit(Triple<Integer, String, Float> triple) {
        Map<String, Map<String, String>> families = new HashMap<>();
        Map<String, String> family = new TreeMap<>();
        family.put("name", triple.getSecond());
        family.put("price", String.valueOf(triple.getThird()));
        families.put("cf", family);
        return new Pair<>(String.valueOf(triple.getFirst()), families);
    }

    /**
     * Convert a triple array to map
     * @return a Map - {row_string: {"cf": {"name": name_string, "price": price_string}}}
     */
    public static Map<String, Map<String, Map<String, String>>> fruits() {
        final Triple<Integer, String, Float>[] triples = new Triple[] {
                new Triple<>(101, "🍉", (float) 800.0),
                new Triple<>(102, "🍓", (float) 150.0),
                new Triple<>(103, "🍎", (float) 120.0),
                new Triple<>(104, "🍋", (float) 200.0),
                new Triple<>(105, "🍊", (float) 115.0),
                new Triple<>(106, "🍌", (float) 110.0)
        };
        Map<String, Map<String, Map<String, String>>> rows = new HashMap<>();
        for (Triple<Integer, String, Float> triple : triples) {
            Pair<String, Map<String, Map<String, String>>> pair = fruit(triple);
            rows.put(pair.getFirst(), pair.getSecond());
        }
        LOG.debug("fruits: {}", rows);
        return rows;
    }

    private static void run(String op, String host, String table, String principal, String keytab) throws IOException {
        HBase db;
        if (host == null) {
            // use hadoop/hbase XMLs in classpath
            db = new HBase();
        } else if (Paths.get(host).toFile().exists()) {
            // use hadoop/hbase XMLs in the specified directory
            db = new HBase(host, principal, keytab);
        } else {
            // use the specified host
            db = new HBase(host, 2181, "/hbase");
        }
        String regex = "^manga:fruit.*";
        String name = "manga:fruit";
        if (table == null) {
            System.out.println(db.listNameSpaces());
            System.out.println(db.listTables("manga"));
        } else {
            name = table;
        }
        switch (op) {
            case "create":
                if (name.matches(regex)) {
                    Map<String, Object> cf = new HashMap<>();
                    cf.put("blockCache", true);
                    cf.put("blockSize", 65536);
                    cf.put("bloomFilter", "ROW");
                    cf.put("compression", "NONE");
                    cf.put("dataBlockEncoding", "NONE");
                    cf.put("inMemory", false);
                    cf.put("keepDeleteCells", "FALSE");
                    cf.put("minVersions", 0);
                    cf.put("scope", 1);
                    cf.put("ttl", 2147483647);
                    cf.put("versions", 1);
                    Map<String, Object> cfMap = new HashMap<>();
                    cfMap.put("cf", cf);
                    db.createTable(table, cfMap, true);
                    System.out.println(name + " created");
                } else {
                    System.out.println("Can only create \"manga:fruit\"");
                }
                return;
            case "drop":
                db.dropTable(name);
                System.out.println(name + " dropped");
                return;
            case "put":
                if (name.matches(regex)) {
                    db.putRows(name, fruits());
                    System.out.println(name + " put");
                } else {
                    System.out.println("Can only put to \"manga:fruit\"");
                }
                return;
            case "add":
                if (name.matches(regex)) {
                    db.putRow(name, fruit(new Triple<>(107, "🍐", (float) 115)));
                    System.out.println(name + " add");
                } else {
                    System.out.println("Can only add to \"manga:fruit\"");
                }
                return;
            case "delete":
                if (name.matches(regex)) {
                    db.deleteRow(name, "107");
                    System.out.println(name + " deleted");
                } else {
                    System.out.println("Can only delete from \"manga:fruit\"");
                }
                return;
            case "count":
                System.out.printf("%s has %d rows%n", name, db.countTableRows(name));
                return;
            case "scan":
                if (name.matches(regex)) {
                    System.out.println(db.scanTable(name));
                    System.out.println(name + " scanned");
                } else {
                    System.out.println("Can only scan \"manga:fruit\"");
                }
                return;
            case "isEmpty":
                System.out.println(name + (db.isTableEmpty(name) ? " is empty" : " is not empty"));
                return;
            case "truncate":
                db.truncateTable(name);
                System.out.println(name + " truncated");
                return;
            default:
                System.out.println("Unknown op: " + op);
        }
        db.close();
    }

    public static void main(String[] args) throws IOException {
        Options options = new Options();
        options.addOption(Option.builder("a")
                .longOpt("action")
                .hasArg()
                .required()
                .desc("Action (create|drop|put|add|delete|count|scan|isEmpty|truncate)").build());
        options.addOption(Option.builder("h")
                .longOpt("host")
                .hasArg()
                .desc("HBase host configuration (hostname or config path)").build());
        options.addOption(Option.builder("t")
                .longOpt("table")
                .hasArg()
                .desc("Table name").build());
        options.addOption(Option.builder("p")
                .longOpt("principal")
                .hasArg()
                .desc("Principal name").build());
        options.addOption(Option.builder("k")
                .longOpt("keytab")
                .hasArg()
                .desc("keytab path").build());
        options.addOption(Option.builder("?")
                .longOpt("help")
                .desc("Show help").build());

        CommandLineParser parser = new DefaultParser();
        HelpFormatter formatter = new HelpFormatter();
        CommandLine cmd;

        try {
            cmd = parser.parse(options, args);
        } catch (ParseException e) {
            System.err.println("Error: " + e.getMessage());
            formatter.printHelp("Fruit", options);
            System.exit(1);
            return;
        }

        // 处理帮助选项
        if (cmd.hasOption("help")) {
            formatter.printHelp("Fruit", options);
            System.exit(0);
        }

        // 提取参数并运行
        String op = cmd.getOptionValue("action");
        String host = cmd.getOptionValue("host");
        String table = cmd.getOptionValue("table");
        String principal = cmd.getOptionValue("principal");
        String keytab = cmd.getOptionValue("keytab");
        run(op, host, table, principal, keytab);
    }
}