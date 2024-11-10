package xo.hbase;

import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.util.Triple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
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

    private static void run(String op, String host, String table) throws IOException {
        // use resources/hbase-site.xml as host is null
        HBase db = host == null ? new HBase(): new HBase(host, 2181, "/hbase");
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
                System.out.println(String.format("%s has %d rows", name, db.countTableRows(name)));
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
        if (args.length > 2) {
            run(args[0], args[1], args[2]);
        } else if (args.length > 1) {
            run(args[0], args[1], null);
        } else if (args.length > 0) {
            run(args[0], null, null);
        } else {
            System.out.println("Usage: Fruit create|drop|put|add|delete|count|scan|isEmpty|truncate host[,host2,...] table");
        }
    }
}