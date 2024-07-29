package xo.fastjson.rule;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.apache.hbase.thirdparty.com.google.common.collect.HashBasedTable;
import org.apache.hbase.thirdparty.com.google.common.collect.Table;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class RuleTest {
    private static final Logger LOG = LoggerFactory.getLogger(RuleTest.class);

    private static final String json = "target/classes/hb_ht_h2.json";
    private static JsonRule rule = null;

    @BeforeClass
    public static void setupBeforeClass() {
        LOG.info(System.getProperty("user.dir"));
        File file = new File(json);
        StringBuilder sb = new StringBuilder();
        if (file.exists() && file.isFile()) {
            try (FileReader fr = new FileReader(file);
                 BufferedReader br = new BufferedReader(fr)) {
                String tmp;
                while ((tmp = br.readLine()) != null) {
                    sb.append(tmp);
                }
            } catch (IOException e) {
                System.out.println(e.getMessage());
            }
        }
        String strJson = sb.toString().replaceAll("\"dmltrack\":\\s*\\[\\]\\s*,*", "");
        rule = JSONUtils.parseRuleWithDefaultValue(strJson, JsonRule.class);
    }

    private Table<Integer, String, String> getZookeeperTable(JSONObject zookeeper) {
        Table<Integer, String, String> table = HashBasedTable.create();
        JSONArray array = (JSONArray) (zookeeper.get("set"));
        for (int i = 0; i < array.size(); i ++) {
            JSONObject object = array.getJSONObject(i);
            table.put(i, "ip", (String) object.get("ip"));
            table.put(i, "port", String.valueOf(object.get("port")));
            table.put(i, "zk_node", (String) object.get("zk_node"));
        }
        return table;
    }

    private void showZookeeper(Table<Integer, String, String> table) {
//        for (Table.Cell<Integer, String, String> cell : table.cellSet()) {
//            LOG.info("{}->{}({})", cell.getRowKey(), cell.getColumnKey(), cell.getValue());
//        }
        List<String> ips = new ArrayList<>();
        String port = null, zk_node = null;
        for (int rowKey : table.rowKeySet()) {
            for (String columnKey : table.columnKeySet()) {
                String value = table.get(rowKey, columnKey);
                if ("ip".equals(columnKey)) {
                    ips.add(value);
                } else if (rowKey == 0) {
                    if ("port".equals(columnKey)) {
                        port = value;
                    } else if ("zk_node".equals(columnKey)) {
                        zk_node = value;
                    }
                }
            }
        }
        LOG.info("ips({}), port({}), zk_node({})", String.join(",", ips), port, zk_node);
    }

    private void showDb(Map<String, Object> db) {
        String name = (String) db.get("name");
        String type = (String) db.get("type");
        LOG.info("name({}), type({})", name, type);
        JSONArray login = (JSONArray) db.get("login");
        if (login != null && login.size() != 0) {
            String user = login.getJSONObject(0).getString("user");
            String pass = login.getJSONObject(0).getString("pass");
            int version = (int) db.get("version");
            LOG.info("user({}), pass({}), version({})", user, pass, version);
        }
        JSONObject zookeeper = (JSONObject) db.get("zookeeper");
//        Table<Integer, String, String> table = getZookeeperTable(zookeeper);
//        showZookeeper(table);
        JSONArray array = (JSONArray) (zookeeper.get("set"));
        List<String> ips = new ArrayList<>();
        String port = null, zk_node = null;
        for (int i = 0; i < array.size(); i ++) {
            JSONObject object = array.getJSONObject(i);
            ips.add((String) object.get("ip"));
            if (i == 0) {
                port = String.valueOf(object.get("port"));
                zk_node = (String) object.get("zk_node");
            }
        }
        LOG.info("ips({}), port({}), zk_node({})", String.join(",", ips), port, zk_node);
    }

    @Test
    public void showSource() {
        LOG.info("\nshowSource");
        showDb(rule.getSource());
    }

    @Test
    public void showTarget() {
        LOG.info("\nshowTarget");
        showDb(rule.getTarget());
    }

    @Test
    public void showRpcServer() {
        LOG.info("\nshowRpcServer");
        String host = rule.getSrc().get("ip").split(",")[0];
        LOG.info("host({})", host);
        JSONObject zookeeper = (JSONObject) rule.getRpcServer().get("zookeeper");
        Table<Integer, String, String> table = getZookeeperTable(zookeeper);
        showZookeeper(table);
        String peer = (String) rule.getRpcServer().get("peer");
        LOG.info("peer({})", peer);
    }

    @Test
    public void showTabMap() {
        LOG.info("\nshowTabMap");
        if ("table".equals(rule.getMaptype())) {
            Map<String, String> tabMap = new HashMap<>();
            for (Map<String, Object> map : rule.getTabmap()) {
                String u = null, u2 = null, t = null, t2 = null;
                for (Map.Entry<String, Object> entry : map.entrySet()) {
                    String key = entry.getKey();
                    JSONObject value = (JSONObject) entry.getValue();
                    for (Map.Entry<String, Object> innerEntry : value.entrySet()) {
                        if ("user".equals(key)) {
                            u = innerEntry.getKey();
                            u2 = (String) innerEntry.getValue();
                        } else if ("tab".equals(key)) {
                            t = innerEntry.getKey();
                            t2 = (String) innerEntry.getValue();
                        }
                    }
                }
                tabMap.put(u + ":" + t, u2 + ":" + t2);
            }
            LOG.info(String.join(",", tabMap.keySet()));
        }
    }
}
