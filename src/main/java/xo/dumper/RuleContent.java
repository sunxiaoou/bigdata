package xo.dumper;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import xo.fastjson.rule.JSONUtils;
import xo.fastjson.rule.JsonRule;
import xo.hbase.HBase;

import java.io.IOException;
import java.util.*;
import java.util.regex.Pattern;

public class RuleContent {
    private static final Logger LOG = LoggerFactory.getLogger(RuleContent.class);

    private final JsonRule rule;

    public RuleContent(String ruleJson) {
        String strJson = ruleJson.replaceAll("\"dmltrack\":\\s*\\[\\]\\s*,*", "");
        this.rule = JSONUtils.parseRuleWithDefaultValue(strJson, JsonRule.class);
    }

    private List<String> getDb(Map<String, Object> db) {
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
        return Arrays.asList(String.join(",", ips), port, zk_node);
    }

    public List<String> checkRule() throws IOException {
        List<String> db = getDb(rule.getSource());
        LOG.info("ips({}), port({}), zk_node({})", db.get(0), db.get(1), db.get(2));
        HBase srcDb = new HBase(db.get(0), Integer.parseInt(db.get(1)), db.get(2));
        List<String> tables = new ArrayList<>();
        String mapType = rule.getMaptype();
        if ("table".equals(mapType)) {
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
                tables.add(u + ":" + t);
            }
        } else if ("user".equals(mapType)) {
            for (String space: rule.getUsermap().keySet()) {
                tables.addAll(srcDb.listTables(space));
            }
        } else {
            tables = srcDb.listTables(Pattern.compile(".*"));
        }
        srcDb.close();
        return tables;
    }

    public static void main(String[] args) throws IOException {
//        final String path = "hb_ht_h2.json";
        final String path = "hb_u_c1_user.json";
        String rule = Utils.readFile(
                Objects.requireNonNull(BackupClient.class.getClassLoader().getResource(path)).getPath());
        assert rule != null;
        RuleContent content = new RuleContent(rule);
        List<String> tables = content.checkRule();
        LOG.info(tables.toString());
    }
}
