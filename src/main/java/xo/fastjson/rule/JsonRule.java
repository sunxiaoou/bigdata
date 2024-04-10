package xo.fastjson.rule;

//import com.I2information.i2soft.replicator.applier.kafka.MsgConstants;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.annotation.JSONField;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class JsonRule {
    private static final Logger LOG = LoggerFactory.getLogger(JsonRule.class);

    private String uuid;
    private String name;
    private String maptype;
    private String tgtType;
    private String tgtTypeMode;
    private Map<String, String> src;
    private Map<String, String> dst;
    private Map<String, Object> source;
    private Map<String, Object> target;
    private Map<String, String> usermap;
    private Map<String, Object> rpcServer;

    private List<Map<String, Object>> tabmap;
    private List<Map<String, Object>> colmap;

    private Map<String ,Object> dmltrack;
    private List<String> excludeTab;

    private Integer kafkaTimeout;
    private int keepDynData;

    @JSONField(name = "json_template")
    private String jsonTemplate;

    @JSONField(name = "dbmap_topic")
    private String kafkaTopic;

    @JSONField(name = "partLoadBalance")
    private String kafkaBalance;

    private String jsonBinaryCode;

    @JSONField(name = "dumpThd")
    private Integer dumpThreads;

    @JSONField(name = "srcType")
    private String sourceDbType;

    @JSONField(name = "full_sync")
    private int fullSynEnabled;

    @JSONField(name = "lsn")
    private String lsn;

    private List<TargetAddColumns> targetAddColumns;

    @JSONField(name = "bw_limit")
    private String bwLimit;

    private LoadFailedOption lderrset;
    private Map<String, String>            ldConflict;
    private Map<String, DmlConflictOption> dmlConflictOption = new HashMap<>();
    private String                         ldConflictInfo    = null;
    private Map<String, DdlFilterOption> actSyncEnable;
    private String rawrule;


    public enum LoadFailedOption {
        CONTINUE("continue"),
        STOPTABLD("stoptabld"),
        STOPLD("stopld");

        private final String option;
        LoadFailedOption(String option) {
            this.option = option;
        }
        public String toString() {
            return option;
        }
        private static final Map<String, LoadFailedOption> fields =
                Stream.of(values()).collect(Collectors.toMap(Object::toString, e -> e));
        public LoadFailedOption from(String option) {
            return fields.get(option);
        }
    }

    public enum DmlConflictOption {
        IRPAFTERDEL("irpafterdel"),
        TOURP("tourp"),
        CHOOSE_BIGGER("choose_bigger"),
        CHOOSE_SMALLER("choose_smaller"),
        EXEC_PROC("sp"),
        TOIRP("toirp"),
        IGNORE("ignore"),
        ERR("err");
        private final String option;
        DmlConflictOption(String option) {
            this.option = option;
        }
        public String toString() {
            return option;
        }
        private static final Map<String, DmlConflictOption> fields =
                Stream.of(values()).collect(Collectors.toMap(Object::toString, e -> e));
        public static DmlConflictOption from(String option) {
            return fields.get(option);
        }
    }

    public enum DdlFilterOption {
        DDLFILTER_FILTERED(0),
        DDLFILTER_PASSON(1);
        private final int option;
        DdlFilterOption(int option) {
            this.option = option;
        }
        public String toString() {
            return String.valueOf(option);
        }
        private static final Map<String, DdlFilterOption> fields =
                Stream.of(values()).collect(Collectors.toMap(Object::toString, e -> e));
        public DdlFilterOption from(DdlFilterOption option) {
            return fields.get(option);
        }
    }

    public LoadFailedOption getLderrset() {
        return lderrset;
    }

    public void setLderrset(LoadFailedOption lderrset) {
        this.lderrset = lderrset;
    }

    public Map<String, DmlConflictOption> getDmlConflictOption() {
        return dmlConflictOption;
    }

    public void setLdConflict(Map<String, String> ldConflict) {
        this.ldConflict = ldConflict;
        this.dmlConflictOption.put("irp", DmlConflictOption.from(ldConflict.get("irp")));
        this.dmlConflictOption.put("urp", DmlConflictOption.from(ldConflict.get("urp")));
        this.dmlConflictOption.put("drp", DmlConflictOption.from(ldConflict.get("drp")));
        DmlConflictOption irpOption = this.dmlConflictOption.get("irp");
        if (irpOption == DmlConflictOption.CHOOSE_BIGGER
                || irpOption == DmlConflictOption.CHOOSE_SMALLER
                || irpOption == DmlConflictOption.EXEC_PROC) {
            this.ldConflictInfo = ldConflict.get("info");
        }
    }

    public String getLdConflictInfo() {
        return this.ldConflictInfo;
    }

    public Map<String, DdlFilterOption> getActSyncEnable(){
        return actSyncEnable;
    }
    public void setActSyncEnable(Map<String, DdlFilterOption> actSyncEnable){
        this.actSyncEnable = actSyncEnable;
    }
    public String getRawrule() {
        return rawrule;
    }

    public void setRawrule(String rawrule) {
        this.rawrule = rawrule;
    }

    public String getUuid() {
        return uuid;
    }

    public void setUuid(String uuid) {
        this.uuid = uuid;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getMaptype() {
        return maptype;
    }

    public void setMaptype(String maptype) {
        this.maptype = maptype;
    }

    public String getTgtType() {
        return tgtType;
    }

    public void setTgtType(String tgtType) {
        this.tgtType = tgtType;
    }

    public String getTgtTypeMode() {
        return tgtTypeMode;
    }

    public void setTgtTypeMode(String tgtTypeMode) {
        this.tgtTypeMode = tgtTypeMode;
    }

    public Map<String, String> getSrc() {
        return src;
    }

    public void setSrc(Map<String, String> src) {
        this.src = src;
    }

    public Map<String, String> getDst() {
        return dst;
    }

    public void setDst(Map<String, String> dst) {
        this.dst = dst;
    }

    public Map<String, Object> getTarget() {
        return target;
    }

    public void setTarget(Map<String, Object> target) {
        this.target = target;
    }

    public Map<String, Object> getSource() {
        return source;
    }

    public void setSource(Map<String, Object> source) {
        this.source = source;
    }

    public Map<String, Object> getRpcServer() {
        return rpcServer;
    }

    public void setRpcServer(Map<String, Object> rpcServer) {
        this.rpcServer = rpcServer;
    }

    public Map<String, String> getUsermap() {
        return usermap;
    }

    public void setUsermap(Map<String, String> usermap) {
        this.usermap = usermap;
    }

    public List<Map<String, Object>> getTabmap() {
        return tabmap;
    }

    public void setTabmap(List<Map<String, Object>> tabmap) {
        this.tabmap = tabmap;
    }
    
    
    public List<Map<String, Object>> getColmap() {
        return colmap;
    }

    public void setColmap(List<Map<String, Object>> colmap) {
        this.colmap = colmap;
    }

    public int getKeepDynData() {
        return keepDynData;
    }

    public void setKeepDynData(int keepDynData) {
        this.keepDynData = keepDynData;
    }

    public Integer getKafkaTimeout() {
        return kafkaTimeout;
    }

    public void setKafkaTimeout(Integer kafkaTimeout) {
        this.kafkaTimeout = kafkaTimeout;
    }

    public String getKafkaTopic() {
        return kafkaTopic;
    }

    public void setKafkaTopic(String kafkaTopic) {
        this.kafkaTopic = kafkaTopic;
    }

    public String getKafkaBalance() {
        return kafkaBalance;
    }

    public void setKafkaBalance(String kafkaBalance) {
        this.kafkaBalance = kafkaBalance;
    }

    public String getJsonBinaryCode() {
        return jsonBinaryCode;
    }

    public void setJsonBinaryCode(String jsonBinaryCode) {
        this.jsonBinaryCode = jsonBinaryCode;
    }

    public Integer getDumpThreads() {
        return dumpThreads;
    }

    public void setDumpThreads(Integer dumpThreads) {
        this.dumpThreads = dumpThreads;
    }

    public String getSourceDbType() {
        return sourceDbType;
    }

    public void setSourceDbType(String v) {
        this.sourceDbType = v;
    }

    public boolean isFullSyncEnabled() {
        return (this.fullSynEnabled==1);
    }

    public void setFullSynEnabled(Integer v) {
        this.fullSynEnabled = v;
    }

    public String getLsn() {
        return lsn;
    }

    public void setLsn(String v) {
        this.lsn = v;
    }

    public String getBwLimit() {
        return bwLimit;
    }

    public void setBwLimit(String bwLimit) {
        this.bwLimit = bwLimit;
    }

    public List<String> getExcludeTab() {
        return excludeTab;
    }

    public void setExcludeTab(List<String> excludeTab) {
        this.excludeTab = excludeTab;
    }

    public Map<String, Object> getDmltrack() {
        return dmltrack;
    }

    public void setDmltrack(Map<String, Object> dml_track) {
        this.dmltrack = dml_track;
    }

    public List<TargetAddColumns> getTargetAddColumns() {
        return targetAddColumns;
    }

    public void setTargetAddColumns(List<TargetAddColumns> targetAddColumns) {
        this.targetAddColumns = targetAddColumns;
    }

    public JSONObject getRdmsParams(DBNode dbConfig) {
        JSONObject rdmsParams = new JSONObject();

        rdmsParams.put("sdbtype", "db2");
        rdmsParams.put("tgtType", tgtType);
        if (StringUtils.isNotBlank(tgtTypeMode)) {
            rdmsParams.put("tgtTypeMode", tgtTypeMode);
        }

        JSONObject targetParams = new JSONObject();
        targetParams.put("dstIP", dbConfig.getIp());
        targetParams.put("port", dbConfig.getPort());
        targetParams.put("dbname", dbConfig.getInstance());
        targetParams.put("user", dbConfig.getUser());
        targetParams.put("pass", dbConfig.getPassword());

        rdmsParams.put("target", targetParams);

        int maptypeValue = MsgConstants.getMapType(maptype);

        rdmsParams.put("rule.uuid", uuid);
        rdmsParams.put("maptype", maptypeValue);

        addMapParamsDriect(rdmsParams, maptypeValue);

        return rdmsParams;
    }

    public JSONObject getKafkaParams() {
        JSONObject kafkaParams = new JSONObject();

        int maptypeValue = MsgConstants.getMapType(maptype);

        kafkaParams.put("msg.version", MsgConstants.MSG_VERSION);
        kafkaParams.put("rule.uuid", uuid);
        kafkaParams.put("maptype", maptypeValue);
        kafkaParams.put("dbmap_topic", kafkaTopic);
        kafkaParams.put("partLoadBalance", StringUtils.isBlank(kafkaBalance) ? "by_table" : kafkaBalance);
        kafkaParams.put("kafkaTimeout", kafkaTimeout);

        kafkaParams.put("broker.servers", target.get("broker_servers"));
        kafkaParams.put("kafka_auth_type", target.get("kafka_auth_type"));
        kafkaParams.put("sasl_plain_pass", target.get("sasl_plain_pass"));
        kafkaParams.put("sasl_plain_user", target.get("sasl_plain_user"));
        kafkaParams.put("kerberos_principal", target.get("kerberos_principal"));
        kafkaParams.put("kerberos_keytab_path", target.get("kerberos_keytab_path"));
        kafkaParams.put("kerberos_service_name", target.get("kerberos_service_name"));

        addMapParams(kafkaParams, maptypeValue);

        return kafkaParams;
    }

    private JSONObject addMapParamsDriect(JSONObject rootObject, int maptypeValue) {
        if (MsgConstants.MAP_TAB == maptypeValue) {
            rootObject.put("tabmap", JSONArray.parseArray(JSON.toJSONString(tabmap)));
        } else if (MsgConstants.MAP_USER == maptypeValue) {
            rootObject.put("usermap", JSONObject.parseObject(JSON.toJSONString(usermap)));
        }
        return rootObject;
    }

    private JSONObject addMapParams(JSONObject rootObject, int maptypeValue) {
        if (MsgConstants.MAP_TAB == maptypeValue) {
            JSONArray tableMapArray = new JSONArray(tabmap.size());

            for (Map<String, Object> mapItem : tabmap) {
                addTableObject(tableMapArray, mapItem);
            }

            rootObject.put("tablemap", tableMapArray);
        } else if (MsgConstants.MAP_USER == maptypeValue) {
            JSONArray userMapArray = new JSONArray(usermap.size());

            for (Map.Entry<String, String> entry : usermap.entrySet()) {
                JSONObject userMap = new JSONObject();
                userMap.put("user", entry.getKey());
                userMap.put("topic", entry.getValue());
                userMapArray.add(userMap);
            }

            rootObject.put("usermap", userMapArray);
        }
        return rootObject;
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private void addTableObject(JSONArray tableMapArray, Map<String, Object> mapItem) {
        JSONObject tableObject = new JSONObject();

        Map<String, String> userMap = (Map<String, String>)mapItem.get("user");
        Map<String, String> tabMap = (Map<String, String>)mapItem.get("tab");

        if (userMap.isEmpty() || tabMap.isEmpty()) {
            return;
        }

        Iterator iter = userMap.entrySet().iterator();
        Map.Entry entry = (Map.Entry) iter.next();
        tableObject.put("owner", entry.getKey());
        tableObject.put("topic", entry.getValue());

        iter = tabMap.entrySet().iterator();
        entry = (Map.Entry) iter.next();
        tableObject.put("tab", entry.getKey());

        tableMapArray.add(tableObject);
    }

    public String getJsonTemplate() {
        return jsonTemplate;
    }

    public void setJsonTemplate(String jsonTemplate) {
        this.jsonTemplate = jsonTemplate;
    }
}
