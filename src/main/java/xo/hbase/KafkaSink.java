package xo.hbase;

import org.apache.hadoop.hbase.util.Bytes;
import xo.fastjson.JsonUtil;
import xo.protobuf.ProtoBuf;

import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos;
import org.apache.hadoop.hbase.wal.WAL;
import org.apache.hadoop.util.StringUtils;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class KafkaSink extends AbstractSink {
    private static final Logger LOG = LoggerFactory.getLogger(KafkaSink.class);
    private final Producer<byte[], byte[]> producer;
    private final boolean isJson;
    private static final String TABLE_MAP_DELIMITER = ":";
    private final Map<String, String> tableMap;

    public KafkaSink(ReplicateConfig config) {
        super(config);
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, config.getSinkKafkaBootstrapServers());
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        properties.put(ProducerConfig.BATCH_SIZE_CONFIG, config.getSinkKafkaBatchSize());
        properties.put(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG, config.getSinkKafkaTransactionTimeoutMs());
        properties.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, config.getSinkKafkaRequestTimeoutMs());
        properties.put(ProducerConfig.RETRIES_CONFIG, config.getSinkKafkaRetries());
        properties.put(ProducerConfig.RETRY_BACKOFF_MS_CONFIG, config.getSinkKafkaRetryBackoffMs());
        properties.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, config.getSinkKafkaSecurityProtocol());
        this.producer = new KafkaProducer<>(properties);
        this.isJson = "json".equals(config.getSinkKafkaSerializer());
        LOG.info("serializer is " + (isJson ? "json": "protoBuf"));
        this.tableMap = new HashMap<>();
        String[] mappings = StringUtils.getStrings(config.getSinkKafkaTopicTableMap());
        for (String mapping: mappings) {
            String[] s = mapping.split(TABLE_MAP_DELIMITER);
            tableMap.put(s[0], s[1]);
        }
        LOG.info("table map: " + tableMap.toString());
    }

    @Override
    public void put(List<AdminProtos.WALEntry> entryProtos, CellScanner cellScanner) {
        List<WAL.Entry> entries = merge(entryProtos, cellScanner);
        for (WAL.Entry entry: entries) {
            // use '.' to replace ':' as table with namespace
            String tableName = tableMap.get(entry.getKey().getTableName().getNameAsString()
                    .replace(TABLE_MAP_DELIMITER, "."));
            byte[] key = isJson ? Bytes.toBytes(JsonUtil.key2Json(entry.getKey())):
                    ProtoBuf.key2Proto(entry.getKey()).toByteArray();
            byte[] edit = isJson ? Bytes.toBytes(JsonUtil.edit2Json(entry.getEdit())):
                    ProtoBuf.edit2Proto(entry.getEdit()).toByteArray();
            ProducerRecord<byte[], byte[]> record = new ProducerRecord<>(tableName, key, edit);
            Future<RecordMetadata> result = producer.send(record);
            try {
                RecordMetadata meta = result.get();
                if (LOG.isDebugEnabled()) {
                    LOG.debug("entry: " + entry.toString());
                } else {
                    LOG.info("sequenceId({})", entry.getKey().getSequenceId());
                }
            } catch (InterruptedException | ExecutionException e) {
                e.printStackTrace();
            }
        }
    }

    @Override
    public void flush() {
        producer.flush();
    }

    @Override
    public List<WAL.Entry> filter(List<WAL.Entry> filter) {
        return null;
    }
}
