package xo.hbase;

import com.google.protobuf.InvalidProtocolBufferException;
import xo.protobuf.EntryProto;
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

    private static final String SINK_KAFKA_BOOTSTRAP_SERVERS = "sink.kafka.bootstrap.servers";
    private static final String SINK_KAFKA_BATCH_SIZE = "sink.kafka.batch.size";
    private static final String SINK_KAFKA_REQUEST_TIMEOUT_MS = "sink.kafka.request.timeout.ms";
    private static final String SINK_KAFKA_RETRIES = "sink.kafka.retries";
    private static final String SINK_KAFKA_RETRY_BACKOFF_MS = "sink.kafka.retry.backoff.ms";
    private static final String SINK_KAFKA_TRANSACTION_TIMEOUT_MS = "sink.kafka.transaction.timeout.ms";
    private static final String SINK_KAFKA_SECURITY_PROTOCOL = "sink.kafka.security.protocol";
    private static final String SINK_KAFKA_TOPIC_TABLE_MAP = "sink.kafka.topic-table-map";
    private static final String TABLE_MAP_DELIMITER = ":";

    private final Producer<byte[], byte[]> producer;
    private Map<String, String> tableMap;

    private Properties getProducerProperties(Properties properties) {
        Properties pp = new Properties();
        pp.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, properties.getProperty(SINK_KAFKA_BOOTSTRAP_SERVERS));
        pp.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        pp.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        pp.put(ProducerConfig.BATCH_SIZE_CONFIG, properties.getProperty(SINK_KAFKA_BATCH_SIZE));
        pp.put(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG, properties.getProperty(SINK_KAFKA_TRANSACTION_TIMEOUT_MS));
        pp.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, properties.getProperty(SINK_KAFKA_REQUEST_TIMEOUT_MS));
        pp.put(ProducerConfig.RETRIES_CONFIG, properties.getProperty(SINK_KAFKA_RETRIES));
        pp.put(ProducerConfig.RETRY_BACKOFF_MS_CONFIG, properties.getProperty(SINK_KAFKA_RETRY_BACKOFF_MS));
        pp.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, properties.getProperty(SINK_KAFKA_SECURITY_PROTOCOL));
        return pp;
    }

    private void createTableMap(Properties properties) {
        this.tableMap = new HashMap<>();
        String[] mappings = StringUtils.getStrings(properties.getProperty(SINK_KAFKA_TOPIC_TABLE_MAP));
        for (String mapping: mappings) {
            String[] s = mapping.split(TABLE_MAP_DELIMITER);
            tableMap.put(s[0], s[1]);
        }
        LOG.info("table map: " + tableMap.toString());
    }

    public KafkaSink(Properties properties) {
        super(properties);
        Properties props = getProducerProperties(properties);
        this.producer = new KafkaProducer<>(props);
        createTableMap(properties);
    }

    @Override
    public void put(List<AdminProtos.WALEntry> entryProtos, CellScanner cellScanner) {
        List<WAL.Entry> entries = merge(entryProtos, cellScanner);
        for (WAL.Entry entry: entries) {
            // use '.' to replace ':' as table with namespace
            String tableName = tableMap.get(entry.getKey().getTableName().getNameAsString()
                    .replace(TABLE_MAP_DELIMITER, "."));
            EntryProto.Key keyProto = ProtoBuf.key2Proto(entry.getKey());
            EntryProto.Edit editProto = ProtoBuf.edit2Proto(entry.getEdit());
            ProducerRecord<byte[], byte[]> record =
                    new ProducerRecord<>(tableName, keyProto.toByteArray(), editProto.toByteArray());
            Future<RecordMetadata> result = producer.send(record);
            RecordMetadata meta = null;
            try {
                meta = result.get();
                LOG.info(EntryProto.Key.parseFrom(record.key()).toString());
            } catch (InterruptedException | ExecutionException | InvalidProtocolBufferException e) {
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
