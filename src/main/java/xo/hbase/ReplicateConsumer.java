package xo.hbase;

import xo.protobuf.EntryProto;
import xo.protobuf.ProtoBuf;

import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.protobuf.ReplicationProtbufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.wal.WAL;
import org.apache.hadoop.hbase.wal.WALEdit;
import org.apache.hadoop.hbase.wal.WALKey;
import org.apache.hadoop.hbase.wal.WALKeyImpl;
import org.apache.hadoop.util.StringUtils;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.*;


public class ReplicateConsumer {
    private static final Logger LOG = LoggerFactory.getLogger(ReplicateConsumer.class);
    private int count = 0;

    private final KafkaConsumer<byte[], byte[]> consumer;
    private final List<String> topics;

    public ReplicateConsumer() {
        ReplicateConfig config = ReplicateConfig.getInstance();
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, config.getKafkaBootstrapServers());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, config.getKafkaGroupId());
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, config.getKafkaEnableAutoCommit());
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, config.getKafkaAutoCommitIntervalMs());
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        this.consumer = new KafkaConsumer<>(props);
        this.topics = Arrays.asList(StringUtils.getStrings(config.getKafkaTopics()));
        LOG.info("topic list:" + topics);
        consumer.subscribe(topics);
    }

    private List<WAL.Entry> poll() throws IOException {
        ConsumerRecords<byte[], byte[]> records = consumer.poll(Duration.ofMillis(100));
        List<WAL.Entry> entries = new ArrayList<>();
        for (ConsumerRecord<byte[], byte[]> record : records) {
            Long offset = record.offset();
            LOG.debug("offset({})", offset);
            WALKey key = ProtoBuf.proto2Key(EntryProto.Key.parseFrom(record.key()));
            WALEdit edit = ProtoBuf.proto2Edit(EntryProto.Edit.parseFrom(record.value()));
            WAL.Entry entry = new WAL.Entry((WALKeyImpl) key, edit);
            if (LOG.isDebugEnabled()) {
                LOG.debug("entry: " + entry.toString());
            } else {
                LOG.info("sequenceId({})", key.getSequenceId());
            }
            entries.add(entry);
            count ++;
        }
        return entries;
    }

    private void put(List<WAL.Entry> entries, HBaseSink sink) {
        try {
            WAL.Entry[] arr = new WAL.Entry[entries.size()];
            Pair<AdminProtos.ReplicateWALEntryRequest, CellScanner> pair =
                    ReplicationProtbufUtil.buildReplicateWALEntryRequest(
                            entries.toArray(arr),
                            null,
                            HConstants.CLUSTER_ID_DEFAULT,
                            null,
                            null);
            sink.put(pair.getFirst().getEntryList(), pair.getSecond());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) throws IOException {
        ReplicateConsumer consumer = new ReplicateConsumer();
        HBaseSink sink = new HBaseSink(ReplicateConfig.getInstance());

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            LOG.info("Total records processed: " + consumer.count);
        }));

        try {
            List<WAL.Entry> entries;
            while (true) {
                entries = consumer.poll();
                consumer.put(entries, sink);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
