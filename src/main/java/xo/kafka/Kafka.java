package xo.kafka;

import org.apache.hadoop.hbase.util.Triple;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class Kafka {
    private static final Logger LOG = LoggerFactory.getLogger(Kafka.class);

    private static Properties loadProperties(String fileName) {
        Properties properties = new Properties();
        try (InputStream inputStream = Kafka.class.getClassLoader().getResourceAsStream(fileName)) {
            if (inputStream != null) {
                properties.load(inputStream);
            } else {
                throw new IOException("Unable to load the properties file: " + fileName);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return properties;
    }

    static class KfkAdmin {
        AdminClient admin;

        KfkAdmin(String host, int port) {
            Properties props = new Properties();
            props.put("bootstrap.servers", String.format("%s:%d", host, port));
            admin = AdminClient.create(props);
        }

       List<String> topics() {
            ListTopicsOptions options = new ListTopicsOptions().listInternal(true); // Include internal topics
            ListTopicsResult topics = admin.listTopics(options);
            Set<String> topicNames;
            try {
                topicNames = topics.names().get();
            } catch (InterruptedException | ExecutionException e) {
                e.printStackTrace();
                return null;
            }
            return new ArrayList<>(topicNames);
        }

        String topicDescription(String name) {
            DescribeTopicsResult result = admin.describeTopics(Collections.singleton(name));
            TopicDescription desc;
            try {
                desc = result.topicNameValues().get(name).get();
            } catch (UnknownTopicOrPartitionException | ExecutionException e) {
                return null;
            } catch (InterruptedException e) {
                e.printStackTrace();
                return null;
            }
            return desc.toString();
        }

        void createTopic(String name, int numPartitions, int replicationFactor) {
            if (topicDescription(name) == null) {
                NewTopic newTopic = new NewTopic(name, numPartitions, (short) replicationFactor);
                KafkaFuture<Void> result = admin.createTopics(Collections.singletonList(newTopic)).all();
                try {
                    result.get();
                } catch (InterruptedException | ExecutionException e) {
                    e.printStackTrace();
                }
                LOG.info("Topic created: " + name);
            }
        }

        void deleteTopic(String name) {
            if (topicDescription(name) != null) {
                DeleteTopicsResult deleteResult = admin.deleteTopics(Collections.singleton(name));
                try {
                    deleteResult.all().get();
                } catch (InterruptedException | ExecutionException e) {
                    e.printStackTrace();
                }
                LOG.info("Topic deleted: " + name);
            }
        }
    }

    static class KfkProducer {
        Producer<String, String> producer;

        KfkProducer(String host, int port) {
            Properties props = new Properties();
            props.put("bootstrap.servers", String.format("%s:%d", host, port));
//            props.put("linger.ms", 1);
            props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
            props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
            producer = new KafkaProducer<>(props);
        }

        void close() {
            producer.close();
        }

        RecordMetadata send(String topic, String key, String value) {
            Future<RecordMetadata> result = producer.send(new ProducerRecord<>(topic, key, value));
            RecordMetadata meta = null;
            try {
                meta = result.get();
            } catch (InterruptedException | ExecutionException e) {
                e.printStackTrace();
            }
            return meta;
        }

        public synchronized void flush() {
            producer.flush();
        }
    }

    static class KfkConsumer {
        KafkaConsumer<String, Object> consumer;

        KfkConsumer(String host, int port, List<String> topics) {
            Properties props = new Properties();
            props.put("bootstrap.servers", String.format("%s:%d", host, port));
            props.put("group.id", "group01");
            props.put("enable.auto.commit", "true");
            props.put("auto.commit.interval.ms", "1000");
            props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
            props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
            consumer = new KafkaConsumer<>(props);
            consumer.subscribe(topics);
        }

        KfkConsumer(String properties, List<String> topics) {
            Properties props = loadProperties(properties);
            consumer = new KafkaConsumer<>(props);
            consumer.subscribe(topics);
        }

        List<Triple<Long, String, Object>> poll(int num) {
            List<Triple<Long, String, Object>> result = new ArrayList<>();
            try {
                while (result.size() < (Math.max(num, 1))) {
                    ConsumerRecords<String, Object> records = consumer.poll(Duration.ofMillis(100));
                    for (ConsumerRecord<String, Object> record : records) {
                        result.add(new Triple<>(record.offset(), record.key(), record.value()));
                    }
                }
            } finally {
                consumer.close();
            }
            return result;
        }
    }

    public static void testProducer(String host, int port) {
        KfkProducer producer = new KfkProducer(host, port);
        for (int i = 0; i < 5; i ++) {
            System.out.println(producer.send("test", "" + (i % 5), Integer.toString(i)));
        }
        producer.flush();
        producer.close();
    }

    public static void testConsumer(String host, int port, String topic) {
        KfkConsumer consumer = new KfkConsumer(host, port, Arrays.asList(topic));
        List<Triple<Long, String, Object>> records = consumer.poll(5);
        for (Triple<Long, String, Object> record: records) {
            System.out.printf("offset = %d, key = %s, value = %s%n",
                    record.getFirst(), record.getSecond(), record.getThird());
        }
    }

    public static void testConsumer2(String properties, String topic) {
        KfkConsumer consumer = new KfkConsumer(properties, Arrays.asList(topic));
        List<Triple<Long, String, Object>> records = consumer.poll(0);
        for (Triple<Long, String, Object> record: records) {
            System.out.println(StandardCharsets.UTF_8.decode((ByteBuffer) record.getThird()).toString());
        }
    }

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        String host = "localhost";
        int port = 9092;
        if (args.length > 0) {
            host = args[0];
        }

        KfkAdmin admin = new KfkAdmin(host, port);
//        admin.createTopic("test", 1, 1);
        System.out.println(admin.topics());
        System.out.println(admin.topicDescription("test"));
//        testProducer(host, port);
//        testConsumer(host, port, "test");
//        testConsumer2("kafka_consumer.properties", "fruit");
//        admin.deleteTopic("test");
    }
}
