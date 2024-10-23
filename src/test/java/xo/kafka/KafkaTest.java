package xo.kafka;

import org.apache.hadoop.hbase.util.Triple;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.*;

public class KafkaTest {
    private static final Logger LOG = LoggerFactory.getLogger(KafkaTest.class);

    String host = "localhost";
    int port = 9092;
    Kafka.KfkAdmin admin;
    String topic = "test";


    @Before
    public void setUp() throws Exception {
        admin = new Kafka.KfkAdmin(host, port);
    }

    @After
    public void tearDown() throws Exception {
    }

    @Test
    public void listTopics() {
        LOG.info("{}", admin.topics());
    }

    @Test
    public void testProducer() {
        Kafka.KfkProducer producer = new Kafka.KfkProducer(host, port);
        for (int i = 0; i < 5; i ++) {
            LOG.info("{}", producer.send("test", "" + (i % 5), Integer.toString(i)));
        }
        producer.flush();
        producer.close();
    }

    @Test
    public void testConsumer() {
        Kafka.KfkConsumer consumer = new Kafka.KfkConsumer(host, port, Arrays.asList(topic));
        List<Triple<Long, String, Object>> records = consumer.poll(5);
        for (Triple<Long, String, Object> record: records) {
            LOG.info("offset({}), key({}), value({})", record.getFirst(), record.getSecond(), record.getThird());
        }
    }

    @Test
    public void testConsumer2() {
    }
}