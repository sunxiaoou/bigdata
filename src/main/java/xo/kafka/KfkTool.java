package xo.kafka;

import org.apache.commons.cli.*;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import xo.utility.HexDump;

import java.util.*;
import java.util.concurrent.ExecutionException;

import java.time.Duration;

public class KfkTool {
    private static final Logger LOG = LoggerFactory.getLogger(KfkTool.class);

    private static final String DEFAULT_HOSTNAME = "localhost";
    private static final String DEFAULT_PORT = "9092";
    private static final Properties producerProps = new Properties();
    private static final Properties consumerProps = new Properties();
    private static final Properties adminProps = new Properties();

    private static void setProperties(String server) {
        // Producer properties
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, server);
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // Consumer properties
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, server);
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "default-consumer-group");
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // Admin client properties
        adminProps.put("bootstrap.servers", server);
    }

    // List all topics
    public static void listTopics() throws ExecutionException, InterruptedException {
        try (AdminClient adminClient = AdminClient.create(adminProps)) {
            LOG.info(adminClient.listTopics().names().get().toString());
        }
    }

    // Show topic description
    public static void showTopic(String topicName) throws ExecutionException, InterruptedException {
        try (AdminClient adminClient = AdminClient.create(adminProps)) {
            DescribeTopicsResult result = adminClient.describeTopics(Collections.singletonList(topicName));
            Map<String, TopicDescription> topicDescriptions = result.all().get();
            topicDescriptions.forEach((name, description) -> {
                LOG.info("Topic: {}", name);
                LOG.info(description.partitions().toString());
            });
        }
    }

    // Create a new topic
    public static void createTopic(String topicName, int numPartitions, short replicationFactor)
            throws InterruptedException {
        try (AdminClient adminClient = AdminClient.create(adminProps)) {
            NewTopic newTopic = new NewTopic(topicName, numPartitions, replicationFactor);
            adminClient.createTopics(Collections.singletonList(newTopic)).all().get();
            LOG.info("Topic({}) created successfully", topicName);
        } catch (ExecutionException | TopicExistsException e) {
            LOG.warn("Topic({}) already exists",  topicName);
        }
    }

    // Delete a topic
    public static void deleteTopic(String topicName) throws InterruptedException {
        try (AdminClient adminClient = AdminClient.create(adminProps)) {
            DeleteTopicsResult result = adminClient.deleteTopics(Collections.singletonList(topicName));
            result.all().get();
            LOG.info("Topic({}) deleted successfully", topicName);
        } catch (ExecutionException | UnknownTopicOrPartitionException e) {
            LOG.warn("Topic({}) doesn't exist",  topicName);
        }
    }

    // Send a record to the topic
    public static void sendRecord(String topicName, String key, String value) {
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(producerProps)) {
            ProducerRecord<String, String> record = new ProducerRecord<>(topicName, key, value);
            producer.send(record);
            LOG.info("Record sent to topic({}) with kv({}, {})",  topicName, key, value);
        }
    }

    // Poll records from the topic
    public static void pollRecords(String topicName, int maxMessages) {
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProps)) {
            consumer.subscribe(Collections.singletonList(topicName), new ConsumerRebalanceListener() {
                @Override
                public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                    // Once partitions are assigned, seek to the beginning of each partition
                    partitions.forEach(partition -> consumer.seekToBeginning(Collections.singleton(partition)));
                }

                @Override
                public void onPartitionsRevoked(Collection<TopicPartition> partitions) {}
            });

            int messageCount = 0;
            consumer.seekToBeginning(consumer.assignment());
            while (messageCount < maxMessages) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
                if (records.count() > 0) {
                    records.forEach(record -> {
                        LOG.info("Offset({}) kv({}, {})", record.offset(), record.key(), record.value());
                        HexDump.hexDump(record.value().getBytes());
                    });
                    LOG.info("Processed {} record(s)", records.count());
                }
                messageCount ++;
            }
        }
    }

    // Print usage information
    private static void printUsage(Options options) {
        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp("KfkTool", options);
    }

    // Main function with command line parsing
    public static void main(String[] args) throws Exception {
        Options options = new Options();

        options.addOption("a", "action", true,
                "Action to perform: listTopics, showTopic, createTopic, deleteTopic, sendRecord, pollRecord");
        options.addOption("h", "hostname", true, "Hostname, default is localhost");
        options.addOption("p", "port", true, "Port number, default is 9092");
        options.addOption("t", "topicName", true, "Name of the topic");
        options.addOption("k", "key", true, "key of a input record");
        options.addOption("v", "value", true, "value of a input record");

        CommandLineParser parser = new DefaultParser();
        CommandLine cmd = parser.parse(options, args);

        // Get action
        String action = cmd.getOptionValue("a");
        if (action == null) {
            System.out.println("Action is required.");
            printUsage(options);
            return;
        }

        // Get hostname and port (default to localhost and 9092 if not provided)
        cmd.hasOption("");
        String hostname = cmd.getOptionValue("h", DEFAULT_HOSTNAME);
        int port = Integer.parseInt(cmd.getOptionValue("p", DEFAULT_PORT));
        String server = hostname + ":" + port;
        setProperties(server);
        String topicName = cmd.getOptionValue("t");

        // Execute the requested action
        switch (action) {
            case "listTopics":
                listTopics();
                break;
            case "showTopic":
                if (cmd.hasOption("topicName")) {
                    showTopic(topicName);
                } else {
                    LOG.error("Please provide a topic name");
                }
                break;
            case "createTopic":
                if (cmd.hasOption("topicName")) {
                    // Default partition and replication factor
                    createTopic(topicName, 1, (short) 1);
                } else {
                    LOG.error("Please provide a topic name");
                }
                break;
            case "deleteTopic":
                if (cmd.hasOption("topicName")) {
                    deleteTopic(topicName);
                } else {
                    LOG.error("Please provide a topic name");
                }
                break;
            case "sendRecord":
                if (cmd.hasOption("topicName") && cmd.hasOption("key") && cmd.hasOption("value")) {
                    String key = cmd.getOptionValue("k");
                    String value = cmd.getOptionValue("v");
                    sendRecord(topicName, key, value);
                } else {
                    LOG.error("Please provide a topic/key/value");
                }
                break;
            case "pollRecords":
                if (cmd.hasOption("topicName")) {
                    pollRecords(topicName, 10); // Default to 10 messages
                } else {
                    LOG.error("Please provide a topic name");
                }
                break;
            default:
                LOG.error("Invalid action: " + action);
                printUsage(options);
        }
    }
}
