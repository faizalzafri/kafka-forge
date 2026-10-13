package com.github.faizalzafri.kafkaapp;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

@Testcontainers
public class RetryDlpConsumerIntegrationTest {

    @Container
    public static KafkaContainer kafka = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:6.2.1"));

    private static String bootstrapServers;
    private static AdminClient adminClient;
    private static KafkaProducer<String, String> testProducer;

    private static final String MAIN_TOPIC = "test-orders";
    private static final String RETRY_TOPIC = "test-orders-retry";
    private static final String DLQ_TOPIC = "test-orders-dlq";

    private static final ObjectMapper MAPPER = new ObjectMapper();

    // Track processing counts for verification
    private static final Map<String, Integer> processAttempts = new ConcurrentHashMap<>();
    private static final List<String> successfulIds = Collections.synchronizedList(new ArrayList<>());

    private static class TestServiceRegistry extends ServiceRegistry {
        @Override
        public void process(ProcessedRecord record) {
            processAttempts.merge(record.getId(), 1, Integer::sum);

            if ("poison-pill".equals(record.getId())) {
                throw new PermanentException("Poison pill detected!");
            }

            if ("transient-success".equals(record.getId())) {
                int attempt = processAttempts.get(record.getId());
                if (attempt < 3) {
                    throw new TransientException("Transient DB connection issue (Attempt " + attempt + ")");
                }
                // Succeed on 3rd attempt
                successfulIds.add(record.getId());
                return;
            }

            if ("transient-exhausted".equals(record.getId())) {
                throw new TransientException("Persistent transient failure");
            }

            // Normal success
            successfulIds.add(record.getId());
        }
    }

    @BeforeAll
    public static void setUp() throws Exception {
        bootstrapServers = kafka.getBootstrapServers();

        // 1. Create AdminClient to provision topics
        Properties adminProps = new Properties();
        adminProps.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        adminClient = AdminClient.create(adminProps);

        List<NewTopic> newTopics = Arrays.asList(
                new NewTopic(MAIN_TOPIC, 1, (short) 1),
                new NewTopic(RETRY_TOPIC, 1, (short) 1),
                new NewTopic(DLQ_TOPIC, 1, (short) 1)
        );
        adminClient.createTopics(newTopics).all().get(10, TimeUnit.SECONDS);

        // 2. Initialize Producer for test cases
        Properties prodProps = new Properties();
        prodProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        prodProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        prodProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        testProducer = new KafkaProducer<>(prodProps);
    }

    @AfterAll
    public static void tearDown() {
        if (testProducer != null) {
            testProducer.close();
        }
        if (adminClient != null) {
            adminClient.close();
        }
    }

    @Test
    public void testRetryAndDlqPipeline() throws Exception {
        // Configure dependencies
        TestServiceRegistry serviceRegistry = new TestServiceRegistry();
        Properties producerProps = new Properties();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        int maxRetries = 2; // Maximum 2 retry attempts
        RetryDlpProcessor processor = new RetryDlpProcessor(
                serviceRegistry,
                producerProps,
                RETRY_TOPIC,
                DLQ_TOPIC,
                maxRetries
        );

        // Define consumer properties
        Properties consumerProps = new Properties();
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "test-group");
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // Start Main and Retry consumers in threads
        KafkaRetryDlpConsumer mainConsumer = new KafkaRetryDlpConsumer(
                consumerProps,
                MAIN_TOPIC,
                processor,
                0 // 0 backoff for main
        );

        Properties retryConsumerProps = new Properties();
        retryConsumerProps.putAll(consumerProps);

        KafkaRetryDlpConsumer retryConsumer = new KafkaRetryDlpConsumer(
                retryConsumerProps,
                RETRY_TOPIC,
                processor,
                1000 // 1 second backoff for quick testing
        );

        Thread mainThread = new Thread(mainConsumer);
        Thread retryThread = new Thread(retryConsumer);
        mainThread.start();
        retryThread.start();

        // 1. Send Happy Path message
        ProcessedRecord happyRecord = new ProcessedRecord("happy-1", "valid content");
        testProducer.send(new ProducerRecord<>(MAIN_TOPIC, happyRecord.getId(), MAPPER.writeValueAsString(happyRecord))).get();

        // 2. Send Poison Pill message
        ProcessedRecord poisonRecord = new ProcessedRecord("poison-pill", "bad format");
        testProducer.send(new ProducerRecord<>(MAIN_TOPIC, poisonRecord.getId(), MAPPER.writeValueAsString(poisonRecord))).get();

        // 3. Send Transient Recoverable message
        ProcessedRecord transientSuccessRecord = new ProcessedRecord("transient-success", "fail-transient");
        testProducer.send(new ProducerRecord<>(MAIN_TOPIC, transientSuccessRecord.getId(), MAPPER.writeValueAsString(transientSuccessRecord))).get();

        // 4. Send Transient Exhausted message
        ProcessedRecord transientExhaustedRecord = new ProcessedRecord("transient-exhausted", "fail-transient");
        testProducer.send(new ProducerRecord<>(MAIN_TOPIC, transientExhaustedRecord.getId(), MAPPER.writeValueAsString(transientExhaustedRecord))).get();

        // Wait for consumers to process everything (up to 30 seconds for reliable execution under load)
        long start = System.currentTimeMillis();
        while (System.currentTimeMillis() - start < 30000) {
            // Check if we processed all expected records
            if (successfulIds.contains("happy-1") && 
                successfulIds.contains("transient-success") && 
                processAttempts.getOrDefault("transient-exhausted", 0) >= 3) {
                break;
            }
            Thread.sleep(500);
        }

        // Shutdown consumers
        mainConsumer.shutdown();
        retryConsumer.shutdown();
        mainThread.join();
        retryThread.join();

        // Assertions:
        // A. Happy-1 should succeed on attempt 1
        assertTrue(successfulIds.contains("happy-1"));
        assertEquals(1, processAttempts.get("happy-1"));

        // B. Transient Success should succeed on attempt 3 (Attempt 1 + 2 retries)
        assertTrue(successfulIds.contains("transient-success"));
        assertEquals(3, processAttempts.get("transient-success"));

        // C. Consume DLQ to check routing and headers
        Properties testConsumerProps = new Properties();
        testConsumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        testConsumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        testConsumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        testConsumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "test-dlq-verification-group");
        testConsumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        KafkaConsumer<String, String> dlqVerifyConsumer = new KafkaConsumer<>(testConsumerProps);
        dlqVerifyConsumer.subscribe(Collections.singleton(DLQ_TOPIC));

        List<ConsumerRecord<String, String>> dlqRecords = new ArrayList<>();
        long dlqPollStart = System.currentTimeMillis();
        while (System.currentTimeMillis() - dlqPollStart < 5000 && dlqRecords.size() < 2) {
            ConsumerRecords<String, String> records = dlqVerifyConsumer.poll(Duration.ofMillis(500));
            for (ConsumerRecord<String, String> r : records) {
                dlqRecords.add(r);
            }
        }
        dlqVerifyConsumer.close();

        assertEquals(2, dlqRecords.size(), "DLQ should contain exactly 2 records: poison-pill and transient-exhausted");

        // Verify headers on DLQ records
        ConsumerRecord<String, String> dlqPoison = dlqRecords.stream()
                .filter(r -> "poison-pill".equals(r.key()))
                .findFirst().orElseThrow(() -> new AssertionError("Poison pill record missing in DLQ"));
        
        assertEquals(MAIN_TOPIC, getHeaderValueAsString(dlqPoison, Headers.ORIGINAL_TOPIC));
        assertNotNull(getHeaderValueAsString(dlqPoison, Headers.EXCEPTION_MESSAGE));
        assertNotNull(getHeaderValueAsString(dlqPoison, Headers.EXCEPTION_STACKTRACE));

        ConsumerRecord<String, String> dlqExhausted = dlqRecords.stream()
                .filter(r -> "transient-exhausted".equals(r.key()))
                .findFirst().orElseThrow(() -> new AssertionError("Transient exhausted record missing in DLQ"));

        assertEquals(MAIN_TOPIC, getHeaderValueAsString(dlqExhausted, Headers.ORIGINAL_TOPIC));
        assertEquals(maxRetries, getHeaderValueAsInt(dlqExhausted, Headers.RETRY_COUNT));
    }

    private String getHeaderValueAsString(ConsumerRecord<String, String> record, String key) {
        Header header = record.headers().lastHeader(key);
        return header != null ? new String(header.value(), StandardCharsets.UTF_8) : null;
    }

    private int getHeaderValueAsInt(ConsumerRecord<String, String> record, String key) {
        Header header = record.headers().lastHeader(key);
        return header != null ? ByteBuffer.wrap(header.value()).getInt() : -1;
    }
}
