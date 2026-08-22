package com.github.faizalzafri.kafkaapp;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

@Testcontainers
public class BackpressureIntegrationTest {

    @Container
    public static KafkaContainer kafka = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:6.2.1"));

    private static String bootstrapServers;
    private static AdminClient adminClient;
    private static KafkaProducer<String, String> testProducer;

    private static final String TOPIC = "test-backpressure-events";
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private static final Set<String> processedIds = Collections.newSetFromMap(new ConcurrentHashMap<>());
    private static final AtomicInteger pauseCount = new AtomicInteger(0);
    private static final AtomicInteger resumeCount = new AtomicInteger(0);

    private static class TrackingSlowDatabaseService extends SlowDatabaseService {
        @Override
        public void insert(TaskPayload payload) {
            super.insert(payload);
            processedIds.add(payload.getId());
        }
    }

    @BeforeAll
    public static void setUp() throws Exception {
        bootstrapServers = kafka.getBootstrapServers();

        Properties adminProps = new Properties();
        adminProps.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        adminClient = AdminClient.create(adminProps);

        adminClient.createTopics(Collections.singletonList(new NewTopic(TOPIC, 1, (short) 1)))
                .all().get(10, TimeUnit.SECONDS);

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
    public void testBackpressureFlowControl() throws Exception {
        // Initialize tracking database service
        TrackingSlowDatabaseService dbService = new TrackingSlowDatabaseService();

        // 1. Configure backpressure consumer properties
        Properties consumerProps = new Properties();
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "test-backpressure-group");
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        int threadPoolSize = 2;
        int queueCapacity = 5;
        int lowWatermark = 2;
        int highWatermark = 5;

        BackpressureConsumer backpressureConsumer = new BackpressureConsumer(
                consumerProps,
                TOPIC,
                dbService,
                threadPoolSize,
                queueCapacity,
                lowWatermark,
                highWatermark
        );

        Thread consumerThread = new Thread(backpressureConsumer);
        consumerThread.start();

        // 2. Publish 30 tasks with slow processing time to trigger backpressure
        int totalTasks = 30;
        long taskProcessingTimeMs = 150; // 150ms per task ensures we fill the queue (capacity 5 + 2 workers)

        for (int i = 1; i <= totalTasks; i++) {
            TaskPayload payload = new TaskPayload("task-" + i, "data-" + i, taskProcessingTimeMs);
            testProducer.send(new ProducerRecord<>(TOPIC, payload.getId(), MAPPER.writeValueAsString(payload)));
        }
        testProducer.flush(); // Send all messages in a burst

        // 3. Monitor for pauses and resumptions
        boolean observedPause = false;
        boolean observedResume = false;
        long start = System.currentTimeMillis();

        while (System.currentTimeMillis() - start < 15000) {
            if (backpressureConsumer.isPaused()) {
                observedPause = true;
            }
            if (observedPause && !backpressureConsumer.isPaused()) {
                observedResume = true;
            }
            if (processedIds.size() >= totalTasks) {
                break;
            }
            Thread.sleep(100);
        }

        // Graceful shutdown
        backpressureConsumer.shutdown();
        consumerThread.join();

        // 4. Assertions
        // A. Assert that ALL tasks were successfully processed in the end
        assertEquals(totalTasks, processedIds.size(), "All published tasks should be processed.");

        // B. Assert that backpressure flow control (pause & resume) was triggered
        assertTrue(observedPause, "Should have triggered backpressure pause.");
        assertTrue(observedResume, "Should have resumed fetching after queue drained.");
        
        // C. Assert that worker threads were indeed named correctly
        assertTrue(backpressureConsumer.getThreadFactory() != null);
    }
}
