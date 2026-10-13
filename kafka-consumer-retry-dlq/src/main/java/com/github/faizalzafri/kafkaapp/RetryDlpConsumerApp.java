package com.github.faizalzafri.kafkaapp;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class RetryDlpConsumerApp {
    private static final Logger logger = LoggerFactory.getLogger(RetryDlpConsumerApp.class);

    private static final String BOOTSTRAP_SERVERS = "localhost:9092";
    private static final String MAIN_TOPIC = "main-orders";
    private static final String RETRY_TOPIC = "main-orders-retry";
    private static final String DLQ_TOPIC = "main-orders-dlq";
    private static final String GROUP_ID = "orders-processing-group";
    private static final int MAX_RETRIES = 3;
    private static final long RETRY_BACKOFF_MS = 5000; // 5 seconds

    public static void main(String[] args) {
        new RetryDlpConsumerApp().run();
    }

    public void run() {
        logger.info("Starting Non-Blocking Retry & DLQ Consumer Application...");

        // 1. Core Services & Processor
        ServiceRegistry serviceRegistry = new ServiceRegistry();
        Properties producerProps = getProducerProperties();
        
        RetryDlpProcessor processor = new RetryDlpProcessor(
                serviceRegistry,
                producerProps,
                RETRY_TOPIC,
                DLQ_TOPIC,
                MAX_RETRIES
        );

        // 2. Configure Consumers
        Properties mainConsumerProps = getConsumerProperties(GROUP_ID);
        Properties retryConsumerProps = getConsumerProperties(GROUP_ID);

        // 3. Instantiate Consumer Instances
        KafkaRetryDlpConsumer mainConsumer = new KafkaRetryDlpConsumer(
                mainConsumerProps,
                MAIN_TOPIC,
                processor,
                0 // No backoff delay for the main ingestion topic
        );

        KafkaRetryDlpConsumer retryConsumer = new KafkaRetryDlpConsumer(
                retryConsumerProps,
                RETRY_TOPIC,
                processor,
                RETRY_BACKOFF_MS // 5 seconds delay to allow transient errors to resolve
        );

        // 4. Start Threads
        Thread mainConsumerThread = new Thread(mainConsumer, "MainConsumerThread");
        Thread retryConsumerThread = new Thread(retryConsumer, "RetryConsumerThread");

        mainConsumerThread.start();
        retryConsumerThread.start();

        // 5. Shutdown Hook
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Shutdown hook triggered. Stopping consumers...");
            mainConsumer.shutdown();
            retryConsumer.shutdown();

            try {
                mainConsumerThread.join(5000);
                retryConsumerThread.join(5000);
            } catch (InterruptedException e) {
                logger.error("Interrupted while waiting for consumer threads to stop", e);
                Thread.currentThread().interrupt();
            }

            processor.close();
            logger.info("Shutdown complete.");
        }, "ShutdownHookThread"));
    }

    private Properties getConsumerProperties(String groupId) {
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        
        // Optimize poll configurations for responsiveness
        properties.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "10");
        return properties;
    }

    private Properties getProducerProperties() {
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        
        // Safe Producer Configurations (Idempotence & Reliability)
        properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        properties.setProperty(ProducerConfig.ACKS_CONFIG, "all");
        properties.setProperty(ProducerConfig.RETRIES_CONFIG, Integer.toString(Integer.MAX_VALUE));
        properties.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "5");
        return properties;
    }
}
