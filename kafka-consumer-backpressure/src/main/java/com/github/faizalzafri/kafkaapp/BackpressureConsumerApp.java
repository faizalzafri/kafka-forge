package com.github.faizalzafri.kafkaapp;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class BackpressureConsumerApp {
    private static final Logger logger = LoggerFactory.getLogger(BackpressureConsumerApp.class);

    private static final String BOOTSTRAP_SERVERS = "localhost:9092";
    private static final String TOPIC = "customer-events";
    private static final String GROUP_ID = "backpressure-demo-group";

    public static void main(String[] args) {
        new BackpressureConsumerApp().run();
    }

    public void run() {
        logger.info("Starting Backpressured Kafka Consumer Application...");

        SlowDatabaseService databaseService = new SlowDatabaseService();
        Properties consumerProps = getConsumerProperties(GROUP_ID);

        // Core Backpressured Consumer configuration
        int threadPoolSize = 4;
        int queueCapacity = 10;
        int lowWatermark = 2;
        int highWatermark = 8;

        BackpressureConsumer backpressureConsumer = new BackpressureConsumer(
                consumerProps,
                TOPIC,
                databaseService,
                threadPoolSize,
                queueCapacity,
                lowWatermark,
                highWatermark
        );

        Thread consumerThread = new Thread(backpressureConsumer, "BackpressureConsumerThread");
        consumerThread.start();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Shutdown hook triggered. Stopping consumer...");
            backpressureConsumer.shutdown();
            try {
                consumerThread.join(12000);
            } catch (InterruptedException e) {
                logger.error("Interrupted while waiting for consumer thread to stop", e);
                Thread.currentThread().interrupt();
            }
            logger.info("Application exited.");
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
        properties.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "20");
        return properties;
    }
}
