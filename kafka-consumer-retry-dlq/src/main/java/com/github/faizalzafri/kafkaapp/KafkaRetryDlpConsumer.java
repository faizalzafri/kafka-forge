package com.github.faizalzafri.kafkaapp;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

public class KafkaRetryDlpConsumer implements Runnable {
    private static final Logger logger = LoggerFactory.getLogger(KafkaRetryDlpConsumer.class);

    private final KafkaConsumer<String, String> consumer;
    private final String topic;
    private final RetryDlpProcessor processor;
    private final long backoffMs;
    private final AtomicBoolean closed = new AtomicBoolean(false);

    public KafkaRetryDlpConsumer(Properties consumerProps,
                                 String topic,
                                 RetryDlpProcessor processor,
                                 long backoffMs) {
        // Force auto commit to false for precise manual offset management
        consumerProps.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        this.consumer = new KafkaConsumer<>(consumerProps);
        this.topic = topic;
        this.processor = processor;
        this.backoffMs = backoffMs;
    }

    @Override
    public void run() {
        try {
            consumer.subscribe(Collections.singleton(topic));
            logger.info("Subscribed to topic '{}' with backoff {} ms.", topic, backoffMs);

            while (!closed.get()) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                
                for (ConsumerRecord<String, String> record : records) {
                    if (closed.get()) {
                        break;
                    }

                    // Enforce backoff if configured
                    if (backoffMs > 0) {
                        long recordAge = System.currentTimeMillis() - record.timestamp();
                        long sleepNeeded = backoffMs - recordAge;
                        
                        if (sleepNeeded > 0) {
                            logger.info("Backoff required. Pausing and seeking partition '{}' offset {} for {} ms.",
                                    record.partition(), record.offset(), sleepNeeded);

                            TopicPartition tp = new TopicPartition(record.topic(), record.partition());
                            consumer.pause(Collections.singleton(tp));
                            consumer.seek(tp, record.offset());

                            long elapsed = 0;
                            while (elapsed < sleepNeeded && !closed.get()) {
                                long step = Math.min(500, sleepNeeded - elapsed);
                                try {
                                    Thread.sleep(step);
                                } catch (InterruptedException e) {
                                    Thread.currentThread().interrupt();
                                    break;
                                }
                                // Send liveness heartbeats to coordinator during backoff sleep
                                consumer.poll(Duration.ofMillis(0));
                                elapsed += step;
                            }
                            
                            consumer.resume(Collections.singleton(tp));
                            break; // Break record loop to repoll after resuming and seeking
                        }
                    }

                    // Process the record
                    processor.process(record);
                }

                // Commit offset manually after batch processing is completed
                if (records.count() > 0 && !closed.get()) {
                    try {
                        consumer.commitSync();
                        logger.debug("Successfully committed offsets for batch.");
                    } catch (Exception e) {
                        logger.error("Failed to commit offsets", e);
                    }
                }
            }
        } catch (WakeupException e) {
            // Ignore exception if closing
            if (!closed.get()) {
                logger.error("Consumer wakeup exception", e);
                throw e;
            }
        } finally {
            logger.info("Closing consumer for topic '{}'...", topic);
            try {
                consumer.close();
            } catch (Exception e) {
                logger.error("Error while closing consumer", e);
            }
            logger.info("Consumer closed.");
        }
    }

    public void shutdown() {
        closed.set(true);
        consumer.wakeup();
    }
}
