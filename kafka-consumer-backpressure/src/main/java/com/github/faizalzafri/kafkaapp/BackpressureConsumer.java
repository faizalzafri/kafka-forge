package com.github.faizalzafri.kafkaapp;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

public class BackpressureConsumer implements Runnable {
    private static final Logger logger = LoggerFactory.getLogger(BackpressureConsumer.class);

    private final KafkaConsumer<String, String> consumer;
    private final String topic;
    private final SlowDatabaseService databaseService;
    private final OffsetTracker offsetTracker;
    private final ExecutorService executor;
    private final ThreadFactory threadFactory;
    
    private final Semaphore semaphore;
    private final int maxPermits;
    private final int lowWatermark;
    private final int highWatermark;
    private final AtomicBoolean isPaused = new AtomicBoolean(false);
    private final AtomicBoolean closed = new AtomicBoolean(false);
    private final ObjectReader reader = new ObjectMapper().readerFor(TaskPayload.class);

    public BackpressureConsumer(Properties consumerProps,
                                String topic,
                                SlowDatabaseService databaseService,
                                int threadPoolSize,
                                int queueCapacity,
                                int lowWatermark,
                                int highWatermark) {
        // Force manual offset commit
        consumerProps.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        this.consumer = new KafkaConsumer<>(consumerProps);
        this.topic = topic;
        this.databaseService = databaseService;
        this.offsetTracker = new OffsetTracker();
        this.lowWatermark = lowWatermark;
        this.highWatermark = highWatermark;

        // Use highWatermark as the semaphore permit capacity
        this.maxPermits = highWatermark;
        this.semaphore = new Semaphore(maxPermits);

        // Java 21 Virtual Thread ThreadFactory and Executor
        this.threadFactory = Thread.ofVirtual().name("db-worker-", 1).factory();
        this.executor = Executors.newThreadPerTaskExecutor(threadFactory);
    }

    @Override
    public void run() {
        try {
            consumer.subscribe(Collections.singleton(topic), new ConsumerRebalanceListener() {
                @Override
                public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                    logger.info("Partitions revoked: {}. Clearing offset tracker.", partitions);
                    offsetTracker.clear(partitions);
                }

                @Override
                public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                    logger.info("Partitions assigned: {}", partitions);
                    isPaused.set(false);
                }
            });

            logger.info("Subscribed to topic '{}'. Flow control watermarks -> Low (Active Tasks): {}, High (Active Tasks): {}", 
                    topic, lowWatermark, highWatermark);

            while (!closed.get()) {
                // 1. Evaluate Backpressure Status (to resume fetching if queue has drained)
                int activeTasks = maxPermits - semaphore.availablePermits();
                evaluateBackpressure(activeTasks);

                // 2. Poll records from Kafka
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

                // 3. Process records using allocation-free iterator on the happy path
                Iterator<ConsumerRecord<String, String>> iterator = records.iterator();
                while (iterator.hasNext()) {
                    if (closed.get()) {
                        break;
                    }

                    // Try to acquire permit. If no permits available, trigger backpressure pause.
                    if (!semaphore.tryAcquire()) {
                        int currentActive = maxPermits - semaphore.availablePermits();
                        logger.warn("Backpressure triggered! Active tasks: {} >= High Watermark: {}. Pausing fetching partitions and seeking back remaining batch.", 
                                currentActive, highWatermark);

                        Set<TopicPartition> assigned = consumer.assignment();
                        if (!assigned.isEmpty()) {
                            consumer.pause(assigned);
                            isPaused.set(true);
                        }

                        // Collect first unprocessed offsets for all partitions remaining in this polled batch
                        Map<TopicPartition, Long> firstUnprocessedOffsets = new HashMap<>();
                        
                        // We must seek back starting from the record we failed to process.
                        // Since tryAcquire failed, iterator.next() hasn't been called for the current record yet.
                        // The next record is returned by iterator.next().
                        if (iterator.hasNext()) {
                            ConsumerRecord<String, String> currentRecord = iterator.next();
                            TopicPartition partition = new TopicPartition(currentRecord.topic(), currentRecord.partition());
                            firstUnprocessedOffsets.put(partition, currentRecord.offset());
                        }

                        while (iterator.hasNext()) {
                            ConsumerRecord<String, String> unprocessed = iterator.next();
                            TopicPartition partition = new TopicPartition(unprocessed.topic(), unprocessed.partition());
                            firstUnprocessedOffsets.putIfAbsent(partition, unprocessed.offset());
                        }

                        for (Map.Entry<TopicPartition, Long> entry : firstUnprocessedOffsets.entrySet()) {
                            consumer.seek(entry.getKey(), entry.getValue());
                            logger.info("Seeked partition '{}' back to offset {}.", entry.getKey().partition(), entry.getValue());
                        }

                        break; // Exit record processing loop
                    }

                    ConsumerRecord<String, String> record = iterator.next();
                    TopicPartition tp = new TopicPartition(record.topic(), record.partition());
                    long offset = record.offset();
                    
                    // Register offset before submitting task to thread pool
                    offsetTracker.register(tp, offset);

                    TaskPayload payload;
                    try {
                        payload = reader.readValue(record.value());
                    } catch (Exception e) {
                        logger.error("Failed to parse JSON for record at offset {}: {}", offset, record.value(), e);
                        offsetTracker.complete(tp, offset);
                        semaphore.release();
                        continue;
                    }
                    
                    try {
                        executor.submit(() -> {
                            try {
                                databaseService.insert(payload);
                            } catch (Exception e) {
                                logger.error("Error inserting record ID: {}", payload.getId(), e);
                            } finally {
                                // Mark offset as completed in tracker and release permit
                                offsetTracker.complete(tp, offset);
                                semaphore.release();
                            }
                        });
                    } catch (RejectedExecutionException ree) {
                        logger.error("Virtual Thread submission rejected for record ID: {}.", payload.getId(), ree);
                        offsetTracker.complete(tp, offset);
                        semaphore.release();
                    }
                }

                // 4. Periodically commit safe contiguous offsets
                commitSafeOffsets();
            }
        } catch (WakeupException e) {
            if (!closed.get()) {
                logger.error("Consumer wakeup exception", e);
                throw e;
            }
        } finally {
            cleanup();
        }
    }

    private void evaluateBackpressure(int activeTasks) {
        Set<TopicPartition> assigned = consumer.assignment();
        if (assigned.isEmpty()) {
            return;
        }

        if (activeTasks <= lowWatermark && isPaused.get()) {
            consumer.resume(assigned);
            isPaused.set(false);
            logger.info("Backpressure resolved! Active tasks: {} <= Low Watermark: {}. Resumed fetching partitions.", 
                    activeTasks, lowWatermark);
        }
    }

    private void commitSafeOffsets() {
        Map<TopicPartition, Long> offsetsToCommit = offsetTracker.getOffsetsToCommit();
        if (!offsetsToCommit.isEmpty()) {
            Map<TopicPartition, OffsetAndMetadata> commitMap = new HashMap<>();
            for (Map.Entry<TopicPartition, Long> entry : offsetsToCommit.entrySet()) {
                commitMap.put(entry.getKey(), new OffsetAndMetadata(entry.getValue()));
            }
            try {
                consumer.commitSync(commitMap);
                logger.info("Committed safe contiguous offsets: {}", commitMap);
            } catch (Exception e) {
                logger.error("Failed to commit offsets: {}", commitMap, e);
            }
        }
    }

    private void cleanup() {
        logger.info("Initiating graceful shutdown for BackpressureConsumer...");
        closed.set(true);
        
        // 1. Shutdown executor pool
        executor.shutdown();
        try {
            logger.info("Awaiting termination of database worker threads...");
            if (!executor.awaitTermination(10, TimeUnit.SECONDS)) {
                logger.warn("Worker threads did not finish within timeout. Force shutting down...");
                executor.shutdownNow();
            }
        } catch (InterruptedException e) {
            logger.error("Interrupted while waiting for executor to terminate", e);
            executor.shutdownNow();
            Thread.currentThread().interrupt();
        }

        // 2. Final offset commit
        try {
            commitSafeOffsets();
        } catch (Exception e) {
            logger.error("Failed final offset commit during cleanup", e);
        }

        // 3. Close Kafka Consumer
        try {
            logger.info("Closing Kafka Consumer...");
            consumer.close();
            logger.info("Kafka Consumer closed.");
        } catch (Exception e) {
            logger.error("Error closing consumer", e);
        }
    }

    public void shutdown() {
        closed.set(true);
        consumer.wakeup();
    }

    // Expose executor for metrics/testing
    public ExecutorService getExecutor() {
        return executor;
    }

    // Expose thread factory for metrics/testing
    public ThreadFactory getThreadFactory() {
        return threadFactory;
    }

    public boolean isPaused() {
        return isPaused.get();
    }
}
