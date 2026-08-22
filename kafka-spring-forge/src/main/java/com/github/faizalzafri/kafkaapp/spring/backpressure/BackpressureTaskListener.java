package com.github.faizalzafri.kafkaapp.spring.backpressure;

import com.github.faizalzafri.kafkaapp.spring.config.KafkaConfig;
import com.github.faizalzafri.kafkaapp.spring.model.TaskEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;

@Component
public class BackpressureTaskListener {
    private static final Logger logger = LoggerFactory.getLogger(BackpressureTaskListener.class);
    public static final String LISTENER_ID = "spring-backpressure-listener";

    private final KafkaListenerEndpointRegistry registry;
    private final Semaphore concurrencyGate = new Semaphore(5); // Maximum 5 concurrent virtual thread tasks
    private final AtomicBoolean isPaused = new AtomicBoolean(false);
    private final List<String> processedTaskIds = new CopyOnWriteArrayList<>();
    private final List<Boolean> isVirtualThreadFlags = new CopyOnWriteArrayList<>();

    public BackpressureTaskListener(KafkaListenerEndpointRegistry registry) {
        this.registry = registry;
    }

    @KafkaListener(
            id = LISTENER_ID,
            topics = KafkaConfig.TOPIC_BACKPRESSURE_TASKS,
            groupId = "spring-backpressure-group",
            concurrency = "3"
    )
    public void processTask(
            @Payload TaskEvent task,
            @Header(name = KafkaHeaders.RECEIVED_PARTITION) int partition,
            @Header(name = KafkaHeaders.OFFSET) long offset
    ) {
        boolean isVirtual = Thread.currentThread().isVirtual();
        isVirtualThreadFlags.add(isVirtual);

        logger.info("Processing Task '{}' on partition {} offset {} (Virtual Thread: {}, Available Permits: {})",
                task.id(), partition, offset, isVirtual, concurrencyGate.availablePermits());

        // Flow control evaluation
        if (concurrencyGate.availablePermits() <= 1 && !isPaused.get()) {
            logger.warn("Backpressure threshold reached! Pausing listener container '{}'", LISTENER_ID);
            MessageListenerContainer container = registry.getListenerContainer(LISTENER_ID);
            if (container != null) {
                container.pause();
                isPaused.set(true);
            }
        }

        try {
            concurrencyGate.acquire();
            // Simulate database or slow external I/O
            if (task.processingTimeMs() > 0) {
                Thread.sleep(task.processingTimeMs());
            }
            processedTaskIds.add(task.id());
            logger.info("Completed Task '{}' on Virtual Thread: {}", task.id(), Thread.currentThread());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            logger.error("Task processing interrupted for task ID: {}", task.id(), e);
        } finally {
            concurrencyGate.release();

            // Resume if queue has drained
            if (concurrencyGate.availablePermits() >= 4 && isPaused.get()) {
                logger.info("Backpressure resolved! Resuming listener container '{}'", LISTENER_ID);
                MessageListenerContainer container = registry.getListenerContainer(LISTENER_ID);
                if (container != null) {
                    container.resume();
                    isPaused.set(false);
                }
            }
        }
    }

    public List<String> getProcessedTaskIds() {
        return processedTaskIds;
    }

    public List<Boolean> getIsVirtualThreadFlags() {
        return isVirtualThreadFlags;
    }

    public boolean isPaused() {
        return isPaused.get();
    }
}
