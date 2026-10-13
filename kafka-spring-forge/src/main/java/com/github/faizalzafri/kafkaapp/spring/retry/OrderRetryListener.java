package com.github.faizalzafri.kafkaapp.spring.retry;

import com.github.faizalzafri.kafkaapp.spring.config.KafkaConfig;
import com.github.faizalzafri.kafkaapp.spring.model.OrderEvent;
import com.github.faizalzafri.kafkaapp.spring.retry.exceptions.PermanentException;
import com.github.faizalzafri.kafkaapp.spring.retry.exceptions.TransientException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.DltHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.RetryableTopic;
import org.springframework.kafka.retrytopic.DltStrategy;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.retry.annotation.Backoff;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

@Component
public class OrderRetryListener {
    private static final Logger logger = LoggerFactory.getLogger(OrderRetryListener.class);

    private final List<String> successfulOrderIds = new CopyOnWriteArrayList<>();
    private final List<String> deadLetterOrderIds = new CopyOnWriteArrayList<>();
    private final Map<String, AtomicInteger> orderAttemptCounts = new ConcurrentHashMap<>();

    @RetryableTopic(
            attempts = "3",
            backoff = @Backoff(delay = 500, multiplier = 2.0, maxDelay = 3000),
            dltStrategy = DltStrategy.FAIL_ON_ERROR,
            exclude = {PermanentException.class},
            autoCreateTopics = "true"
    )
    @KafkaListener(
            topics = KafkaConfig.TOPIC_ORDERS,
            groupId = "spring-orders-consumer-group"
    )
    public void processOrder(
            @Payload OrderEvent order,
            @Header(name = KafkaHeaders.RECEIVED_TOPIC) String topic,
            @Header(name = KafkaHeaders.RECEIVED_PARTITION) int partition,
            @Header(name = KafkaHeaders.OFFSET) long offset
    ) {
        orderAttemptCounts.computeIfAbsent(order.orderId(), k -> new AtomicInteger(0)).incrementAndGet();
        int attempt = orderAttemptCounts.get(order.orderId()).get();

        logger.info("Processing Order '{}' on topic '{}' partition {} offset {} (Attempt: {})",
                order.orderId(), topic, partition, offset, attempt);

        if (order.status() != null && order.status().contains("poison")) {
            logger.error("Permanent poison pill detected for order ID '{}'. Routing directly to DLT.", order.orderId());
            throw new PermanentException("Permanent poison pill for order: " + order.orderId());
        }

        if (order.status() != null && order.status().contains("fail-transient")) {
            logger.warn("Simulated transient failure for order ID '{}' (Attempt: {}).", order.orderId(), attempt);
            throw new TransientException("Simulated transient downstream error for order: " + order.orderId());
        }

        logger.info("Successfully processed order '{}'", order.orderId());
        successfulOrderIds.add(order.orderId());
    }

    @DltHandler
    public void handleDeadLetter(
            @Payload OrderEvent order,
            @Header(name = KafkaHeaders.RECEIVED_TOPIC) String topic,
            @Header(name = KafkaHeaders.RECEIVED_PARTITION) int partition,
            @Header(name = KafkaHeaders.OFFSET) long offset
    ) {
        logger.error("DLT Handler invoked for order '{}' on topic '{}' partition {} offset {}",
                order.orderId(), topic, partition, offset);
        deadLetterOrderIds.add(order.orderId());
    }

    public List<String> getSuccessfulOrderIds() {
        return successfulOrderIds;
    }

    public List<String> getDeadLetterOrderIds() {
        return deadLetterOrderIds;
    }

    public Map<String, AtomicInteger> getOrderAttemptCounts() {
        return orderAttemptCounts;
    }
}
