package com.github.faizalzafri.kafkaapp.spring.basic;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.CompletableFuture;

@Service
public class EventProducerService {
    private static final Logger logger = LoggerFactory.getLogger(EventProducerService.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final KafkaTemplate<String, String> kafkaTemplate;

    public EventProducerService(KafkaTemplate<String, String> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public CompletableFuture<SendResult<String, String>> sendEvent(String topic, String key, Object payload) {
        logger.info("Sending message to topic '{}' with key '{}': {}", topic, key, payload);

        String jsonPayload;
        try {
            if (payload instanceof String str && (str.startsWith("{") || str.startsWith("["))) {
                jsonPayload = str;
            } else {
                jsonPayload = MAPPER.writeValueAsString(payload);
            }
        } catch (Exception e) {
            throw new RuntimeException("Failed to serialize payload to JSON", e);
        }

        ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, jsonPayload);
        record.headers().add(new RecordHeader("source-system", "spring-producer".getBytes(StandardCharsets.UTF_8)));
        record.headers().add(new RecordHeader("timestamp", String.valueOf(System.currentTimeMillis()).getBytes(StandardCharsets.UTF_8)));

        CompletableFuture<SendResult<String, String>> future = kafkaTemplate.send(record);

        future.whenComplete((result, ex) -> {
            if (ex != null) {
                logger.error("Failed to send message to topic '{}' key '{}'", topic, key, ex);
            } else {
                logger.info("Successfully sent message to topic '{}' partition {} offset {}",
                        topic,
                        result.getRecordMetadata().partition(),
                        result.getRecordMetadata().offset());
            }
        });

        return future;
    }
}
