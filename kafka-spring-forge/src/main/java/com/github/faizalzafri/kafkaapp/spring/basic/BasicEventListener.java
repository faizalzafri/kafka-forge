package com.github.faizalzafri.kafkaapp.spring.basic;

import com.github.faizalzafri.kafkaapp.spring.config.KafkaConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

import java.util.concurrent.CopyOnWriteArrayList;
import java.util.List;

@Component
public class BasicEventListener {
    private static final Logger logger = LoggerFactory.getLogger(BasicEventListener.class);

    private final List<String> receivedEvents = new CopyOnWriteArrayList<>();

    @KafkaListener(
            topics = KafkaConfig.TOPIC_BASIC_EVENTS,
            groupId = "spring-basic-consumer-group"
    )
    public void listen(
            @Payload String event,
            @Header(name = KafkaHeaders.RECEIVED_KEY, required = false) String key,
            @Header(name = KafkaHeaders.RECEIVED_PARTITION, required = false) Integer partition,
            @Header(name = KafkaHeaders.OFFSET, required = false) Long offset,
            @Header(name = "source-system", required = false) byte[] sourceSystem
    ) {
        String source = sourceSystem != null ? new String(sourceSystem) : "unknown";
        logger.info("Received event on topic '{}' partition {} offset {} key '{}' from source '{}': {}",
                KafkaConfig.TOPIC_BASIC_EVENTS, partition, offset, key, source, event);

        receivedEvents.add(event);
    }

    public List<String> getReceivedEvents() {
        return receivedEvents;
    }
}
