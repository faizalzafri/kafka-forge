package com.github.faizalzafri.kafkaapp.spring.streams;

import com.github.faizalzafri.kafkaapp.spring.config.KafkaConfig;
import com.github.faizalzafri.kafkaapp.spring.model.TweetEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

@Component
public class ImportantTweetListener {
    private static final Logger logger = LoggerFactory.getLogger(ImportantTweetListener.class);

    private final List<TweetEvent> receivedImportantTweets = new CopyOnWriteArrayList<>();

    @KafkaListener(
            topics = KafkaConfig.TOPIC_IMPORTANT_TWEETS,
            groupId = "spring-important-tweets-verifier-group"
    )
    public void listen(@Payload TweetEvent tweet) {
        logger.info("Important Tweet received from Streams Topology: {}", tweet);
        receivedImportantTweets.add(tweet);
    }

    public List<TweetEvent> getReceivedImportantTweets() {
        return receivedImportantTweets;
    }
}
