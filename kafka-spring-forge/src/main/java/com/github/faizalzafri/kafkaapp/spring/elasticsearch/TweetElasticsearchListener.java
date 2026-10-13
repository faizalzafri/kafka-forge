package com.github.faizalzafri.kafkaapp.spring.elasticsearch;

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
public class TweetElasticsearchListener {
    private static final Logger logger = LoggerFactory.getLogger(TweetElasticsearchListener.class);

    private final TweetRepository tweetRepository;
    private final List<String> indexedTweetIds = new CopyOnWriteArrayList<>();

    public TweetElasticsearchListener(TweetRepository tweetRepository) {
        this.tweetRepository = tweetRepository;
    }

    @KafkaListener(
            topics = KafkaConfig.TOPIC_TWEETS,
            groupId = "spring-elasticsearch-sink-group"
    )
    public void ingestTweet(@Payload TweetEvent tweet) {
        logger.info("Ingesting Tweet to Elasticsearch: ID='{}', Text='{}', Followers={}",
                tweet.id(), tweet.text(), tweet.followersCount());

        TweetDocument doc = new TweetDocument(tweet.id(), tweet.text(), tweet.followersCount());
        tweetRepository.save(doc);

        indexedTweetIds.add(tweet.id());
        logger.info("Successfully indexed Tweet document ID '{}' in Elasticsearch.", tweet.id());
    }

    public List<String> getIndexedTweetIds() {
        return indexedTweetIds;
    }
}
