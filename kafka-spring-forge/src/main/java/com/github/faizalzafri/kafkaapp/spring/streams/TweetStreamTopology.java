package com.github.faizalzafri.kafkaapp.spring.streams;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.faizalzafri.kafkaapp.spring.config.KafkaConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class TweetStreamTopology {
    private static final Logger logger = LoggerFactory.getLogger(TweetStreamTopology.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();

    @Bean
    public KStream<String, String> processTweetStream(StreamsBuilder streamsBuilder) {
        KStream<String, String> stream = streamsBuilder.stream(
                KafkaConfig.TOPIC_TWEETS,
                Consumed.with(Serdes.String(), Serdes.String())
        );

        KStream<String, String> filteredStream = stream.filter((key, value) -> {
            try {
                JsonNode node = MAPPER.readTree(value);
                int followers = node.path("followersCount").asInt(0);
                boolean isImportant = followers > 10000;
                logger.info("Kafka Streams Filter evaluation: Key='{}', Followers={}, Passed={}",
                        key, followers, isImportant);
                return isImportant;
            } catch (Exception e) {
                logger.error("Error evaluating tweet stream payload: {}", value, e);
                return false;
            }
        });

        filteredStream.to(
                KafkaConfig.TOPIC_IMPORTANT_TWEETS,
                Produced.with(Serdes.String(), Serdes.String())
        );

        return stream;
    }
}
