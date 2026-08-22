package com.github.faizalzafri.kafkaapp.spring.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.support.converter.JsonMessageConverter;
import org.springframework.kafka.support.converter.RecordMessageConverter;

@Configuration
public class KafkaConfig {

    public static final String TOPIC_BASIC_EVENTS = "spring-basic-events";
    public static final String TOPIC_ORDERS = "spring-orders";
    public static final String TOPIC_ORDERS_DLT = "spring-orders-dlt";
    public static final String TOPIC_TWEETS = "spring-tweets";
    public static final String TOPIC_IMPORTANT_TWEETS = "spring-important-tweets";
    public static final String TOPIC_BACKPRESSURE_TASKS = "spring-backpressure-tasks";

    @Bean
    public RecordMessageConverter jsonMessageConverter() {
        return new JsonMessageConverter();
    }

    @Bean
    public NewTopic basicEventsTopic() {
        return TopicBuilder.name(TOPIC_BASIC_EVENTS).partitions(2).replicas(1).build();
    }

    @Bean
    public NewTopic ordersTopic() {
        return TopicBuilder.name(TOPIC_ORDERS).partitions(2).replicas(1).build();
    }

    @Bean
    public NewTopic tweetsTopic() {
        return TopicBuilder.name(TOPIC_TWEETS).partitions(2).replicas(1).build();
    }

    @Bean
    public NewTopic importantTweetsTopic() {
        return TopicBuilder.name(TOPIC_IMPORTANT_TWEETS).partitions(2).replicas(1).build();
    }

    @Bean
    public NewTopic backpressureTasksTopic() {
        return TopicBuilder.name(TOPIC_BACKPRESSURE_TASKS).partitions(2).replicas(1).build();
    }
}
