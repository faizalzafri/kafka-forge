package com.github.faizalzafri.kafkaapp.spring;

import com.github.faizalzafri.kafkaapp.spring.basic.BasicEventListener;
import com.github.faizalzafri.kafkaapp.spring.basic.EventProducerService;
import com.github.faizalzafri.kafkaapp.spring.backpressure.BackpressureTaskListener;
import com.github.faizalzafri.kafkaapp.spring.config.KafkaConfig;
import com.github.faizalzafri.kafkaapp.spring.elasticsearch.TweetElasticsearchListener;
import com.github.faizalzafri.kafkaapp.spring.elasticsearch.TweetRepository;
import com.github.faizalzafri.kafkaapp.spring.model.OrderEvent;
import com.github.faizalzafri.kafkaapp.spring.model.TaskEvent;
import com.github.faizalzafri.kafkaapp.spring.model.TweetEvent;
import com.github.faizalzafri.kafkaapp.spring.retry.OrderRetryListener;
import com.github.faizalzafri.kafkaapp.spring.streams.ImportantTweetListener;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.annotation.DirtiesContext;

import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

@SpringBootTest(properties = {
        "spring.kafka.bootstrap-servers=${spring.embedded.kafka.brokers}",
        "spring.kafka.streams.bootstrap-servers=${spring.embedded.kafka.brokers}",
        "spring.kafka.streams.properties.auto.offset.reset=earliest",
        "spring.kafka.streams.properties.commit.interval.ms=100",
        "spring.kafka.streams.auto-offset-reset=earliest"
})
@EmbeddedKafka(
        partitions = 2,
        topics = {
                KafkaConfig.TOPIC_BASIC_EVENTS,
                KafkaConfig.TOPIC_ORDERS,
                KafkaConfig.TOPIC_TWEETS,
                KafkaConfig.TOPIC_IMPORTANT_TWEETS,
                KafkaConfig.TOPIC_BACKPRESSURE_TASKS
        }
)
@DirtiesContext
public class KafkaSpringBootIntegrationTest {

    @MockBean
    private TweetRepository tweetRepository;

    @Autowired
    private EventProducerService producerService;

    @Autowired
    private BasicEventListener basicEventListener;

    @Autowired
    private OrderRetryListener orderRetryListener;

    @Autowired
    private BackpressureTaskListener backpressureTaskListener;

    @Autowired
    private TweetElasticsearchListener tweetElasticsearchListener;

    @Autowired
    private ImportantTweetListener importantTweetListener;

    @Test
    public void testBasicEventPublishAndConsume() throws Exception {
        producerService.sendEvent(KafkaConfig.TOPIC_BASIC_EVENTS, "key-1", "Hello Spring Kafka!");

        long start = System.currentTimeMillis();
        while (System.currentTimeMillis() - start < 25000) {
            if (!basicEventListener.getReceivedEvents().isEmpty()) {
                break;
            }
            TimeUnit.MILLISECONDS.sleep(200);
        }

        assertFalse(basicEventListener.getReceivedEvents().isEmpty(), "Basic event should be received");
        assertTrue(basicEventListener.getReceivedEvents().contains("Hello Spring Kafka!"));
    }

    @Test
    public void testOrderNonBlockingRetriesAndDlt() throws Exception {
        // 1. Happy Path Order
        OrderEvent happyOrder = new OrderEvent("order-happy-1", "Laptop", 1200.0, "SUCCESS");
        producerService.sendEvent(KafkaConfig.TOPIC_ORDERS, happyOrder.orderId(), happyOrder);

        // 2. Poison Pill Order (Immediate DLT)
        OrderEvent poisonOrder = new OrderEvent("order-poison-2", "Defective Item", 0.0, "poison-error");
        producerService.sendEvent(KafkaConfig.TOPIC_ORDERS, poisonOrder.orderId(), poisonOrder);

        // 3. Transient Failure Order (Retry 3x before DLT)
        OrderEvent transientOrder = new OrderEvent("order-transient-3", "Mobile", 800.0, "fail-transient");
        producerService.sendEvent(KafkaConfig.TOPIC_ORDERS, transientOrder.orderId(), transientOrder);

        long start = System.currentTimeMillis();
        while (System.currentTimeMillis() - start < 25000) {
            if (orderRetryListener.getSuccessfulOrderIds().contains("order-happy-1") &&
                orderRetryListener.getDeadLetterOrderIds().contains("order-poison-2") &&
                orderRetryListener.getDeadLetterOrderIds().contains("order-transient-3")) {
                break;
            }
            TimeUnit.MILLISECONDS.sleep(200);
        }

        assertTrue(orderRetryListener.getSuccessfulOrderIds().contains("order-happy-1"), "Happy order should succeed");
        assertTrue(orderRetryListener.getDeadLetterOrderIds().contains("order-poison-2"), "Poison order should be in DLT");
        assertTrue(orderRetryListener.getDeadLetterOrderIds().contains("order-transient-3"), "Exhausted transient order should be in DLT");
        assertEquals(3, orderRetryListener.getOrderAttemptCounts().get("order-transient-3").get(), "Transient order should be retried 3 times");
    }

    @Test
    public void testVirtualThreadsAndBackpressureFlow() throws Exception {
        for (int i = 1; i <= 6; i++) {
            TaskEvent task = new TaskEvent("task-spring-" + i, "data-" + i, 50);
            producerService.sendEvent(KafkaConfig.TOPIC_BACKPRESSURE_TASKS, task.id(), task);
        }

        long start = System.currentTimeMillis();
        while (System.currentTimeMillis() - start < 25000) {
            if (backpressureTaskListener.getProcessedTaskIds().size() >= 6) {
                break;
            }
            TimeUnit.MILLISECONDS.sleep(200);
        }

        assertEquals(6, backpressureTaskListener.getProcessedTaskIds().size(), "All 6 tasks should be processed");
        assertTrue(backpressureTaskListener.getIsVirtualThreadFlags().contains(true), "Tasks should execute on Java 21 Virtual Threads");
    }

    @Test
    public void testStreamsFilterAndElasticsearchIngestion() throws Exception {
        // Send normal tweet (5000 followers)
        TweetEvent normalTweet = new TweetEvent("tweet-1", "Casual update", 5000);
        producerService.sendEvent(KafkaConfig.TOPIC_TWEETS, normalTweet.id(), normalTweet);

        // Send viral tweet (25000 followers)
        TweetEvent viralTweet = new TweetEvent("tweet-2", "Breaking news announcement!", 25000);
        producerService.sendEvent(KafkaConfig.TOPIC_TWEETS, viralTweet.id(), viralTweet);

        long start = System.currentTimeMillis();
        while (System.currentTimeMillis() - start < 25000) {
            if (tweetElasticsearchListener.getIndexedTweetIds().contains("tweet-1") &&
                tweetElasticsearchListener.getIndexedTweetIds().contains("tweet-2") &&
                !importantTweetListener.getReceivedImportantTweets().isEmpty()) {
                break;
            }
            TimeUnit.MILLISECONDS.sleep(200);
        }

        assertTrue(tweetElasticsearchListener.getIndexedTweetIds().contains("tweet-1"), "Tweet 1 should be ingested");
        assertTrue(tweetElasticsearchListener.getIndexedTweetIds().contains("tweet-2"), "Tweet 2 should be ingested");
        assertFalse(importantTweetListener.getReceivedImportantTweets().isEmpty(), "Viral tweet should pass Streams filter");
        assertEquals("tweet-2", importantTweetListener.getReceivedImportantTweets().get(0).id());
    }
}
