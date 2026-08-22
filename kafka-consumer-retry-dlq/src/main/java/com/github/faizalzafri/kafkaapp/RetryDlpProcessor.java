package com.github.faizalzafri.kafkaapp;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Properties;

public class RetryDlpProcessor {
    private static final Logger logger = LoggerFactory.getLogger(RetryDlpProcessor.class);
    private final ObjectReader reader = new ObjectMapper().readerFor(ProcessedRecord.class);

    private final ServiceRegistry serviceRegistry;
    private final KafkaProducer<String, String> producer;
    private final String retryTopic;
    private final String dlqTopic;
    private final int maxRetries;

    public RetryDlpProcessor(ServiceRegistry serviceRegistry,
                             Properties producerProps,
                             String retryTopic,
                             String dlqTopic,
                             int maxRetries) {
        this.serviceRegistry = serviceRegistry;
        this.producer = new KafkaProducer<>(producerProps);
        this.retryTopic = retryTopic;
        this.dlqTopic = dlqTopic;
        this.maxRetries = maxRetries;
    }

    public void process(ConsumerRecord<String, String> record) {
        ProcessedRecord processedRecord = null;
        try {
            processedRecord = reader.readValue(record.value());
            if (processedRecord == null) {
                throw new ServiceRegistry.PermanentException("Parsed record is null");
            }
            // 2. Invoke Business Service
            serviceRegistry.process(processedRecord);

        } catch (com.fasterxml.jackson.core.JsonProcessingException e) {
            logger.error("JSON Syntax error (Poison Pill) for record offset {}. Routing to DLQ.", record.offset(), e);
            sendToDlq(record, "JsonProcessingException: " + e.getMessage(), getStackTrace(e));
        } catch (ServiceRegistry.PermanentException e) {
            logger.error("Permanent processing exception for record offset {}. Routing to DLQ.", record.offset(), e);
            sendToDlq(record, e.getMessage(), getStackTrace(e));
        } catch (ServiceRegistry.TransientException e) {
            logger.warn("Transient processing exception for record offset {}. Evaluating retry strategy.", record.offset(), e);
            handleRetry(record, e);
        } catch (Exception e) {
            logger.error("Unexpected error occurred while processing record offset {}. Routing to DLQ.", record.offset(), e);
            sendToDlq(record, "UnexpectedException: " + e.getMessage(), getStackTrace(e));
        }
    }

    private void handleRetry(ConsumerRecord<String, String> record, Exception exception) {
        int currentRetry = getHeaderAsInt(record, Headers.RETRY_COUNT, 0);

        if (currentRetry < maxRetries) {
            int nextRetry = currentRetry + 1;
            logger.info("Retrying record (attempt {}/{}). Routing to retry topic '{}'.", nextRetry, maxRetries, retryTopic);
            sendToRetryTopic(record, nextRetry, exception);
        } else {
            logger.error("Max retries ({}) exhausted for record. Routing to DLQ topic '{}'.", maxRetries, dlqTopic);
            sendToDlq(record, "MaxRetriesExceededException: " + exception.getMessage(), getStackTrace(exception));
        }
    }

    private void sendToRetryTopic(ConsumerRecord<String, String> record, int retryAttempt, Exception exception) {
        ProducerRecord<String, String> retryRecord = new ProducerRecord<>(retryTopic, record.key(), record.value());
        
        // Copy existing headers
        for (Header header : record.headers()) {
            retryRecord.headers().add(header);
        }

        // Overwrite or add retry metadata headers
        replaceHeader(retryRecord, Headers.RETRY_COUNT, intToBytes(retryAttempt));
        
        // Preserve original coordinates across retries
        setIfAbsent(retryRecord, Headers.ORIGINAL_TOPIC, stringToBytes(record.topic()));
        setIfAbsent(retryRecord, Headers.ORIGINAL_PARTITION, intToBytes(record.partition()));
        setIfAbsent(retryRecord, Headers.ORIGINAL_OFFSET, longToBytes(record.offset()));
        
        replaceHeader(retryRecord, Headers.EXCEPTION_MESSAGE, stringToBytes(exception.getMessage()));
        replaceHeader(retryRecord, Headers.EXCEPTION_STACKTRACE, stringToBytes(getStackTrace(exception)));

        producer.send(retryRecord, (metadata, e) -> {
            if (e != null) {
                logger.error("Failed to route message to retry topic: {}", retryTopic, e);
            } else {
                logger.debug("Successfully routed message to retry topic: {} partition: {} offset: {}",
                        metadata.topic(), metadata.partition(), metadata.offset());
            }
        });
    }

    private void sendToDlq(ConsumerRecord<String, String> record, String exceptionMessage, String stackTrace) {
        ProducerRecord<String, String> dlqRecord = new ProducerRecord<>(dlqTopic, record.key(), record.value());

        // Copy existing headers
        for (Header header : record.headers()) {
            dlqRecord.headers().add(header);
        }

        // Add failure context metadata (preserving original coordinates if present)
        setIfAbsent(dlqRecord, Headers.ORIGINAL_TOPIC, stringToBytes(record.topic()));
        setIfAbsent(dlqRecord, Headers.ORIGINAL_PARTITION, intToBytes(record.partition()));
        setIfAbsent(dlqRecord, Headers.ORIGINAL_OFFSET, longToBytes(record.offset()));
        
        replaceHeader(dlqRecord, Headers.EXCEPTION_MESSAGE, stringToBytes(exceptionMessage));
        replaceHeader(dlqRecord, Headers.EXCEPTION_STACKTRACE, stringToBytes(stackTrace));

        producer.send(dlqRecord, (metadata, e) -> {
            if (e != null) {
                logger.error("Failed to route message to DLQ topic: {}", dlqTopic, e);
            } else {
                logger.debug("Successfully routed message to DLQ topic: {} partition: {} offset: {}",
                        metadata.topic(), metadata.partition(), metadata.offset());
            }
        });
    }

    private void replaceHeader(ProducerRecord<String, String> record, String key, byte[] value) {
        // Remove existing header first to avoid duplicates
        record.headers().remove(key);
        record.headers().add(key, value);
    }

    private void setIfAbsent(ProducerRecord<String, String> record, String key, byte[] value) {
        if (record.headers().lastHeader(key) == null) {
            record.headers().add(key, value);
        }
    }

    private int getHeaderAsInt(ConsumerRecord<String, String> record, String key, int defaultValue) {
        Header header = record.headers().lastHeader(key);
        if (header != null && header.value() != null) {
            return bytesToInt(header.value());
        }
        return defaultValue;
    }

    private byte[] intToBytes(int value) {
        return ByteBuffer.allocate(4).putInt(value).array();
    }

    private int bytesToInt(byte[] bytes) {
        if (bytes == null || bytes.length < 4) {
            return 0;
        }
        return ByteBuffer.wrap(bytes).getInt();
    }

    private byte[] longToBytes(long value) {
        return ByteBuffer.allocate(8).putLong(value).array();
    }

    private byte[] stringToBytes(String value) {
        if (value == null) {
            return new byte[0];
        }
        return value.getBytes(StandardCharsets.UTF_8);
    }

    private String getStackTrace(Throwable throwable) {
        StringWriter sw = new StringWriter();
        PrintWriter pw = new PrintWriter(sw);
        throwable.printStackTrace(pw);
        return sw.toString();
    }

    public void close() {
        if (producer != null) {
            producer.close();
        }
    }
}
