package com.github.faizalzafri.kafkaapp;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ServiceRegistry {
    private static final Logger logger = LoggerFactory.getLogger(ServiceRegistry.class);

    public static class TransientException extends RuntimeException {
        public TransientException(String message) {
            super(message);
        }
    }

    public static class PermanentException extends RuntimeException {
        public PermanentException(String message) {
            super(message);
        }
    }

    public void process(ProcessedRecord record) {
        logger.info("Processing record: {}", record);

        if (record.getContent() == null) {
            throw new PermanentException("Content cannot be null. This is a permanent failure (poison pill).");
        }

        if (record.getContent().contains("poison")) {
            throw new PermanentException("Poison pill detected: " + record.getContent());
        }

        if (record.getContent().contains("fail-transient")) {
            throw new TransientException("Simulated transient connection failure: " + record.getContent());
        }

        logger.info("Successfully processed record: {}", record.getId());
    }
}
