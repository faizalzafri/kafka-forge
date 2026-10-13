package com.github.faizalzafri.kafkaapp;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SlowDatabaseService {
    private static final Logger logger = LoggerFactory.getLogger(SlowDatabaseService.class);

    public void insert(TaskPayload payload) {
        logger.info("Database write started for record ID: {}", payload.getId());
        
        long sleepTime = payload.getProcessingTimeMs();
        if (sleepTime > 0) {
            try {
                Thread.sleep(sleepTime);
            } catch (InterruptedException e) {
                logger.warn("Database write interrupted for record ID: {}", payload.getId());
                Thread.currentThread().interrupt();
            }
        }
        
        logger.info("Database write completed for record ID: {}", payload.getId());
    }
}
