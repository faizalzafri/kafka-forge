package com.github.faizalzafri.kafkaapp.spring.model;

public record TaskEvent(String id, String content, long processingTimeMs) {}
