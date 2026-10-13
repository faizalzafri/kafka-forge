package com.github.faizalzafri.kafkaapp;

public final class Headers {
    private Headers() {}

    public static final String RETRY_COUNT = "x-retry-count";
    public static final String ORIGINAL_TOPIC = "x-original-topic";
    public static final String ORIGINAL_PARTITION = "x-original-partition";
    public static final String ORIGINAL_OFFSET = "x-original-offset";
    public static final String EXCEPTION_MESSAGE = "x-exception-message";
    public static final String EXCEPTION_STACKTRACE = "x-exception-stacktrace";
}
