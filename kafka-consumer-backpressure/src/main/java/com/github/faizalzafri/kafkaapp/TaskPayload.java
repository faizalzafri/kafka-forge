package com.github.faizalzafri.kafkaapp;

public class TaskPayload {
    private String id;
    private String content;
    private long processingTimeMs;

    public TaskPayload() {}

    public TaskPayload(String id, String content, long processingTimeMs) {
        this.id = id;
        this.content = content;
        this.processingTimeMs = processingTimeMs;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getContent() {
        return content;
    }

    public void setContent(String content) {
        this.content = content;
    }

    public long getProcessingTimeMs() {
        return processingTimeMs;
    }

    public void setProcessingTimeMs(long processingTimeMs) {
        this.processingTimeMs = processingTimeMs;
    }

    @Override
    public String toString() {
        return "TaskPayload{" +
                "id='" + id + '\'' +
                ", content='" + content + '\'' +
                ", processingTimeMs=" + processingTimeMs +
                '}';
    }
}
