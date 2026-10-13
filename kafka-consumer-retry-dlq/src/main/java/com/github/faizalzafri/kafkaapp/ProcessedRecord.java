package com.github.faizalzafri.kafkaapp;

public class ProcessedRecord {
    private String id;
    private String content;

    public ProcessedRecord() {}

    public ProcessedRecord(String id, String content) {
        this.id = id;
        this.content = content;
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

    @Override
    public String toString() {
        return "ProcessedRecord{" +
                "id='" + id + '\'' +
                ", content='" + content + '\'' +
                '}';
    }
}
