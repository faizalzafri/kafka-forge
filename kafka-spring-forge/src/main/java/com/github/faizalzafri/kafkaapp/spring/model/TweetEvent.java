package com.github.faizalzafri.kafkaapp.spring.model;

public record TweetEvent(String id, String text, int followersCount) {}
