package com.github.faizalzafri.kafkaapp.spring.model;

public record OrderEvent(String orderId, String item, double amount, String status) {}
