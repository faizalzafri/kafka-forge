package com.github.faizalzafri.kafkaapp.spring.retry.exceptions;

public class TransientException extends RuntimeException {
    public TransientException(String message) {
        super(message);
    }
}
