package com.github.faizalzafri.kafkaapp.spring.retry.exceptions;

public class PermanentException extends RuntimeException {
    public PermanentException(String message) {
        super(message);
    }
}
