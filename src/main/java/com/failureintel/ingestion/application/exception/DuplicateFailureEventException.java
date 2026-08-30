package com.failureintel.ingestion.application.exception;

public class DuplicateFailureEventException extends RuntimeException {

    public DuplicateFailureEventException(String traceId) {
        super("Failure event already exists for traceId: " + traceId);
    }
}
