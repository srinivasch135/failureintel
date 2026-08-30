package com.failureintel.ingestion.application.exception;

import java.util.UUID;

public class FailureEventNotFoundException extends RuntimeException {
    public FailureEventNotFoundException(UUID eventID) {
        super("FailureEvent not found for eventID: " + eventID);
    }

    public FailureEventNotFoundException(String traceId) {
        super("FailureEvent not found for traceId: " + traceId);
    }
}
