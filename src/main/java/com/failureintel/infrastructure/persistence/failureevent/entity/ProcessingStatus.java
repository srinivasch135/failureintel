package com.failureintel.infrastructure.persistence.failureevent.entity;

public enum ProcessingStatus {
    RECEIVED,
    PROCESSING,
    NORMALIZED,
    QUEUED,
    PROCESSED,
    RETRYABLE,
    FAILED

}
