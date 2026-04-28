package com.failureintel.infrastructure.persistence.entity;

public enum ProcessingStatus {
    RECEIVED,
    NORMALIZED,
    QUEUED,
    PROCESSED,
    FAILED
}
