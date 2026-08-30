package com.failureintel.infrastructure.persistence.failureevent.entity;

public enum ProcessingStatus {
    RECEIVED,
    NORMALIZED,
    QUEUED,
    PROCESSED,
    FAILED,

}
