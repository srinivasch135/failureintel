package com.failureintel.ingestion.application.service;

import java.util.Objects;
import java.util.UUID;

/** Identifies a durable grouping-work claim generation. */
public record ClaimedGroupingWork(UUID eventId, int attemptNumber) {
    public ClaimedGroupingWork {
        Objects.requireNonNull(eventId, "eventId must not be null");
        if (attemptNumber <= 0) {
            throw new IllegalArgumentException("attemptNumber must be greater than zero");
        }
    }
}
