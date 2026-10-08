package com.failureintel.ingestion.application.service;

import java.util.Objects;
import java.util.UUID;

/**
 * Identifies the processing attempt currently owned by a worker.
 *
 * <p>The attempt number is part of the claim identity. An event may be
 * claimed again after a lease expires, so an event ID alone is not enough to
 * prevent an older worker from completing a newer attempt.</p>
 */
public record ClaimedFailureEvent(UUID eventId, int attemptNumber) {

    public ClaimedFailureEvent {
        Objects.requireNonNull(eventId, "eventId must not be null");
        if (attemptNumber <= 0) {
            throw new IllegalArgumentException("attemptNumber must be greater than zero");
        }
    }
}
