package com.failureintel.ingestion.application.exception;

import com.failureintel.ingestion.application.service.ClaimedFailureEvent;
import com.failureintel.infrastructure.persistence.failureevent.entity.FailureEventEntity;

public class StaleFailureEventClaimException extends RuntimeException {

    public StaleFailureEventClaimException(
            ClaimedFailureEvent claim,
            FailureEventEntity currentEvent) {
        super("Processing claim is no longer current for eventId="
                + claim.eventId()
                + ": expected attempt "
                + claim.attemptNumber()
                + ", current status="
                + currentEvent.getProcessingStatus()
                + ", current attempt="
                + currentEvent.getAttemptCount());
    }
}
