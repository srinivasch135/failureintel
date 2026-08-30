package com.failureintel.infrastructure.persistence.normalizedFailureEvent.mapper;

import com.failureintel.ingestion.domain.model.NormalizedFailureEvent;
import com.failureintel.infrastructure.persistence.failureevent.entity.FailureEventEntity;
import com.failureintel.infrastructure.persistence.normalizedFailureEvent.entity.NormalizedFailureEventEntity;

import java.util.Objects;

public final class NormalizedFailureEventEntityMapper {

    private NormalizedFailureEventEntityMapper() {
    }

    public static NormalizedFailureEventEntity fromDomain(
            NormalizedFailureEvent normalizedEvent,
            FailureEventEntity failureEvent) {
        Objects.requireNonNull(normalizedEvent, "normalizedEvent must not be null");
        Objects.requireNonNull(failureEvent, "failureEvent must not be null");

        NormalizedFailureEventEntity entity = new NormalizedFailureEventEntity();
        entity.setEventId(failureEvent.getEventId());
        entity.setFailureEvent(failureEvent);
        entity.applyNormalizedEvent(normalizedEvent);
        entity.setFailureReason(failureEvent.getFailureReason());
        return entity;
    }
}
