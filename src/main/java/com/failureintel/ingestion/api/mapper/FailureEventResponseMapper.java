package com.failureintel.ingestion.api.mapper;

import com.failureintel.ingestion.api.dto.FailureEventResponse;
import com.failureintel.infrastructure.persistence.failureevent.entity.FailureEventEntity;
import com.failureintel.infrastructure.persistence.normalizedFailureEvent.entity.NormalizedFailureEventEntity;

import java.util.Objects;

public final class FailureEventResponseMapper {

    private FailureEventResponseMapper() {
    }

    public static FailureEventResponse fromEntities(
            FailureEventEntity failureEvent,
            NormalizedFailureEventEntity normalizedFailureEvent) {
        Objects.requireNonNull(failureEvent, "failureEvent must not be null");

        return new FailureEventResponse(
                failureEvent.getEventId(),
                valueOrFallback(
                        normalizedFailureEvent,
                        NormalizedFailureEventEntity::getNormalizedTraceId,
                        failureEvent.getTraceId()),
                failureEvent.getIngestedAt(),
                failureEvent.getProcessingStatus() == null ? null : failureEvent.getProcessingStatus().name(),
                valueOrFallback(
                        normalizedFailureEvent,
                        NormalizedFailureEventEntity::getNormalizedServiceName,
                        failureEvent.getServiceName()),
                valueOrFallback(
                        normalizedFailureEvent,
                        NormalizedFailureEventEntity::getNormalizedEnvironment,
                        failureEvent.getEnvironment()),
                valueOrFallback(
                        normalizedFailureEvent,
                        NormalizedFailureEventEntity::getNormalizedEventType,
                        failureEvent.getEventType()),
                valueOrFallback(
                        normalizedFailureEvent,
                        NormalizedFailureEventEntity::getNormalizedErrorType,
                        failureEvent.getErrorType()),
                valueOrFallback(
                        normalizedFailureEvent,
                        NormalizedFailureEventEntity::getNormalizedErrorMessage,
                        failureEvent.getMessage()),
                valueOrFallback(
                        normalizedFailureEvent,
                        NormalizedFailureEventEntity::getNormalizedDependencyTarget,
                        failureEvent.getDependencyTarget()),
                valueOrFallback(
                        normalizedFailureEvent,
                        NormalizedFailureEventEntity::getNormalizedSeverity,
                        failureEvent.getSeverityHint()),
                valueOrFallback(
                        normalizedFailureEvent,
                        NormalizedFailureEventEntity::getNormalizedOccurredAt,
                        failureEvent.getOccurredAt()));
    }

    private static <T> T valueOrFallback(
            NormalizedFailureEventEntity normalizedFailureEvent,
            NormalizedValue<T> normalizedValue,
            T fallback) {
        if (normalizedFailureEvent == null) {
            return fallback;
        }

        T value = normalizedValue.get(normalizedFailureEvent);
        return value == null ? fallback : value;
    }

    @FunctionalInterface
    private interface NormalizedValue<T> {
        T get(NormalizedFailureEventEntity normalizedFailureEvent);
    }
}
