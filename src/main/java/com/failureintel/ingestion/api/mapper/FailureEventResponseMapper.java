package com.failureintel.ingestion.api.mapper;

import com.failureintel.ingestion.api.dto.FailureEventResponse;
import com.failureintel.infrastructure.persistence.failureevent.entity.FailureEventEntity;
import com.failureintel.infrastructure.persistence.normalizedFailureEvent.entity.NormalizedFailureEventEntity;

import java.util.Objects;

public final class FailureEventResponseMapper {

    private static final String UNSUPPORTED_PAYLOAD = "UNSUPPORTED_PAYLOAD";
    private static final String MALFORMED_EVENT = "MALFORMED_EVENT";
    private static final String INSUFFICIENT_FAILURE_DATA = "INSUFFICIENT_FAILURE_DATA";
    private static final String DATABASE_UNAVAILABLE = "DATABASE_UNAVAILABLE";
    private static final String DATABASE_TIMEOUT = "DATABASE_TIMEOUT";
    private static final String DATABASE_CONCURRENCY_FAILURE = "DATABASE_CONCURRENCY_FAILURE";
    private static final String TEMPORARY_INFRASTRUCTURE_FAILURE = "TEMPORARY_INFRASTRUCTURE_FAILURE";
    private static final String NORMALIZED_WRITE_FAILURE = "NORMALIZED_WRITE_FAILURE";
    private static final String UNEXPECTED_PROCESSING_FAILURE = "UNEXPECTED_PROCESSING_FAILURE";
    private static final String WORKER_LEASE_EXPIRED = "WORKER_LEASE_EXPIRED";
    private static final String RETRY_EXHAUSTED = "RETRY_EXHAUSTED";

    private static final String UNKNOWN_FAILURE_REASON = "Failure-event processing did not complete.";
    private static final String RETRYABLE_FAILURE_REASON =
            "Failure-event processing could not complete; another attempt is scheduled.";

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
                        failureEvent.getOccurredAt()),
                failureEvent.getAttemptCount(),
                failureEvent.getLastAttemptAt(),
                failureEvent.getNextAttemptAt(),
                safeFailureCode(failureEvent.getFailureCode()),
                safeFailureReason(failureEvent.getFailureCode(), failureEvent.getFailureReason()),
                normalizedFailureEvent != null,
                normalizedFailureEvent == null ? null : normalizedFailureEvent.getNormalizationStatus(),
                normalizedFailureEvent == null ? null : normalizedFailureEvent.getNormalizedAt());
    }

    private static String safeFailureCode(String failureCode) {
        if (failureCode == null) {
            return null;
        }

        return switch (failureCode) {
            case UNSUPPORTED_PAYLOAD,
                    MALFORMED_EVENT,
                    INSUFFICIENT_FAILURE_DATA,
                    DATABASE_UNAVAILABLE,
                    DATABASE_TIMEOUT,
                    DATABASE_CONCURRENCY_FAILURE,
                    TEMPORARY_INFRASTRUCTURE_FAILURE,
                    NORMALIZED_WRITE_FAILURE,
                    UNEXPECTED_PROCESSING_FAILURE,
                    WORKER_LEASE_EXPIRED,
                    RETRY_EXHAUSTED -> failureCode;
            default -> null;
        };
    }

    private static String safeFailureReason(String failureCode, String storedFailureReason) {
        if (failureCode == null || failureCode.isBlank()) {
            return storedFailureReason == null || storedFailureReason.isBlank()
                    ? null
                    : UNKNOWN_FAILURE_REASON;
        }

        return switch (failureCode) {
            case UNSUPPORTED_PAYLOAD -> "The failure-event format is not supported.";
            case MALFORMED_EVENT -> "The failure event could not be interpreted.";
            case INSUFFICIENT_FAILURE_DATA -> "The failure event does not contain enough failure information.";
            case DATABASE_UNAVAILABLE,
                    DATABASE_TIMEOUT,
                    DATABASE_CONCURRENCY_FAILURE,
                    TEMPORARY_INFRASTRUCTURE_FAILURE,
                    NORMALIZED_WRITE_FAILURE,
                    UNEXPECTED_PROCESSING_FAILURE -> RETRYABLE_FAILURE_REASON;
            case WORKER_LEASE_EXPIRED -> "The previous failure-event processing attempt did not complete.";
            case RETRY_EXHAUSTED -> "Automatic failure-event processing attempts have been exhausted.";
            default -> UNKNOWN_FAILURE_REASON;
        };
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
