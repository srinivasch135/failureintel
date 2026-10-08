package com.failureintel.ingestion.api.mapper;

import com.failureintel.ingestion.api.dto.FailureEventResponse;
import com.failureintel.ingestion.domain.normalization.NormalizationStatus;
import com.failureintel.infrastructure.persistence.failureevent.entity.FailureEventEntity;
import com.failureintel.infrastructure.persistence.failureevent.entity.ProcessingStatus;
import com.failureintel.infrastructure.persistence.normalizedFailureEvent.entity.NormalizedFailureEventEntity;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class FailureEventResponseMapperTest {

    @Test
    void shouldUseNormalizedValuesWhenTheyExist() {
        UUID eventId = UUID.randomUUID();
        Instant rawOccurredAt = Instant.parse("2026-08-03T20:00:00Z");
        Instant ingestedAt = Instant.parse("2026-08-03T20:00:05Z");
        Instant normalizedOccurredAt = Instant.parse("2026-08-03T19:59:58Z");

        FailureEventEntity failureEvent = failureEvent(eventId, rawOccurredAt, ingestedAt);

        NormalizedFailureEventEntity normalizedFailureEvent = new NormalizedFailureEventEntity();
        normalizedFailureEvent.setEventId(eventId);
        normalizedFailureEvent.setNormalizedTraceId("normalized-trace");
        normalizedFailureEvent.setNormalizedServiceName("payment-service");
        normalizedFailureEvent.setNormalizedEnvironment("prod");
        normalizedFailureEvent.setNormalizedEventType("exception");
        normalizedFailureEvent.setNormalizedErrorType("PSQLException");
        normalizedFailureEvent.setNormalizedErrorMessage("Connection timeout");
        normalizedFailureEvent.setNormalizedDependencyTarget("payment-database");
        normalizedFailureEvent.setNormalizedSeverity("high");
        normalizedFailureEvent.setNormalizedOccurredAt(normalizedOccurredAt);
        normalizedFailureEvent.setNormalizationStatus(NormalizationStatus.FULLY_NORMALIZED);
        Instant normalizedAt = Instant.parse("2026-08-03T20:00:06Z");
        normalizedFailureEvent.setNormalizedAt(normalizedAt);

        FailureEventResponse response = FailureEventResponseMapper.fromEntities(
                failureEvent,
                normalizedFailureEvent);

        assertEquals(eventId, response.eventId());
        assertEquals("normalized-trace", response.traceId());
        assertEquals(ingestedAt, response.ingestedAt());
        assertEquals("NORMALIZED", response.processingStatus());
        assertEquals(1, response.attemptCount());
        assertEquals(ingestedAt, response.lastAttemptAt());
        assertNull(response.nextAttemptAt());
        assertNull(response.failureCode());
        assertNull(response.failureReason());
        assertTrue(response.normalizedAvailable());
        assertEquals(NormalizationStatus.FULLY_NORMALIZED, response.normalizationStatus());
        assertEquals(normalizedAt, response.normalizedAt());
        assertEquals("payment-service", response.serviceName());
        assertEquals("prod", response.environment());
        assertEquals("exception", response.eventType());
        assertEquals("PSQLException", response.errorType());
        assertEquals("Connection timeout", response.message());
        assertEquals("payment-database", response.dependencyTarget());
        assertEquals("high", response.severity());
        assertEquals(normalizedOccurredAt, response.occurredAt());
    }

    @Test
    void shouldUseRawValuesWhenNormalizedEventIsMissing() {
        UUID eventId = UUID.randomUUID();
        Instant occurredAt = Instant.parse("2026-08-03T20:00:00Z");
        Instant ingestedAt = Instant.parse("2026-08-03T20:00:05Z");

        FailureEventEntity failureEvent = failureEvent(eventId, occurredAt, ingestedAt);

        FailureEventResponse response = FailureEventResponseMapper.fromEntities(
                failureEvent,
                null);

        assertEquals(eventId, response.eventId());
        assertEquals("raw-trace", response.traceId());
        assertEquals(ingestedAt, response.ingestedAt());
        assertEquals("NORMALIZED", response.processingStatus());
        assertEquals(1, response.attemptCount());
        assertEquals(ingestedAt, response.lastAttemptAt());
        assertNull(response.nextAttemptAt());
        assertNull(response.failureCode());
        assertNull(response.failureReason());
        assertFalse(response.normalizedAvailable());
        assertNull(response.normalizationStatus());
        assertNull(response.normalizedAt());
        assertEquals("raw-service", response.serviceName());
        assertEquals("raw-env", response.environment());
        assertEquals("raw-event-type", response.eventType());
        assertEquals("raw-error-type", response.errorType());
        assertEquals("raw message", response.message());
        assertEquals("raw-dependency", response.dependencyTarget());
        assertEquals("raw-severity", response.severity());
        assertEquals(occurredAt, response.occurredAt());
    }

    @Test
    void shouldExposeRetryMetadataAndOnlySafeFailureExplanation() {
        Instant occurredAt = Instant.parse("2026-08-03T20:00:00Z");
        Instant ingestedAt = Instant.parse("2026-08-03T20:00:05Z");
        Instant lastAttemptAt = Instant.parse("2026-08-03T20:01:00Z");
        Instant nextAttemptAt = Instant.parse("2026-08-03T20:06:00Z");
        FailureEventEntity failureEvent = receivedFailureEvent(UUID.randomUUID(), occurredAt, ingestedAt);
        failureEvent.claimForProcessing(lastAttemptAt);
        failureEvent.markRetryable(
                "DATABASE_TIMEOUT",
                "SQLException at db.internal: password=secret raw payload={token=private}",
                nextAttemptAt);

        FailureEventResponse response = FailureEventResponseMapper.fromEntities(failureEvent, null);

        assertEquals("RETRYABLE", response.processingStatus());
        assertEquals(1, response.attemptCount());
        assertEquals(lastAttemptAt, response.lastAttemptAt());
        assertEquals(nextAttemptAt, response.nextAttemptAt());
        assertEquals("DATABASE_TIMEOUT", response.failureCode());
        assertEquals(
                "Failure-event processing could not complete; another attempt is scheduled.",
                response.failureReason());
        assertFalse(response.normalizedAvailable());
        assertNull(response.normalizationStatus());
        assertNull(response.normalizedAt());
    }

    @Test
    void shouldMapKnownFailureCodesToSafePublicReasons() {
        Map<String, String> expectedReasons = Map.ofEntries(
                Map.entry("UNSUPPORTED_PAYLOAD", "The failure-event format is not supported."),
                Map.entry("MALFORMED_EVENT", "The failure event could not be interpreted."),
                Map.entry("INSUFFICIENT_FAILURE_DATA",
                        "The failure event does not contain enough failure information."),
                Map.entry("DATABASE_UNAVAILABLE",
                        "Failure-event processing could not complete; another attempt is scheduled."),
                Map.entry("DATABASE_TIMEOUT",
                        "Failure-event processing could not complete; another attempt is scheduled."),
                Map.entry("DATABASE_CONCURRENCY_FAILURE",
                        "Failure-event processing could not complete; another attempt is scheduled."),
                Map.entry("TEMPORARY_INFRASTRUCTURE_FAILURE",
                        "Failure-event processing could not complete; another attempt is scheduled."),
                Map.entry("NORMALIZED_WRITE_FAILURE",
                        "Failure-event processing could not complete; another attempt is scheduled."),
                Map.entry("UNEXPECTED_PROCESSING_FAILURE",
                        "Failure-event processing could not complete; another attempt is scheduled."),
                Map.entry("WORKER_LEASE_EXPIRED",
                        "The previous failure-event processing attempt did not complete."),
                Map.entry("RETRY_EXHAUSTED",
                        "Automatic failure-event processing attempts have been exhausted."));

        expectedReasons.forEach((failureCode, expectedReason) -> {
            FailureEventEntity failureEvent = failedEvent(failureCode,
                    "SQLException from db.internal: password=secret; raw payload={token=private}");

            FailureEventResponse response = FailureEventResponseMapper.fromEntities(failureEvent, null);

            assertEquals(failureCode, response.failureCode(), failureCode);
            assertEquals(expectedReason, response.failureReason(), failureCode);
        });
    }

    @Test
    void shouldHideUnknownFailureCodeAndArbitraryStoredReason() {
        FailureEventEntity failureEvent = failedEvent(
                "SQLException jdbc:postgresql://db.internal password=secret",
                "stack trace: token=private raw payload={creditCard=1234}");

        FailureEventResponse response = FailureEventResponseMapper.fromEntities(failureEvent, null);

        assertNull(response.failureCode());
        assertEquals("Failure-event processing did not complete.", response.failureReason());
        assertFalse(response.failureReason().contains("secret"));
        assertFalse(response.failureReason().contains("db.internal"));
        assertFalse(response.failureReason().contains("creditCard"));
    }

    @Test
    void shouldUseGenericSafeReasonWhenLegacyReasonHasNoCode() {
        FailureEventEntity failureEvent = failureEvent(
                UUID.randomUUID(),
                Instant.parse("2026-08-03T20:00:00Z"),
                Instant.parse("2026-08-03T20:00:05Z"));
        failureEvent.setFailureReason("Internal exception: secret=private at db.internal");

        FailureEventResponse response = FailureEventResponseMapper.fromEntities(failureEvent, null);

        assertNull(response.failureCode());
        assertEquals("Failure-event processing did not complete.", response.failureReason());
    }

    private static FailureEventEntity failedEvent(String failureCode, String storedFailureReason) {
        FailureEventEntity failureEvent = receivedFailureEvent(
                UUID.randomUUID(),
                Instant.parse("2026-08-03T20:00:00Z"),
                Instant.parse("2026-08-03T20:00:05Z"));
        Instant lastAttemptAt = Instant.parse("2026-08-03T20:01:00Z");
        failureEvent.claimForProcessing(lastAttemptAt);
        failureEvent.markFailed(failureCode, storedFailureReason);
        return failureEvent;
    }

    private static FailureEventEntity receivedFailureEvent(
            UUID eventId,
            Instant occurredAt,
            Instant ingestedAt) {
        FailureEventEntity failureEvent = new FailureEventEntity();
        failureEvent.setEventId(eventId);
        failureEvent.setTraceId("raw-trace");
        failureEvent.setIngestedAt(ingestedAt);
        failureEvent.setProcessingStatus(ProcessingStatus.RECEIVED);
        failureEvent.setServiceName("raw-service");
        failureEvent.setEnvironment("raw-env");
        failureEvent.setEventType("raw-event-type");
        failureEvent.setErrorType("raw-error-type");
        failureEvent.setMessage("raw message");
        failureEvent.setDependencyTarget("raw-dependency");
        failureEvent.setSeverityHint("raw-severity");
        failureEvent.setOccurredAt(occurredAt);
        return failureEvent;
    }

    private static FailureEventEntity failureEvent(
            UUID eventId,
            Instant occurredAt,
            Instant ingestedAt) {
        FailureEventEntity failureEvent = new FailureEventEntity();
        failureEvent.setEventId(eventId);
        failureEvent.setTraceId("raw-trace");
        failureEvent.setIngestedAt(ingestedAt);
        failureEvent.setProcessingStatus(ProcessingStatus.RECEIVED);
        failureEvent.claimForProcessing(ingestedAt);
        failureEvent.markNormalized();
        failureEvent.setServiceName("raw-service");
        failureEvent.setEnvironment("raw-env");
        failureEvent.setEventType("raw-event-type");
        failureEvent.setErrorType("raw-error-type");
        failureEvent.setMessage("raw message");
        failureEvent.setDependencyTarget("raw-dependency");
        failureEvent.setSeverityHint("raw-severity");
        failureEvent.setOccurredAt(occurredAt);
        return failureEvent;
    }
}
