package com.failureintel.ingestion.api.mapper;

import com.failureintel.ingestion.api.dto.FailureEventResponse;
import com.failureintel.infrastructure.persistence.failureevent.entity.FailureEventEntity;
import com.failureintel.infrastructure.persistence.failureevent.entity.ProcessingStatus;
import com.failureintel.infrastructure.persistence.normalizedFailureEvent.entity.NormalizedFailureEventEntity;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;

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

        FailureEventResponse response = FailureEventResponseMapper.fromEntities(
                failureEvent,
                normalizedFailureEvent);

        assertEquals(eventId, response.eventId());
        assertEquals("normalized-trace", response.traceId());
        assertEquals(ingestedAt, response.ingestedAt());
        assertEquals("NORMALIZED", response.processingStatus());
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
        assertEquals("raw-service", response.serviceName());
        assertEquals("raw-env", response.environment());
        assertEquals("raw-event-type", response.eventType());
        assertEquals("raw-error-type", response.errorType());
        assertEquals("raw message", response.message());
        assertEquals("raw-dependency", response.dependencyTarget());
        assertEquals("raw-severity", response.severity());
        assertEquals(occurredAt, response.occurredAt());
    }

    private static FailureEventEntity failureEvent(
            UUID eventId,
            Instant occurredAt,
            Instant ingestedAt) {
        FailureEventEntity failureEvent = new FailureEventEntity();
        failureEvent.setEventId(eventId);
        failureEvent.setTraceId("raw-trace");
        failureEvent.setIngestedAt(ingestedAt);
        failureEvent.setProcessingStatus(ProcessingStatus.NORMALIZED);
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
