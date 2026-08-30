package com.failureintel.infrastructure.persistence.normalizedFailureEvent.mapper;

import com.failureintel.ingestion.domain.model.NormalizedFailureEvent;
import com.failureintel.ingestion.domain.normalization.NormalizationStatus;
import com.failureintel.infrastructure.persistence.failureevent.entity.FailureEventEntity;
import com.failureintel.infrastructure.persistence.normalizedFailureEvent.entity.NormalizedFailureEventEntity;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;

class NormalizedFailureEventEntityMapperTest {

    @Test
    void shouldMapNormalizedFailureEventDomainObjectToEntity() {
        UUID eventId = UUID.randomUUID();
        Instant occurredAt = Instant.parse("2026-08-03T20:00:00Z");
        Instant normalizedAt = Instant.parse("2026-08-03T20:00:05Z");

        FailureEventEntity failureEvent = new FailureEventEntity();
        failureEvent.setEventId(eventId);
        failureEvent.setFailureReason("preserved failure reason");

        NormalizedFailureEvent normalizedEvent = new NormalizedFailureEvent(
                eventId,
                "payment-service",
                "prod",
                "exception",
                "PSQLException",
                "Connection timeout",
                "payment-database",
                "trace-123",
                "high",
                occurredAt,
                normalizedAt,
                NormalizationStatus.FULLY_NORMALIZED,
                Map.of("message", "Connection timeout"),
                Map.of("source", "datadog"),
                Map.of("missingFields", 0));

        NormalizedFailureEventEntity entity = NormalizedFailureEventEntityMapper.fromDomain(
                normalizedEvent,
                failureEvent);

        assertEquals(eventId, entity.getEventId());
        assertSame(failureEvent, entity.getFailureEvent());
        assertEquals(normalizedEvent.getNormalizedPayload(), entity.getNormalizedPayload());
        assertEquals("payment-service", entity.getNormalizedServiceName());
        assertEquals("prod", entity.getNormalizedEnvironment());
        assertEquals("exception", entity.getNormalizedEventType());
        assertEquals("PSQLException", entity.getNormalizedErrorType());
        assertEquals("Connection timeout", entity.getNormalizedErrorMessage());
        assertEquals("payment-database", entity.getNormalizedDependencyTarget());
        assertEquals("trace-123", entity.getNormalizedTraceId());
        assertEquals("high", entity.getNormalizedSeverity());
        assertEquals(occurredAt, entity.getNormalizedOccurredAt());
        assertEquals(normalizedAt, entity.getNormalizedAt());
        assertEquals(NormalizationStatus.FULLY_NORMALIZED, entity.getNormalizationStatus());
        assertEquals(normalizedEvent.getNormalizationMetadata(), entity.getNormalizationMetadata());
        assertEquals("preserved failure reason", entity.getFailureReason());
    }
}
