package com.failureintel.ingestion.domain.pipeline;

import com.failureintel.ingestion.domain.model.NormalizedFailureEvent;
import com.failureintel.ingestion.domain.model.ParsedFailureEvent;
import com.failureintel.ingestion.domain.model.RawFailureEvent;
import com.failureintel.ingestion.domain.normalization.FailureEventNormalizer;
import com.failureintel.ingestion.domain.parser.FailureEventParser;
import com.failureintel.ingestion.domain.parser.GenericJsonFailureEventParser;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

public class FailureEventParsingNormalizationPipelineTest {

    private static final Instant OCCURRED_AT = Instant.parse("2024-06-01T12:00:00Z");
    private static final Instant RECEIVED_AT = Instant.parse("2024-06-01T12:05:00Z");

    private static final Instant NORMALIZED_AT = Instant.parse("2026-07-30T13:00:10Z");

    private FailureEventParser parser;
    private FailureEventNormalizer normalizer;

    @BeforeEach
    void setUp() {
        parser = new GenericJsonFailureEventParser();
        Clock fixedClock = Clock.fixed(NORMALIZED_AT, ZoneOffset.UTC);
        normalizer = new FailureEventNormalizer(fixedClock);
    }

    @Test
    void shouldParseAndNormalizeDatabaseFailureSuccessfully() {
        RawFailureEvent rawEvent = createDatabaseFailureEvent();
        assertTrue(parser.supports(rawEvent), "Generic JSON parser should support this raw event");
        ParsedFailureEvent parsedEvent = parser.parse(rawEvent);
        NormalizedFailureEvent normalizedEvent = normalizer.normalize(parsedEvent);
        verifyParsedFields(parsedEvent);
        verifyNormalizedFields(normalizedEvent);
    }

    private RawFailureEvent createDatabaseFailureEvent() {
        return new RawFailureEvent(
                UUID.randomUUID(),
                "custom-api",
                "Payment-Service",
                "PRODUCTION",
                "ERROR",
                "PSQLException",
                "Connection timeout after 5000ms",
                "orders-postgres",
                "trace-123",
                "SEV2",
                OCCURRED_AT,
                RECEIVED_AT,
                Map.of(
                        "service", " Payment-Service ",
                        "environment", "PRODUCTION",
                        "level", "ERROR",
                        "exception", "PSQLException",
                        "message", "Connection timeout after 5000ms",
                        "dependency", "orders-postgres",
                        "traceId", "trace-123",
                        "severity", "SEV2",
                        "timestamp", OCCURRED_AT.toString()),
                Map.of(
                        "host", "payment-node-01",
                        "region", "us-east-1"));
    }

    private void verifyParsedFields(ParsedFailureEvent parsedEvent) {
        assertNotNull(parsedEvent, "Parsed event should not be null");
        assertFalse(parsedEvent.isMalformed(), "Parsed event should not be malformed");
        assertEquals("Payment-Service", parsedEvent.getServiceName(), "Service name should match");
        assertFalse(
                parsedEvent.isMalformed(),
                "A valid event must not be marked as malformed");

        assertEquals(
                "PRODUCTION",
                parsedEvent.getEnvironment());

        assertEquals(
                "ERROR",
                parsedEvent.getEventType());

        assertEquals(
                "PSQLException",
                parsedEvent.getErrorType());

        assertEquals(
                "Connection timeout after 5000ms",
                parsedEvent.getErrorMessage());

        assertEquals(
                "orders-postgres",
                parsedEvent.getDependencyTarget());

        assertEquals(
                "trace-123",
                parsedEvent.getTraceId());

        assertEquals(
                "SEV2",
                parsedEvent.getSeverityHint());

        assertEquals(
                OCCURRED_AT,
                parsedEvent.getOccurredAt());

    }

    private void verifyNormalizedFields(NormalizedFailureEvent normalizedEvent) {
        assertNotNull(normalizedEvent);

        assertEquals(
                "payment-service",
                normalizedEvent.getServiceName());

        assertEquals(
                "prod",
                normalizedEvent.getEnvironment());

        assertEquals(
                "exception",
                normalizedEvent.getEventType());

        assertEquals(
                "high",
                normalizedEvent.getSeverity());

        assertEquals(
                "PSQLException",
                normalizedEvent.getErrorType());

        assertEquals(
                "Connection timeout after 5000ms",
                normalizedEvent.getErrorMessage());

        assertEquals(
                "orders-postgres",
                normalizedEvent.getDependencyTarget());

        assertEquals(
                "trace-123",
                normalizedEvent.getTraceId());

        assertEquals(
                NORMALIZED_AT,
                normalizedEvent.getNormalizedAt());
    }
}
