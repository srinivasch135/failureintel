package com.failureintel.ingestion.domain.parser;

import com.failureintel.ingestion.domain.model.ParsedFailureEvent;
import com.failureintel.ingestion.domain.model.RawFailureEvent;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.Map;

import static com.failureintel.test.support.FailureEventTestFixtures.OCCURRED_AT;
import static com.failureintel.test.support.FailureEventTestFixtures.rawEvent;
import static org.junit.jupiter.api.Assertions.*;

class GenericJsonFailureEventParserTest {

    private final GenericJsonFailureEventParser parser = new GenericJsonFailureEventParser();

    @Test
    void shouldRejectNullAndEmptyPayloadsAsUnsupported() {
        assertFalse(parser.supports(null));
        assertFalse(parser.supports(rawEvent(null)));
        assertFalse(parser.supports(rawEvent(Map.of())));
    }

    @Test
    void shouldReturnMalformedResultForEmptyPayloadWhenCalledDirectly() {
        ParsedFailureEvent result = parser.parse(rawEvent(Map.of()));

        assertTrue(result.isMalformed());
        assertEquals("Raw payload is null or empty", result.getErrorMessage());
        assertEquals(1, result.getParsingWarnings().size());
    }

    @Test
    void shouldRejectNullEvent() {
        IllegalArgumentException exception = assertThrows(
                IllegalArgumentException.class,
                () -> parser.parse(null));

        assertEquals("rawFailureEvent cannot be null", exception.getMessage());
    }

    @Test
    void shouldExtractAliasesFromNestedPayloadIgnoringKeyCase() {
        Map<String, Object> payload = Map.of(
                "context", Map.of(
                        "APPLICATION", "inventory-service",
                        "stage", "testing",
                        "kind", "request_timeout",
                        "exceptionType", "timeout_exception",
                        "MSG", "Inventory request timed out",
                        "downstreamService", "inventory-db",
                        "correlationId", "trace-parser-001",
                        "LEVEL", "warning",
                        "timestamp", OCCURRED_AT.toString()));

        ParsedFailureEvent result = parser.parse(rawEvent(payload));

        assertFalse(result.isMalformed());
        assertEquals("inventory-service", result.getServiceName());
        assertEquals("testing", result.getEnvironment());
        assertEquals("request_timeout", result.getEventType());
        assertEquals("timeout_exception", result.getErrorType());
        assertEquals("Inventory request timed out", result.getErrorMessage());
        assertEquals("inventory-db", result.getDependencyTarget());
        assertEquals("trace-parser-001", result.getTraceId());
        assertEquals("warning", result.getSeverityHint());
        assertEquals(OCCURRED_AT, result.getOccurredAt());
        assertEquals(payload, result.getExtractedFields());
    }

    @Test
    void shouldPreferDirectFieldsOverPayloadAliases() {
        RawFailureEvent rawEvent = rawEvent(
                "direct-service",
                "prod",
                "ERROR",
                "PSQLException",
                "Direct message",
                "direct-db",
                "trace-direct",
                "HIGH",
                OCCURRED_AT,
                Map.of(
                        "service", "payload-service",
                        "env", "dev",
                        "message", "Payload message",
                        "timestamp", "2020-01-01T00:00:00Z"),
                Map.of("tenant", "acme"));

        ParsedFailureEvent result = parser.parse(rawEvent);

        assertEquals("direct-service", result.getServiceName());
        assertEquals("prod", result.getEnvironment());
        assertEquals("Direct message", result.getErrorMessage());
        assertEquals(OCCURRED_AT, result.getOccurredAt());
        assertEquals(Map.of("tenant", "acme"), result.getSourceMetadata());
    }

    @Test
    void shouldIgnoreInvalidPayloadTimestamp() {
        ParsedFailureEvent result = parser.parse(rawEvent(Map.of(
                "message", "Failure with invalid timestamp",
                "timestamp", "not-an-instant")));

        assertNull(result.getOccurredAt());
        assertEquals("Failure with invalid timestamp", result.getErrorMessage());
    }
}
