package com.failureintel.ingestion.domain.normalization;

import com.failureintel.ingestion.domain.model.NormalizedFailureEvent;
import com.failureintel.ingestion.domain.model.ParsedFailureEvent;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

class FailureEventNormalizerTest {

    private static final Instant FIXED_TIME = Instant.parse("2026-07-30T03:25:10Z");

    private FailureEventNormalizer failureEventNormalizer;

    @BeforeEach
    void setUp() {
        Clock fixedClock = Clock.fixed(
                FIXED_TIME,
                ZoneOffset.UTC);

        failureEventNormalizer = new FailureEventNormalizer(fixedClock);
    }

    @Test
    void shouldNormalizeDatabaseFailureSuccessfully() {

        ParsedFailureEvent parsedEvent = ParsedFailureEvent.parsed(
                null,
                "payment-service",
                "prod",
                "ERROR",
                "PSQLException",
                "Connection timeout after 5000ms",
                "postgres-primary",
                "trace-123",
                "HIGH",
                FIXED_TIME);

        NormalizedFailureEvent normalizedEvent = failureEventNormalizer.normalize(parsedEvent);

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
                "PSQLException",
                normalizedEvent.getErrorType());

        assertEquals(
                "Connection timeout after 5000ms",
                normalizedEvent.getErrorMessage());

        assertEquals(
                "high",
                normalizedEvent.getSeverity());

        assertEquals(
                FIXED_TIME,
                normalizedEvent.getOccurredAt());

        assertEquals(
                NormalizationStatus.FULLY_NORMALIZED,
                normalizedEvent.getNormalizationStatus());
    }

    @Test
    void shouldDefaultMissingServiceName() {

        ParsedFailureEvent parsedEvent = ParsedFailureEvent.parsed(
                null,
                null,
                "prod",
                "ERROR",
                "PSQLException",
                "Connection timeout after 5000ms",
                null,
                null,
                "HIGH",
                FIXED_TIME);

        NormalizedFailureEvent normalizedEvent = failureEventNormalizer.normalize(parsedEvent);

        assertNotNull(normalizedEvent);

        assertEquals(
                "unknown_service",
                normalizedEvent.getServiceName());

        assertEquals(
                "prod",
                normalizedEvent.getEnvironment());

        assertEquals(
                "exception",
                normalizedEvent.getEventType());

        assertEquals(
                "PSQLException",
                normalizedEvent.getErrorType());

        assertEquals(
                "Connection timeout after 5000ms",
                normalizedEvent.getErrorMessage());

        assertEquals(
                "high",
                normalizedEvent.getSeverity());

        assertEquals(
                FIXED_TIME,
                normalizedEvent.getOccurredAt());

        assertEquals(
                NormalizationStatus.PARTIALLY_NORMALIZED,
                normalizedEvent.getNormalizationStatus());
    }

    @Test
    void shouldDefaultMissingEnvironment() {

        ParsedFailureEvent parsedEvent = ParsedFailureEvent.parsed(
                null,
                "payment-service",
                null,
                "ERROR",
                "PSQLException",
                "Connection timeout after 5000ms",
                null,
                null,
                "HIGH",
                FIXED_TIME);

        NormalizedFailureEvent normalizedEvent = failureEventNormalizer.normalize(parsedEvent);

        assertNotNull(normalizedEvent);

        assertEquals(
                "payment-service",
                normalizedEvent.getServiceName());

        assertEquals(
                "unknown",
                normalizedEvent.getEnvironment());

        assertEquals(
                "exception",
                normalizedEvent.getEventType());

        assertEquals(
                "PSQLException",
                normalizedEvent.getErrorType());

        assertEquals(
                "Connection timeout after 5000ms",
                normalizedEvent.getErrorMessage());

        assertEquals(
                "high",
                normalizedEvent.getSeverity());

        assertEquals(
                FIXED_TIME,
                normalizedEvent.getOccurredAt());

        assertEquals(
                NormalizationStatus.PARTIALLY_NORMALIZED,
                normalizedEvent.getNormalizationStatus());
    }

    @Test
    void shouldDefaultMissingErrorMessage() {
        ParsedFailureEvent parsedEvent = ParsedFailureEvent.parsed(
                null,
                "payment-service",
                "prod",
                "ERROR",
                "PSQLException",
                null,
                null,
                null,
                "HIGH",
                FIXED_TIME);

        NormalizedFailureEvent normalizedEvent = failureEventNormalizer.normalize(parsedEvent);

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
                "PSQLException",
                normalizedEvent.getErrorType());

        assertEquals(
                "",
                normalizedEvent.getErrorMessage());

        assertEquals(
                "high",
                normalizedEvent.getSeverity());

        assertEquals(
                FIXED_TIME,
                normalizedEvent.getOccurredAt());

        assertEquals(
                NormalizationStatus.PARTIALLY_NORMALIZED,
                normalizedEvent.getNormalizationStatus());
    }

    @Test
    void shouldDefaultMissingSeverity() {

        ParsedFailureEvent parsedEvent = ParsedFailureEvent.parsed(
                null,
                "payment-service",
                "prod",
                "ERROR",
                "PSQLException",
                "Connection timeout after 5000ms",
                null,
                null,
                null,
                FIXED_TIME);

        NormalizedFailureEvent normalizedEvent = failureEventNormalizer.normalize(parsedEvent);

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
                "PSQLException",
                normalizedEvent.getErrorType());

        assertEquals(
                "Connection timeout after 5000ms",
                normalizedEvent.getErrorMessage());

        assertEquals(
                "unknown",
                normalizedEvent.getSeverity());

        assertEquals(
                FIXED_TIME,
                normalizedEvent.getOccurredAt());

        assertEquals(
                NormalizationStatus.PARTIALLY_NORMALIZED,
                normalizedEvent.getNormalizationStatus());
    }

    @Test
    void shouldDefaultMissingOccurredAtUsingInjectedClock() {

        ParsedFailureEvent parsedEvent = ParsedFailureEvent.parsed(
                null,
                "payment-service",
                "prod",
                "ERROR",
                "PSQLException",
                "Connection timeout after 5000ms",
                null,
                null,
                "HIGH",
                null);

        NormalizedFailureEvent normalizedEvent = failureEventNormalizer.normalize(parsedEvent);

        assertNotNull(normalizedEvent);

        assertEquals(
                FIXED_TIME,
                normalizedEvent.getOccurredAt());

        assertEquals(
                NormalizationStatus.PARTIALLY_NORMALIZED,
                normalizedEvent.getNormalizationStatus());
    }

    @Test
    void shouldPreserveUnknownErrorType() {

        ParsedFailureEvent parsedEvent = ParsedFailureEvent.parsed(
                null,
                "payment-service",
                "prod",
                "ERROR",
                "SomeRandomVendorException",
                "Something failed unexpectedly",
                null,
                null,
                "HIGH",
                FIXED_TIME);

        NormalizedFailureEvent normalizedEvent = failureEventNormalizer.normalize(parsedEvent);

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
                "SomeRandomVendorException",
                normalizedEvent.getErrorType());

        assertEquals(
                "Something failed unexpectedly",
                normalizedEvent.getErrorMessage());

        assertEquals(
                "high",
                normalizedEvent.getSeverity());

        assertEquals(
                FIXED_TIME,
                normalizedEvent.getOccurredAt());
        assertEquals(
                Map.of(),
                normalizedEvent.getNormalizationMetadata());

        assertEquals(
                NormalizationStatus.FULLY_NORMALIZED,
                normalizedEvent.getNormalizationStatus());
    }

    @Test
    void shouldNormalizeCaseAndWhitespace() {

        ParsedFailureEvent parsedEvent = ParsedFailureEvent.parsed(
                null,
                "  PAYMENT-SERVICE  ",
                "  PROD  ",
                "  ERROR  ",
                "PSQLException",
                "Connection timeout after 5000ms",
                null,
                null,
                "  HIGH  ",
                FIXED_TIME);

        NormalizedFailureEvent normalizedEvent = failureEventNormalizer.normalize(parsedEvent);

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
                NormalizationStatus.FULLY_NORMALIZED,
                normalizedEvent.getNormalizationStatus());
    }

    @Test
    void shouldPreserveUnknownSeverityAndUseUnknownCanonicalSeverity() {

        ParsedFailureEvent parsedEvent = ParsedFailureEvent.parsed(
                null,
                "payment-service",
                "prod",
                "ERROR",
                "PSQLException",
                "Connection timeout after 5000ms",
                null,
                null,
                "EXTREME",
                FIXED_TIME);

        NormalizedFailureEvent normalizedEvent = failureEventNormalizer.normalize(parsedEvent);

        assertNotNull(normalizedEvent);

        assertEquals(
                "payment-service",
                normalizedEvent.getServiceName());

        assertEquals(
                "prod",
                normalizedEvent.getEnvironment());

        assertEquals(
                "unknown",
                normalizedEvent.getSeverity());

        assertEquals(
                NormalizationStatus.PARTIALLY_NORMALIZED,
                normalizedEvent.getNormalizationStatus());
    }

    @Test
    void shouldMarkUnknownEnvironmentAsPartiallyNormalized() {
        ParsedFailureEvent parsedEvent = ParsedFailureEvent.parsed(
                null,
                "payment-service",
                "sandbox",
                "ERROR",
                "PSQLException",
                "Connection timeout after 5000ms",
                null,
                null,
                "HIGH",
                FIXED_TIME);

        NormalizedFailureEvent normalizedEvent = failureEventNormalizer.normalize(parsedEvent);

        assertEquals("unknown", normalizedEvent.getEnvironment());
        assertEquals(true, normalizedEvent.getNormalizationMetadata().get("unknownEnvironmentValue"));
        assertEquals(
                NormalizationStatus.PARTIALLY_NORMALIZED,
                normalizedEvent.getNormalizationStatus());
    }

    @Test
    void shouldPreserveUnknownEventTypeAndMarkItAsPartiallyNormalized() {
        ParsedFailureEvent parsedEvent = ParsedFailureEvent.parsed(
                null,
                "payment-service",
                "prod",
                "vendor_outage",
                "PSQLException",
                "Connection timeout after 5000ms",
                null,
                null,
                "HIGH",
                FIXED_TIME);

        NormalizedFailureEvent normalizedEvent = failureEventNormalizer.normalize(parsedEvent);

        assertEquals("vendor_outage", normalizedEvent.getEventType());
        assertEquals(true, normalizedEvent.getNormalizationMetadata().get("unknownEventTypeValue"));
        assertEquals(
                NormalizationStatus.PARTIALLY_NORMALIZED,
                normalizedEvent.getNormalizationStatus());
    }

    @Test
    void shouldMarkEventWithoutUsefulFailureInformationAsMalformed() {

        ParsedFailureEvent parsedEvent = ParsedFailureEvent.parsed(
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null);

        NormalizedFailureEvent normalizedEvent = failureEventNormalizer.normalize(parsedEvent);

        assertNotNull(normalizedEvent);

        assertEquals(
                NormalizationStatus.MALFORMED,
                normalizedEvent.getNormalizationStatus());

        assertEquals(
                "unknown_service",
                normalizedEvent.getServiceName());

        assertEquals(
                "unknown",
                normalizedEvent.getEnvironment());

        assertEquals(
                "unknown",
                normalizedEvent.getSeverity());

        assertEquals(
                "",
                normalizedEvent.getErrorMessage());
    }
}
