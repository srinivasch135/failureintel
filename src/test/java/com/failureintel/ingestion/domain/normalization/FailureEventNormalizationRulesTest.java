package com.failureintel.ingestion.domain.normalization;

import com.failureintel.ingestion.domain.model.NormalizedFailureEvent;
import com.failureintel.ingestion.domain.model.ParsedFailureEvent;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.time.Clock;
import java.time.ZoneOffset;

import static com.failureintel.test.support.FailureEventTestFixtures.OCCURRED_AT;
import static com.failureintel.test.support.FailureEventTestFixtures.parsedEvent;
import static org.junit.jupiter.api.Assertions.assertEquals;

class FailureEventNormalizationRulesTest {

    private FailureEventNormalizer normalizer;

    @BeforeEach
    void setUp() {
        normalizer = new FailureEventNormalizer(Clock.fixed(OCCURRED_AT, ZoneOffset.UTC));
    }

    @ParameterizedTest(name = "environment {0} becomes {1}")
    @CsvSource({
            "production, prod",
            "prod, prod",
            "development, dev",
            "dev, dev",
            "staging, staging",
            "stage, staging",
            "qa, qa",
            "test, qa",
            "testing, qa"
    })
    void shouldNormalizeEnvironmentAliases(String input, String expected) {
        NormalizedFailureEvent result = normalizer.normalize(parsedEvent(input, "ERROR", "PSQLException", "HIGH"));

        assertEquals(expected, result.getEnvironment());
        assertEquals(NormalizationStatus.FULLY_NORMALIZED, result.getNormalizationStatus());
    }

    @ParameterizedTest(name = "event type {0} becomes {1}")
    @CsvSource({
            "exception, exception",
            "error, exception",
            "application_error, exception",
            "timeout, timeout",
            "request_timeout, timeout",
            "dependency, dependency_failure",
            "dependency_failure, dependency_failure",
            "external_service_failure, dependency_failure",
            "validation, validation_error",
            "validation_error, validation_error",
            "bad_request, validation_error"
    })
    void shouldNormalizeEventTypeAliases(String input, String expected) {
        NormalizedFailureEvent result = normalizer.normalize(parsedEvent("prod", input, "PSQLException", "HIGH"));

        assertEquals(expected, result.getEventType());
        assertEquals(NormalizationStatus.FULLY_NORMALIZED, result.getNormalizationStatus());
    }

    @ParameterizedTest(name = "error type {0} becomes {1}")
    @CsvSource({
            "psqlexception, PSQLException",
            "postgres_exception, PSQLException",
            "postgresql_error, PSQLException",
            "timeoutexception, TimeoutException",
            "timeout_exception, TimeoutException",
            "request_timeout_exception, TimeoutException"
    })
    void shouldNormalizeKnownErrorTypeAliases(String input, String expected) {
        NormalizedFailureEvent result = normalizer.normalize(parsedEvent("prod", "ERROR", input, "HIGH"));

        assertEquals(expected, result.getErrorType());
        assertEquals(NormalizationStatus.FULLY_NORMALIZED, result.getNormalizationStatus());
    }

    @ParameterizedTest(name = "severity {0} becomes {1}")
    @CsvSource({
            "critical, critical", "fatal, critical", "sev1, critical", "p0, critical",
            "high, high", "error, high", "sev2, high", "p1, high",
            "medium, medium", "warn, medium", "warning, medium", "sev3, medium", "p2, medium",
            "low, low", "info, low", "sev4, low", "p3, low"
    })
    void shouldNormalizeSeverityAliases(String input, String expected) {
        NormalizedFailureEvent result = normalizer.normalize(parsedEvent("prod", "ERROR", "PSQLException", input));

        assertEquals(expected, result.getSeverity());
        assertEquals(NormalizationStatus.FULLY_NORMALIZED, result.getNormalizationStatus());
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "password=hunter2",
            "token=abc123",
            "apiKey=key-value",
            "secret=top-secret"
    })
    void shouldSanitizeSensitiveValuesInErrorMessages(String sensitiveValue) {
        ParsedFailureEvent event = ParsedFailureEvent.parsed(
                null,
                "payment-service",
                "prod",
                "ERROR",
                "PSQLException",
                "Database failure " + sensitiveValue + " request aborted",
                null,
                "trace-sensitive",
                "HIGH",
                OCCURRED_AT);

        NormalizedFailureEvent result = normalizer.normalize(event);

        assertEquals(
                "Database failure " + sensitiveValue.substring(0, sensitiveValue.indexOf('=') + 1)
                        + "**** request aborted",
                result.getErrorMessage());
        assertEquals(true, result.getNormalizationMetadata().get("errorMessageSanitized"));
        assertEquals(NormalizationStatus.FULLY_NORMALIZED, result.getNormalizationStatus());
    }
}
