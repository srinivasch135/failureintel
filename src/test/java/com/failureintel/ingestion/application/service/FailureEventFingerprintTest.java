package com.failureintel.ingestion.application.service;

import com.failureintel.ingestion.domain.model.RawFailureEvent;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.Map;

import static com.failureintel.test.support.FailureEventTestFixtures.rawEvent;
import static com.failureintel.test.support.FailureEventTestFixtures.OCCURRED_AT;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class FailureEventFingerprintTest {

    @Test
    void shouldIgnoreJsonObjectPropertyOrder() {
        Map<String, Object> firstPayload = new LinkedHashMap<>();
        firstPayload.put("host", "payment-prod-01");
        firstPayload.put("region", "us-east-1");
        Map<String, Object> secondPayload = new LinkedHashMap<>();
        secondPayload.put("region", "us-east-1");
        secondPayload.put("host", "payment-prod-01");

        RawFailureEvent first = rawEvent(
                "Payment-Service", "production", "ERROR", "PSQLException",
                "Connection timeout", "payment-database", "trace-001", "HIGH",
                OCCURRED_AT, firstPayload, Map.of("source", "a"));
        RawFailureEvent second = rawEvent(
                "Payment-Service", "production", "ERROR", "PSQLException",
                "Connection timeout", "payment-database", "trace-999", "HIGH",
                OCCURRED_AT, secondPayload, Map.of("source", "a"));

        assertEquals(FailureEventFingerprint.calculate(first), FailureEventFingerprint.calculate(second));
    }

    @Test
    void shouldChangeWhenRawContentChangesAndIncludeVersion() {
        RawFailureEvent first = rawEvent(Map.of("message", "timeout"));
        RawFailureEvent second = rawEvent(Map.of("message", "connection refused"));

        String fingerprint = FailureEventFingerprint.calculate(first);
        assertTrue(fingerprint.matches("v1:[0-9a-f]{64}"));
        assertNotEquals(fingerprint, FailureEventFingerprint.calculate(second));
    }
}
