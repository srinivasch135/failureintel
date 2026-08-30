package com.failureintel.test.support;

import com.failureintel.ingestion.api.dto.FailureEventIngestionRequest;
import com.failureintel.ingestion.domain.model.ParsedFailureEvent;
import com.failureintel.ingestion.domain.model.RawFailureEvent;

import java.time.Instant;
import java.util.Map;
import java.util.UUID;

public final class FailureEventTestFixtures {

    public static final Instant OCCURRED_AT = Instant.parse("2026-08-03T20:00:00Z");
    public static final Instant RECEIVED_AT = Instant.parse("2026-08-03T20:00:05Z");

    private FailureEventTestFixtures() {
    }

    public static FailureEventIngestionRequest validRequest(String traceId) {
        FailureEventIngestionRequest request = new FailureEventIngestionRequest();
        request.setServerName("datadog");
        request.setServiceName("Payment-Service");
        request.setEnvironment("production");
        request.setEventType("ERROR");
        request.setErrorType("PSQLException");
        request.setErrorMessage("Connection timeout after 5000ms");
        request.setDependencyTarget("payment-database");
        request.setTraceId(traceId);
        request.setSeverityHint("HIGH");
        request.setOccurredAt(OCCURRED_AT);
        request.setRawPayload(Map.of(
                "host", "payment-prod-01",
                "region", "us-east-1"));
        return request;
    }

    public static RawFailureEvent rawEvent(Map<String, Object> payload) {
        return rawEvent(null, null, null, null, null, null, null, null, null, payload, Map.of());
    }

    public static RawFailureEvent rawEvent(
            String serviceName,
            String environment,
            String eventType,
            String errorType,
            String errorMessage,
            String dependencyTarget,
            String traceId,
            String severityHint,
            Instant occurredAt,
            Map<String, Object> payload,
            Map<String, Object> metadata) {
        return new RawFailureEvent(
                UUID.fromString("7ce1765f-57c2-41f7-88ab-3e10c6ca9e77"),
                "test-source",
                serviceName,
                environment,
                eventType,
                errorType,
                errorMessage,
                dependencyTarget,
                traceId,
                severityHint,
                occurredAt,
                RECEIVED_AT,
                payload,
                metadata);
    }

    public static ParsedFailureEvent parsedEvent(
            String environment,
            String eventType,
            String errorType,
            String severity) {
        return ParsedFailureEvent.parsed(
                null,
                "payment-service",
                environment,
                eventType,
                errorType,
                "Connection timeout after 5000ms",
                "payment-database",
                "trace-contract-001",
                severity,
                OCCURRED_AT);
    }
}
