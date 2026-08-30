package com.failureintel.ingestion.domain.model;

import java.time.Instant;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;

public class RawFailureEvent {
    private final UUID rawEventId;
    private final String sourceSystem;
    private final String serviceName;
    private final String environment;
    private final String eventType;
    private final String errorType;
    private final String errorMessage;
    private final String dependencyTarget;
    private final String traceId;
    private final String severityHint;
    private final Instant occurredAt;
    private final Instant receivedAt;
    private final Map<String, Object> rawPayload;
    private final Map<String, Object> metaData;

    public RawFailureEvent(UUID rawEventId, String sourceSystem, String serviceName, String environment,
            String eventType, String errorType, String errorMessage, String dependencyTarget, String traceId,
            String severityHint, Instant occurredAt, Instant receivedAt, Map<String, Object> rawPayload,
            Map<String, Object> metaData) {
        this.rawEventId = Objects.requireNonNull(rawEventId, "rawEventId cannot be null");
        this.sourceSystem = sourceSystem;
        this.serviceName = serviceName;
        this.environment = environment;
        this.eventType = eventType;
        this.errorType = errorType;
        this.errorMessage = errorMessage;
        this.dependencyTarget = dependencyTarget;
        this.traceId = traceId;
        this.severityHint = severityHint;
        this.occurredAt = occurredAt;
        this.receivedAt = Objects.requireNonNull(receivedAt, "receivedAt cannot be null");
        this.rawPayload = rawPayload;
        this.metaData = metaData;

    }

    public UUID getRawEventId() {
        return rawEventId;
    }

    public String getSourceSystem() {
        return sourceSystem;
    }

    public String getServiceName() {
        return serviceName;
    }

    public String getEnvironment() {
        return environment;
    }

    public String getEventType() {
        return eventType;
    }

    public String getErrorType() {
        return errorType;
    }

    public String getErrorMessage() {
        return errorMessage;
    }

    public String getDependencyTarget() {
        return dependencyTarget;
    }

    public String getTraceId() {
        return traceId;
    }

    public String getSeverityHint() {
        return severityHint;
    }

    public Instant getOccurredAt() {
        return occurredAt;
    }

    public Instant getOccuredAt() {
        return occurredAt;
    }

    public Instant getReceivedAt() {
        return receivedAt;
    }

    public Map<String, Object> getRawPayload() {
        return rawPayload;
    }

    public Map<String, Object> getMetaData() {
        return metaData;
    }
}