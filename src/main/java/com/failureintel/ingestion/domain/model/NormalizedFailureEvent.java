package com.failureintel.ingestion.domain.model;

import com.failureintel.ingestion.domain.normalization.NormalizationStatus;

import java.time.Instant;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;

public class NormalizedFailureEvent {
    private final UUID eventId;
    private final String serviceName;
    private final String environment;
    private final String eventType;
    private final String errorType;
    private final String errorMessage;
    private final String dependencyTarget;
    private final String traceId;
    private final String severity;
    private final Instant occurredAt;
    private final Instant normalizedAt;
    private final NormalizationStatus normalizationStatus;
    private final Map<String, Object> normalizedPayload;
    private final Map<String, Object> sourceMetadata;
    private final Map<String, Object> normalizationMetadata;

    public NormalizedFailureEvent(
            UUID eventId,
            String serviceName,
            String environment,
            String eventType,
            String errorType,
            String errorMessage,
            String dependencyTarget,
            String traceId,
            String severity,
            Instant occurredAt,
            Instant normalizedAt,
            NormalizationStatus normalizationStatus,
            Map<String, Object> normalizedPayload,
            Map<String, Object> sourceMetadata,
            Map<String, Object> normalizationMetadata) {
        this.eventId = Objects.requireNonNull(eventId, "eventId must not be null");
        this.serviceName = serviceName == null ? null : serviceName.trim();
        this.environment = environment;
        this.eventType = eventType;
        this.errorType = errorType;
        this.errorMessage = errorMessage;
        this.dependencyTarget = dependencyTarget;
        this.traceId = traceId;
        this.severity = severity;
        this.occurredAt = occurredAt;
        this.normalizedAt = normalizedAt == null ? Instant.now() : normalizedAt;
        this.normalizationStatus = Objects.requireNonNull(normalizationStatus, "normalizationStatus must not be null");
        this.normalizedPayload = normalizedPayload == null ? Collections.emptyMap()
                : Collections.unmodifiableMap(normalizedPayload);
        this.sourceMetadata = sourceMetadata == null ? Collections.emptyMap()
                : Collections.unmodifiableMap(sourceMetadata);
        this.normalizationMetadata = normalizationMetadata == null
                ? Map.of()
                : Map.copyOf(normalizationMetadata);
    }

    public UUID getEventId() {
        return eventId;
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

    public String getSeverity() {
        return severity;
    }

    public Instant getOccurredAt() {
        return occurredAt;
    }

    public Instant getNormalizedAt() {
        return normalizedAt;
    }

    public NormalizationStatus getNormalizationStatus() {
        return normalizationStatus;
    }

    public Map<String, Object> getNormalizedPayload() {
        return normalizedPayload;
    }

    public Map<String, Object> getSourceMetadata() {
        return sourceMetadata;
    }

    public Map<String, Object> getNormalizationMetadata() {
        return normalizationMetadata;
    }
}
