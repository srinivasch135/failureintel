package com.failureintel.ingestion.api.dto;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Size;
import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;

import java.time.Instant;
import java.util.Map;

@AllArgsConstructor
@NoArgsConstructor
public class FailureEventIngestionRequest {
    @NotNull(message = "Occurred at is required")
    private Instant occurredAt;
    @NotNull(message = "Service name is required")
    @Size(max = 120, message = "Service name must not exceed 120 characters")
    private String serviceName;
    @NotNull(message = "Server name is required")
    @Size(max = 255, message = "Server name must not exceed 255 characters")
    private String serverName;
    @NotBlank(message = "Environment is required")
    @Size(max = 50, message = "Environment must not exceed 50 characters")
    private String environment;
    @NotBlank(message = "Event type is required")
    @Size(max = 80, message = "Event type must not exceed 80 characters")
    private String eventType;
    @Size(max = 150, message = "Error type must not exceed 150 characters")
    private String errorType;
    @NotBlank(message = "Error message is required")
    private String errorMessage;
    @Size(max = 200, message = "Dependency target must not exceed 200 characters")
    private String dependencyTarget;
    @Size(max = 120, message = "Trace ID must not exceed 120 characters")
    private String traceId;
    @Size(max = 255, message = "Idempotency key must not exceed 255 characters")
    private String idempotencyKey;
    @Size(max = 50, message = "Severity hint must not exceed 50 characters")
    private String severityHint;
    @NotNull(message = "Raw payload is required")
    private Map<String, Object> rawPayload;

    /**
     * Compatibility constructor retained for callers compiled against the
     * pre-idempotency request shape. New callers may use the generated
     * all-arguments constructor including idempotencyKey.
     */
    public FailureEventIngestionRequest(
            Instant occurredAt,
            String serviceName,
            String serverName,
            String environment,
            String eventType,
            String errorType,
            String errorMessage,
            String dependencyTarget,
            String traceId,
            String severityHint,
            Map<String, Object> rawPayload) {
        this(
                occurredAt,
                serviceName,
                serverName,
                environment,
                eventType,
                errorType,
                errorMessage,
                dependencyTarget,
                traceId,
                null,
                severityHint,
                rawPayload);
    }

    public Instant getOccurredAt() {
        return occurredAt;
    }

    public void setOccurredAt(Instant occurredAt) {
        this.occurredAt = occurredAt;
    }

    public Instant getOccuredAt() {
        return occurredAt;
    }

    public void setOccuredAt(Instant occuredAt) {
        this.occurredAt = occuredAt;
    }

    public String getServiceName() {
        return serviceName;
    }

    public void setServiceName(String serviceName) {
        this.serviceName = serviceName;
    }

    public String getServerName() {
        return serverName;
    }

    public void setServerName(String serverName) {
        this.serverName = serverName;
    }

    public String getSourceSystem() {
        return serverName;
    }

    public void setSourceSystem(String sourceSystem) {
        this.serverName = sourceSystem;
    }

    public String getEnvironment() {
        return environment;
    }

    public void setEnvironment(String environment) {
        this.environment = environment;
    }

    public String getEventType() {
        return eventType;
    }

    public void setEventType(String eventType) {
        this.eventType = eventType;
    }

    public String getErrorType() {
        return errorType;
    }

    public void setErrorType(String errorType) {
        this.errorType = errorType;
    }

    public String getErrorMessage() {
        return errorMessage;
    }

    public void setErrorMessage(String errorMessage) {
        this.errorMessage = errorMessage;
    }

    public String getDependencyTarget() {
        return dependencyTarget;
    }

    public void setDependencyTarget(String dependencyTarget) {
        this.dependencyTarget = dependencyTarget;
    }

    public String getTraceId() {
        return traceId;
    }

    public void setTraceId(String traceId) {
        this.traceId = traceId;
    }

    public String getIdempotencyKey() {
        return idempotencyKey;
    }

    public void setIdempotencyKey(String idempotencyKey) {
        this.idempotencyKey = idempotencyKey;
    }

    public String getSeverityHint() {
        return severityHint;
    }

    public void setSeverityHint(String severityHint) {
        this.severityHint = severityHint;
    }

    public Map<String, Object> getRawPayload() {
        return rawPayload;
    }

    public void setRawPayload(Map<String, Object> rawPayload) {
        this.rawPayload = rawPayload;
    }

}
