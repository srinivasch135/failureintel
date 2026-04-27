package com.failureintel.ingestion.api.dto;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import java.time.Instant;
import java.util.Map;

public class FailureEventIngestionRequest {
    @NotNull(message = "Occurred at is required")
    private Instant occurredAt;
    @NotNull(message = "Service name is required")
    private String serviceName;
    @NotNull(message = "Server name is required")
    private String serverName;
    @NotBlank(message = "Environment is required")
    private String environment;
    @NotBlank(message = "Event type is required")
    private String eventType;
    private String errorType;
    @NotBlank(message = "Error message is required")
    private String errorMessage;
    private String dependencyTarget;
    private String traceId;
    private String severityHint;
    @NotNull(message = "Raw payload is required")
    private Map<String, Object> rawPayload;

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
