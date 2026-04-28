package com.failureintel.infrastructure.persistence;

import jakarta.persistence.*;

import java.beans.BeanProperty;
import java.lang.annotation.Inherited;
import java.time.Instant;
import java.util.UUID;

import javax.annotation.processing.Generated;

@Entity
@Table(name = "failure_events")
public class FailureEventEntity {

    @Id
    @Column(name = "event_id", nullable = false, updatable = false)
    private UUID eventId;
    @Column(name = "occurred_at", nullable = false)
    private Instant occurredAt;
    @Column(name = "failure_type", nullable = false)
    private String failureType;
    @Column(name = "ingested_at", nullable = false, updatable = false)
    private Instant ingestedAt;
    @Column(name = "service_name", nullable = false, length = 120)
    private String serviceName;
    @Column(name = "environment", nullable = false, length = 50)
    private String environment;
    @Column(name = "event_type", nullable = false, length = 80)
    private String eventType;
    @Column(name = "error_type", length = 150)
    private String errorType;
    @Column(name = "message", columnDefinition = "TEXT")
    private String message;
    @Column(name = "dependency_target", length = 200)
    private String dependencyTarget;
    @Column(name = "trace_id", length = 120)
    private String traceId;
    @Column(name = "severity_hint", length = 50)
    private String severityHint;
    @Column(name = "raw_payload", columnDefinition = "TEXT")
    private String rawPayload;
    @Column(name = "normalized_payload", columnDefinition = "TEXT")
    private String normalizedPayload;
    @Enumerated(EnumType.STRING)
    @Column(name = "processing_status", nullable = false, length = 20)
    private ProcessingStatus processingStatus;
    @Column(name = "incident_id")
    private UUID incidentId;

    @PrePersist
    public void OnCreate() {
        if (this.ingestedAt == null) {
            this.ingestedAt = Instant.now();
        }
        if (this.processingStatus == null) {
            this.processingStatus = ProcessingStatus.RECEIVED;
        }
    }

    public UUID getEventId() {
        return eventId;
    }

    public void setEventId(UUID eventId) {
        this.eventId = eventId;
    }

    public Instant getOccurredAt() {
        return occurredAt;
    }

    public void setOccurredAt(Instant occurredAt) {
        this.occurredAt = occurredAt;
    }

    public Instant getIngestedAt() {
        return ingestedAt;
    }

    public void setIngestedAt(Instant ingestedAt) {
        this.ingestedAt = ingestedAt;
    }

    public String getServiceName() {
        return serviceName;
    }

    public void setServiceName(String serviceName) {
        this.serviceName = serviceName;
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

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
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

    public String getRawPayload() {
        return rawPayload;
    }

    public void setRawPayload(String rawPayload) {
        this.rawPayload = rawPayload;
    }

    public String getNormalizedPayload() {
        return normalizedPayload;
    }

    public void setNormalizedPayload(String normalizedPayload) {
        this.normalizedPayload = normalizedPayload;
    }

    public ProcessingStatus getProcessingStatus() {
        return processingStatus;
    }

    public void setProcessingStatus(ProcessingStatus processingStatus) {
        this.processingStatus = processingStatus;
    }

    public UUID getIncidentId() {
        return incidentId;
    }

    public void setIncidentId(UUID incidentId) {
        this.incidentId = incidentId;
    }

}
