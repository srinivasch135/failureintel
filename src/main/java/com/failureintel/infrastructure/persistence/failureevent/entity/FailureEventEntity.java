package com.failureintel.infrastructure.persistence.failureevent.entity;

import jakarta.persistence.*;
import org.hibernate.annotations.JdbcTypeCode;
import org.hibernate.type.SqlTypes;

import java.util.Map;
import java.util.Objects;

import java.time.Instant;
import java.util.UUID;

@Entity
@Table(name = "failure_event")
public class FailureEventEntity {

    @Id
    @Column(name = "event_id", nullable = false, updatable = false)
    private UUID eventId;
    @Column(name = "occurred_at", nullable = false)
    private Instant occurredAt;
    @Column(name = "failure_type")
    private String failureType;
    @Column(name = "ingested_at", nullable = false, updatable = false)
    private Instant ingestedAt;
    @Column(name = "service_name", nullable = false, length = 120)
    private String serviceName;
    @Column(name = "server_name", length = 255)
    private String sourceSystem;
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
    @Column(name = "idempotency_key", length = 255)
    private String idempotencyKey;
    @Column(name = "ingestion_fingerprint", length = 67)
    private String ingestionFingerprint;
    @Column(name = "severity_hint", length = 50)
    private String severityHint;
    @Column(name = "raw_payload", columnDefinition = "TEXT")
    private String rawPayload;
    @Column(name = "processing_status", nullable = false, length = 20)
    @Enumerated(EnumType.STRING)
    private ProcessingStatus processingStatus;
    @JdbcTypeCode(SqlTypes.JSON)
    @Column(name = "source_metadata", columnDefinition = "jsonb")
    private Map<String, Object> sourceMetadata;
    @Column(name = "attempt_count", nullable = false)
    private Integer attemptCount;
    @Column(name = "last_attempt_at")
    private Instant lastAttemptAt;
    @Column(name = "next_attempt_at")
    private Instant nextAttemptAt;
    @Column(name = "processing_started_at")
    private Instant processingStartedAt;
    @Column(name = "failure_code", length = 80)
    private String failureCode;
    @Column(name = "failure_reason", columnDefinition = "TEXT")
    private String failureReason;
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
        if (this.attemptCount == null) {
            this.attemptCount = 0;
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

    public String getSourceSystem() {
        return sourceSystem;
    }

    public void setSourceSystem(String sourceSystem) {
        this.sourceSystem = sourceSystem;
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

    public String getIdempotencyKey() {
        return idempotencyKey;
    }

    public void setIdempotencyKey(String idempotencyKey) {
        this.idempotencyKey = idempotencyKey;
    }

    public String getIngestionFingerprint() {
        return ingestionFingerprint;
    }

    public void setIngestionFingerprint(String ingestionFingerprint) {
        this.ingestionFingerprint = ingestionFingerprint;
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

    public ProcessingStatus getProcessingStatus() {
        return processingStatus;
    }

    public void setProcessingStatus(ProcessingStatus processingStatus) {
        this.processingStatus = processingStatus;
    }

    public String getFailureReason() {
        return failureReason;
    }

    public void setFailureReason(String failureReason) {
        this.failureReason = failureReason;
    }

    public UUID getIncidentId() {
        return incidentId;
    }

    public void setIncidentId(UUID incidentId) {
        this.incidentId = incidentId;
    }

    public Map<String, Object> getSourceMetadata() {
        return sourceMetadata;
    }

    public void setSourceMetadata(Map<String, Object> sourceMetadata) {
        this.sourceMetadata = sourceMetadata;
    }

    public Integer getAttemptCount() {
        return attemptCount;
    }

    void setAttemptCount(Integer attemptCount) {
        this.attemptCount = attemptCount;
    }

    public Instant getLastAttemptAt() {
        return lastAttemptAt;
    }

    void setLastAttemptAt(Instant lastAttemptAt) {
        this.lastAttemptAt = lastAttemptAt;
    }

    public Instant getNextAttemptAt() {
        return nextAttemptAt;
    }

    void setNextAttemptAt(Instant nextAttemptAt) {
        this.nextAttemptAt = nextAttemptAt;
    }

    public Instant getProcessingStartedAt() {
        return processingStartedAt;
    }

    void setProcessingStartedAt(Instant processingStartedAt) {
        this.processingStartedAt = processingStartedAt;
    }

    public String getFailureCode() {
        return failureCode;
    }

    void setFailureCode(String failureCode) {
        this.failureCode = failureCode;
    }

    public void claimForProcessing(Instant attemptStartedAt) {
        Objects.requireNonNull(attemptStartedAt, "attemptStartedAt must not be null");
        requireStatus(ProcessingStatus.RECEIVED, ProcessingStatus.RETRYABLE);

        int currentAttemptCount = attemptCount == null ? 0 : attemptCount;
        if (currentAttemptCount < 0) {
            throw new IllegalStateException("attemptCount must not be negative");
        }

        this.processingStatus = ProcessingStatus.PROCESSING;
        this.attemptCount = Math.incrementExact(currentAttemptCount);
        this.lastAttemptAt = attemptStartedAt;
        this.processingStartedAt = attemptStartedAt;
        this.nextAttemptAt = null;
        this.failureCode = null;
        this.failureReason = null;
    }

    public void markNormalized() {
        requireStatus(ProcessingStatus.PROCESSING);

        this.processingStatus = ProcessingStatus.NORMALIZED;
        this.processingStartedAt = null;
        this.nextAttemptAt = null;
        this.failureCode = null;
        this.failureReason = null;
    }

    public void markRetryable(String failureCode, String failureReason, Instant nextAttemptAt) {
        requireFailureDetails(failureCode, failureReason);
        Objects.requireNonNull(nextAttemptAt, "nextAttemptAt must not be null");
        requireStatus(ProcessingStatus.PROCESSING);

        this.processingStatus = ProcessingStatus.RETRYABLE;
        this.processingStartedAt = null;
        this.nextAttemptAt = nextAttemptAt;
        this.failureCode = failureCode.trim();
        this.failureReason = failureReason.trim();
    }

    public void markFailed(String failureCode, String failureReason) {
        requireFailureDetails(failureCode, failureReason);
        requireStatus(ProcessingStatus.PROCESSING);

        this.processingStatus = ProcessingStatus.FAILED;
        this.processingStartedAt = null;
        this.nextAttemptAt = null;
        this.failureCode = failureCode.trim();
        this.failureReason = failureReason.trim();
    }

    public void recoverExpiredLease(String failureCode, String failureReason, Instant nextAttemptAt) {
        requireFailureDetails(failureCode, failureReason);
        Objects.requireNonNull(nextAttemptAt, "nextAttemptAt must not be null");
        requireStatus(ProcessingStatus.PROCESSING);

        this.processingStatus = ProcessingStatus.RETRYABLE;
        this.processingStartedAt = null;
        this.nextAttemptAt = nextAttemptAt;
        this.failureCode = failureCode.trim();
        this.failureReason = failureReason.trim();
    }

    private void requireStatus(ProcessingStatus... allowedStatuses) {
        for (ProcessingStatus allowedStatus : allowedStatuses) {
            if (this.processingStatus == allowedStatus) {
                return;
            }
        }

        throw new IllegalStateException(
                "Cannot transition failure event from status " + this.processingStatus);
    }

    private void requireFailureDetails(String failureCode, String failureReason) {
        if (failureCode == null || failureCode.isBlank()) {
            throw new IllegalArgumentException("failureCode must not be blank");
        }
        if (failureReason == null || failureReason.isBlank()) {
            throw new IllegalArgumentException("failureReason must not be blank");
        }
    }

}
