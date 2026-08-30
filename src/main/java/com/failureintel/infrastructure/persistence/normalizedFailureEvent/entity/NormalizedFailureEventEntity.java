package com.failureintel.infrastructure.persistence.normalizedFailureEvent.entity;

import com.failureintel.ingestion.domain.model.NormalizedFailureEvent;
import com.failureintel.ingestion.domain.normalization.NormalizationStatus;
import com.failureintel.infrastructure.persistence.failureevent.entity.FailureEventEntity;
import jakarta.persistence.*;
import org.hibernate.annotations.JdbcTypeCode;
import org.hibernate.annotations.OnDelete;
import org.hibernate.annotations.OnDeleteAction;
import org.springframework.data.domain.Persistable;
import org.hibernate.type.SqlTypes;

import java.time.Instant;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;

@Entity
@Table(name = "normalized_failure_event")
public class NormalizedFailureEventEntity implements Persistable<UUID> {
    @Id
    @Column(name = "event_id", nullable = false)
    private UUID eventId;

    @OneToOne(fetch = FetchType.LAZY)
    @MapsId
    @JoinColumn(name = "event_id")
    @OnDelete(action = OnDeleteAction.CASCADE)
    private FailureEventEntity failureEvent;

    @JdbcTypeCode(SqlTypes.JSON)
    @Column(name = "normalized_payload", columnDefinition = "jsonb")
    private Map<String, Object> normalizedPayload;

    @Column(name = "normalized_service_name")
    private String normalizedServiceName;

    @Column(name = "normalized_environment", length = 100)
    private String normalizedEnvironment;

    @Column(name = "normalized_event_type", length = 100)
    private String normalizedEventType;

    @Column(name = "normalized_error_type")
    private String normalizedErrorType;

    @Column(name = "normalized_error_message", columnDefinition = "TEXT")
    private String normalizedErrorMessage;

    @Column(name = "normalized_dependency_target")
    private String normalizedDependencyTarget;

    @Column(name = "normalized_trace_id")
    private String normalizedTraceId;

    @Column(name = "normalized_severity", length = 50)
    private String normalizedSeverity;

    @Column(name = "normalized_occurred_at")
    private Instant normalizedOccurredAt;

    @Enumerated(EnumType.STRING)
    @Column(name = "normalization_status", nullable = false, length = 50)
    private NormalizationStatus normalizationStatus;

    @JdbcTypeCode(SqlTypes.JSON)
    @Column(name = "normalization_metadata", columnDefinition = "jsonb")
    private Map<String, Object> normalizationMetadata;

    @Column(name = "failure_reason", columnDefinition = "TEXT")
    private String failureReason;

    @Column(name = "normalized_at")
    private Instant normalizedAt;

    @Column(name = "created_at", nullable = false, updatable = false)
    private Instant createdAt;

    @Column(name = "updated_at", nullable = false)
    private Instant updatedAt;

    @PrePersist
    public void onCreate() {
        Instant now = Instant.now();
        if (this.normalizationStatus == null) {
            this.normalizationStatus = NormalizationStatus.PARTIALLY_NORMALIZED;
        }
        if (this.createdAt == null) {
            this.createdAt = now;
        }
        if (this.updatedAt == null) {
            this.updatedAt = now;
        }
    }

    @PreUpdate
    public void onUpdate() {
        this.updatedAt = Instant.now();
    }

    public UUID getEventId() {
        return eventId;
    }

    @Override
    public UUID getId() {
        return eventId;
    }

    @Override
    @Transient
    public boolean isNew() {
        return createdAt == null;
    }

    public void setEventId(UUID eventId) {
        this.eventId = eventId;
    }

    public FailureEventEntity getFailureEvent() {
        return failureEvent;
    }

    public void setFailureEvent(FailureEventEntity failureEvent) {
        this.failureEvent = failureEvent;
        if (failureEvent != null) {
            this.eventId = failureEvent.getEventId();
        }
    }

    public Map<String, Object> getNormalizedPayload() {
        return normalizedPayload;
    }

    public void setNormalizedPayload(Map<String, Object> normalizedPayload) {
        this.normalizedPayload = normalizedPayload;
    }

    public String getNormalizedServiceName() {
        return normalizedServiceName;
    }

    public void setNormalizedServiceName(String normalizedServiceName) {
        this.normalizedServiceName = normalizedServiceName;
    }

    public String getNormalizedEnvironment() {
        return normalizedEnvironment;
    }

    public void setNormalizedEnvironment(String normalizedEnvironment) {
        this.normalizedEnvironment = normalizedEnvironment;
    }

    public String getNormalizedEventType() {
        return normalizedEventType;
    }

    public void setNormalizedEventType(String normalizedEventType) {
        this.normalizedEventType = normalizedEventType;
    }

    public String getNormalizedErrorType() {
        return normalizedErrorType;
    }

    public void setNormalizedErrorType(String normalizedErrorType) {
        this.normalizedErrorType = normalizedErrorType;
    }

    public String getNormalizedErrorMessage() {
        return normalizedErrorMessage;
    }

    public void setNormalizedErrorMessage(String normalizedErrorMessage) {
        this.normalizedErrorMessage = normalizedErrorMessage;
    }

    public String getNormalizedDependencyTarget() {
        return normalizedDependencyTarget;
    }

    public void setNormalizedDependencyTarget(String normalizedDependencyTarget) {
        this.normalizedDependencyTarget = normalizedDependencyTarget;
    }

    public String getNormalizedTraceId() {
        return normalizedTraceId;
    }

    public void setNormalizedTraceId(String normalizedTraceId) {
        this.normalizedTraceId = normalizedTraceId;
    }

    public String getNormalizedSeverity() {
        return normalizedSeverity;
    }

    public void setNormalizedSeverity(String normalizedSeverity) {
        this.normalizedSeverity = normalizedSeverity;
    }

    public Instant getNormalizedOccurredAt() {
        return normalizedOccurredAt;
    }

    public void setNormalizedOccurredAt(Instant normalizedOccurredAt) {
        this.normalizedOccurredAt = normalizedOccurredAt;
    }

    public NormalizationStatus getNormalizationStatus() {
        return normalizationStatus;
    }

    public void setNormalizationStatus(NormalizationStatus normalizationStatus) {
        this.normalizationStatus = normalizationStatus;
    }

    public Map<String, Object> getNormalizationMetadata() {
        return normalizationMetadata;
    }

    public void setNormalizationMetadata(Map<String, Object> normalizationMetadata) {
        this.normalizationMetadata = normalizationMetadata;
    }

    public String getFailureReason() {
        return failureReason;
    }

    public void setFailureReason(String failureReason) {
        this.failureReason = failureReason;
    }

    public Instant getNormalizedAt() {
        return normalizedAt;
    }

    public void setNormalizedAt(Instant normalizedAt) {
        this.normalizedAt = normalizedAt;
    }

    public Instant getCreatedAt() {
        return createdAt;
    }

    public void setCreatedAt(Instant createdAt) {
        this.createdAt = createdAt;
    }

    public Instant getUpdatedAt() {
        return updatedAt;
    }

    public void setUpdatedAt(Instant updatedAt) {
        this.updatedAt = updatedAt;
    }

    public void applyNormalizedEvent(NormalizedFailureEvent normalizedEvent) {
        Objects.requireNonNull(normalizedEvent, "normalizedEvent cannot be null");
        this.normalizedPayload = normalizedEvent.getNormalizedPayload();
        this.normalizedServiceName = normalizedEvent.getServiceName();
        this.normalizedEnvironment = normalizedEvent.getEnvironment();
        this.normalizedEventType = normalizedEvent.getEventType();
        this.normalizedErrorType = normalizedEvent.getErrorType();
        this.normalizedErrorMessage = normalizedEvent.getErrorMessage();
        this.normalizedDependencyTarget = normalizedEvent.getDependencyTarget();
        this.normalizedTraceId = normalizedEvent.getTraceId();
        this.normalizedSeverity = normalizedEvent.getSeverity();
        this.normalizedOccurredAt = normalizedEvent.getOccurredAt();
        this.normalizedAt = normalizedEvent.getNormalizedAt();
        this.normalizationStatus = normalizedEvent.getNormalizationStatus();
        this.normalizationMetadata = normalizedEvent.getNormalizationMetadata();
    }

}
