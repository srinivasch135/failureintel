package com.failureintel.infrastructure.persistence.incidentgrouping.entity;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

import java.time.LocalDateTime;
import java.util.UUID;

@Entity
@Table(name = "incident")
public class IncidentEntity {
    @Id
    @Column(name = "incident_id", nullable = false)
    private UUID incidentId;

    @Column(name = "service_name", nullable = false)
    private String serviceName;

    @Column(nullable = false, length = 100)
    private String environment;

    @Column(name = "error_type", nullable = false)
    private String errorType;

    @Column(name = "dependency_target")
    private String dependencyTarget;

    @Column(nullable = false, length = 10)
    private String status;

    @Column(name = "first_occurred_at", nullable = false)
    private LocalDateTime firstOccurredAt;

    @Column(name = "last_occurred_at", nullable = false)
    private LocalDateTime lastOccurredAt;

    @Column(name = "inactivity_expires_at", nullable = false)
    private LocalDateTime inactivityExpiresAt;

    @Column(name = "closed_at")
    private LocalDateTime closedAt;

    @Column(name = "associated_event_count", nullable = false)
    private long associatedEventCount;

    protected IncidentEntity() {}

    public IncidentEntity(UUID incidentId, String serviceName, String environment, String errorType,
            String dependencyTarget, String status, LocalDateTime firstOccurredAt, LocalDateTime lastOccurredAt,
            LocalDateTime inactivityExpiresAt, LocalDateTime closedAt, long associatedEventCount) {
        this.incidentId = incidentId;
        this.serviceName = serviceName;
        this.environment = environment;
        this.errorType = errorType;
        this.dependencyTarget = dependencyTarget;
        this.status = status;
        this.firstOccurredAt = firstOccurredAt;
        this.lastOccurredAt = lastOccurredAt;
        this.inactivityExpiresAt = inactivityExpiresAt;
        this.closedAt = closedAt;
        this.associatedEventCount = associatedEventCount;
    }

    public UUID getIncidentId() { return incidentId; }
    public String getServiceName() { return serviceName; }
    public String getEnvironment() { return environment; }
    public String getErrorType() { return errorType; }
    public String getDependencyTarget() { return dependencyTarget; }
    public String getStatus() { return status; }
    public LocalDateTime getFirstOccurredAt() { return firstOccurredAt; }
    public LocalDateTime getLastOccurredAt() { return lastOccurredAt; }
    public LocalDateTime getInactivityExpiresAt() { return inactivityExpiresAt; }
    public LocalDateTime getClosedAt() { return closedAt; }
    public long getAssociatedEventCount() { return associatedEventCount; }

    public void associateOccurrence(LocalDateTime occurredAt, LocalDateTime inactivityExpiresAt) {
        if (!"ACTIVE".equals(status)) {
            throw new IllegalStateException("Only ACTIVE incidents can accept a new occurrence");
        }
        if (occurredAt.isBefore(firstOccurredAt)) {
            firstOccurredAt = occurredAt;
        }
        if (occurredAt.isAfter(lastOccurredAt)) {
            lastOccurredAt = occurredAt;
        }
        this.inactivityExpiresAt = inactivityExpiresAt;
        associatedEventCount++;
    }
}
