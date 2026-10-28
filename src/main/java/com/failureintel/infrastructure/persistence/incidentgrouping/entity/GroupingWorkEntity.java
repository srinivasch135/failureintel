package com.failureintel.infrastructure.persistence.incidentgrouping.entity;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

import java.time.LocalDateTime;
import java.util.UUID;

@Entity
@Table(name = "grouping_work")
public class GroupingWorkEntity {
    @Id
    @Column(name = "event_id", nullable = false)
    private UUID eventId;

    @Column(nullable = false, length = 12)
    private String status;

    @Column(name = "attempt_count", nullable = false)
    private int attemptCount;

    @Column(name = "created_at", nullable = false, updatable = false)
    private LocalDateTime createdAt;

    @Column(name = "last_attempt_at")
    private LocalDateTime lastAttemptAt;

    @Column(name = "processing_started_at")
    private LocalDateTime processingStartedAt;

    @Column(name = "next_attempt_at")
    private LocalDateTime nextAttemptAt;

    @Column(name = "failure_code", length = 80)
    private String failureCode;

    @Column(name = "failure_reason", columnDefinition = "text")
    private String failureReason;

    @Column(name = "terminal_outcome", length = 12)
    private String terminalOutcome;

    @Column(name = "unassigned_reason", length = 80)
    private String unassignedReason;

    protected GroupingWorkEntity() {}

    public GroupingWorkEntity(UUID eventId, String status, int attemptCount, LocalDateTime createdAt,
            LocalDateTime lastAttemptAt, LocalDateTime processingStartedAt, LocalDateTime nextAttemptAt,
            String failureCode, String failureReason, String terminalOutcome, String unassignedReason) {
        this.eventId = eventId;
        this.status = status;
        this.attemptCount = attemptCount;
        this.createdAt = createdAt;
        this.lastAttemptAt = lastAttemptAt;
        this.processingStartedAt = processingStartedAt;
        this.nextAttemptAt = nextAttemptAt;
        this.failureCode = failureCode;
        this.failureReason = failureReason;
        this.terminalOutcome = terminalOutcome;
        this.unassignedReason = unassignedReason;
    }

    public UUID getEventId() { return eventId; }
    public String getStatus() { return status; }
    public int getAttemptCount() { return attemptCount; }
    public LocalDateTime getCreatedAt() { return createdAt; }
    public LocalDateTime getLastAttemptAt() { return lastAttemptAt; }
    public LocalDateTime getProcessingStartedAt() { return processingStartedAt; }
    public LocalDateTime getNextAttemptAt() { return nextAttemptAt; }
    public String getFailureCode() { return failureCode; }
    public String getFailureReason() { return failureReason; }
    public String getTerminalOutcome() { return terminalOutcome; }
    public String getUnassignedReason() { return unassignedReason; }

    public void claimForProcessing(LocalDateTime claimedAt) {
        if (!"PENDING".equals(status) && !"RETRYABLE".equals(status)) {
            throw new IllegalStateException("Only eligible grouping work can be claimed");
        }
        status = "PROCESSING";
        attemptCount++;
        lastAttemptAt = claimedAt;
        processingStartedAt = claimedAt;
        nextAttemptAt = null;
        failureCode = null;
        failureReason = null;
    }

    public void completeAssociated() {
        complete("ASSOCIATED", null);
    }

    public void completeUnassigned(String reason) {
        if (reason == null || reason.isBlank()) {
            throw new IllegalArgumentException("unassigned reason must not be blank");
        }
        complete("UNASSIGNED", reason);
    }

    public void markRetryable(LocalDateTime retryAt, String code, String reason) {
        requireProcessing();
        if (retryAt == null || code == null || code.isBlank() || reason == null || reason.isBlank()) {
            throw new IllegalArgumentException("retry details must not be blank");
        }
        status = "RETRYABLE";
        processingStartedAt = null;
        nextAttemptAt = retryAt;
        terminalOutcome = null;
        unassignedReason = null;
        failureCode = code;
        failureReason = reason;
    }

    public void markFailed(String code, String reason) {
        requireProcessing();
        if (code == null || code.isBlank() || reason == null || reason.isBlank()) {
            throw new IllegalArgumentException("failure details must not be blank");
        }
        status = "FAILED";
        processingStartedAt = null;
        nextAttemptAt = null;
        terminalOutcome = null;
        unassignedReason = null;
        failureCode = code;
        failureReason = reason;
    }

    public void recoverExpiredLease(LocalDateTime retryAt, boolean exhausted) {
        requireProcessing();
        if (exhausted) {
            markFailed("GROUPING_RETRY_EXHAUSTED", "Grouping worker lease expired after attempts were exhausted");
        } else {
            markRetryable(retryAt, "GROUPING_WORKER_LEASE_EXPIRED",
                    "Grouping worker did not complete the claimed attempt before lease expiry");
        }
    }

    private void requireProcessing() {
        if (!"PROCESSING".equals(status)) {
            throw new IllegalStateException("Only processing grouping work can record a failure");
        }
    }

    private void complete(String outcome, String reason) {
        if (!"PROCESSING".equals(status)) {
            throw new IllegalStateException("Only processing grouping work can be completed");
        }
        status = "COMPLETED";
        processingStartedAt = null;
        nextAttemptAt = null;
        terminalOutcome = outcome;
        unassignedReason = reason;
        failureCode = null;
        failureReason = null;
    }
}
