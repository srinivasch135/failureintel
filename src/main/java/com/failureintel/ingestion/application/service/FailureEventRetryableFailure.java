package com.failureintel.ingestion.application.service;

import org.springframework.dao.DataAccessResourceFailureException;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.dao.PessimisticLockingFailureException;
import org.springframework.dao.QueryTimeoutException;
import org.springframework.dao.TransientDataAccessException;

import java.util.Objects;

/** Stable retryable classifications for failures during event processing. */
public enum FailureEventRetryableFailure {
    DATABASE_UNAVAILABLE(
            "DATABASE_UNAVAILABLE",
            "Database unavailable during failure-event processing"),
    DATABASE_TIMEOUT(
            "DATABASE_TIMEOUT",
            "Database operation timed out during failure-event processing"),
    DATABASE_CONCURRENCY_FAILURE(
            "DATABASE_CONCURRENCY_FAILURE",
            "Database concurrency conflict during failure-event processing"),
    TEMPORARY_INFRASTRUCTURE_FAILURE(
            "TEMPORARY_INFRASTRUCTURE_FAILURE",
            "Temporary infrastructure failure during failure-event processing"),
    NORMALIZED_WRITE_FAILURE(
            "NORMALIZED_WRITE_FAILURE",
            "Normalized failure event could not be persisted"),
    UNEXPECTED_PROCESSING_FAILURE(
            "UNEXPECTED_PROCESSING_FAILURE",
            "Unexpected failure during failure-event processing");

    private final String code;
    private final String reason;

    FailureEventRetryableFailure(String code, String reason) {
        this.code = code;
        this.reason = reason;
    }

    public String getCode() {
        return code;
    }

    public String getReason() {
        return reason;
    }

    public static FailureEventRetryableFailure classify(Throwable failure) {
        Objects.requireNonNull(failure, "failure must not be null");

        boolean databaseTimeout = false;
        boolean databaseConcurrencyFailure = false;
        boolean normalizedWriteFailure = false;
        boolean databaseUnavailable = false;
        boolean temporaryInfrastructureFailure = false;

        for (Throwable cause = failure; cause != null; cause = cause.getCause()) {
            if (cause instanceof QueryTimeoutException) {
                databaseTimeout = true;
            }
            if (cause instanceof PessimisticLockingFailureException) {
                databaseConcurrencyFailure = true;
            }
            if (cause instanceof DataIntegrityViolationException) {
                normalizedWriteFailure = true;
            }
            if (cause instanceof DataAccessResourceFailureException) {
                databaseUnavailable = true;
            }
            if (cause instanceof TransientDataAccessException) {
                temporaryInfrastructureFailure = true;
            }
        }

        if (databaseTimeout) {
            return DATABASE_TIMEOUT;
        }
        if (databaseConcurrencyFailure) {
            return DATABASE_CONCURRENCY_FAILURE;
        }
        if (normalizedWriteFailure) {
            return NORMALIZED_WRITE_FAILURE;
        }
        if (databaseUnavailable) {
            return DATABASE_UNAVAILABLE;
        }
        if (temporaryInfrastructureFailure) {
            return TEMPORARY_INFRASTRUCTURE_FAILURE;
        }
        return UNEXPECTED_PROCESSING_FAILURE;
    }
}
