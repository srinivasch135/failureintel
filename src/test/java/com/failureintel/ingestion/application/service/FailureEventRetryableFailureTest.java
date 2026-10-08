package com.failureintel.ingestion.application.service;

import org.junit.jupiter.api.Test;
import org.springframework.dao.CannotAcquireLockException;
import org.springframework.dao.DataAccessResourceFailureException;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.dao.QueryTimeoutException;
import org.springframework.dao.TransientDataAccessResourceException;
import org.springframework.transaction.TransactionSystemException;

import static org.junit.jupiter.api.Assertions.assertEquals;

class FailureEventRetryableFailureTest {

    @Test
    void shouldClassifyDatabaseUnavailable() {
        assertClassification(
                new DataAccessResourceFailureException("password=secret"),
                FailureEventRetryableFailure.DATABASE_UNAVAILABLE,
                "Database unavailable during failure-event processing");
    }

    @Test
    void shouldClassifyDatabaseTimeout() {
        assertClassification(
                new QueryTimeoutException("sensitive SQL text"),
                FailureEventRetryableFailure.DATABASE_TIMEOUT,
                "Database operation timed out during failure-event processing");
    }

    @Test
    void shouldPreferSpecificCauseWhenWrappedByAnotherRetryableFailure() {
        assertClassification(
                new TransactionSystemException(
                        "outer transaction detail",
                        new QueryTimeoutException("sensitive SQL text")),
                FailureEventRetryableFailure.DATABASE_TIMEOUT,
                "Database operation timed out during failure-event processing");
    }

    @Test
    void shouldClassifyDatabaseConcurrencyFailure() {
        assertClassification(
                new CannotAcquireLockException("database-specific detail"),
                FailureEventRetryableFailure.DATABASE_CONCURRENCY_FAILURE,
                "Database concurrency conflict during failure-event processing");
    }

    @Test
    void shouldClassifyOtherTransientDataAccessFailureAsInfrastructureFailure() {
        assertClassification(
                new TransientDataAccessResourceException("unstable vendor detail"),
                FailureEventRetryableFailure.TEMPORARY_INFRASTRUCTURE_FAILURE,
                "Temporary infrastructure failure during failure-event processing");
    }

    @Test
    void shouldClassifyNormalizedWriteConstraintFailureWithoutPersistingExceptionText() {
        assertClassification(
                new DataIntegrityViolationException("table=normalized event secret=value"),
                FailureEventRetryableFailure.NORMALIZED_WRITE_FAILURE,
                "Normalized failure event could not be persisted");
    }

    @Test
    void shouldClassifyUnknownProcessingFailureWithoutPersistingExceptionText() {
        assertClassification(
                new IllegalStateException("secret=value"),
                FailureEventRetryableFailure.UNEXPECTED_PROCESSING_FAILURE,
                "Unexpected failure during failure-event processing");
    }

    private void assertClassification(
            Throwable failure,
            FailureEventRetryableFailure expected,
            String safeReason) {
        FailureEventRetryableFailure actual = FailureEventRetryableFailure.classify(failure);

        assertEquals(expected, actual);
        assertEquals(expected.name(), actual.getCode());
        assertEquals(safeReason, actual.getReason());
    }
}
