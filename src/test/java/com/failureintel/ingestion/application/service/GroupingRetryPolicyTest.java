package com.failureintel.ingestion.application.service;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class GroupingRetryPolicyTest {
    private final GroupingFailureClassifier classifier = new GroupingFailureClassifier();

    @Test
    void boundsDelaySchedule() {
        GroupingRetryPolicy policy = new GroupingRetryPolicy(5,
                List.of(Duration.ofSeconds(10), Duration.ofMinutes(1)));

        assertEquals(Duration.ofSeconds(10), policy.delayAfter(1));
        assertEquals(Duration.ofMinutes(1), policy.delayAfter(4));
    }

    @Test
    void postgresStateTransient() {
        GroupingFailureClassifier.Failure failure = classifier.classify(
                new java.sql.SQLException("serialization failure", "40001"));

        assertTrue(failure.transientFailure());
        assertEquals("GROUPING_DATABASE_TRANSIENT", failure.code());
    }

    @Test
    void unexpectedIsPermanent() {
        GroupingFailureClassifier.Failure failure = classifier.classify(
                new IllegalArgumentException("invalid state"));

        assertFalse(failure.transientFailure());
        assertEquals("GROUPING_FAILURE", failure.code());
    }
}
