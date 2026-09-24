package com.failureintel.infrastructure.persistence.failureevent.entity;

import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.Arrays;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertNull;

class FailureEventEntityTest {

    private static final Instant FIRST_ATTEMPT = Instant.parse("2026-09-10T12:00:00Z");
    private static final Instant RETRY_AT = Instant.parse("2026-09-10T12:05:00Z");

    @Test
    void shouldApplyReceivedAndZeroAttemptDefaultsBeforePersist() {
        FailureEventEntity entity = new FailureEventEntity();

        entity.OnCreate();

        assertNotNull(entity.getIngestedAt());
        assertEquals(ProcessingStatus.RECEIVED, entity.getProcessingStatus());
        assertEquals(0, entity.getAttemptCount());
    }

    @Test
    void shouldPreserveExplicitLifecycleValuesBeforePersist() {
        Instant ingestedAt = Instant.parse("2026-09-04T12:00:00Z");
        FailureEventEntity entity = new FailureEventEntity();
        entity.setIngestedAt(ingestedAt);
        entity.setProcessingStatus(ProcessingStatus.PROCESSING);
        entity.setAttemptCount(2);

        entity.OnCreate();

        assertEquals(ingestedAt, entity.getIngestedAt());
        assertEquals(ProcessingStatus.PROCESSING, entity.getProcessingStatus());
        assertEquals(2, entity.getAttemptCount());
    }

    @Test
    void shouldClaimReceivedEventForProcessingAndClearPreviousAttemptDetails() {
        FailureEventEntity entity = entityWithStatus(ProcessingStatus.RECEIVED);
        entity.setAttemptCount(0);
        entity.setNextAttemptAt(RETRY_AT);
        entity.setFailureCode("OLD_FAILURE");
        entity.setFailureReason("old reason");

        entity.claimForProcessing(FIRST_ATTEMPT);

        assertEquals(ProcessingStatus.PROCESSING, entity.getProcessingStatus());
        assertEquals(1, entity.getAttemptCount());
        assertEquals(FIRST_ATTEMPT, entity.getLastAttemptAt());
        assertEquals(FIRST_ATTEMPT, entity.getProcessingStartedAt());
        assertNull(entity.getNextAttemptAt());
        assertNull(entity.getFailureCode());
        assertNull(entity.getFailureReason());
    }

    @Test
    void shouldClaimRetryableEventAndIncrementAttemptCount() {
        FailureEventEntity entity = entityWithStatus(ProcessingStatus.RETRYABLE);
        entity.setAttemptCount(2);

        entity.claimForProcessing(FIRST_ATTEMPT);

        assertEquals(ProcessingStatus.PROCESSING, entity.getProcessingStatus());
        assertEquals(3, entity.getAttemptCount());
        assertEquals(FIRST_ATTEMPT, entity.getLastAttemptAt());
        assertEquals(FIRST_ATTEMPT, entity.getProcessingStartedAt());
    }

    @Test
    void shouldTreatNullAttemptCountAsZeroWhenClaiming() {
        FailureEventEntity entity = entityWithStatus(ProcessingStatus.RECEIVED);

        entity.claimForProcessing(FIRST_ATTEMPT);

        assertEquals(1, entity.getAttemptCount());
    }

    @Test
    void shouldMarkProcessingEventAsNormalizedAndClearProcessingMetadata() {
        FailureEventEntity entity = entityWithStatus(ProcessingStatus.PROCESSING);
        entity.setAttemptCount(2);
        entity.setLastAttemptAt(FIRST_ATTEMPT);
        entity.setProcessingStartedAt(FIRST_ATTEMPT);
        entity.setNextAttemptAt(RETRY_AT);
        entity.setFailureCode("TEMPORARY_FAILURE");
        entity.setFailureReason("temporary reason");

        entity.markNormalized();

        assertEquals(ProcessingStatus.NORMALIZED, entity.getProcessingStatus());
        assertEquals(2, entity.getAttemptCount());
        assertEquals(FIRST_ATTEMPT, entity.getLastAttemptAt());
        assertNull(entity.getProcessingStartedAt());
        assertNull(entity.getNextAttemptAt());
        assertNull(entity.getFailureCode());
        assertNull(entity.getFailureReason());
    }

    @Test
    void shouldMarkProcessingEventAsRetryableAndRecordRetryDetails() {
        FailureEventEntity entity = entityWithStatus(ProcessingStatus.PROCESSING);
        entity.setProcessingStartedAt(FIRST_ATTEMPT);

        entity.markRetryable(" TEMPORARY_FAILURE ", " temporary reason ", RETRY_AT);

        assertEquals(ProcessingStatus.RETRYABLE, entity.getProcessingStatus());
        assertEquals("TEMPORARY_FAILURE", entity.getFailureCode());
        assertEquals("temporary reason", entity.getFailureReason());
        assertEquals(RETRY_AT, entity.getNextAttemptAt());
        assertNull(entity.getProcessingStartedAt());
    }

    @Test
    void shouldMarkProcessingEventAsFailedAndClearRetryMetadata() {
        FailureEventEntity entity = entityWithStatus(ProcessingStatus.PROCESSING);
        entity.setProcessingStartedAt(FIRST_ATTEMPT);
        entity.setNextAttemptAt(RETRY_AT);

        entity.markFailed(" MALFORMED_EVENT ", " malformed event ");

        assertEquals(ProcessingStatus.FAILED, entity.getProcessingStatus());
        assertEquals("MALFORMED_EVENT", entity.getFailureCode());
        assertEquals("malformed event", entity.getFailureReason());
        assertNull(entity.getProcessingStartedAt());
        assertNull(entity.getNextAttemptAt());
    }

    @Test
    void shouldMarkFinalProcessingAttemptAsRetryExhausted() {
        FailureEventEntity entity = entityWithStatus(ProcessingStatus.PROCESSING);
        entity.setAttemptCount(5);
        entity.setProcessingStartedAt(FIRST_ATTEMPT);

        entity.markRetryExhausted(" DATABASE_TIMEOUT ", " database operation timed out ");

        assertEquals(ProcessingStatus.FAILED, entity.getProcessingStatus());
        assertEquals(5, entity.getAttemptCount());
        assertEquals("RETRY_EXHAUSTED", entity.getFailureCode());
        assertEquals(
                "Automatic retries exhausted after DATABASE_TIMEOUT: database operation timed out",
                entity.getFailureReason());
        assertNull(entity.getProcessingStartedAt());
        assertNull(entity.getNextAttemptAt());
    }

    @Test
    void shouldTerminalizeAlreadyRetryableExhaustedEventWithoutIncrementingAttempts() {
        FailureEventEntity entity = entityWithStatus(ProcessingStatus.RETRYABLE);
        entity.setAttemptCount(5);
        entity.setNextAttemptAt(RETRY_AT);

        entity.markRetryExhausted("DATABASE_TIMEOUT", "database operation timed out");

        assertEquals(ProcessingStatus.FAILED, entity.getProcessingStatus());
        assertEquals(5, entity.getAttemptCount());
        assertEquals("RETRY_EXHAUSTED", entity.getFailureCode());
        assertNull(entity.getNextAttemptAt());
    }

    @Test
    void shouldRecoverProcessingLeaseAsRetryableAndClearLeaseMetadata() {
        FailureEventEntity entity = entityWithStatus(ProcessingStatus.PROCESSING);
        entity.setProcessingStartedAt(FIRST_ATTEMPT);

        entity.recoverExpiredLease(" WORKER_LEASE_EXPIRED ", " worker stopped ", RETRY_AT);

        assertEquals(ProcessingStatus.RETRYABLE, entity.getProcessingStatus());
        assertEquals("WORKER_LEASE_EXPIRED", entity.getFailureCode());
        assertEquals("worker stopped", entity.getFailureReason());
        assertEquals(RETRY_AT, entity.getNextAttemptAt());
        assertNull(entity.getProcessingStartedAt());
    }

    @Test
    void shouldRejectClaimFromAnyNonClaimableStatus() {
        Arrays.stream(ProcessingStatus.values())
                .filter(status -> status != ProcessingStatus.RECEIVED
                        && status != ProcessingStatus.RETRYABLE)
                .forEach(status -> {
                    FailureEventEntity entity = entityWithStatus(status);

                    assertThrows(
                            IllegalStateException.class,
                            () -> entity.claimForProcessing(FIRST_ATTEMPT));
                    assertEquals(status, entity.getProcessingStatus());
                });
    }

    @Test
    void shouldRejectProcessingOnlyTransitionsFromEveryNonProcessingStatus() {
        Stream.of(
                        ProcessingStatus.RECEIVED,
                        ProcessingStatus.NORMALIZED,
                        ProcessingStatus.QUEUED,
                        ProcessingStatus.PROCESSED,
                        ProcessingStatus.FAILED)
                .forEach(status -> {
                    FailureEventEntity entity = entityWithStatus(status);

                    assertThrows(IllegalStateException.class, entity::markNormalized);
                    assertThrows(
                            IllegalStateException.class,
                            () -> entity.markRetryable("TEMPORARY_FAILURE", "temporary", RETRY_AT));
                    assertThrows(
                            IllegalStateException.class,
                            () -> entity.markFailed("PERMANENT_FAILURE", "permanent"));
                    assertThrows(
                            IllegalStateException.class,
                            () -> entity.markRetryExhausted("TEMPORARY_FAILURE", "temporary"));
                    assertThrows(
                            IllegalStateException.class,
                            () -> entity.recoverExpiredLease(
                                    "WORKER_LEASE_EXPIRED", "expired", RETRY_AT));
                    assertEquals(status, entity.getProcessingStatus());
                });
    }

    @Test
    void shouldRejectInvalidLifecycleArgumentsBeforeChangingState() {
        FailureEventEntity entity = entityWithStatus(ProcessingStatus.RECEIVED);

        assertThrows(NullPointerException.class, () -> entity.claimForProcessing(null));
        assertEquals(ProcessingStatus.RECEIVED, entity.getProcessingStatus());

        entity.setProcessingStatus(ProcessingStatus.PROCESSING);
        assertThrows(
                IllegalArgumentException.class,
                () -> entity.markRetryable(" ", "reason", RETRY_AT));
        assertEquals(ProcessingStatus.PROCESSING, entity.getProcessingStatus());

        assertThrows(
                IllegalArgumentException.class,
                () -> entity.markFailed("PERMANENT_FAILURE", " "));
        assertEquals(ProcessingStatus.PROCESSING, entity.getProcessingStatus());

        assertThrows(
                NullPointerException.class,
                () -> entity.recoverExpiredLease("WORKER_LEASE_EXPIRED", "expired", null));
        assertEquals(ProcessingStatus.PROCESSING, entity.getProcessingStatus());
    }

    private FailureEventEntity entityWithStatus(ProcessingStatus status) {
        FailureEventEntity entity = new FailureEventEntity();
        entity.setProcessingStatus(status);
        return entity;
    }
}
