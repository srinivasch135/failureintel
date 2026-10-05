package com.failureintel.ingestion.application.service;

import com.failureintel.infrastructure.persistence.failureevent.entity.FailureEventEntity;
import com.failureintel.infrastructure.persistence.failureevent.entity.ProcessingStatus;
import com.failureintel.infrastructure.persistence.failureevent.repository.FailureEventRepository;
import io.micrometer.core.instrument.MeterRegistry;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.dao.OptimisticLockingFailureException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.bean.override.mockito.MockitoSpyBean;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;
import org.springframework.transaction.support.TransactionTemplate;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;

@SpringBootTest
@Testcontainers
@TestPropertySource(properties = {
        "spring.jpa.hibernate.ddl-auto=validate",
        "failure-event.processing.retry.max-attempts=5"
})
class FailureEventClaimServiceIntegrationTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(FailureEventClaimServiceIntegrationTest.class);
    private static final Instant ELIGIBLE_RETRY_AT = Instant.parse("2026-09-15T12:00:00Z");

    @Container
    @ServiceConnection
    static final PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>(
            DockerImageName.parse("postgres:16-alpine"))
            .withDatabaseName("failureintel_claim_test")
            .withUsername("testuser")
            .withPassword("testpassword")
            .withLogConsumer(new Slf4jLogConsumer(LOGGER));

    @Autowired
    private FailureEventClaimService claimService;

    @Autowired
    private FailureEventRecoveryService recoveryService;

    @Autowired
    private FailureEventRetryStateRecorder retryStateRecorder;

    @Autowired
    private FailureEventRetryPolicy retryPolicy;

    @Autowired
    private FailureEventRepository failureEventRepository;

    @MockitoSpyBean
    private FailureEventRepository failureEventRepositorySpy;

    @MockitoSpyBean
    private FailureEventRetryPolicy retryPolicySpy;

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @Autowired
    private PlatformTransactionManager transactionManager;

    @Autowired
    private MeterRegistry meterRegistry;

    @AfterEach
    void cleanUp() {
        failureEventRepository.deleteAll();
    }

    @Test
    void shouldClaimReceivedAndDueRetryableEventsOnlyWithinBatchLimit() {
        FailureEventEntity received = persistEvent("received", ProcessingStatus.RECEIVED,
                Instant.parse("2026-09-15T10:00:00Z"));
        FailureEventEntity dueRetryable = persistRetryableEvent("due-retryable",
                Instant.parse("2026-09-15T10:01:00Z"), ELIGIBLE_RETRY_AT);
        FailureEventEntity futureRetryable = persistRetryableEvent("future-retryable",
                Instant.parse("2026-09-15T10:02:00Z"), Instant.parse("2999-01-01T00:00:00Z"));
        FailureEventEntity processing = persistProcessingEvent(
                "processing",
                Instant.now());
        FailureEventEntity normalized = persistEvent("normalized", ProcessingStatus.NORMALIZED,
                Instant.parse("2026-09-15T10:04:00Z"));
        FailureEventEntity failed = persistEvent("failed", ProcessingStatus.FAILED,
                Instant.parse("2026-09-15T10:05:00Z"));

        List<ClaimedFailureEvent> claims = claimService.claimNextEligibleForProcessing(2);
        List<UUID> claimedIds = claims.stream()
                .map(ClaimedFailureEvent::eventId)
                .toList();

        assertEquals(List.of(received.getEventId(), dueRetryable.getEventId()), claimedIds);
        assertClaimed(received.getEventId(), 1);
        assertClaimed(dueRetryable.getEventId(), 2);
        assertStatus(futureRetryable.getEventId(), ProcessingStatus.RETRYABLE);
        assertStatus(processing.getEventId(), ProcessingStatus.PROCESSING);
        assertStatus(normalized.getEventId(), ProcessingStatus.NORMALIZED);
        assertStatus(failed.getEventId(), ProcessingStatus.FAILED);
    }

    @Test
    void shouldRecordClaimMetadataAndClearPreviousRetryDetails() {
        FailureEventEntity retryable = persistRetryableEvent(
                "retryable-metadata",
                Instant.parse("2026-09-15T10:00:00Z"),
                ELIGIBLE_RETRY_AT);

        List<ClaimedFailureEvent> claims = claimService.claimNextEligibleForProcessing(1);
        List<UUID> claimedIds = claims.stream()
                .map(ClaimedFailureEvent::eventId)
                .toList();

        assertEquals(List.of(retryable.getEventId()), claimedIds);
        FailureEventEntity claimed = failureEventRepository.findById(retryable.getEventId()).orElseThrow();
        assertEquals(ProcessingStatus.PROCESSING, claimed.getProcessingStatus());
        assertEquals(2, claimed.getAttemptCount());
        assertNotNull(claimed.getLastAttemptAt());
        assertEquals(claimed.getLastAttemptAt(), claimed.getProcessingStartedAt());
        assertNull(claimed.getNextAttemptAt());
        assertNull(claimed.getFailureCode());
        assertNull(claimed.getFailureReason());
    }

    @Test
    void shouldRejectNonPositiveBatchSizeWithoutQueryingRepository() {
        assertThrows(
                IllegalArgumentException.class,
                () -> claimService.claimNextEligibleForProcessing(0));
        assertEquals(0, failureEventRepository.count());
    }

    @Test
    void shouldClaimDisjointBatchesWhenWorkersRunConcurrently() throws Exception {
        Set<UUID> expectedIds = new HashSet<>();
        for (int index = 0; index < 6; index++) {
            expectedIds.add(persistEvent(
                    "concurrent-" + index,
                    ProcessingStatus.RECEIVED,
                    Instant.parse("2026-09-15T10:00:00Z").plusSeconds(index)).getEventId());
        }

        CountDownLatch start = new CountDownLatch(1);
        ExecutorService executor = Executors.newFixedThreadPool(2);
        try {
            Future<List<ClaimedFailureEvent>> first = executor.submit(() -> claimAfter(start, 3));
            Future<List<ClaimedFailureEvent>> second = executor.submit(() -> claimAfter(start, 3));
            start.countDown();

            Set<UUID> firstBatch = first.get(10, TimeUnit.SECONDS).stream()
                    .map(ClaimedFailureEvent::eventId)
                    .collect(Collectors.toSet());
            Set<UUID> secondBatch = second.get(10, TimeUnit.SECONDS).stream()
                    .map(ClaimedFailureEvent::eventId)
                    .collect(Collectors.toSet());
            Set<UUID> claimedIds = new HashSet<>(firstBatch);
            claimedIds.addAll(secondBatch);

            assertEquals(3, firstBatch.size());
            assertEquals(3, secondBatch.size());
            assertTrue(disjoint(firstBatch, secondBatch));
            assertEquals(expectedIds, claimedIds);
            assertEquals(6, failureEventRepository.findByProcessingStatus(
                    ProcessingStatus.PROCESSING,
                    org.springframework.data.domain.Pageable.unpaged()).getTotalElements());
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    void shouldSkipALockedRowWhenAnotherWorkerOwnsIt() throws Exception {
        FailureEventEntity lockedEvent = persistEvent(
                "locked-oldest",
                ProcessingStatus.RECEIVED,
                Instant.parse("2026-09-15T10:00:00Z"));
        FailureEventEntity availableEvent = persistEvent(
                "available-next",
                ProcessingStatus.RECEIVED,
                Instant.parse("2026-09-15T10:01:00Z"));

        CountDownLatch lockAcquired = new CountDownLatch(1);
        CountDownLatch releaseLock = new CountDownLatch(1);
        ExecutorService executor = Executors.newFixedThreadPool(2);
        try {
            Future<?> lockHolder = executor.submit(() -> holdRowLock(
                    lockedEvent.getEventId(),
                    lockAcquired,
                    releaseLock));
            assertTrue(lockAcquired.await(5, TimeUnit.SECONDS));

            Future<List<ClaimedFailureEvent>> claim = executor.submit(
                    () -> claimService.claimNextEligibleForProcessing(1));
            List<UUID> claimedIds = claim.get(5, TimeUnit.SECONDS).stream()
                    .map(ClaimedFailureEvent::eventId)
                    .toList();

            assertEquals(List.of(availableEvent.getEventId()), claimedIds);
            releaseLock.countDown();
            lockHolder.get(5, TimeUnit.SECONDS);
        } finally {
            releaseLock.countDown();
            executor.shutdownNow();
        }
    }

    @Test
    void shouldAdvanceAttemptsAndExhaustExactlyAtConfiguredMaximum() {
        double retriesBefore = counter("retry.scheduled", "source", "processing");
        double processingFailuresBefore = counter("failed", "source", "processing");
        FailureEventEntity event = persistEvent(
                "retry-limit-boundary",
                ProcessingStatus.RECEIVED,
                Instant.parse("2026-09-15T10:00:00Z"));
        int maxAttempts = retryPolicy.getMaxAttempts();

        for (int expectedAttempt = 1; expectedAttempt <= maxAttempts; expectedAttempt++) {
            List<ClaimedFailureEvent> claims = claimService.claimNextEligibleForProcessing(1);
            assertEquals(1, claims.size());
            ClaimedFailureEvent claim = claims.get(0);
            assertEquals(event.getEventId(), claim.eventId());
            assertEquals(expectedAttempt, claim.attemptNumber());

            Optional<Instant> nextAttemptAt = retryPolicy.nextAttemptAt(expectedAttempt);
            assertEquals(expectedAttempt < maxAttempts, nextAttemptAt.isPresent());
            assertTrue(retryStateRecorder.recordFailure(
                    claim,
                    FailureEventRetryableFailure.DATABASE_TIMEOUT,
                    nextAttemptAt));

            FailureEventEntity persisted = failureEventRepository.findById(event.getEventId()).orElseThrow();
            assertEquals(expectedAttempt, persisted.getAttemptCount());
            if (expectedAttempt < maxAttempts) {
                assertEquals(ProcessingStatus.RETRYABLE, persisted.getProcessingStatus());
                jdbcTemplate.update(
                        "UPDATE failure_event SET next_attempt_at = ? WHERE event_id = ?",
                        Timestamp.from(Instant.now().minusSeconds(1)),
                        event.getEventId());
            } else {
                assertEquals(ProcessingStatus.FAILED, persisted.getProcessingStatus());
                assertEquals("RETRY_EXHAUSTED", persisted.getFailureCode());
                assertNull(persisted.getNextAttemptAt());
            }
        }

        assertTrue(claimService.claimNextEligibleForProcessing(1).isEmpty());
        assertEquals(retriesBefore + retryPolicy.getMaxAttempts() - 1,
                counter("retry.scheduled", "source", "processing"));
        assertEquals(processingFailuresBefore + 1, counter("failed", "source", "processing"));
    }

    @Test
    void shouldTerminalizeRetryableRowAtLimitEvenWhenItsRetryTimeIsInTheFuture() {
        double claimFailuresBefore = counter("failed", "source", "claim");
        FailureEventEntity event = persistRetryableEvent(
                "retry-limit-config-change",
                Instant.parse("2026-09-15T10:00:00Z"),
                Instant.parse("2999-01-01T00:00:00Z"));
        int exhaustedAttemptCount = retryPolicy.getMaxAttempts();
        jdbcTemplate.update(
                "UPDATE failure_event SET attempt_count = ?, version = version + 1 WHERE event_id = ?",
                exhaustedAttemptCount,
                event.getEventId());

        assertTrue(claimService.claimNextEligibleForProcessing(1).isEmpty());
        assertEquals(claimFailuresBefore + 1, counter("failed", "source", "claim"));

        FailureEventEntity persisted = failureEventRepository.findById(event.getEventId()).orElseThrow();
        assertEquals(ProcessingStatus.FAILED, persisted.getProcessingStatus());
        assertEquals(exhaustedAttemptCount, persisted.getAttemptCount());
        assertEquals("RETRY_EXHAUSTED", persisted.getFailureCode());
        assertNull(persisted.getNextAttemptAt());
        assertTrue(claimService.claimNextEligibleForProcessing(1).isEmpty());
    }

    @Test
    void shouldNotCountClaimExhaustionWhenClaimTransactionRollsBack() {
        FailureEventEntity event = persistRetryableEvent(
                "claim-exhaustion-rollback",
                Instant.parse("2026-09-15T10:00:00Z"),
                Instant.parse("2999-01-01T00:00:00Z"));
        jdbcTemplate.update(
                "UPDATE failure_event SET attempt_count = ?, version = version + 1 WHERE event_id = ?",
                retryPolicy.getMaxAttempts(),
                event.getEventId());
        double failuresBefore = counter("failed", "source", "claim");

        doAnswer(invocation -> {
            int maxAttempts = (int) invocation.callRealMethod();
            TransactionSynchronizationManager.registerSynchronization(new TransactionSynchronization() {
                @Override
                public void beforeCommit(boolean readOnly) {
                    throw new IllegalStateException("simulated claim commit failure");
                }
            });
            return maxAttempts;
        }).when(retryPolicySpy).getMaxAttempts();

        assertThrows(IllegalStateException.class, () -> claimService.claimNextEligibleForProcessing(1));

        assertEquals(failuresBefore, counter("failed", "source", "claim"));
        assertEquals(ProcessingStatus.RETRYABLE,
                failureEventRepository.findById(event.getEventId()).orElseThrow().getProcessingStatus());
    }

    @Test
    void shouldRecoverExpiredProcessingClaimAndAllowItToBeClaimedAgainAfterBackoff() {
        double recoveriesBefore = counter("lease.recovered", "result", "retryable");
        double recoveryRetriesBefore = counter("retry.scheduled", "source", "recovery");
        FailureEventEntity abandoned = persistProcessingEvent(
                "abandoned-processing-claim",
                Instant.now().minusSeconds(3600));

        recoveryService.recoverExpiredClaims(1);
        assertEquals(recoveriesBefore + 1, counter("lease.recovered", "result", "retryable"));
        assertEquals(recoveryRetriesBefore + 1, counter("retry.scheduled", "source", "recovery"));
        assertTrue(claimService.claimNextEligibleForProcessing(1).isEmpty(),
                "An abandoned attempt should respect the normal retry backoff");

        FailureEventEntity recovered = failureEventRepository.findById(abandoned.getEventId()).orElseThrow();
        assertEquals(ProcessingStatus.RETRYABLE, recovered.getProcessingStatus());
        assertEquals(1, recovered.getAttemptCount());
        assertEquals("WORKER_LEASE_EXPIRED", recovered.getFailureCode());
        assertNotNull(recovered.getNextAttemptAt());
        Instant observedAt = Instant.now();
        assertTrue(recovered.getNextAttemptAt().isAfter(observedAt.plusSeconds(47)),
                "First recovery retry should use the configured one-minute delay and jitter range");
        assertTrue(recovered.getNextAttemptAt().isBefore(observedAt.plusSeconds(73)),
                "First recovery retry should not exceed the configured one-minute delay and jitter range");
        assertNull(recovered.getProcessingStartedAt());

        jdbcTemplate.update(
                "UPDATE failure_event SET next_attempt_at = ?, version = version + 1 WHERE event_id = ?",
                Timestamp.from(Instant.now().minusSeconds(1)),
                abandoned.getEventId());
        List<ClaimedFailureEvent> retryClaims = claimService.claimNextEligibleForProcessing(1);

        assertEquals(1, retryClaims.size());
        assertEquals(abandoned.getEventId(), retryClaims.get(0).eventId());
        assertEquals(2, retryClaims.get(0).attemptNumber());
    }

    @Test
    void shouldFailAnExpiredProcessingClaimWhenItsAttemptLimitIsReached() {
        double recoveriesBefore = counter("lease.recovered", "result", "failed");
        double recoveryFailuresBefore = counter("failed", "source", "recovery");
        FailureEventEntity abandoned = persistProcessingEvent(
                "abandoned-attempt-limit",
                Instant.now().minusSeconds(3600));
        int maxAttempts = retryPolicy.getMaxAttempts();
        jdbcTemplate.update(
                "UPDATE failure_event SET attempt_count = ?, version = version + 1 WHERE event_id = ?",
                maxAttempts,
                abandoned.getEventId());

        recoveryService.recoverExpiredClaims(1);
        assertEquals(recoveriesBefore + 1, counter("lease.recovered", "result", "failed"));
        assertEquals(recoveryFailuresBefore + 1, counter("failed", "source", "recovery"));
        assertTrue(claimService.claimNextEligibleForProcessing(1).isEmpty());

        FailureEventEntity recovered = failureEventRepository.findById(abandoned.getEventId()).orElseThrow();
        assertEquals(ProcessingStatus.FAILED, recovered.getProcessingStatus());
        assertEquals(maxAttempts, recovered.getAttemptCount());
        assertEquals("RETRY_EXHAUSTED", recovered.getFailureCode());
        assertNull(recovered.getProcessingStartedAt());
        assertNull(recovered.getNextAttemptAt());
    }

    @Test
    void shouldLeaveAnUnexpiredProcessingLeaseUntouched() {
        Instant attemptStartedAt = Instant.now();
        FailureEventEntity active = persistProcessingEvent("active-processing-lease", attemptStartedAt);
        long versionBeforeRecovery = jdbcTemplate.queryForObject(
                "SELECT version FROM failure_event WHERE event_id = ?",
                Long.class,
                active.getEventId());

        recoveryService.recoverExpiredClaims(1);

        FailureEventEntity afterRecovery = failureEventRepository.findById(active.getEventId()).orElseThrow();
        assertEquals(ProcessingStatus.PROCESSING, afterRecovery.getProcessingStatus());
        assertEquals(1, afterRecovery.getAttemptCount());
        assertTrue(Math.abs(Duration.between(attemptStartedAt, afterRecovery.getProcessingStartedAt()).toNanos())
                        <= 1_000,
                "The persisted lease timestamp should match the inserted value within PostgreSQL precision");
        assertNull(afterRecovery.getNextAttemptAt());
        assertEquals(versionBeforeRecovery, jdbcTemplate.queryForObject(
                "SELECT version FROM failure_event WHERE event_id = ?",
                Long.class,
                active.getEventId()),
                "Recovery must not update or version-bump an unexpired claim");
    }

    @Test
    void shouldRecoverOneStaleAttemptOnlyOnceWhenRecoveryCallsOverlap() throws Exception {
        FailureEventEntity abandoned = persistProcessingEvent(
                "concurrent-recovery-attempt",
                Instant.now().minusSeconds(3600));
        long versionBeforeRecovery = jdbcTemplate.queryForObject(
                "SELECT version FROM failure_event WHERE event_id = ?",
                Long.class,
                abandoned.getEventId());
        CountDownLatch firstRecoveryLockedRow = new CountDownLatch(1);
        CountDownLatch releaseFirstRecovery = new CountDownLatch(1);
        AtomicBoolean pauseFirstRecovery = new AtomicBoolean(true);
        doAnswer(invocation -> {
            if (pauseFirstRecovery.compareAndSet(true, false)) {
                firstRecoveryLockedRow.countDown();
                awaitLatch(releaseFirstRecovery, "Interrupted while holding the first recovery transaction");
            }
            return invocation.callRealMethod();
        }).when(retryPolicySpy).nextAttemptAt(1);

        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            Future<?> firstRecovery = executor.submit(() -> recoveryService.recoverExpiredClaims(1));
            assertTrue(firstRecoveryLockedRow.await(5, TimeUnit.SECONDS),
                    "The first recovery should hold the selected row lock while calculating backoff");

            recoveryService.recoverExpiredClaims(1);

            releaseFirstRecovery.countDown();
            firstRecovery.get(5, TimeUnit.SECONDS);
        } finally {
            releaseFirstRecovery.countDown();
            executor.shutdownNow();
        }

        FailureEventEntity recovered = failureEventRepository.findById(abandoned.getEventId()).orElseThrow();
        assertEquals(ProcessingStatus.RETRYABLE, recovered.getProcessingStatus());
        assertEquals(1, recovered.getAttemptCount(), "Recovery must not consume another attempt");
        assertEquals("WORKER_LEASE_EXPIRED", recovered.getFailureCode());
        assertNull(recovered.getProcessingStartedAt());
        assertEquals(versionBeforeRecovery + 1, jdbcTemplate.queryForObject(
                "SELECT version FROM failure_event WHERE event_id = ?",
                Long.class,
                abandoned.getEventId()),
                "Exactly one recovery transition should commit for the stale attempt");
    }

    @Test
    void shouldNotLetAnOlderRetryRecorderOverwriteRecovery() throws Exception {
        FailureEventEntity abandoned = persistProcessingEvent(
                "recovery-retry-recorder-race",
                Instant.now().minusSeconds(3600));
        ClaimedFailureEvent oldClaim = new ClaimedFailureEvent(abandoned.getEventId(), 1);
        FailureEventEntity staleSnapshot = failureEventRepository.findById(abandoned.getEventId()).orElseThrow();
        CountDownLatch retryRecorderReadyToSave = new CountDownLatch(1);
        CountDownLatch allowRetryRecorderToSave = new CountDownLatch(1);
        doAnswer(invocation -> {
            retryRecorderReadyToSave.countDown();
            awaitLatch(allowRetryRecorderToSave, "Interrupted while pausing the retry recorder");
            return Optional.of(staleSnapshot);
        }).when(failureEventRepositorySpy).findById(any());

        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            Future<Throwable> retryRecord = executor.submit(() -> {
                try {
                    retryStateRecorder.recordFailure(
                            oldClaim,
                            FailureEventRetryableFailure.DATABASE_TIMEOUT,
                            Optional.of(Instant.now().plusSeconds(60)));
                    return null;
                } catch (RuntimeException failure) {
                    return failure;
                }
            });

            assertTrue(retryRecorderReadyToSave.await(5, TimeUnit.SECONDS),
                    "The retry recorder should load the old PROCESSING version before recovery");
            recoveryService.recoverExpiredClaims(1);
            allowRetryRecorderToSave.countDown();

            Throwable staleRetryFailure = retryRecord.get(5, TimeUnit.SECONDS);
            assertInstanceOf(OptimisticLockingFailureException.class, staleRetryFailure,
                    "The old retry recorder must fail its commit after recovery advances the row version");
        } finally {
            allowRetryRecorderToSave.countDown();
            executor.shutdownNow();
        }

        assertEquals(ProcessingStatus.RETRYABLE.name(), jdbcTemplate.queryForObject(
                "SELECT processing_status FROM failure_event WHERE event_id = ?",
                String.class,
                abandoned.getEventId()));
        assertEquals(1, jdbcTemplate.queryForObject(
                "SELECT attempt_count FROM failure_event WHERE event_id = ?",
                Integer.class,
                abandoned.getEventId()));
        assertEquals("WORKER_LEASE_EXPIRED", jdbcTemplate.queryForObject(
                "SELECT failure_code FROM failure_event WHERE event_id = ?",
                String.class,
                abandoned.getEventId()));
        assertNull(jdbcTemplate.queryForObject(
                "SELECT processing_started_at FROM failure_event WHERE event_id = ?",
                Timestamp.class,
                abandoned.getEventId()));
        assertNotNull(jdbcTemplate.queryForObject(
                "SELECT next_attempt_at FROM failure_event WHERE event_id = ?",
                Timestamp.class,
                abandoned.getEventId()));
    }

    @Test
    void shouldUseLastAttemptThenIngestedAtWhenProcessingStartIsMissing() {
        Instant oldTimestamp = Instant.now().minusSeconds(3600);
        FailureEventEntity lastAttemptFallback = persistProcessingEvent(
                "missing-start-last-attempt-fallback",
                oldTimestamp);
        FailureEventEntity ingestionFallback = persistProcessingEvent(
                "missing-start-ingested-at-fallback",
                oldTimestamp.minusSeconds(60));
        FailureEventEntity recentLastAttempt = persistProcessingEvent(
                "missing-start-recent-last-attempt",
                oldTimestamp.minusSeconds(120));

        jdbcTemplate.update(
                "UPDATE failure_event SET processing_started_at = NULL, version = version + 1 WHERE event_id = ?",
                lastAttemptFallback.getEventId());
        jdbcTemplate.update(
                "UPDATE failure_event SET processing_started_at = NULL, last_attempt_at = NULL, "
                        + "version = version + 1 WHERE event_id = ?",
                ingestionFallback.getEventId());
        jdbcTemplate.update(
                "UPDATE failure_event SET processing_started_at = NULL, "
                        + "last_attempt_at = CURRENT_TIMESTAMP + INTERVAL '1 day', "
                        + "version = version + 1 WHERE event_id = ?",
                recentLastAttempt.getEventId());

        recoveryService.recoverExpiredClaims(3);
        assertTrue(claimService.claimNextEligibleForProcessing(3).isEmpty());

        FailureEventEntity recoveredFromLastAttempt = failureEventRepository
                .findById(lastAttemptFallback.getEventId()).orElseThrow();
        assertEquals(ProcessingStatus.RETRYABLE, recoveredFromLastAttempt.getProcessingStatus());
        assertEquals(1, recoveredFromLastAttempt.getAttemptCount());
        assertEquals("WORKER_LEASE_EXPIRED", recoveredFromLastAttempt.getFailureCode());
        assertNotNull(recoveredFromLastAttempt.getNextAttemptAt());

        FailureEventEntity recoveredFromIngestedAt = failureEventRepository
                .findById(ingestionFallback.getEventId()).orElseThrow();
        assertEquals(ProcessingStatus.RETRYABLE, recoveredFromIngestedAt.getProcessingStatus());
        assertEquals(1, recoveredFromIngestedAt.getAttemptCount());

        FailureEventEntity stillProcessing = failureEventRepository
                .findById(recentLastAttempt.getEventId()).orElseThrow();
        assertEquals(ProcessingStatus.PROCESSING, stillProcessing.getProcessingStatus());
        assertEquals(1, stillProcessing.getAttemptCount());
    }

    @Test
    void shouldIncludeLeaseTimestampExactlyAtExpiryCutoff() {
        Instant expiryCutoff = Instant.parse("2026-09-15T12:00:00Z");
        FailureEventEntity exactlyExpired = persistProcessingEvent(
                "processing-lease-exact-cutoff",
                expiryCutoff);
        FailureEventEntity notYetExpired = persistProcessingEvent(
                "processing-lease-after-cutoff",
                expiryCutoff.plusSeconds(1));

        List<FailureEventEntity> selected = new TransactionTemplate(transactionManager)
                .execute(status -> failureEventRepository.lockExpiredProcessingClaims(expiryCutoff, 10));

        assertNotNull(selected);
        assertEquals(List.of(exactlyExpired.getEventId()), selected.stream()
                .map(FailureEventEntity::getEventId)
                .toList());
        assertEquals(ProcessingStatus.PROCESSING,
                failureEventRepository.findById(notYetExpired.getEventId()).orElseThrow().getProcessingStatus());
    }

    @Test
    void shouldSelectOnlyExpiredProcessingRowsWithinTheRequestedBatch() {
        Instant cutoff = Instant.parse("2026-09-15T12:00:00Z");
        FailureEventEntity oldestExpired = persistProcessingEvent(
                "expired-processing-oldest",
                cutoff.minusSeconds(2));
        FailureEventEntity boundaryExpired = persistProcessingEvent(
                "expired-processing-boundary",
                cutoff);
        FailureEventEntity unexpired = persistProcessingEvent(
                "unexpired-processing",
                cutoff.plusSeconds(1));
        FailureEventEntity otherStatus = persistEvent(
                "expired-received",
                ProcessingStatus.RECEIVED,
                cutoff.minusSeconds(10));

        List<FailureEventEntity> selected = new TransactionTemplate(transactionManager)
                .execute(status -> failureEventRepository.lockExpiredProcessingClaims(cutoff, 2));

        assertNotNull(selected);
        assertEquals(List.of(oldestExpired.getEventId(), boundaryExpired.getEventId()), selected.stream()
                .map(FailureEventEntity::getEventId)
                .toList());
        assertEquals(1, selected.get(0).getAttemptCount());
        assertEquals(1, selected.get(1).getAttemptCount());
        assertEquals(ProcessingStatus.PROCESSING,
                failureEventRepository.findById(unexpired.getEventId()).orElseThrow().getProcessingStatus());
        assertEquals(ProcessingStatus.RECEIVED,
                failureEventRepository.findById(otherStatus.getEventId()).orElseThrow().getProcessingStatus());
    }

    @Test
    void shouldSkipAnExpiredRowLockedByAnotherSelectionAndSelectAnotherRow() throws Exception {
        FailureEventEntity firstExpired = persistProcessingEvent(
                "locked-expired-processing-claim",
                Instant.parse("2026-09-15T11:00:00Z"));
        FailureEventEntity secondExpired = persistProcessingEvent(
                "available-expired-processing-claim",
                Instant.parse("2026-09-15T11:01:00Z"));

        CountDownLatch lockAcquired = new CountDownLatch(1);
        CountDownLatch releaseLock = new CountDownLatch(1);
        ExecutorService executor = Executors.newFixedThreadPool(2);
        try {
            Future<List<UUID>> firstSelection = executor.submit(() -> {
                return new TransactionTemplate(transactionManager).execute(status -> {
                    List<UUID> selected = failureEventRepository
                            .lockExpiredProcessingClaims(Instant.parse("2026-09-15T12:00:00Z"), 1)
                            .stream()
                            .map(FailureEventEntity::getEventId)
                            .toList();
                    lockAcquired.countDown();
                    try {
                        assertTrue(releaseLock.await(20, TimeUnit.SECONDS));
                    } catch (InterruptedException exception) {
                        Thread.currentThread().interrupt();
                        throw new IllegalStateException("Interrupted while holding recovery selection lock", exception);
                    }
                    return selected;
                });
            });
            assertTrue(lockAcquired.await(5, TimeUnit.SECONDS));

            List<FailureEventEntity> secondSelection = executor.submit(() -> new TransactionTemplate(transactionManager)
                    .execute(status -> failureEventRepository
                            .lockExpiredProcessingClaims(Instant.parse("2026-09-15T12:00:00Z"), 1)))
                    .get(3, TimeUnit.SECONDS);

            assertEquals(List.of(secondExpired.getEventId()), secondSelection.stream()
                    .map(FailureEventEntity::getEventId)
                    .toList());

            releaseLock.countDown();
            assertEquals(List.of(firstExpired.getEventId()), firstSelection.get(5, TimeUnit.SECONDS));
        } finally {
            releaseLock.countDown();
            executor.shutdownNow();
        }
    }

    @Test
    void shouldKeepCommittedRecoveryWhenTheFollowingClaimTransactionFails() {
        FailureEventEntity abandoned = persistProcessingEvent(
                "recovery-commit-before-claim-failure",
                Instant.now().minusSeconds(3600));
        doThrow(new IllegalStateException("simulated claim selection failure"))
                .when(failureEventRepositorySpy)
                .lockNextEligibleForProcessing(any(Instant.class), anyInt(), anyInt());

        recoveryService.recoverExpiredClaims(1);

        new TransactionTemplate(transactionManager).executeWithoutResult(status -> jdbcTemplate.queryForObject(
                "SELECT event_id FROM failure_event WHERE event_id = ? FOR UPDATE NOWAIT",
                UUID.class,
                abandoned.getEventId()));
        assertThrows(
                IllegalStateException.class,
                () -> claimService.claimNextEligibleForProcessing(1));

        FailureEventEntity recovered = failureEventRepository.findById(abandoned.getEventId()).orElseThrow();
        assertEquals(ProcessingStatus.RETRYABLE, recovered.getProcessingStatus());
        assertEquals(1, recovered.getAttemptCount());
        assertEquals("WORKER_LEASE_EXPIRED", recovered.getFailureCode());
        assertNotNull(recovered.getNextAttemptAt());
    }

    @Test
    void shouldRollBackAllRecoveryTransitionsWhenRecoveryFailsBeforeCommit() {
        double recoveredBefore = counter("lease.recovered", "result", "retryable");
        double retriesBefore = counter("retry.scheduled", "source", "recovery");
        FailureEventEntity firstExpired = persistProcessingEvent(
                "recovery-rollback-first",
                Instant.now().minusSeconds(3600));
        FailureEventEntity secondExpired = persistProcessingEvent(
                "recovery-rollback-second",
                Instant.now().minusSeconds(3500));
        doReturn(Optional.of(Instant.now().plusSeconds(60)))
                .doThrow(new IllegalStateException("simulated recovery failure"))
                .when(retryPolicySpy)
                .nextAttemptAt(1);

        assertThrows(IllegalStateException.class, () -> recoveryService.recoverExpiredClaims(2));

        FailureEventEntity firstAfterRollback = failureEventRepository.findById(firstExpired.getEventId())
                .orElseThrow();
        FailureEventEntity secondAfterRollback = failureEventRepository.findById(secondExpired.getEventId())
                .orElseThrow();
        assertEquals(ProcessingStatus.PROCESSING, firstAfterRollback.getProcessingStatus());
        assertEquals(ProcessingStatus.PROCESSING, secondAfterRollback.getProcessingStatus());
        assertEquals(1, firstAfterRollback.getAttemptCount());
        assertEquals(1, secondAfterRollback.getAttemptCount());
        assertEquals(recoveredBefore, counter("lease.recovered", "result", "retryable"));
        assertEquals(retriesBefore, counter("retry.scheduled", "source", "recovery"));

        doReturn(Optional.of(Instant.now().plusSeconds(60)))
                .when(retryPolicySpy)
                .nextAttemptAt(1);
        recoveryService.recoverExpiredClaims(2);

        assertEquals(ProcessingStatus.RETRYABLE,
                failureEventRepository.findById(firstExpired.getEventId()).orElseThrow().getProcessingStatus());
        assertEquals(ProcessingStatus.RETRYABLE,
                failureEventRepository.findById(secondExpired.getEventId()).orElseThrow().getProcessingStatus());
        assertEquals(recoveredBefore + 2, counter("lease.recovered", "result", "retryable"));
        assertEquals(retriesBefore + 2, counter("retry.scheduled", "source", "recovery"));
    }

    @Test
    void shouldRecordOnlyOneFailureForDuplicateCallsUsingTheSameClaim() {
        FailureEventEntity event = persistEvent(
                "duplicate-retry-record",
                ProcessingStatus.RECEIVED,
                Instant.parse("2026-09-15T10:00:00Z"));
        ClaimedFailureEvent claim = claimService.claimNextEligibleForProcessing(1).get(0);
        Optional<Instant> retryAt = Optional.of(Instant.now().plusSeconds(60));

        assertTrue(retryStateRecorder.recordFailure(
                claim,
                FailureEventRetryableFailure.DATABASE_TIMEOUT,
                retryAt));
        assertFalse(retryStateRecorder.recordFailure(
                claim,
                FailureEventRetryableFailure.DATABASE_UNAVAILABLE,
                retryAt));

        FailureEventEntity persisted = failureEventRepository.findById(event.getEventId()).orElseThrow();
        assertEquals(ProcessingStatus.RETRYABLE, persisted.getProcessingStatus());
        assertEquals(1, persisted.getAttemptCount());
        assertEquals("DATABASE_TIMEOUT", persisted.getFailureCode());
    }

    @Test
    void shouldIgnoreFailureFromAnOlderClaimAfterANewerAttemptHasStarted() {
        FailureEventEntity event = persistEvent(
                "stale-retry-record",
                ProcessingStatus.RECEIVED,
                Instant.parse("2026-09-15T10:00:00Z"));
        ClaimedFailureEvent oldClaim = claimService.claimNextEligibleForProcessing(1).get(0);
        assertTrue(retryStateRecorder.recordFailure(
                oldClaim,
                FailureEventRetryableFailure.DATABASE_TIMEOUT,
                Optional.of(Instant.now().minusSeconds(1))));

        ClaimedFailureEvent currentClaim = claimService.claimNextEligibleForProcessing(1).get(0);
        assertEquals(2, currentClaim.attemptNumber());
        double retriesBeforeStaleRecord = counter("retry.scheduled", "source", "processing");
        assertFalse(retryStateRecorder.recordFailure(
                oldClaim,
                FailureEventRetryableFailure.DATABASE_UNAVAILABLE,
                Optional.of(Instant.now().plusSeconds(60))));
        assertEquals(retriesBeforeStaleRecord, counter("retry.scheduled", "source", "processing"));

        FailureEventEntity persisted = failureEventRepository.findById(event.getEventId()).orElseThrow();
        assertEquals(ProcessingStatus.PROCESSING, persisted.getProcessingStatus());
        assertEquals(2, persisted.getAttemptCount());
        assertNull(persisted.getFailureCode());
        assertNull(persisted.getNextAttemptAt());
    }

    @Test
    void shouldAllowOnlyOneCompetingFailureUpdateForTheSameClaim() throws Exception {
        FailureEventEntity event = persistEvent(
                "concurrent-retry-record",
                ProcessingStatus.RECEIVED,
                Instant.parse("2026-09-15T10:00:00Z"));
        ClaimedFailureEvent claim = claimService.claimNextEligibleForProcessing(1).get(0);
        CountDownLatch start = new CountDownLatch(1);
        ExecutorService executor = Executors.newFixedThreadPool(2);
        try {
            Future<Boolean> first = executor.submit(() -> recordAfter(
                    start, claim, FailureEventRetryableFailure.DATABASE_TIMEOUT));
            Future<Boolean> second = executor.submit(() -> recordAfter(
                    start, claim, FailureEventRetryableFailure.DATABASE_UNAVAILABLE));
            start.countDown();

            int successfulUpdates = (first.get(10, TimeUnit.SECONDS) ? 1 : 0)
                    + (second.get(10, TimeUnit.SECONDS) ? 1 : 0);
            assertEquals(1, successfulUpdates);
        } finally {
            executor.shutdownNow();
        }

        FailureEventEntity persisted = failureEventRepository.findById(event.getEventId()).orElseThrow();
        assertEquals(ProcessingStatus.RETRYABLE, persisted.getProcessingStatus());
        assertEquals(1, persisted.getAttemptCount());
        assertTrue(List.of("DATABASE_TIMEOUT", "DATABASE_UNAVAILABLE")
                .contains(persisted.getFailureCode()));
    }

    private boolean recordAfter(
            CountDownLatch start,
            ClaimedFailureEvent claim,
            FailureEventRetryableFailure failure) throws InterruptedException {
        assertTrue(start.await(5, TimeUnit.SECONDS));
        try {
            return retryStateRecorder.recordFailure(
                    claim,
                    failure,
                    Optional.of(Instant.now().plusSeconds(60)));
        } catch (OptimisticLockingFailureException exception) {
            return false;
        }
    }

    private List<ClaimedFailureEvent> claimAfter(CountDownLatch start, int batchSize) throws InterruptedException {
        assertTrue(start.await(5, TimeUnit.SECONDS));
        return claimService.claimNextEligibleForProcessing(batchSize);
    }

    private void holdRowLock(UUID eventId, CountDownLatch lockAcquired, CountDownLatch releaseLock) {
        new TransactionTemplate(transactionManager).executeWithoutResult(status -> {
            jdbcTemplate.queryForObject(
                    "SELECT event_id FROM failure_event WHERE event_id = ? FOR UPDATE",
                    UUID.class,
                    eventId);
            lockAcquired.countDown();
            try {
                assertTrue(releaseLock.await(5, TimeUnit.SECONDS));
            } catch (InterruptedException exception) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException("Interrupted while holding row lock", exception);
            }
        });
    }

    private void awaitLatch(CountDownLatch latch, String interruptedMessage) {
        try {
            if (!latch.await(10, TimeUnit.SECONDS)) {
                throw new IllegalStateException("Timed out waiting for recovery race synchronization");
            }
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(interruptedMessage, exception);
        }
    }

    private FailureEventEntity persistEvent(String traceId, ProcessingStatus status, Instant ingestedAt) {
        FailureEventEntity event = new FailureEventEntity();
        event.setEventId(UUID.randomUUID());
        event.setOccurredAt(ingestedAt);
        event.setIngestedAt(ingestedAt);
        event.setSourceSystem("test-source");
        event.setServiceName("test-service");
        event.setEnvironment("test");
        event.setEventType("error");
        event.setTraceId(traceId);
        event.setRawPayload("{}");
        event.setProcessingStatus(status);
        return failureEventRepository.saveAndFlush(event);
    }

    private FailureEventEntity persistProcessingEvent(String traceId, Instant attemptStartedAt) {
        FailureEventEntity event = persistEvent(
                traceId,
                ProcessingStatus.RECEIVED,
                attemptStartedAt);
        event.claimForProcessing(attemptStartedAt);
        return failureEventRepository.saveAndFlush(event);
    }

    private FailureEventEntity persistRetryableEvent(
            String traceId,
            Instant ingestedAt,
            Instant nextAttemptAt) {
        FailureEventEntity event = persistEvent(traceId, ProcessingStatus.RECEIVED, ingestedAt);
        event.claimForProcessing(ingestedAt);
        event.markRetryable("TEMPORARY_FAILURE", "temporary processing failure", nextAttemptAt);
        return failureEventRepository.saveAndFlush(event);
    }

    private void assertClaimed(UUID eventId, int expectedAttemptCount) {
        FailureEventEntity event = failureEventRepository.findById(eventId).orElseThrow();
        assertEquals(ProcessingStatus.PROCESSING, event.getProcessingStatus());
        assertEquals(expectedAttemptCount, event.getAttemptCount());
        assertNotNull(event.getProcessingStartedAt());
        assertEquals(event.getProcessingStartedAt(), event.getLastAttemptAt());
    }

    private void assertStatus(UUID eventId, ProcessingStatus expectedStatus) {
        assertEquals(expectedStatus,
                failureEventRepository.findById(eventId).orElseThrow().getProcessingStatus());
    }

    private double counter(String name, String... tags) {
        var search = meterRegistry.get("failureintel.events." + name);
        for (int i = 0; i < tags.length; i += 2) {
            search = search.tag(tags[i], tags[i + 1]);
        }
        return search.counter().count();
    }

    private boolean disjoint(Set<UUID> first, Set<UUID> second) {
        Set<UUID> intersection = new HashSet<>(first);
        intersection.retainAll(second);
        return intersection.isEmpty();
    }
}
