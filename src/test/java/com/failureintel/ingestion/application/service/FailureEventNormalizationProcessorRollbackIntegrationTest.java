package com.failureintel.ingestion.application.service;

import com.failureintel.infrastructure.persistence.failureevent.entity.FailureEventEntity;
import com.failureintel.infrastructure.persistence.failureevent.entity.ProcessingStatus;
import com.failureintel.infrastructure.persistence.failureevent.repository.FailureEventRepository;
import com.failureintel.ingestion.domain.model.NormalizedFailureEvent;
import com.failureintel.ingestion.domain.normalization.FailureEventNormalizer;
import com.failureintel.ingestion.domain.normalization.NormalizationStatus;
import com.failureintel.infrastructure.persistence.normalizedFailureEvent.entity.NormalizedFailureEventEntity;
import com.failureintel.infrastructure.persistence.normalizedFailureEvent.repository.NormalizedFailureEventRepository;
import io.micrometer.core.instrument.MeterRegistry;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import com.failureintel.test.support.PostgresTestContainer;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.dao.OptimisticLockingFailureException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;

import java.sql.Timestamp;
import java.time.Instant;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static com.failureintel.test.support.FailureEventTestFixtures.validRequest;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

@SpringBootTest
@TestPropertySource(properties = "spring.jpa.hibernate.ddl-auto=validate")
class FailureEventNormalizationProcessorRollbackIntegrationTest {

    private static final Logger LOGGER =
            LoggerFactory.getLogger(FailureEventNormalizationProcessorRollbackIntegrationTest.class);

    @DynamicPropertySource
    static void postgresProperties(DynamicPropertyRegistry registry) {
        PostgresTestContainer.registerDatabase(registry, "failureintel_normalization_rollback_test");
    }

    @Autowired
    private FailureEventIngestionService ingestionService;

    @Autowired
    private FailureEventClaimService claimService;

    @Autowired
    private FailureEventRecoveryService recoveryService;

    @Autowired
    private FailureEventNormalizationProcessor processor;

    @Autowired
    private FailureEventProcessingService processingService;

    @Autowired
    private FailureEventRetryStateRecorder retryStateRecorder;

    @Autowired
    private FailureEventRepository failureEventRepository;

    @Autowired
    private NormalizedFailureEventRepository normalizedFailureEventRepository;

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @Autowired
    private MeterRegistry meterRegistry;

    @MockitoBean
    private FailureEventNormalizer failureEventNormalizer;

    @AfterEach
    void cleanUp() {
        failureEventRepository.deleteAll();
    }

    @Test
    void shouldRollBackRawNormalizationStatusWhenNormalizedPersistenceFails() {
        double normalizedBefore = counter("normalized");
        when(failureEventNormalizer.normalize(any()))
                .thenReturn(normalizedEventExceedingDatabaseColumnLength());

        UUID eventId = UUID.fromString(
                ingestionService.ingestFailureEvent(validRequest("trace-normalization-rollback-001")));
        ClaimedFailureEvent claim = claimService
                .claimNextEligibleForProcessing(1)
                .get(0);

        assertEquals(eventId, claim.eventId());
        assertThrows(DataIntegrityViolationException.class, () -> processor.process(claim));

        assertEquals(
                ProcessingStatus.PROCESSING,
                failureEventRepository.findById(eventId).orElseThrow().getProcessingStatus());
        assertFalse(normalizedFailureEventRepository.existsById(eventId));
        assertEquals(normalizedBefore, counter("normalized"));
    }

    @Test
    void shouldRecordRetryableAfterNormalizedWriteTransactionRollsBack() {
        double retriesBefore = counter("retry.scheduled", "source", "processing");
        double processingAttemptsBefore = processingTimerCount();
        double normalizedBefore = counter("normalized");
        when(failureEventNormalizer.normalize(any()))
                .thenReturn(normalizedEventExceedingDatabaseColumnLength());

        UUID eventId = UUID.fromString(
                ingestionService.ingestFailureEvent(validRequest("trace-normalization-retry-001")));
        ClaimedFailureEvent claim = claimService
                .claimNextEligibleForProcessing(1)
                .get(0);

        processingService.process(claim);

        FailureEventEntity persistedRawEvent = failureEventRepository.findById(eventId).orElseThrow();
        assertEquals(eventId, claim.eventId());
        assertEquals(ProcessingStatus.RETRYABLE, persistedRawEvent.getProcessingStatus());
        assertEquals(1, persistedRawEvent.getAttemptCount());
        assertEquals("NORMALIZED_WRITE_FAILURE", persistedRawEvent.getFailureCode());
        assertEquals(
                "Normalized failure event could not be persisted",
                persistedRawEvent.getFailureReason());
        assertNotNull(persistedRawEvent.getNextAttemptAt());
        assertTrue(persistedRawEvent.getNextAttemptAt().isAfter(Instant.now()));
        assertFalse(normalizedFailureEventRepository.existsById(eventId));
        assertEquals(retriesBefore + 1, counter("retry.scheduled", "source", "processing"));
        assertEquals(processingAttemptsBefore + 1, processingTimerCount());
        assertEquals(normalizedBefore, counter("normalized"));
    }

    @Test
    void shouldClaimDueRetryAndCommitNormalizationAndRawStatusTogether() {
        double retriesBefore = counter("retry.scheduled", "source", "processing");
        double normalizedBefore = counter("normalized");
        double processingAttemptsBefore = processingTimerCount();
        when(failureEventNormalizer.normalize(any()))
                .thenReturn(
                        normalizedEventExceedingDatabaseColumnLength(),
                        successfulNormalizedEvent("payment-service"));

        UUID eventId = UUID.fromString(
                ingestionService.ingestFailureEvent(validRequest("trace-normalization-retry-success-001")));
        ClaimedFailureEvent initialClaim = claimService
                .claimNextEligibleForProcessing(1)
                .get(0);

        processingService.process(initialClaim);

        FailureEventEntity retryableEvent = failureEventRepository.findById(eventId).orElseThrow();
        assertEquals(ProcessingStatus.RETRYABLE, retryableEvent.getProcessingStatus());
        assertEquals(1, retryableEvent.getAttemptCount());
        assertFalse(normalizedFailureEventRepository.existsById(eventId));

        jdbcTemplate.update(
                "UPDATE failure_event SET next_attempt_at = ? WHERE event_id = ?",
                Timestamp.from(Instant.now().minusSeconds(1)),
                eventId);
        ClaimedFailureEvent retryClaim = claimService
                .claimNextEligibleForProcessing(1)
                .stream()
                .filter(claim -> claim.eventId().equals(eventId))
                .findFirst()
                .orElseThrow();

        processingService.process(retryClaim);

        FailureEventEntity normalizedRawEvent = failureEventRepository.findById(eventId).orElseThrow();
        NormalizedFailureEventEntity normalizedEvent = normalizedFailureEventRepository
                .findById(eventId)
                .orElseThrow();
        assertEquals(2, retryClaim.attemptNumber());
        assertEquals(ProcessingStatus.NORMALIZED, normalizedRawEvent.getProcessingStatus());
        assertEquals(2, normalizedRawEvent.getAttemptCount());
        assertEquals("payment-service", normalizedEvent.getNormalizedServiceName());
        assertNull(normalizedRawEvent.getFailureCode());
        assertNull(normalizedRawEvent.getNextAttemptAt());
        assertEquals(1, normalizedFailureEventRepository.count());
        assertEquals(retriesBefore + 1, counter("retry.scheduled", "source", "processing"));
        assertEquals(normalizedBefore + 1, counter("normalized"));
        assertEquals(processingAttemptsBefore + 2, processingTimerCount());
    }

    @Test
    void shouldRecordRetryableWhenNormalizationTransactionFailsDuringCompletion() {
        double normalizedBefore = counter("normalized");
        double retriesBefore = counter("retry.scheduled", "source", "processing");
        when(failureEventNormalizer.normalize(any())).thenAnswer(invocation -> {
            TransactionSynchronizationManager.registerSynchronization(new TransactionSynchronization() {
                @Override
                public void beforeCommit(boolean readOnly) {
                    throw new DataIntegrityViolationException("synthetic commit-phase write failure");
                }
            });
            return successfulNormalizedEvent("payment-service");
        });

        UUID eventId = UUID.fromString(
                ingestionService.ingestFailureEvent(validRequest("trace-normalization-commit-failure-001")));
        ClaimedFailureEvent claim = claimService
                .claimNextEligibleForProcessing(1)
                .get(0);

        processingService.process(claim);

        FailureEventEntity persistedRawEvent = failureEventRepository.findById(eventId).orElseThrow();
        assertEquals(ProcessingStatus.RETRYABLE, persistedRawEvent.getProcessingStatus());
        assertEquals(1, persistedRawEvent.getAttemptCount());
        assertEquals("NORMALIZED_WRITE_FAILURE", persistedRawEvent.getFailureCode());
        assertFalse(normalizedFailureEventRepository.existsById(eventId));
        assertNotNull(persistedRawEvent.getNextAttemptAt());
        assertEquals(normalizedBefore, counter("normalized"));
        assertEquals(retriesBefore + 1, counter("retry.scheduled", "source", "processing"));
    }

    @Test
    void shouldRollBackStaleAttemptWhenRecoveryAndNewClaimWinBeforeItCommits() throws Exception {
        UUID eventId = UUID.fromString(
                ingestionService.ingestFailureEvent(validRequest("trace-stale-attempt-race-001")));
        ClaimedFailureEvent attemptN = claimService
                .claimNextEligibleForProcessing(1)
                .get(0);
        assertEquals(1, attemptN.attemptNumber());

        CountDownLatch normalizationStarted = new CountDownLatch(1);
        CountDownLatch finishNormalization = new CountDownLatch(1);
        when(failureEventNormalizer.normalize(any())).thenAnswer(invocation -> {
            normalizationStarted.countDown();
            if (!finishNormalization.await(10, TimeUnit.SECONDS)) {
                throw new IllegalStateException("Timed out waiting to finish stale normalization");
            }
            return successfulNormalizedEvent("stale-attempt-service");
        });

        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            Future<Throwable> staleAttemptResult = executor.submit(() -> {
                try {
                    processor.process(attemptN);
                    return null;
                } catch (Throwable failure) {
                    return failure;
                }
            });

            assertTrue(normalizationStarted.await(5, TimeUnit.SECONDS),
                    "Attempt N should load and verify its claim before the lease is recovered");

            jdbcTemplate.update(
                    "UPDATE failure_event SET processing_started_at = ?, version = version + 1 "
                            + "WHERE event_id = ?",
                    Timestamp.from(Instant.now().minusSeconds(3600)),
                    eventId);

            recoveryService.recoverExpiredClaims(1);
            FailureEventEntity recovered = failureEventRepository.findById(eventId).orElseThrow();
            assertEquals(ProcessingStatus.RETRYABLE, recovered.getProcessingStatus());
            assertEquals(1, recovered.getAttemptCount(), "Recovery must not count another attempt");
            assertEquals("WORKER_LEASE_EXPIRED", recovered.getFailureCode());
            assertNotNull(recovered.getNextAttemptAt());
            assertNull(recovered.getProcessingStartedAt());

            jdbcTemplate.update(
                    "UPDATE failure_event SET next_attempt_at = ?, version = version + 1 "
                            + "WHERE event_id = ?",
                    Timestamp.from(Instant.now().minusSeconds(1)),
                    eventId);
            ClaimedFailureEvent attemptNPlus1 = claimService
                    .claimNextEligibleForProcessing(1)
                    .stream()
                    .filter(claim -> claim.eventId().equals(eventId))
                    .findFirst()
                    .orElseThrow();
            assertEquals(2, attemptNPlus1.attemptNumber());

            finishNormalization.countDown();
            Throwable staleFailure = staleAttemptResult.get(10, TimeUnit.SECONDS);
            assertNotNull(staleFailure,
                    "Attempt N must fail when its transaction tries to commit with an obsolete entity version");
            assertInstanceOf(OptimisticLockingFailureException.class, staleFailure);

            FailureEventEntity currentAttempt = failureEventRepository.findById(eventId).orElseThrow();
            assertEquals(ProcessingStatus.PROCESSING, currentAttempt.getProcessingStatus());
            assertEquals(2, currentAttempt.getAttemptCount());
            assertNotNull(currentAttempt.getProcessingStartedAt());
            assertEquals(currentAttempt.getLastAttemptAt(), currentAttempt.getProcessingStartedAt());
            assertNull(currentAttempt.getFailureCode());
            assertNull(currentAttempt.getFailureReason());
            assertNull(currentAttempt.getNextAttemptAt());
            assertFalse(normalizedFailureEventRepository.existsById(eventId),
                    "The normalized row from stale Attempt N must roll back with its raw status update");

            assertFalse(retryStateRecorder.recordFailure(
                    attemptN,
                    FailureEventRetryableFailure.DATABASE_TIMEOUT,
                    Optional.of(Instant.now().plusSeconds(60))),
                    "Attempt N must not record retry state after Attempt N+1 owns the event");

            FailureEventEntity afterStaleFailureRecord = failureEventRepository.findById(eventId).orElseThrow();
            assertEquals(ProcessingStatus.PROCESSING, afterStaleFailureRecord.getProcessingStatus());
            assertEquals(2, afterStaleFailureRecord.getAttemptCount());
            assertNull(afterStaleFailureRecord.getFailureCode());
            assertNull(afterStaleFailureRecord.getNextAttemptAt());
            assertFalse(normalizedFailureEventRepository.existsById(eventId));
        } finally {
            finishNormalization.countDown();
            executor.shutdownNow();
            assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));
        }
    }

    private NormalizedFailureEvent normalizedEventExceedingDatabaseColumnLength() {
        return new NormalizedFailureEvent(
                UUID.randomUUID(),
                "x".repeat(256),
                "prod",
                "exception",
                "PSQLException",
                "Connection timeout",
                "payment-database",
                "trace-normalization-rollback-001",
                "high",
                Instant.parse("2026-08-03T20:00:00Z"),
                Instant.parse("2026-08-03T20:00:01Z"),
                NormalizationStatus.FULLY_NORMALIZED,
                Map.of("source", "test"),
                Map.of(),
                Map.of());
    }

    private NormalizedFailureEvent successfulNormalizedEvent(String serviceName) {
        return new NormalizedFailureEvent(
                UUID.randomUUID(),
                serviceName,
                "prod",
                "exception",
                "PSQLException",
                "Connection timeout",
                "payment-database",
                "trace-normalization-retry-success-001",
                "high",
                Instant.parse("2026-08-03T20:00:00Z"),
                Instant.parse("2026-08-03T20:00:01Z"),
                NormalizationStatus.FULLY_NORMALIZED,
                Map.of("source", "test"),
                Map.of(),
                Map.of());
    }

    private double counter(String name, String... tags) {
        var search = meterRegistry.get("failureintel.events." + name);
        for (int i = 0; i < tags.length; i += 2) {
            search = search.tag(tags[i], tags[i + 1]);
        }
        return search.counter().count();
    }

    private double processingTimerCount() {
        return meterRegistry.get("failureintel.events.processing.duration").timer().count();
    }
}
