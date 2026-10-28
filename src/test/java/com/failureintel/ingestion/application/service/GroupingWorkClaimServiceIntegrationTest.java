package com.failureintel.ingestion.application.service;

import com.failureintel.infrastructure.persistence.incidentgrouping.entity.GroupingWorkEntity;
import com.failureintel.infrastructure.persistence.incidentgrouping.repository.GroupingWorkRepository;
import com.failureintel.test.support.PostgresTestContainer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.context.bean.override.mockito.MockitoSpyBean;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;
import org.springframework.transaction.support.TransactionTemplate;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.doAnswer;

@SpringBootTest(properties = "failure-event.grouping.retry.max-attempts=3")
class GroupingWorkClaimServiceIntegrationTest {
    private static final String SERVICE_NAME = "grouping-claim-test";

    @DynamicPropertySource
    static void postgresProperties(DynamicPropertyRegistry registry) {
        PostgresTestContainer.registerDatabase(registry, "failureintel_normalization_processor_test");
    }

    @Autowired private JdbcTemplate jdbcTemplate;
    @Autowired private DataSource dataSource;
    @Autowired private GroupingWorkRepository groupingWorkRepository;
    @Autowired private GroupingWorkClaimService claimService;
    @MockitoSpyBean private GroupingRetryPolicy retryPolicy;
    @Autowired private PlatformTransactionManager transactionManager;

    private final List<UUID> eventIds = new ArrayList<>();

    @BeforeEach
    void cleanFixtures() {
        deleteFixtures();
    }

    @AfterEach
    void cleanUp() {
        deleteFixtures();
        eventIds.clear();
        Mockito.reset(retryPolicy);
    }

    @Test
    void claimsPendingAndDueRetryableRowsAndExcludesOtherStatesPrematureRetriesAndExhaustedAttempts() {
        LocalDateTime databaseNow = groupingWorkRepository.currentDatabaseTime();
        UUID pending = newWork("PENDING", 0, null, null, null, null);
        UUID due = newWork("RETRYABLE", 1, databaseNow.minusMinutes(1),
                databaseNow.minusMinutes(1), "OLD_FAILURE", "old reason");
        UUID premature = newWork("RETRYABLE", 1, databaseNow.plusHours(1),
                databaseNow.minusMinutes(1), "OLD_FAILURE", "old reason");
        UUID exhausted = newWork("RETRYABLE", 3, databaseNow.minusMinutes(1),
                databaseNow.minusMinutes(1), "OLD_FAILURE", "old reason");
        UUID exhaustedPending = newWork("PENDING", 3, null, null, null, null);
        UUID processing = newWork("PROCESSING", 1, null, databaseNow, null, null);
        UUID failed = newWork("FAILED", 1, null, null, "OLD_FAILURE", "old reason");

        List<ClaimedGroupingWork> claims = claimService.claimNextEligibleForProcessing(2);

        assertEquals(List.of(pending, due), claims.stream().map(ClaimedGroupingWork::eventId).toList());
        assertEquals(List.of(1, 2), claims.stream().map(ClaimedGroupingWork::attemptNumber).toList());
        assertEquals("RETRYABLE", status(premature));
        assertEquals("RETRYABLE", status(exhausted));
        assertEquals("PENDING", status(exhaustedPending));
        assertEquals("PROCESSING", status(processing));
        assertEquals("FAILED", status(failed));
    }

    @Test
    void persistsCompleteClaimMetadataBeforeReturningToCaller() throws Exception {
        LocalDateTime databaseNow = groupingWorkRepository.currentDatabaseTime();
        UUID eventId = newWork("RETRYABLE", 1, databaseNow.minusMinutes(1),
                databaseNow.minusMinutes(2), "OLD_FAILURE", "old reason");

        List<ClaimedGroupingWork> claims = claimService.claimNextEligibleForProcessing(1);

        assertFalse(TransactionSynchronizationManager.isActualTransactionActive());
        assertEquals(1, claims.size());
        GroupingWorkEntity persisted = groupingWorkRepository.findById(eventId).orElseThrow();
        assertEquals("PROCESSING", persisted.getStatus());
        assertEquals(2, persisted.getAttemptCount());
        assertNotNull(persisted.getLastAttemptAt());
        assertEquals(persisted.getLastAttemptAt(), persisted.getProcessingStartedAt());
        assertNull(persisted.getNextAttemptAt());
        assertNull(persisted.getFailureCode());
        assertNull(persisted.getFailureReason());

        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            assertEquals("PROCESSING", executor.submit(() -> status(eventId)).get(5, TimeUnit.SECONDS));
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    void skipsRowsLockedByAnotherClaimAndDoesNotReturnTheSameWorkTwice() throws Exception {
        UUID first = newWork("PENDING", 0, null, null, null, null);
        UUID second = newWork("PENDING", 0, null, null, null, null);
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try (Connection connection = dataSource.getConnection()) {
            connection.setAutoCommit(false);
            try (PreparedStatement lock = connection.prepareStatement(
                    "SELECT event_id FROM grouping_work WHERE event_id = ? FOR UPDATE")) {
                lock.setObject(1, first);
                lock.executeQuery().close();
            }

            Future<List<ClaimedGroupingWork>> competingClaim = executor.submit(
                    () -> claimService.claimNextEligibleForProcessing(1));
            List<ClaimedGroupingWork> claims = competingClaim.get(5, TimeUnit.SECONDS);
            assertEquals(List.of(second), claims.stream().map(ClaimedGroupingWork::eventId).toList());
            assertTrue(claimService.claimNextEligibleForProcessing(1).isEmpty());

            connection.commit();
            assertEquals(List.of(first), claimService.claimNextEligibleForProcessing(1).stream()
                    .map(ClaimedGroupingWork::eventId).toList());
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    void rollsBackAllClaimMetadataWhenTheClaimTransactionCannotCommit() {
        UUID eventId = newWork("PENDING", 0, null, null, null, null);
        doAnswer(invocation -> {
            int maxAttempts = (int) invocation.callRealMethod();
            TransactionSynchronizationManager.registerSynchronization(new TransactionSynchronization() {
                @Override
                public void beforeCommit(boolean readOnly) {
                    throw new IllegalStateException("simulated grouping claim commit failure");
                }
            });
            return maxAttempts;
        }).when(retryPolicy).maxAttempts();

        assertThrows(IllegalStateException.class, () -> claimService.claimNextEligibleForProcessing(1));

        GroupingWorkEntity persisted = groupingWorkRepository.findById(eventId).orElseThrow();
        assertEquals("PENDING", persisted.getStatus());
        assertEquals(0, persisted.getAttemptCount());
        assertNull(persisted.getLastAttemptAt());
        assertNull(persisted.getProcessingStartedAt());
    }

    @Test
    void usesIncreasingAttemptNumbersAsDistinctOwnershipGenerationsAfterReassignment() {
        UUID eventId = newWork("PENDING", 0, null, null, null, null);
        ClaimedGroupingWork firstClaim = claimService.claimNextEligibleForProcessing(1).get(0);

        jdbcTemplate.update("""
                UPDATE grouping_work
                SET status = 'RETRYABLE', processing_started_at = NULL,
                    next_attempt_at = CURRENT_TIMESTAMP - INTERVAL '1 second'
                WHERE event_id = ?
                """, eventId);

        ClaimedGroupingWork secondClaim = claimService.claimNextEligibleForProcessing(1).get(0);

        assertEquals(eventId, firstClaim.eventId());
        assertEquals(eventId, secondClaim.eventId());
        assertEquals(1, firstClaim.attemptNumber());
        assertEquals(2, secondClaim.attemptNumber());
    }

    @Test
    void ownershipVerificationLocksTheProcessingRowUntilTransactionCompletion() throws Exception {
        UUID eventId = newWork("PENDING", 0, null, null, null, null);
        ClaimedGroupingWork claim = claimService.claimNextEligibleForProcessing(1).get(0);
        CountDownLatch ownershipLocked = new CountDownLatch(1);
        CountDownLatch releaseOwnership = new CountDownLatch(1);
        ExecutorService executor = Executors.newFixedThreadPool(2);
        try {
            Future<?> ownerTransaction = executor.submit(() -> new TransactionTemplate(transactionManager)
                    .executeWithoutResult(transaction -> {
                        assertTrue(groupingWorkRepository.lockCurrentClaim(eventId, claim.attemptNumber()).isPresent());
                        ownershipLocked.countDown();
                        await(releaseOwnership);
                    }));
            assertTrue(ownershipLocked.await(5, TimeUnit.SECONDS));

            CountDownLatch recoveryStarted = new CountDownLatch(1);
            Future<Integer> recoveryUpdate = executor.submit(() -> {
                recoveryStarted.countDown();
                return jdbcTemplate.update("""
                        UPDATE grouping_work SET status = 'RETRYABLE', processing_started_at = NULL,
                            next_attempt_at = CURRENT_TIMESTAMP WHERE event_id = ?
                        """, eventId);
            });
            assertTrue(recoveryStarted.await(5, TimeUnit.SECONDS));
            Thread.sleep(150);
            assertFalse(recoveryUpdate.isDone(), "recovery must wait for the ownership transaction lock");

            releaseOwnership.countDown();
            ownerTransaction.get(5, TimeUnit.SECONDS);
            assertEquals(1, recoveryUpdate.get(5, TimeUnit.SECONDS));
            assertTrue(groupingWorkRepository.lockCurrentClaim(eventId, claim.attemptNumber()).isEmpty());
        } finally {
            releaseOwnership.countDown();
            executor.shutdownNow();
        }
    }

    private UUID newWork(
            String status,
            int attempts,
            LocalDateTime nextAttemptAt,
            LocalDateTime lastAttemptAt,
            String failureCode,
            String failureReason) {
        UUID eventId = UUID.randomUUID();
        eventIds.add(eventId);
        jdbcTemplate.update("""
                INSERT INTO failure_event (
                    event_id, occurred_at, ingested_at, service_name, environment,
                    event_type, processing_status, version
                ) VALUES (?, now(), now(), ?, 'test', 'exception', 'NORMALIZED', 0)
                """, eventId, SERVICE_NAME);
        jdbcTemplate.update("""
                INSERT INTO normalized_failure_event(event_id, normalization_status)
                VALUES (?, 'FULLY_NORMALIZED')
                """, eventId);
        jdbcTemplate.update("""
                INSERT INTO grouping_work (
                    event_id, status, attempt_count, next_attempt_at, last_attempt_at,
                    processing_started_at, failure_code, failure_reason
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """, eventId, status, attempts, nextAttemptAt, lastAttemptAt,
                "PROCESSING".equals(status) ? lastAttemptAt : null, failureCode, failureReason);
        return eventId;
    }

    private String status(UUID eventId) {
        return jdbcTemplate.queryForObject("SELECT status FROM grouping_work WHERE event_id = ?", String.class,
                eventId);
    }

    private void deleteFixtures() {
        jdbcTemplate.update("""
                DELETE FROM grouping_work WHERE event_id IN (
                    SELECT event_id FROM failure_event WHERE service_name = ?
                )
                """, SERVICE_NAME);
        jdbcTemplate.update("""
                DELETE FROM normalized_failure_event WHERE event_id IN (
                    SELECT event_id FROM failure_event WHERE service_name = ?
                )
                """, SERVICE_NAME);
        jdbcTemplate.update("DELETE FROM failure_event WHERE service_name = ?", SERVICE_NAME);
    }

    private static void await(CountDownLatch latch) {
        try {
            assertTrue(latch.await(5, TimeUnit.SECONDS));
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted while holding grouping ownership lock", exception);
        }
    }
}
