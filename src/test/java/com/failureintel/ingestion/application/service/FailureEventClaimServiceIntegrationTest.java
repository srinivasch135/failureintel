package com.failureintel.ingestion.application.service;

import com.failureintel.infrastructure.persistence.failureevent.entity.FailureEventEntity;
import com.failureintel.infrastructure.persistence.failureevent.entity.ProcessingStatus;
import com.failureintel.infrastructure.persistence.failureevent.repository.FailureEventRepository;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.TestPropertySource;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.time.Instant;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@SpringBootTest
@Testcontainers
@TestPropertySource(properties = "spring.jpa.hibernate.ddl-auto=validate")
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
    private FailureEventRepository failureEventRepository;

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @Autowired
    private PlatformTransactionManager transactionManager;

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
        FailureEventEntity processing = persistEvent("processing", ProcessingStatus.PROCESSING,
                Instant.parse("2026-09-15T10:03:00Z"));
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

    private boolean disjoint(Set<UUID> first, Set<UUID> second) {
        Set<UUID> intersection = new HashSet<>(first);
        intersection.retainAll(second);
        return intersection.isEmpty();
    }
}
