package com.failureintel.ingestion.application.service;

import com.failureintel.FailureintelApplication;
import com.failureintel.infrastructure.persistence.failureevent.entity.FailureEventEntity;
import com.failureintel.infrastructure.persistence.failureevent.entity.ProcessingStatus;
import com.failureintel.infrastructure.persistence.failureevent.repository.FailureEventRepository;
import com.failureintel.infrastructure.persistence.normalizedFailureEvent.repository.NormalizedFailureEventRepository;
import com.failureintel.infrastructure.persistence.incidentgrouping.repository.GroupingWorkRepository;
import com.failureintel.test.support.PostgresTestContainer;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.util.UUID;
import java.util.Optional;

import static com.failureintel.test.support.FailureEventTestFixtures.validRequest;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class FailureEventProcessingWorkerRestartIntegrationTest {

    @Test
    void groupingPauseAndResume() {
        UUID eventId;
        try (ConfigurableApplicationContext normalizationContext = startApplicationContext(true, false)) {
            var request = validRequest("grouping-pause-" + UUID.randomUUID());
            request.setServiceName("grouping-pause-" + UUID.randomUUID());
            eventId = UUID.fromString(normalizationContext.getBean(FailureEventIngestionService.class)
                    .ingestFailureEvent(request));

            Awaitility.await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
                assertEquals(ProcessingStatus.NORMALIZED,
                        normalizationContext.getBean(FailureEventRepository.class)
                                .findById(eventId).orElseThrow().getProcessingStatus());
                assertEquals(1, normalizationContext.getBean(JdbcTemplate.class).queryForObject(
                        "SELECT COUNT(*) FROM grouping_work WHERE event_id = ? AND status = 'PENDING'",
                        Integer.class, eventId));
                assertEquals(0, normalizationContext.getBean(JdbcTemplate.class).queryForObject(
                        "SELECT attempt_count FROM grouping_work WHERE event_id = ?", Integer.class, eventId));
            });
        }

        try (ConfigurableApplicationContext pausedContext = startApplicationContext(false, false)) {
            Awaitility.await().during(Duration.ofMillis(400)).atMost(Duration.ofSeconds(2)).untilAsserted(() -> {
                assertEquals("PENDING", pausedContext.getBean(GroupingWorkRepository.class)
                        .findById(eventId).orElseThrow().getStatus());
                assertEquals(0, pausedContext.getBean(JdbcTemplate.class).queryForObject(
                        "SELECT attempt_count FROM grouping_work WHERE event_id = ?", Integer.class, eventId));
            });
        }

        try (ConfigurableApplicationContext groupingContext = startApplicationContext(false, true)) {
            JdbcTemplate jdbcTemplate = groupingContext.getBean(JdbcTemplate.class);
            Awaitility.await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
                assertEquals("COMPLETED", groupingContext.getBean(GroupingWorkRepository.class)
                        .findById(eventId).orElseThrow().getStatus());
                assertEquals(1, groupingContext.getBean(JdbcTemplate.class).queryForObject(
                        "SELECT COUNT(*) FROM incident_membership WHERE event_id = ?", Integer.class, eventId));
                assertEquals(1, groupingContext.getBean(JdbcTemplate.class).queryForObject(
                        "SELECT attempt_count FROM grouping_work WHERE event_id = ?", Integer.class, eventId));
                assertEquals(ProcessingStatus.NORMALIZED,
                        groupingContext.getBean(FailureEventRepository.class)
                                .findById(eventId).orElseThrow().getProcessingStatus());
                assertEquals(1, countNormalized(groupingContext.getBean(JdbcTemplate.class), eventId));
            });
            UUID incidentId = jdbcTemplate.queryForObject(
                    "SELECT incident_id FROM incident_membership WHERE event_id = ?", UUID.class, eventId);
            new TransactionTemplate(groupingContext.getBean(PlatformTransactionManager.class))
                    .executeWithoutResult(transaction -> {
                        jdbcTemplate.update("DELETE FROM incident_membership WHERE event_id = ?", eventId);
                        jdbcTemplate.update("DELETE FROM grouping_work WHERE event_id = ?", eventId);
                        jdbcTemplate.update("DELETE FROM incident WHERE incident_id = ?", incidentId);
                        groupingContext.getBean(NormalizedFailureEventRepository.class).deleteById(eventId);
                        groupingContext.getBean(FailureEventRepository.class).deleteById(eventId);
                    });
        }
    }

    @Test
    void shouldResumePersistedWorkAfterRestart() {
        UUID abandonedClaimId;
        UUID receivedEventId;
        UUID retryableEventId;
        UUID normalizedEventId;
        try (ConfigurableApplicationContext initialContext = startApplicationContext(false)) {
            FailureEventIngestionService ingestionService = initialContext.getBean(
                    FailureEventIngestionService.class);
            normalizedEventId = UUID.fromString(ingestionService.ingestFailureEvent(
                    validRequest("worker-restart-normalized-" + UUID.randomUUID())));
            ClaimedFailureEvent normalizedClaim = initialContext.getBean(FailureEventClaimService.class)
                    .claimNextEligibleForProcessing(1).stream()
                    .filter(candidate -> candidate.eventId().equals(normalizedEventId))
                    .findFirst().orElseThrow();
            initialContext.getBean(FailureEventNormalizationProcessor.class).process(normalizedClaim);
            assertEquals(1, initialContext.getBean(JdbcTemplate.class).queryForObject(
                    "SELECT COUNT(*) FROM grouping_work WHERE event_id = ? AND status = 'PENDING'",
                    Integer.class, normalizedEventId));

            abandonedClaimId = UUID.fromString(ingestionService.ingestFailureEvent(
                    validRequest("worker-restart-claimed-" + UUID.randomUUID())));

            ClaimedFailureEvent claim = initialContext.getBean(FailureEventClaimService.class)
                    .claimNextEligibleForProcessing(1)
                    .get(0);
            assertEquals(abandonedClaimId, claim.eventId());
            assertEquals(ProcessingStatus.PROCESSING,
                    initialContext.getBean(FailureEventRepository.class)
                            .findById(abandonedClaimId).orElseThrow().getProcessingStatus());

            initialContext.getBean(JdbcTemplate.class).update(
                    "UPDATE failure_event SET processing_started_at = ?, version = version + 1 "
                            + "WHERE event_id = ?",
                    Timestamp.from(Instant.now().minusSeconds(3600)),
                    abandonedClaimId);

            retryableEventId = UUID.fromString(ingestionService.ingestFailureEvent(
                    validRequest("worker-restart-retryable-" + UUID.randomUUID())));
            ClaimedFailureEvent retryableClaim = initialContext.getBean(FailureEventClaimService.class)
                    .claimNextEligibleForProcessing(1)
                    .stream()
                    .filter(candidate -> candidate.eventId().equals(retryableEventId))
                    .findFirst()
                    .orElseThrow();
            boolean recorded = initialContext.getBean(FailureEventRetryStateRecorder.class).recordFailure(
                    retryableClaim,
                    FailureEventRetryableFailure.DATABASE_TIMEOUT,
                    Optional.of(Instant.now().plusSeconds(60)));
            assertTrue(recorded);
            FailureEventEntity retryable = initialContext.getBean(FailureEventRepository.class)
                    .findById(retryableEventId)
                    .orElseThrow();
            assertEquals(ProcessingStatus.RETRYABLE, retryable.getProcessingStatus());
            assertEquals(1, retryable.getAttemptCount());
            assertEquals("DATABASE_TIMEOUT", retryable.getFailureCode());
            assertNotNull(retryable.getNextAttemptAt());
            assertNull(retryable.getProcessingStartedAt());

            receivedEventId = UUID.fromString(ingestionService.ingestFailureEvent(
                    validRequest("worker-restart-received-" + UUID.randomUUID())));

            FailureEventEntity persisted = initialContext.getBean(FailureEventRepository.class)
                    .findById(receivedEventId)
                    .orElseThrow();
            assertEquals(ProcessingStatus.RECEIVED, persisted.getProcessingStatus());

        }

        try (ConfigurableApplicationContext restartedContext = startApplicationContext(true)) {
            FailureEventRepository failureEventRepository = restartedContext.getBean(FailureEventRepository.class);
            NormalizedFailureEventRepository normalizedRepository = restartedContext.getBean(
                    NormalizedFailureEventRepository.class);
            JdbcTemplate jdbcTemplate = restartedContext.getBean(JdbcTemplate.class);
            GroupingWorkRepository groupingWorkRepository = restartedContext.getBean(GroupingWorkRepository.class);

            assertTrue(failureEventRepository.existsById(abandonedClaimId),
                    "The raw event should remain stored when the first application context closes");
            assertTrue(failureEventRepository.existsById(receivedEventId),
                    "The received event should remain stored when the first application context closes");
            assertEquals("PENDING", groupingWorkRepository.findById(normalizedEventId).orElseThrow().getStatus());
            assertEquals(1, jdbcTemplate.queryForObject(
                    "SELECT COUNT(*) FROM grouping_work WHERE event_id = ?",
                    Integer.class, normalizedEventId));

            Awaitility.await()
                    .atMost(Duration.ofSeconds(10))
                    .untilAsserted(() -> {
                        FailureEventEntity recovered = failureEventRepository.findById(abandonedClaimId)
                                .orElseThrow();
                        assertEquals(ProcessingStatus.RETRYABLE, recovered.getProcessingStatus());
                        assertEquals(1, recovered.getAttemptCount(),
                                "Recovery must not consume another attempt");
                        assertEquals("WORKER_LEASE_EXPIRED", recovered.getFailureCode());
                        assertNotNull(recovered.getNextAttemptAt());
                        assertTrue(recovered.getNextAttemptAt().isAfter(Instant.now()),
                                "The retry should wait for its configured backoff");
                        assertNull(recovered.getProcessingStartedAt());
                    });

            jdbcTemplate.update(
                    "UPDATE failure_event SET next_attempt_at = ?, version = version + 1 WHERE event_id = ?",
                    Timestamp.from(Instant.now().minusSeconds(1)),
                    abandonedClaimId);

            jdbcTemplate.update(
                    "UPDATE failure_event SET next_attempt_at = ?, version = version + 1 WHERE event_id = ?",
                    Timestamp.from(Instant.now().minusSeconds(1)),
                    retryableEventId);

            Awaitility.await()
                    .atMost(Duration.ofSeconds(10))
                    .untilAsserted(() -> {
                        assertEquals(ProcessingStatus.NORMALIZED, failureEventRepository.findById(abandonedClaimId)
                                .orElseThrow().getProcessingStatus());
                        assertEquals(ProcessingStatus.NORMALIZED, failureEventRepository.findById(receivedEventId)
                                .orElseThrow().getProcessingStatus());
                        assertEquals(ProcessingStatus.NORMALIZED, failureEventRepository.findById(retryableEventId)
                                .orElseThrow().getProcessingStatus());
                        assertTrue(normalizedRepository.existsById(abandonedClaimId));
                        assertTrue(normalizedRepository.existsById(receivedEventId));
                        assertTrue(normalizedRepository.existsById(retryableEventId));
                        assertEquals(2, failureEventRepository.findById(abandonedClaimId)
                                .orElseThrow().getAttemptCount());
                        assertEquals(2, failureEventRepository.findById(retryableEventId)
                                .orElseThrow().getAttemptCount());
                        assertEquals(1, countNormalized(jdbcTemplate, abandonedClaimId));
                        assertEquals(1, countNormalized(jdbcTemplate, receivedEventId));
                        assertEquals(1, countNormalized(jdbcTemplate, retryableEventId));
                    });

            assertTrue(failureEventRepository.existsById(abandonedClaimId));
            assertTrue(failureEventRepository.existsById(receivedEventId));
            assertTrue(failureEventRepository.existsById(retryableEventId));
            jdbcTemplate.update(
                    "DELETE FROM grouping_work WHERE event_id IN (?, ?, ?, ?)",
                    abandonedClaimId, receivedEventId, retryableEventId, normalizedEventId);
            normalizedRepository.deleteAllById(
                    java.util.List.of(abandonedClaimId, receivedEventId, retryableEventId, normalizedEventId));
            failureEventRepository.deleteAllById(
                    java.util.List.of(abandonedClaimId, receivedEventId, retryableEventId, normalizedEventId));
        }
    }

    private int countNormalized(JdbcTemplate jdbcTemplate, UUID eventId) {
        return jdbcTemplate.queryForObject(
                "SELECT COUNT(*) FROM normalized_failure_event WHERE event_id = ?",
                Integer.class,
                eventId);
    }

    private ConfigurableApplicationContext startApplicationContext(boolean workerEnabled) {
        return startApplicationContext(workerEnabled, false);
    }

    private ConfigurableApplicationContext startApplicationContext(
            boolean normalizationEnabled, boolean groupingEnabled) {
        return new SpringApplicationBuilder(FailureintelApplication.class)
                .web(WebApplicationType.NONE)
                .registerShutdownHook(false)
                .run(
                        "--spring.datasource.url=" + PostgresTestContainer.jdbcUrl(
                                "failureintel_worker_restart_test"),
                        "--spring.datasource.username=" + PostgresTestContainer.username(),
                        "--spring.datasource.password=" + PostgresTestContainer.password(),
                        "--spring.jpa.hibernate.ddl-auto=validate",
                        "--failure-event.processing.worker.enabled=" + normalizationEnabled,
                        "--failure-event.processing.worker.fixed-delay=100ms",
                        "--failure-event.processing.worker.recovery-scan-interval=100ms",
                        "--failure-event.processing.worker.shutdown-await=5s",
                        "--failure-event.grouping.worker.enabled=" + groupingEnabled,
                        "--failure-event.grouping.worker.fixed-delay=100ms",
                        "--failure-event.grouping.worker.processing-lease-timeout=200ms",
                        "--failure-event.grouping.worker.shutdown-await=5s",
                        "--failure-event.processing.retry.delays=PT1M",
                        "--spring.main.banner-mode=off");
    }
}
