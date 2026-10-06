package com.failureintel.ingestion.application.service;

import com.failureintel.FailureintelApplication;
import com.failureintel.infrastructure.persistence.failureevent.entity.FailureEventEntity;
import com.failureintel.infrastructure.persistence.failureevent.entity.ProcessingStatus;
import com.failureintel.infrastructure.persistence.failureevent.repository.FailureEventRepository;
import com.failureintel.infrastructure.persistence.normalizedFailureEvent.repository.NormalizedFailureEventRepository;
import com.failureintel.test.support.PostgresTestContainer;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.jdbc.core.JdbcTemplate;

import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.util.UUID;

import static com.failureintel.test.support.FailureEventTestFixtures.validRequest;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class FailureEventProcessingWorkerRestartIntegrationTest {

    @Test
    void shouldResumeReceivedAndAbandonedProcessingEventsAfterApplicationContextRestart() {
        UUID abandonedClaimId;
        UUID receivedEventId;
        try (ConfigurableApplicationContext initialContext = startApplicationContext(false)) {
            FailureEventIngestionService ingestionService = initialContext.getBean(
                    FailureEventIngestionService.class);
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

            assertTrue(failureEventRepository.existsById(abandonedClaimId),
                    "The raw event should remain stored when the first application context closes");
            assertTrue(failureEventRepository.existsById(receivedEventId),
                    "The received event should remain stored when the first application context closes");

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

            Awaitility.await()
                    .atMost(Duration.ofSeconds(10))
                    .untilAsserted(() -> {
                        assertEquals(ProcessingStatus.NORMALIZED, failureEventRepository.findById(abandonedClaimId)
                                .orElseThrow().getProcessingStatus());
                        assertEquals(ProcessingStatus.NORMALIZED, failureEventRepository.findById(receivedEventId)
                                .orElseThrow().getProcessingStatus());
                        assertTrue(normalizedRepository.existsById(abandonedClaimId));
                        assertTrue(normalizedRepository.existsById(receivedEventId));
                        assertEquals(2, failureEventRepository.findById(abandonedClaimId)
                                .orElseThrow().getAttemptCount());
                    });
        }
    }

    private ConfigurableApplicationContext startApplicationContext(boolean workerEnabled) {
        return new SpringApplicationBuilder(FailureintelApplication.class)
                .web(WebApplicationType.NONE)
                .registerShutdownHook(false)
                .run(
                        "--spring.datasource.url=" + PostgresTestContainer.jdbcUrl(
                                "failureintel_worker_restart_test"),
                        "--spring.datasource.username=" + PostgresTestContainer.username(),
                        "--spring.datasource.password=" + PostgresTestContainer.password(),
                        "--spring.jpa.hibernate.ddl-auto=validate",
                        "--failure-event.processing.worker.enabled=" + workerEnabled,
                        "--failure-event.processing.worker.fixed-delay=100ms",
                        "--failure-event.processing.worker.recovery-scan-interval=100ms",
                        "--failure-event.processing.worker.shutdown-await=5s",
                        "--failure-event.processing.retry.delays=PT1M",
                        "--spring.main.banner-mode=off");
    }
}
