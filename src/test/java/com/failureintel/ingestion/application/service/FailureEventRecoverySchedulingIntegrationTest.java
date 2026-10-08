package com.failureintel.ingestion.application.service;

import com.failureintel.infrastructure.persistence.failureevent.entity.FailureEventEntity;
import com.failureintel.infrastructure.persistence.failureevent.entity.ProcessingStatus;
import com.failureintel.infrastructure.persistence.failureevent.repository.FailureEventRepository;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import com.failureintel.test.support.PostgresTestContainer;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.bean.override.mockito.MockitoBean;

import java.time.Duration;
import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static com.failureintel.test.support.FailureEventTestFixtures.validRequest;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;

@SpringBootTest
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
@TestPropertySource(properties = {
        "spring.jpa.hibernate.ddl-auto=validate",
        "failure-event.processing.worker.enabled=true",
        "failure-event.processing.worker.fixed-delay=50ms",
        "failure-event.processing.worker.recovery-scan-interval=50ms",
        "failure-event.processing.worker.batch-size=1",
        "failure-event.processing.worker.concurrency=1",
        "failure-event.processing.worker.shutdown-await=0s"
})
class FailureEventRecoverySchedulingIntegrationTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(FailureEventRecoverySchedulingIntegrationTest.class);

    @DynamicPropertySource
    static void postgresProperties(DynamicPropertyRegistry registry) {
        PostgresTestContainer.registerDatabase(registry, "failureintel_recovery_schedule_test");
    }

    @Autowired
    private FailureEventIngestionService ingestionService;

    @Autowired
    private FailureEventRepository failureEventRepository;

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @MockitoBean
    private FailureEventProcessingService processingService;

    @Test
    void shouldRecoverExpiredEventWhileNormalWorkerWaitsForProcessing() throws Exception {
        CountDownLatch processingStarted = new CountDownLatch(1);
        CountDownLatch releaseProcessing = new CountDownLatch(1);
        CountDownLatch processingFinished = new CountDownLatch(1);
        doAnswer(invocation -> {
            processingStarted.countDown();
            try {
                assertTrue(releaseProcessing.await(10, TimeUnit.SECONDS));
                return null;
            } finally {
                processingFinished.countDown();
            }
        }).when(processingService).process(any(ClaimedFailureEvent.class));

        UUID processingEventId = UUID.fromString(
                ingestionService.ingestFailureEvent(validRequest("schedule-blocked-a-" + UUID.randomUUID())));
        try {
            assertTrue(processingStarted.await(10, TimeUnit.SECONDS),
                    "The normal worker should claim event A and enter its blocked processing task");
            assertEquals(ProcessingStatus.PROCESSING,
                    failureEventRepository.findById(processingEventId).orElseThrow().getProcessingStatus());
            assertEquals(1L, processingFinished.getCount(), "Event A processing should remain blocked");

            UUID abandonedEventId = UUID.fromString(
                    ingestionService.ingestFailureEvent(validRequest("schedule-expired-b-" + UUID.randomUUID())));
            Instant oldLease = Instant.now().minus(Duration.ofHours(1));
            jdbcTemplate.update(
                    "UPDATE failure_event SET processing_status = 'PROCESSING', attempt_count = 1, "
                            + "processing_started_at = ?, last_attempt_at = ?, version = version + 1 "
                            + "WHERE event_id = ?",
                    java.sql.Timestamp.from(oldLease),
                    java.sql.Timestamp.from(oldLease),
                    abandonedEventId);

            org.awaitility.Awaitility.await()
                    .atMost(Duration.ofSeconds(10))
                    .untilAsserted(() -> {
                        FailureEventEntity recovered = failureEventRepository.findById(abandonedEventId)
                                .orElseThrow();
                        assertEquals(ProcessingStatus.RETRYABLE, recovered.getProcessingStatus());
                        assertEquals(1, recovered.getAttemptCount());
                        assertEquals("WORKER_LEASE_EXPIRED", recovered.getFailureCode());
                    });

            assertEquals(ProcessingStatus.PROCESSING,
                    failureEventRepository.findById(processingEventId).orElseThrow().getProcessingStatus());
            assertEquals(1L, processingFinished.getCount(), "Recovery must complete before event A is released");
        } finally {
            releaseProcessing.countDown();
        }
    }
}
