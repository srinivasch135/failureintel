package com.failureintel.ingestion.application.service;

import com.failureintel.infrastructure.persistence.failureevent.entity.ProcessingStatus;
import com.failureintel.infrastructure.persistence.failureevent.repository.FailureEventRepository;
import com.failureintel.infrastructure.persistence.normalizedFailureEvent.repository.NormalizedFailureEventRepository;
import io.micrometer.core.instrument.MeterRegistry;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.boot.test.context.SpringBootTest;
import com.failureintel.test.support.PostgresTestContainer;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.bean.override.mockito.MockitoBean;

import java.util.UUID;
import java.util.LinkedHashMap;
import java.util.Map;

import static com.failureintel.test.support.FailureEventTestFixtures.validRequest;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.verifyNoInteractions;

@SpringBootTest
@TestPropertySource(properties = "spring.jpa.hibernate.ddl-auto=validate")
class FailureEventDurableCaptureIntegrationTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(FailureEventDurableCaptureIntegrationTest.class);

    @DynamicPropertySource
    static void postgresProperties(DynamicPropertyRegistry registry) {
        PostgresTestContainer.registerDatabase(registry, "failureintel_durable_capture_test");
    }

    @Autowired
    private FailureEventIngestionService ingestionService;

    @Autowired
    private FailureEventRepository failureEventRepository;

    @Autowired
    private MeterRegistry meterRegistry;

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @Autowired
    private ApplicationContext applicationContext;

    @MockitoBean
    private NormalizedFailureEventRepository normalizedFailureEventRepository;

    @AfterEach
    void cleanUp() {
        failureEventRepository.deleteAll();
    }

    @Test
    void shouldCommitRawRowWithoutCallingNormalizedRepository() {
        String eventId = ingestionService.ingestFailureEvent(validRequest("trace-durable-capture-001"));

        var persisted = failureEventRepository.findById(UUID.fromString(eventId)).orElseThrow();
        assertEquals(ProcessingStatus.RECEIVED, persisted.getProcessingStatus());
        assertEquals(1, failureEventRepository.count());
        verifyNoInteractions(normalizedFailureEventRepository);
    }

    @Test
    void shouldExposeDatabaseBackedBacklogAndOldestAgeUsingIngestionTime() {
        Map<ProcessingStatus, Integer> unfinishedAges = new LinkedHashMap<>();
        unfinishedAges.put(ProcessingStatus.RECEIVED, 60);
        unfinishedAges.put(ProcessingStatus.PROCESSING, 40);
        unfinishedAges.put(ProcessingStatus.RETRYABLE, 20);
        Map<ProcessingStatus, Integer> terminalAges = new LinkedHashMap<>();
        terminalAges.put(ProcessingStatus.FAILED, 120);
        terminalAges.put(ProcessingStatus.NORMALIZED, 140);
        terminalAges.put(ProcessingStatus.QUEUED, 160);
        terminalAges.put(ProcessingStatus.PROCESSED, 180);

        unfinishedAges.forEach((status, ageSeconds) -> persistEventWithStatus(status, ageSeconds));
        terminalAges.forEach((status, ageSeconds) -> persistEventWithStatus(status, ageSeconds));

        assertEquals(1.0, meterRegistry.get("failureintel.events.backlog")
                .tag("status", "received").gauge().value());
        assertEquals(1.0, meterRegistry.get("failureintel.events.backlog")
                .tag("status", "processing").gauge().value());
        assertEquals(1.0, meterRegistry.get("failureintel.events.backlog")
                .tag("status", "retryable").gauge().value());

        double oldestAgeSeconds = meterRegistry.get("failureintel.events.backlog.oldest.age").gauge().value();
        assertTrue(oldestAgeSeconds >= 59.0 && oldestAgeSeconds <= 62.0,
                "age should derive from the oldest unfinished ingested_at timestamp");

        assertNull(meterRegistry.find("failureintel.events.backlog").tag("status", "failed").gauge(),
                "only unfinished lifecycle states receive backlog gauges");
        assertTrue(applicationContext.getBeansOfType(FailureEventProcessingWorker.class).isEmpty(),
                "the worker is disabled in this test context");

        // Gauges query PostgreSQL at observation time; a committed status change is visible on the next read.
        jdbcTemplate.update("UPDATE failure_event SET processing_status = 'FAILED' " +
                "WHERE processing_status = 'RECEIVED'");
        assertEquals(0.0, meterRegistry.get("failureintel.events.backlog")
                .tag("status", "received").gauge().value());
        assertEquals(1.0, meterRegistry.get("failureintel.events.backlog")
                .tag("status", "processing").gauge().value());
        assertEquals(1.0, meterRegistry.get("failureintel.events.backlog")
                .tag("status", "retryable").gauge().value());
    }

    @Test
    void shouldReportZeroForAnAvailableDatabaseWithNoBacklog() {
        assertEquals(0, failureEventRepository.count());
        assertEquals(0.0, meterRegistry.get("failureintel.events.backlog")
                .tag("status", "received").gauge().value());
        assertEquals(0.0, meterRegistry.get("failureintel.events.backlog")
                .tag("status", "processing").gauge().value());
        assertEquals(0.0, meterRegistry.get("failureintel.events.backlog")
                .tag("status", "retryable").gauge().value());
        assertEquals(0.0, meterRegistry.get("failureintel.events.backlog.oldest.age").gauge().value());
    }

    private void persistEventWithStatus(ProcessingStatus status, int ingestedAgeSeconds) {
        String eventId = ingestionService.ingestFailureEvent(
                validRequest("trace-metrics-" + status.name().toLowerCase()));
        UUID id = UUID.fromString(eventId);
        jdbcTemplate.update("""
                UPDATE failure_event
                SET processing_status = ?,
                    ingested_at = CURRENT_TIMESTAMP - (? * INTERVAL '1 second'),
                    occurred_at = CURRENT_TIMESTAMP - (? * INTERVAL '1 second'),
                    next_attempt_at = CASE WHEN ? = 'RETRYABLE'
                        THEN CURRENT_TIMESTAMP + INTERVAL '1 day' ELSE NULL END
                WHERE event_id = ?
                """, status.name(), ingestedAgeSeconds,
                status == ProcessingStatus.RECEIVED ? 3600 : ingestedAgeSeconds, status.name(),
                id);
    }
}
