package com.failureintel.ingestion.application.service;

import com.failureintel.infrastructure.persistence.failureevent.entity.FailureEventEntity;
import com.failureintel.infrastructure.persistence.failureevent.entity.ProcessingStatus;
import com.failureintel.infrastructure.persistence.failureevent.repository.FailureEventRepository;
import com.failureintel.ingestion.domain.model.NormalizedFailureEvent;
import com.failureintel.ingestion.domain.normalization.FailureEventNormalizer;
import com.failureintel.ingestion.domain.normalization.NormalizationStatus;
import com.failureintel.infrastructure.persistence.normalizedFailureEvent.repository.NormalizedFailureEventRepository;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.time.Instant;
import java.util.Map;
import java.util.UUID;

import static com.failureintel.test.support.FailureEventTestFixtures.validRequest;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

@SpringBootTest
@Testcontainers
@TestPropertySource(properties = "spring.jpa.hibernate.ddl-auto=validate")
class FailureEventNormalizationProcessorRollbackIntegrationTest {

    private static final Logger LOGGER =
            LoggerFactory.getLogger(FailureEventNormalizationProcessorRollbackIntegrationTest.class);

    @Container
    @ServiceConnection
    static final PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>(
            DockerImageName.parse("postgres:16-alpine"))
            .withDatabaseName("failureintel_normalization_rollback_test")
            .withUsername("testuser")
            .withPassword("testpassword")
            .withLogConsumer(new Slf4jLogConsumer(LOGGER));

    @Autowired
    private FailureEventIngestionService ingestionService;

    @Autowired
    private FailureEventClaimService claimService;

    @Autowired
    private FailureEventNormalizationProcessor processor;

    @Autowired
    private FailureEventProcessingService processingService;

    @Autowired
    private FailureEventRepository failureEventRepository;

    @Autowired
    private NormalizedFailureEventRepository normalizedFailureEventRepository;

    @MockitoBean
    private FailureEventNormalizer failureEventNormalizer;

    @AfterEach
    void cleanUp() {
        failureEventRepository.deleteAll();
    }

    @Test
    void shouldRollBackRawNormalizationStatusWhenNormalizedPersistenceFails() {
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
    }

    @Test
    void shouldRecordRetryableAfterNormalizedWriteTransactionRollsBack() {
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
}
