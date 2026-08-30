package com.failureintel.ingestion.application.service;

import com.failureintel.ingestion.api.dto.FailureEventIngestionRequest;
import com.failureintel.ingestion.domain.normalization.NormalizationStatus;
import com.failureintel.infrastructure.persistence.failureevent.entity.FailureEventEntity;
import com.failureintel.infrastructure.persistence.failureevent.entity.ProcessingStatus;
import com.failureintel.infrastructure.persistence.failureevent.repository.FailureEventRepository;
import com.failureintel.infrastructure.persistence.normalizedFailureEvent.entity.NormalizedFailureEventEntity;
import com.failureintel.infrastructure.persistence.normalizedFailureEvent.repository.NormalizedFailureEventRepository;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.time.Instant;
import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

@SpringBootTest
@Testcontainers
class FailureEventIngestionServiceIntegrationTest {

    @Container
    @ServiceConnection
    static final PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>(
            DockerImageName.parse("postgres:16-alpine"))
            .withDatabaseName("failureintel_ingestion_test")
            .withUsername("testuser")
            .withPassword("testpassword");

    @Autowired
    private FailureEventIngestionService ingestionService;

    @Autowired
    private FailureEventRepository failureEventRepository;

    @Autowired
    private NormalizedFailureEventRepository normalizedFailureEventRepository;

    @AfterEach
    void cleanUp() {
        normalizedFailureEventRepository.deleteAll();
        failureEventRepository.deleteAll();
    }

    @Test
    void shouldIngestNormalizeAndPersistFailureEvent() {
        Instant occurredAt = Instant.parse("2026-06-18T12:00:00Z");

        FailureEventIngestionRequest request = new FailureEventIngestionRequest();
        request.setOccurredAt(occurredAt);
        request.setServerName("checkout-api");
        request.setServiceName("Payment-Service");
        request.setEnvironment("production");
        request.setEventType("ERROR");
        request.setErrorType("PSQLException");
        request.setErrorMessage("Connection timed out");
        request.setTraceId("trace-123");
        request.setSeverityHint("HIGH");
        request.setRawPayload(Map.of("message", "Connection timed out"));

        String eventId = ingestionService.ingestFailureEvent(request);

        assertNotNull(eventId);

        FailureEventEntity savedEvent = failureEventRepository.findById(UUID.fromString(eventId))
                .orElseThrow();

        assertEquals(eventId, savedEvent.getEventId().toString());

        assertNotNull(savedEvent.getRawPayload());
        assertTrue(savedEvent.getRawPayload().contains("\"message\":\"Connection timed out\""));

        assertEquals(ProcessingStatus.NORMALIZED, savedEvent.getProcessingStatus());

        NormalizedFailureEventEntity normalizedEntity = normalizedFailureEventRepository.findById(UUID.fromString(eventId))
                .orElseThrow();

        assertEquals(savedEvent.getEventId(), normalizedEntity.getEventId());
        assertNotNull(normalizedEntity.getNormalizedPayload());
        assertEquals("Connection timed out", normalizedEntity.getNormalizedPayload().get("message"));
        assertEquals("payment-service", normalizedEntity.getNormalizedServiceName());
        assertEquals("prod", normalizedEntity.getNormalizedEnvironment());
        assertEquals("exception", normalizedEntity.getNormalizedEventType());
        assertEquals("PSQLException", normalizedEntity.getNormalizedErrorType());
        assertEquals("high", normalizedEntity.getNormalizedSeverity());
        assertEquals(occurredAt, normalizedEntity.getNormalizedOccurredAt());
        assertNotNull(normalizedEntity.getNormalizationMetadata());
        assertEquals(NormalizationStatus.FULLY_NORMALIZED, normalizedEntity.getNormalizationStatus());
    }
}
