package com.failureintel.ingestion.application.service;

import com.failureintel.ingestion.api.dto.FailureEventIngestionRequest;
import com.failureintel.ingestion.domain.model.RawFailureEvent;
import com.failureintel.infrastructure.persistence.failureevent.entity.FailureEventEntity;
import com.failureintel.infrastructure.persistence.failureevent.entity.ProcessingStatus;
import com.failureintel.infrastructure.persistence.failureevent.mapper.FailureEventEntityMapper;
import com.failureintel.infrastructure.persistence.failureevent.repository.FailureEventRepository;
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
import static com.failureintel.test.support.FailureEventTestFixtures.OCCURRED_AT;
import static com.failureintel.test.support.FailureEventTestFixtures.rawEvent;

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
    void shouldCaptureRawFailureEventAsReceived() {
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

        assertEquals("checkout-api", savedEvent.getSourceSystem());
        assertEquals("Payment-Service", savedEvent.getServiceName());
        assertEquals("production", savedEvent.getEnvironment());
        assertEquals("ERROR", savedEvent.getEventType());

        assertNotNull(savedEvent.getRawPayload());
        assertTrue(savedEvent.getRawPayload().contains("\"message\":\"Connection timed out\""));

        assertEquals(ProcessingStatus.RECEIVED, savedEvent.getProcessingStatus());
        assertEquals(0, savedEvent.getAttemptCount());
        assertNull(savedEvent.getLastAttemptAt());
        assertNull(savedEvent.getNextAttemptAt());
        assertNull(savedEvent.getProcessingStartedAt());
        assertNull(savedEvent.getFailureCode());
        assertNull(savedEvent.getFailureReason());
        assertFalse(normalizedFailureEventRepository.existsById(UUID.fromString(eventId)));
    }

    @Test
    void shouldPersistWorkerMetadataAndApplyRawEventDefaults() {
        UUID eventId = UUID.randomUUID();
        Instant occurredAt = Instant.parse("2026-09-05T12:00:00Z");
        Instant ingestedAt = Instant.parse("2026-09-05T12:00:05Z");
        Map<String, Object> sourceMetadata = Map.of(
                "collector", "datadog",
                "region", "us-east-1");

        FailureEventEntity event = new FailureEventEntity();
        event.setEventId(eventId);
        event.setOccurredAt(occurredAt);
        event.setIngestedAt(ingestedAt);
        event.setSourceSystem("datadog");
        event.setServiceName("payment-service");
        event.setEnvironment("prod");
        event.setEventType("error");
        event.setRawPayload("{}");
        event.setSourceMetadata(sourceMetadata);

        failureEventRepository.saveAndFlush(event);

        FailureEventEntity persisted = failureEventRepository.findById(eventId).orElseThrow();

        assertEquals(ProcessingStatus.RECEIVED, persisted.getProcessingStatus());
        assertEquals(0, persisted.getAttemptCount());
        assertEquals(sourceMetadata, persisted.getSourceMetadata());
        assertEquals(occurredAt, persisted.getOccurredAt());
        assertEquals(ingestedAt, persisted.getIngestedAt());
        assertNull(persisted.getLastAttemptAt());
        assertNull(persisted.getNextAttemptAt());
        assertNull(persisted.getProcessingStartedAt());
        assertNull(persisted.getFailureCode());
    }

    @Test
    void shouldRoundTripPersistedRawFailureEventWithoutChangingValues() {
        RawFailureEvent original = rawEvent(
                "Payment-Service",
                "PRODUCTION",
                "ERROR",
                "postgres_exception",
                "Connection timeout",
                "payment-database",
                "trace-persisted-roundtrip-001",
                "SEV2",
                OCCURRED_AT,
                Map.of(
                        "Environment", "Payload-Environment",
                        "nested", Map.of("attempt", 3)),
                Map.of(
                        "collector", "datadog",
                        "region", "us-east-1"));

        failureEventRepository.saveAndFlush(FailureEventEntityMapper.fromRaw(original));

        FailureEventEntity persisted = failureEventRepository
                .findById(original.getRawEventId())
                .orElseThrow();
        RawFailureEvent reconstructed = FailureEventEntityMapper.toRaw(persisted);

        assertEquals(original.getRawEventId(), reconstructed.getRawEventId());
        assertEquals(original.getSourceSystem(), reconstructed.getSourceSystem());
        assertEquals(original.getServiceName(), reconstructed.getServiceName());
        assertEquals(original.getEnvironment(), reconstructed.getEnvironment());
        assertEquals(original.getEventType(), reconstructed.getEventType());
        assertEquals(original.getErrorType(), reconstructed.getErrorType());
        assertEquals(original.getErrorMessage(), reconstructed.getErrorMessage());
        assertEquals(original.getDependencyTarget(), reconstructed.getDependencyTarget());
        assertEquals(original.getTraceId(), reconstructed.getTraceId());
        assertEquals(original.getSeverityHint(), reconstructed.getSeverityHint());
        assertEquals(original.getOccurredAt(), reconstructed.getOccurredAt());
        assertEquals(original.getReceivedAt(), reconstructed.getReceivedAt());
        assertEquals(original.getRawPayload(), reconstructed.getRawPayload());
        assertEquals(original.getMetaData(), reconstructed.getMetaData());
        assertEquals(ProcessingStatus.RECEIVED, persisted.getProcessingStatus());
    }
}
