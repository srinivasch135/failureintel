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
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.TestPropertySource;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

import java.time.Instant;
import java.util.Map;
import java.util.UUID;
import static org.junit.jupiter.api.Assertions.*;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@Testcontainers
@TestPropertySource(properties = {
                "spring.jpa.hibernate.ddl-auto=validate"
})
class FailureEventPipelineIntegrationTest {
        private static final Logger LOGGER = LoggerFactory.getLogger(FailureEventPipelineIntegrationTest.class);

        @Container
        @ServiceConnection
        static final PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>(
                        DockerImageName.parse("postgres:16-alpine"))
                        .withDatabaseName("failureintel_pipeline_test")
                        .withUsername("testuser")
                        .withPassword("testpassword")
                        .withLogConsumer(new Slf4jLogConsumer(LOGGER));

        @Autowired
        private FailureEventIngestionService ingestionService;
        @Autowired
        private FailureEventRepository failureEventRepository;
        @Autowired
        private NormalizedFailureEventRepository normalizedFailureEventRepository;
        @Autowired
        private JdbcTemplate jdbcTemplate;

        @AfterEach
        void cleanUpDB() {
                normalizedFailureEventRepository.deleteAll();
                failureEventRepository.deleteAll();

        }

        @Test
        void shouldProcessNormalizePersistAndReadFailureEvent() {
                FailureEventIngestionRequest request = createValidRequest();
                String returnedEventId = ingestionService.ingestFailureEvent(request);
                FailureEventEntity savedEvent = failureEventRepository.findById(toRepositoryId(returnedEventId))
                                .orElseThrow(() -> new AssertionError("Expected persisted failure event: "
                                                + returnedEventId));
                NormalizedFailureEventEntity normalizedEvent = normalizedFailureEventRepository
                                .findById(savedEvent.getEventId())
                                .orElseThrow(() -> new AssertionError("Expected persisted normalized failure event: "
                                                + returnedEventId));
                verifySavedEvent(savedEvent, request);
                verifyNormalizedFields(normalizedEvent);
                verifyProcessingStatus(savedEvent, normalizedEvent);
                verifyPersistenceFields(savedEvent, normalizedEvent);
                verifyRowsShareForeignKey(savedEvent.getEventId());
                verifyFailureEventRow(savedEvent.getEventId());
                verifyFailureEventNoLongerOwnsNormalizedColumns();
                verifyNormalizedFailureEventRow(savedEvent.getEventId());
        }

        @Test
        void shouldPersistUnknownCanonicalValuesAsPartiallyNormalized() {
                FailureEventIngestionRequest request = createValidRequest();
                request.setTraceId("trace-integration-partial-001");
                request.setEnvironment("sandbox");
                request.setEventType("vendor_outage");

                UUID eventId = toRepositoryId(ingestionService.ingestFailureEvent(request));
                FailureEventEntity savedEvent = failureEventRepository.findById(eventId).orElseThrow();
                NormalizedFailureEventEntity normalizedEvent = normalizedFailureEventRepository
                                .findById(eventId)
                                .orElseThrow();

                assertEquals(ProcessingStatus.NORMALIZED, savedEvent.getProcessingStatus());
                assertEquals(NormalizationStatus.PARTIALLY_NORMALIZED,
                                normalizedEvent.getNormalizationStatus());
                assertEquals("unknown", normalizedEvent.getNormalizedEnvironment());
                assertEquals("vendor_outage", normalizedEvent.getNormalizedEventType());
                assertEquals(true,
                                normalizedEvent.getNormalizationMetadata().get("unknownEnvironmentValue"));
                assertEquals(true,
                                normalizedEvent.getNormalizationMetadata().get("unknownEventTypeValue"));
        }

        @Test
        void shouldPersistEmptyPayloadAsFailedWithoutNormalizedRow() {
                FailureEventIngestionRequest request = createValidRequest();
                request.setTraceId("trace-integration-empty-payload-001");
                request.setRawPayload(Map.of());

                UUID eventId = toRepositoryId(ingestionService.ingestFailureEvent(request));
                FailureEventEntity savedEvent = failureEventRepository.findById(eventId).orElseThrow();

                assertEquals(ProcessingStatus.FAILED, savedEvent.getProcessingStatus());
                assertTrue(savedEvent.getFailureReason().contains("Unsupported or empty raw payload"));
                assertFalse(normalizedFailureEventRepository.existsById(eventId));
                assertEquals(1, failureEventRepository.count());
                assertEquals(0, normalizedFailureEventRepository.count());
        }

        @Test
        void shouldPersistEventWithoutUsefulFailureDataAsFailedWithoutNormalizedRow() {
                FailureEventIngestionRequest request = createValidRequest();
                request.setServiceName(" ");
                request.setEnvironment(" ");
                request.setEventType(" ");
                request.setErrorType(null);
                request.setErrorMessage(" ");
                request.setDependencyTarget(null);
                request.setTraceId(null);
                request.setSeverityHint(null);
                request.setRawPayload(Map.of("host", "unknown-host"));

                UUID eventId = toRepositoryId(ingestionService.ingestFailureEvent(request));
                FailureEventEntity savedEvent = failureEventRepository.findById(eventId).orElseThrow();

                assertEquals(ProcessingStatus.FAILED, savedEvent.getProcessingStatus());
                assertEquals("Event does not contain minimum useful failure data",
                                savedEvent.getFailureReason());
                assertFalse(normalizedFailureEventRepository.existsById(eventId));
                assertEquals(1, failureEventRepository.count());
                assertEquals(0, normalizedFailureEventRepository.count());
        }

        @Test
        void shouldRejectDuplicateTraceIdWithoutCreatingAdditionalRows() {
                FailureEventIngestionRequest firstRequest = createValidRequest();

                UUID originalEventId = toRepositoryId(ingestionService.ingestFailureEvent(firstRequest));

                IllegalArgumentException exception = assertThrows(
                                IllegalArgumentException.class,
                                () -> ingestionService.ingestFailureEvent(createValidRequest()));

                assertEquals(
                                "Failure event already exists for traceId: trace-integration-001",
                                exception.getMessage());
                assertEquals(1, failureEventRepository.count());
                assertEquals(1, normalizedFailureEventRepository.count());
                assertTrue(failureEventRepository.existsById(originalEventId));
                assertTrue(normalizedFailureEventRepository.existsById(originalEventId));
        }

        public FailureEventIngestionRequest createValidRequest() {
                FailureEventIngestionRequest request = new FailureEventIngestionRequest();
                request.setServerName("datadog");
                request.setServiceName("payment-service");
                request.setEnvironment("PROD");
                request.setEventType("ERROR");
                request.setErrorType("PSQLException");
                request.setErrorMessage("Connection timeout after 5000ms");
                request.setDependencyTarget("payment-database");
                request.setTraceId("trace-integration-001");
                request.setSeverityHint("ERROR");
                request.setOccurredAt(Instant.parse("2026-08-03T20:00:00Z"));
                request.setRawPayload(Map.of(
                                "host", "payment-prod-01",
                                "region", "us-east-1"));
                return request;
        }

        private UUID toRepositoryId(String eventId) {
                try {
                        return UUID.fromString(eventId);
                } catch (IllegalArgumentException e) {
                        throw new AssertionError("Invalid event ID format: " + eventId, e);
                }
        }

        private void verifySavedEvent(FailureEventEntity savedEvent, FailureEventIngestionRequest request) {
                assertNotNull(savedEvent);

                assertEquals(
                                "payment-service",
                                savedEvent.getServiceName());

                assertEquals(
                                "trace-integration-001",
                                savedEvent.getTraceId());

                assertEquals(
                                "PSQLException",
                                savedEvent.getErrorType());

                assertEquals(
                                "Connection timeout after 5000ms",
                                savedEvent.getMessage());

                assertEquals(
                                Instant.parse("2026-08-03T20:00:00Z"),
                                savedEvent.getOccurredAt());

                assertNotNull(savedEvent.getRawPayload());
        }

        private void verifyNormalizedFields(NormalizedFailureEventEntity normalizedEvent) {
                assertEquals(
                                "payment-service",
                                normalizedEvent.getNormalizedServiceName());

                assertEquals(
                                "prod",
                                normalizedEvent.getNormalizedEnvironment());

                assertEquals(
                                "exception",
                                normalizedEvent.getNormalizedEventType());

                assertEquals(
                                "PSQLException",
                                normalizedEvent.getNormalizedErrorType());

                assertEquals(
                                "high",
                                normalizedEvent.getNormalizedSeverity());
        }

        private void verifyProcessingStatus(
                        FailureEventEntity savedEvent,
                        NormalizedFailureEventEntity normalizedEvent) {
                assertEquals(
                                ProcessingStatus.NORMALIZED,
                                savedEvent.getProcessingStatus());

                assertEquals(
                                NormalizationStatus.FULLY_NORMALIZED,
                                normalizedEvent.getNormalizationStatus());
        }

        private void verifyPersistenceFields(
                        FailureEventEntity savedEvent,
                        NormalizedFailureEventEntity normalizedEvent) {
                assertNotNull(savedEvent.getEventId());
                assertNotNull(savedEvent.getIngestedAt());

                assertFalse(
                                savedEvent.getRawPayload().isEmpty(),
                                "Raw payload should be preserved");

                assertFalse(
                                normalizedEvent.getNormalizedPayload().isEmpty(),
                                "Normalized payload should be preserved");
        }

        private void verifyRowsShareForeignKey(UUID eventId) {
                Integer joinedRowCount = jdbcTemplate.queryForObject(
                                """
                                                SELECT COUNT(*)
                                                FROM failure_event fe
                                                JOIN normalized_failure_event nfe
                                                  ON nfe.event_id = fe.event_id
                                                WHERE fe.event_id = ?
                                                """,
                                Integer.class,
                                eventId);

                assertEquals(1, joinedRowCount);
        }

        private void verifyFailureEventRow(UUID eventId) {
                Map<String, Object> row = jdbcTemplate.queryForMap(
                                """
                                                SELECT service_name,
                                                       environment,
                                                       event_type,
                                                       error_type,
                                                       message,
                                                       dependency_target,
                                                       trace_id,
                                                       severity_hint,
                                                       processing_status,
                                                       raw_payload
                                                FROM failure_event
                                                WHERE event_id = ?
                                                """,
                                eventId);

                assertEquals("payment-service", row.get("service_name"));
                assertEquals("PROD", row.get("environment"));
                assertEquals("ERROR", row.get("event_type"));
                assertEquals("PSQLException", row.get("error_type"));
                assertEquals("Connection timeout after 5000ms", row.get("message"));
                assertEquals("payment-database", row.get("dependency_target"));
                assertEquals("trace-integration-001", row.get("trace_id"));
                assertEquals("ERROR", row.get("severity_hint"));
                assertEquals(ProcessingStatus.NORMALIZED.name(), row.get("processing_status"));
                assertTrue(row.get("raw_payload").toString().contains("payment-prod-01"));
        }

        private void verifyFailureEventNoLongerOwnsNormalizedColumns() {
                Integer oldNormalizedColumnCount = jdbcTemplate.queryForObject(
                                """
                                                SELECT COUNT(*)
                                                FROM information_schema.columns
                                                WHERE table_schema = current_schema()
                                                  AND table_name = 'failure_event'
                                                  AND column_name IN (
                                                      'normalized_payload',
                                                      'normalized_service_name',
                                                      'normalized_environment',
                                                      'normalized_event_type',
                                                      'normalized_error_type',
                                                      'normalized_error_message',
                                                      'normalized_dependency_target',
                                                      'normalized_trace_id',
                                                      'normalized_severity',
                                                      'normalized_occurred_at',
                                                      'normalization_status',
                                                      'normalization_metadata'
                                                  )
                                                """,
                                Integer.class);

                assertEquals(0, oldNormalizedColumnCount);
        }

        private void verifyNormalizedFailureEventRow(UUID eventId) {
                Map<String, Object> row = jdbcTemplate.queryForMap(
                                """
                                                SELECT normalized_service_name,
                                                       normalized_environment,
                                                       normalized_event_type,
                                                       normalized_error_type,
                                                       normalized_error_message,
                                                       normalized_dependency_target,
                                                       normalized_trace_id,
                                                       normalized_severity,
                                                       normalization_status,
                                                       normalized_payload
                                                FROM normalized_failure_event
                                                WHERE event_id = ?
                                                """,
                                eventId);

                assertEquals("payment-service", row.get("normalized_service_name"));
                assertEquals("prod", row.get("normalized_environment"));
                assertEquals("exception", row.get("normalized_event_type"));
                assertEquals("PSQLException", row.get("normalized_error_type"));
                assertEquals("Connection timeout after 5000ms", row.get("normalized_error_message"));
                assertEquals("payment-database", row.get("normalized_dependency_target"));
                assertEquals("trace-integration-001", row.get("normalized_trace_id"));
                assertEquals("high", row.get("normalized_severity"));
                assertEquals(NormalizationStatus.FULLY_NORMALIZED.name(), row.get("normalization_status"));

                String normalizedPayload = row.get("normalized_payload").toString();
                assertTrue(normalizedPayload.contains("payment-prod-01"));
                assertTrue(normalizedPayload.contains("us-east-1"));
        }

}
