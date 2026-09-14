package com.failureintel.ingestion.application.service;

import com.failureintel.ingestion.api.dto.FailureEventIngestionRequest;
import com.failureintel.ingestion.application.exception.DuplicateFailureEventException;
import com.failureintel.infrastructure.persistence.failureevent.entity.FailureEventEntity;
import com.failureintel.infrastructure.persistence.failureevent.entity.ProcessingStatus;
import com.failureintel.infrastructure.persistence.failureevent.repository.FailureEventRepository;
import com.failureintel.infrastructure.persistence.normalizedFailureEvent.repository.NormalizedFailureEventRepository;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.test.context.TestPropertySource;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

import java.time.Instant;
import java.sql.Timestamp;
import java.util.Map;
import java.util.UUID;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
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
        void shouldCaptureAndPersistRawFailureEventWithoutNormalizing() {
                FailureEventIngestionRequest request = createValidRequest();
                String returnedEventId = ingestionService.ingestFailureEvent(request);
                FailureEventEntity savedEvent = failureEventRepository.findById(toRepositoryId(returnedEventId))
                                .orElseThrow(() -> new AssertionError("Expected persisted failure event: "
                                                + returnedEventId));

                verifySavedEvent(savedEvent, request);
                assertEquals(ProcessingStatus.RECEIVED, savedEvent.getProcessingStatus());
                assertEquals(0, savedEvent.getAttemptCount());
                assertNull(savedEvent.getFailureReason());
                assertFalse(normalizedFailureEventRepository.existsById(savedEvent.getEventId()));
                verifyFailureEventRow(savedEvent.getEventId());
                verifyFailureEventNoLongerOwnsNormalizedColumns();
        }

        @Test
        void shouldPreserveUnknownRawValuesForLaterProcessing() {
                FailureEventIngestionRequest request = createValidRequest();
                request.setTraceId("trace-integration-partial-001");
                request.setEnvironment("sandbox");
                request.setEventType("vendor_outage");

                UUID eventId = toRepositoryId(ingestionService.ingestFailureEvent(request));
                FailureEventEntity savedEvent = failureEventRepository.findById(eventId).orElseThrow();

                assertEquals(ProcessingStatus.RECEIVED, savedEvent.getProcessingStatus());
                assertEquals("sandbox", savedEvent.getEnvironment());
                assertEquals("vendor_outage", savedEvent.getEventType());
                assertFalse(normalizedFailureEventRepository.existsById(eventId));
        }

        @Test
        void shouldPersistEmptyPayloadAsReceivedForLaterClassification() {
                FailureEventIngestionRequest request = createValidRequest();
                request.setTraceId("trace-integration-empty-payload-001");
                request.setRawPayload(Map.of());

                UUID eventId = toRepositoryId(ingestionService.ingestFailureEvent(request));
                FailureEventEntity savedEvent = failureEventRepository.findById(eventId).orElseThrow();

                assertEquals(ProcessingStatus.RECEIVED, savedEvent.getProcessingStatus());
                assertNull(savedEvent.getFailureReason());
                assertFalse(normalizedFailureEventRepository.existsById(eventId));
                assertEquals(1, failureEventRepository.count());
                assertEquals(0, normalizedFailureEventRepository.count());
        }

        @Test
        void shouldPersistStorableEventWithoutUsefulDataAsReceivedForLaterClassification() {
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

                assertEquals(ProcessingStatus.RECEIVED, savedEvent.getProcessingStatus());
                assertNull(savedEvent.getFailureReason());
                assertFalse(normalizedFailureEventRepository.existsById(eventId));
                assertEquals(1, failureEventRepository.count());
                assertEquals(0, normalizedFailureEventRepository.count());
        }

        @Test
        void shouldReturnExistingEventForEquivalentLegacyTraceRetry() {
                FailureEventIngestionRequest firstRequest = createValidRequest();

                UUID originalEventId = toRepositoryId(ingestionService.ingestFailureEvent(firstRequest));

                UUID retriedEventId = toRepositoryId(ingestionService.ingestFailureEvent(createValidRequest()));

                assertEquals(originalEventId, retriedEventId);
                assertEquals(1, failureEventRepository.count());
                assertEquals(0, normalizedFailureEventRepository.count());
                assertTrue(failureEventRepository.existsById(originalEventId));
                assertFalse(normalizedFailureEventRepository.existsById(originalEventId));
        }

        @Test
        void shouldAllowDistinctExplicitIdempotencyKeysForEventsWithSameTraceId() {
                FailureEventIngestionRequest firstRequest = createValidRequest();
                firstRequest.setIdempotencyKey("event-001");
                FailureEventIngestionRequest secondRequest = createValidRequest();
                secondRequest.setIdempotencyKey("event-002");

                UUID firstEventId = toRepositoryId(ingestionService.ingestFailureEvent(firstRequest));
                UUID secondEventId = toRepositoryId(ingestionService.ingestFailureEvent(secondRequest));

                assertNotEquals(firstEventId, secondEventId);
                assertEquals(2, failureEventRepository.count());
                assertEquals(2, failureEventRepository.findAll().stream()
                                .map(FailureEventEntity::getTraceId)
                                .filter("trace-integration-001"::equals)
                                .count());
        }

        @Test
        void shouldReturnOneEventIdForConcurrentEquivalentRequests() throws Exception {
                FailureEventIngestionRequest firstRequest = createValidRequest();
                firstRequest.setIdempotencyKey("event-concurrent-001");

                ExecutorService executor = Executors.newFixedThreadPool(2);
                CountDownLatch ready = new CountDownLatch(2);
                Callable<String> ingest = () -> {
                        ready.countDown();
                        ready.await();
                        return ingestionService.ingestFailureEvent(firstRequest);
                };
                try {
                        List<Future<String>> results = executor.invokeAll(List.of(ingest, ingest));
                        String firstEventId = results.get(0).get();
                        String secondEventId = results.get(1).get();

                        assertEquals(firstEventId, secondEventId);
                        assertEquals(1, failureEventRepository.count());
                } finally {
                        executor.shutdownNow();
                }
        }

        @Test
        void shouldRejectSameExplicitIdempotencyKeyWhenContentDiffers() {
                FailureEventIngestionRequest firstRequest = createValidRequest();
                firstRequest.setIdempotencyKey("event-conflict-001");
                ingestionService.ingestFailureEvent(firstRequest);

                FailureEventIngestionRequest conflictingRequest = createValidRequest();
                conflictingRequest.setIdempotencyKey("event-conflict-001");
                conflictingRequest.setErrorMessage("Different failure content");

                assertThrows(
                                DuplicateFailureEventException.class,
                                () -> ingestionService.ingestFailureEvent(conflictingRequest));
                assertEquals(1, failureEventRepository.count());
        }

        @Test
        void shouldEnforceIdempotencyKeyUniquenessInPostgreSql() {
                FailureEventIngestionRequest firstRequest = createValidRequest();
                firstRequest.setTraceId("trace-database-unique-001");
                firstRequest.setIdempotencyKey("database-unique-001");
                ingestionService.ingestFailureEvent(firstRequest);

                assertThrows(
                                DataIntegrityViolationException.class,
                                () -> jdbcTemplate.update(
                                                """
                                                                INSERT INTO failure_event (
                                                                    event_id,
                                                                    occurred_at,
                                                                    ingested_at,
                                                                    service_name,
                                                                    environment,
                                                                    event_type,
                                                                    trace_id,
                                                                    idempotency_key,
                                                                    processing_status
                                                                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                                                                """,
                                                UUID.randomUUID(),
                                                Timestamp.from(Instant.parse("2026-08-03T20:00:00Z")),
                                                Timestamp.from(Instant.parse("2026-08-03T20:00:01Z")),
                                                "Payment-Service",
                                                "PROD",
                                                "ERROR",
                                                "trace-other-001",
                                                "key:database-unique-001",
                                                ProcessingStatus.RECEIVED.name()));

                assertEquals(1, failureEventRepository.count());
                assertEquals(0, normalizedFailureEventRepository.count());
        }

        public FailureEventIngestionRequest createValidRequest() {
                FailureEventIngestionRequest request = new FailureEventIngestionRequest();
                request.setServerName("datadog");
                request.setServiceName("Payment-Service");
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
                                "Payment-Service",
                                savedEvent.getServiceName());

                assertEquals(
                                "datadog",
                                savedEvent.getSourceSystem());

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

        private void verifyFailureEventRow(UUID eventId) {
                Map<String, Object> row = jdbcTemplate.queryForMap(
                                """
                                                SELECT service_name,
                                                       server_name,
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

                assertEquals("Payment-Service", row.get("service_name"));
                assertEquals("datadog", row.get("server_name"));
                assertEquals("PROD", row.get("environment"));
                assertEquals("ERROR", row.get("event_type"));
                assertEquals("PSQLException", row.get("error_type"));
                assertEquals("Connection timeout after 5000ms", row.get("message"));
                assertEquals("payment-database", row.get("dependency_target"));
                assertEquals("trace-integration-001", row.get("trace_id"));
                assertEquals("ERROR", row.get("severity_hint"));
                assertEquals(ProcessingStatus.RECEIVED.name(), row.get("processing_status"));
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

}
