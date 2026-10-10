package com.failureintel.ingestion.api;

import com.failureintel.infrastructure.persistence.failureevent.repository.FailureEventRepository;
import com.failureintel.infrastructure.persistence.failureevent.entity.FailureEventEntity;
import com.failureintel.infrastructure.persistence.failureevent.entity.ProcessingStatus;
import com.failureintel.infrastructure.persistence.normalizedFailureEvent.entity.NormalizedFailureEventEntity;
import com.failureintel.infrastructure.persistence.normalizedFailureEvent.repository.NormalizedFailureEventRepository;
import com.failureintel.ingestion.api.dto.FailureEventIngestionRequest;
import com.failureintel.ingestion.api.dto.FailureEventResponse;
import com.failureintel.ingestion.application.service.FailureEventIngestionService;
import com.failureintel.ingestion.application.service.FailureEventQueryService;
import com.failureintel.ingestion.domain.normalization.FailureEventNormalizer;
import com.failureintel.ingestion.domain.normalization.NormalizationStatus;
import com.failureintel.ingestion.domain.parser.FailureEventParser;
import jakarta.persistence.EntityManager;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import com.failureintel.test.support.PostgresTestContainer;
import org.springframework.dao.DataAccessResourceFailureException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.bean.override.mockito.MockitoSpyBean;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.boot.webmvc.test.autoconfigure.AutoConfigureMockMvc;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.context.TestPropertySource;

import java.time.Instant;
import java.time.LocalDateTime;
import java.io.ByteArrayInputStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;
import static org.springframework.http.MediaType.APPLICATION_JSON;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@AutoConfigureMockMvc
@TestPropertySource(properties = {
        "spring.jpa.hibernate.ddl-auto=validate",
        "failure-event.ingestion.max-request-size-bytes=4096",
        "failure-event.processing.worker.enabled=false"
})
class FailureEventControllerIntegrationTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(FailureEventControllerIntegrationTest.class);

    @DynamicPropertySource
    static void postgresProperties(DynamicPropertyRegistry registry) {
        PostgresTestContainer.registerDatabase(registry, "failureintel_controller_test");
    }

    @Autowired
    private MockMvc mockMvc;

    @LocalServerPort
    private int port;

    private final HttpClient httpClient = HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(5))
            .build();

    @Autowired
    private FailureEventIngestionService ingestionService;

    @Autowired
    private FailureEventQueryService queryService;

    @Autowired
    private EntityManager entityManager;

    @Autowired
    private PlatformTransactionManager transactionManager;

    @Autowired
    private FailureEventRepository failureEventRepository;

    @Autowired
    private NormalizedFailureEventRepository normalizedFailureEventRepository;

    @Autowired
    private JdbcTemplate jdbcTemplate;

    private final ObjectMapper objectMapper = new ObjectMapper();

    @MockitoSpyBean
    private FailureEventParser failureEventParser;

    @MockitoSpyBean
    private FailureEventNormalizer failureEventNormalizer;

    @MockitoSpyBean
    private NormalizedFailureEventRepository normalizedFailureEventRepositorySpy;

    @AfterEach
    void cleanUp() {
        normalizedFailureEventRepository.deleteAll();
        failureEventRepository.deleteAll();
    }

    @Test
    void shouldAcceptOnlyAfterCommittingRawEventWithoutParsingOrNormalizing() throws Exception {
        String traceId = "trace-http-durable-capture-" + UUID.randomUUID();
        String requestBody = validRequestJson().replace("trace-controller-001", traceId);

        HttpResponse<String> response = postOverHttpForBody("/api/v1/failure-events", requestBody);

        assertEquals(202, response.statusCode());
        JsonNode accepted = objectMapper.readTree(response.body());
        assertTrue(accepted.hasNonNull("eventId"));
        assertEquals("RECEIVED", accepted.path("status").asText());
        UUID eventId = UUID.fromString(accepted.path("eventId").asText());

        Map<String, Object> committedRaw = jdbcTemplate.queryForMap("""
                SELECT server_name, service_name, environment, event_type, error_type, message,
                       dependency_target, trace_id, severity_hint, occurred_at, raw_payload,
                       processing_status, attempt_count, processing_started_at, last_attempt_at,
                       next_attempt_at, failure_code, failure_reason
                FROM failure_event WHERE event_id = ?
                """, eventId);

        assertEquals("datadog", committedRaw.get("server_name"));
        assertEquals("Payment-Service", committedRaw.get("service_name"));
        assertEquals("production", committedRaw.get("environment"));
        assertEquals("ERROR", committedRaw.get("event_type"));
        assertEquals("PSQLException", committedRaw.get("error_type"));
        assertEquals("Connection timeout after 5000ms", committedRaw.get("message"));
        assertEquals("payment-database", committedRaw.get("dependency_target"));
        assertEquals(traceId, committedRaw.get("trace_id"));
        assertEquals("HIGH", committedRaw.get("severity_hint"));
        assertEquals(LocalDateTime.of(2026, 8, 3, 20, 0),
                ((java.sql.Timestamp) committedRaw.get("occurred_at")).toLocalDateTime());
        assertEquals("{\"host\":\"payment-prod-01\",\"region\":\"us-east-1\"}",
                committedRaw.get("raw_payload"));
        assertEquals("RECEIVED", committedRaw.get("processing_status"));
        assertEquals(0, ((Number) committedRaw.get("attempt_count")).intValue());
        assertNull(committedRaw.get("processing_started_at"));
        assertNull(committedRaw.get("last_attempt_at"));
        assertNull(committedRaw.get("next_attempt_at"));
        assertNull(committedRaw.get("failure_code"));
        assertNull(committedRaw.get("failure_reason"));
        assertFalse(normalizedFailureEventRepository.existsById(eventId));
        org.mockito.Mockito.verifyNoInteractions(failureEventParser, failureEventNormalizer);
    }

    @Test
    void shouldReturnServerErrorAndRollBackWhenPostgresRejectsCaptureAtCommit() throws Exception {
        String traceId = "trace-http-commit-rejection-" + UUID.randomUUID();
        String functionName = "test_reject_failure_event_commit_" + UUID.randomUUID().toString().replace("-", "");
        String triggerName = "test_reject_failure_event_commit_" + UUID.randomUUID().toString().replace("-", "");
        boolean fixtureAttempted = false;

        try {
            fixtureAttempted = true;
            installDeferredFailureEventTrigger(functionName, triggerName, traceId);

            HttpResponse<String> response = postOverHttpForBody(
                    "/api/v1/failure-events",
                    validRequestJson().replace("trace-controller-001", traceId));

            assertEquals(500, response.statusCode());
            JsonNode error = objectMapper.readTree(response.body());
            assertFalse(error.has("eventId"));
            assertTrue(error.path("error").asText().endsWith("ERROR"));
            assertEquals(0, jdbcTemplate.queryForObject(
                    "SELECT COUNT(*) FROM failure_event WHERE trace_id = ?", Integer.class, traceId));
            assertEquals(0, jdbcTemplate.queryForObject("""
                    SELECT COUNT(*) FROM normalized_failure_event
                    WHERE normalized_trace_id = ?
                    """, Integer.class, traceId));
            org.mockito.Mockito.verifyNoInteractions(failureEventParser, failureEventNormalizer);
        } finally {
            if (fixtureAttempted) {
                removeDeferredFailureEventTrigger(functionName, triggerName);
            }
        }
    }

    @Test
    void shouldDurablyCaptureMissingOrBlankErrorMessage() throws Exception {
        String[] errorMessageValues = {"null", "\"\"", "\"   \""};

        for (int index = 0; index < errorMessageValues.length; index++) {
            String traceId = "trace-missing-message-" + index;
            String request = """
                    {
                      "serverName": "datadog",
                      "serviceName": "payment-service",
                      "environment": "production",
                      "eventType": "ERROR",
                      "errorMessage": %s,
                      "occurredAt": "2026-08-03T20:00:00Z",
                      "traceId": "%s",
                      "rawPayload": {"details": "original payload retained"}
                    }
                    """.formatted(errorMessageValues[index], traceId);

            mockMvc.perform(post("/api/v1/failure-events")
                            .contentType(APPLICATION_JSON)
                            .content(request))
                    .andExpect(status().isAccepted());

            FailureEventEntity persisted = failureEventRepository
                    .findFirstByTraceIdOrderByIngestedAtDescEventIdDesc(traceId)
                    .orElseThrow();
            assertEquals(ProcessingStatus.RECEIVED, persisted.getProcessingStatus());
            assertEquals("{\"details\":\"original payload retained\"}", persisted.getRawPayload());
            if (index == 0) {
                assertNull(persisted.getMessage());
            } else if (index == 1) {
                assertEquals("", persisted.getMessage());
            } else {
                assertEquals("   ", persisted.getMessage());
            }
        }
    }

    @Test
    void shouldRejectIdempotencyKeyHeaderLongerThanStorageLimit() throws Exception {
        String oversizedKey = "k".repeat(FailureEventIngestionRequest.MAX_IDEMPOTENCY_KEY_LENGTH + 1);

        mockMvc.perform(post("/api/v1/failure-events")
                        .contentType(APPLICATION_JSON)
                        .header("Idempotency-Key", oversizedKey)
                        .content(validRequestJson()))
                .andExpect(status().isBadRequest());

        assertEquals(0, failureEventRepository.count());
    }

    @Test
    void shouldPersistMaximumLengthIdempotencyKeyHeaderWithinColumnLimit() throws Exception {
        String idempotencyKey = "k".repeat(FailureEventIngestionRequest.MAX_IDEMPOTENCY_KEY_LENGTH);

        mockMvc.perform(post("/api/v1/failure-events")
                        .contentType(APPLICATION_JSON)
                        .header("Idempotency-Key", idempotencyKey)
                        .content(validRequestJson()))
                .andExpect(status().isAccepted());

        FailureEventEntity persisted = failureEventRepository.findAll().get(0);
        assertEquals("key:" + idempotencyKey, persisted.getIdempotencyKey());
        assertEquals(255, persisted.getIdempotencyKey().length());
    }

    @Test
    void shouldReturnFailureEventByIdFromDatabase() throws Exception {
        String eventId = ingestionService.ingestFailureEvent(validRequest());

        mockMvc.perform(get("/api/v1/failure-events/{eventId}", eventId))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.eventId").value(eventId))
                .andExpect(jsonPath("$.traceId").value("trace-controller-001"))
                .andExpect(jsonPath("$.processingStatus").value("RECEIVED"))
                .andExpect(jsonPath("$.serviceName").value("Payment-Service"))
                .andExpect(jsonPath("$.environment").value("production"))
                .andExpect(jsonPath("$.eventType").value("ERROR"))
                .andExpect(jsonPath("$.errorType").value("PSQLException"))
                .andExpect(jsonPath("$.message").value("Connection timeout after 5000ms"))
                .andExpect(jsonPath("$.dependencyTarget").value("payment-database"))
                .andExpect(jsonPath("$.severity").value("HIGH"))
                .andExpect(jsonPath("$.occurredAt").value("2026-08-03T20:00:00Z"));
    }

    @Test
    void shouldExposePersistedMetadataForEveryProcessingLifecycleState() throws Exception {
        FailureEventEntity received = persistEventWithStatus("RECEIVED", ProcessingStatus.RECEIVED);
        FailureEventEntity processing = persistEventWithStatus("PROCESSING", ProcessingStatus.PROCESSING);
        FailureEventEntity retryable = persistEventWithStatus("RETRYABLE", ProcessingStatus.RETRYABLE);
        FailureEventEntity failed = persistEventWithStatus("FAILED", ProcessingStatus.FAILED);
        FailureEventEntity normalized = persistEventWithStatus("NORMALIZED", ProcessingStatus.NORMALIZED);
        persistNormalizedEvent(normalized);

        mockMvc.perform(get("/api/v1/failure-events/{eventId}", received.getEventId()))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.processingStatus").value("RECEIVED"))
                .andExpect(jsonPath("$.attemptCount").value(0))
                .andExpect(jsonPath("$.lastAttemptAt").doesNotExist())
                .andExpect(jsonPath("$.nextAttemptAt").doesNotExist())
                .andExpect(jsonPath("$.failureCode").doesNotExist())
                .andExpect(jsonPath("$.failureReason").doesNotExist())
                .andExpect(jsonPath("$.normalizedAvailable").value(false));

        mockMvc.perform(get("/api/v1/failure-events/{eventId}", processing.getEventId()))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.processingStatus").value("PROCESSING"))
                .andExpect(jsonPath("$.attemptCount").value(1))
                .andExpect(jsonPath("$.lastAttemptAt").value("2026-08-03T20:00:01Z"))
                .andExpect(jsonPath("$.nextAttemptAt").doesNotExist())
                .andExpect(jsonPath("$.failureCode").doesNotExist())
                .andExpect(jsonPath("$.failureReason").doesNotExist())
                .andExpect(jsonPath("$.normalizedAvailable").value(false));

        mockMvc.perform(get("/api/v1/failure-events/{eventId}", retryable.getEventId()))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.processingStatus").value("RETRYABLE"))
                .andExpect(jsonPath("$.attemptCount").value(1))
                .andExpect(jsonPath("$.nextAttemptAt").exists())
                .andExpect(jsonPath("$.failureCode").value("DATABASE_TIMEOUT"))
                .andExpect(jsonPath("$.failureReason").value(
                        "Failure-event processing could not complete; another attempt is scheduled."))
                .andExpect(jsonPath("$.normalizedAvailable").value(false));

        mockMvc.perform(get("/api/v1/failure-events/{eventId}", failed.getEventId()))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.processingStatus").value("FAILED"))
                .andExpect(jsonPath("$.attemptCount").value(1))
                .andExpect(jsonPath("$.nextAttemptAt").doesNotExist())
                .andExpect(jsonPath("$.failureCode").value("MALFORMED_EVENT"))
                .andExpect(jsonPath("$.failureReason").value("The failure event could not be interpreted."))
                .andExpect(jsonPath("$.normalizedAvailable").value(false));

        mockMvc.perform(get("/api/v1/failure-events/{eventId}", normalized.getEventId()))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.processingStatus").value("NORMALIZED"))
                .andExpect(jsonPath("$.attemptCount").value(1))
                .andExpect(jsonPath("$.failureCode").doesNotExist())
                .andExpect(jsonPath("$.failureReason").doesNotExist())
                .andExpect(jsonPath("$.nextAttemptAt").doesNotExist())
                .andExpect(jsonPath("$.normalizedAvailable").value(true))
                .andExpect(jsonPath("$.normalizationStatus").value("FULLY_NORMALIZED"))
                .andExpect(jsonPath("$.normalizedAt").value("2026-08-03T20:00:02Z"));
    }

    @Test
    void shouldPreserveNormalizedStatusWhenNormalizedRowIsMissing() throws Exception {
        FailureEventEntity normalizedWithoutRow = persistEventWithStatus(
                "normalized-without-row",
                ProcessingStatus.NORMALIZED);

        mockMvc.perform(get("/api/v1/failure-events/{eventId}", normalizedWithoutRow.getEventId()))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.processingStatus").value("NORMALIZED"))
                .andExpect(jsonPath("$.normalizedAvailable").value(false))
                .andExpect(jsonPath("$.normalizationStatus").doesNotExist())
                .andExpect(jsonPath("$.normalizedAt").doesNotExist());
    }

    @Test
    void shouldReturnServerErrorWhenNormalizedLookupFails() throws Exception {
        FailureEventEntity event = persistEventWithStatus("lookup-failure", ProcessingStatus.RECEIVED);
        doThrow(new DataAccessResourceFailureException("simulated database failure"))
                .when(normalizedFailureEventRepositorySpy).findById(event.getEventId());

        mockMvc.perform(get("/api/v1/failure-events/{eventId}", event.getEventId()))
                .andExpect(status().isInternalServerError())
                .andExpect(jsonPath("$.error").value("DATABASE_ERROR"));
    }

    @Test
    void shouldKeepDirectReadOnOneSnapshotDuringConcurrentNormalization() throws Exception {
        FailureEventEntity processingEvent = persistEventWithStatus(
                "snapshot-race",
                ProcessingStatus.PROCESSING);
        UUID eventId = processingEvent.getEventId();
        CountDownLatch rawReadCompleted = new CountDownLatch(1);
        CountDownLatch continueRead = new CountDownLatch(1);
        doAnswer(invocation -> {
            rawReadCompleted.countDown();
            if (!continueRead.await(10, TimeUnit.SECONDS)) {
                throw new IllegalStateException("Timed out waiting to continue direct read");
            }
            return Optional.ofNullable(entityManager.find(NormalizedFailureEventEntity.class, eventId));
        }).when(normalizedFailureEventRepositorySpy).findById(eventId);

        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            Future<FailureEventResponse> responseFuture = executor.submit(() -> queryService.getFailureEvent(eventId));
            assertTrue(rawReadCompleted.await(10, TimeUnit.SECONDS), "direct read did not load raw state");

            TransactionTemplate writerTransaction = new TransactionTemplate(transactionManager);
            writerTransaction.executeWithoutResult(status -> {
                FailureEventEntity rawEvent = entityManager.find(FailureEventEntity.class, eventId);
                rawEvent.markNormalized();
                NormalizedFailureEventEntity normalizedEvent = normalizedEvent(rawEvent);
                entityManager.persist(normalizedEvent);
            });
            continueRead.countDown();

            FailureEventResponse concurrentResponse = responseFuture.get(10, TimeUnit.SECONDS);
            assertEquals("PROCESSING", concurrentResponse.processingStatus());
            assertFalse(concurrentResponse.normalizedAvailable());

            FailureEventResponse subsequentResponse = queryService.getFailureEvent(eventId);
            assertEquals("NORMALIZED", subsequentResponse.processingStatus());
            assertTrue(subsequentResponse.normalizedAvailable());
        } finally {
            continueRead.countDown();
            executor.shutdownNow();
        }
    }

    @Test
    void shouldExposeOperationalMetricsAndHealthThroughActuator() throws Exception {
        ingestionService.ingestFailureEvent(validRequest());

        mockMvc.perform(get("/actuator/metrics/failureintel.events.raw.accepted"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.name").value("failureintel.events.raw.accepted"))
                .andExpect(jsonPath("$.measurements[0].statistic").value("COUNT"))
                .andExpect(jsonPath("$.measurements[0].value").isNumber())
                .andExpect(jsonPath("$.availableTags.length()").value(0));

        mockMvc.perform(get("/actuator/metrics/failureintel.events.backlog"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.name").value("failureintel.events.backlog"))
                .andExpect(jsonPath("$.availableTags.length()").value(1))
                .andExpect(jsonPath("$.availableTags[0].tag").value("status"))
                .andExpect(jsonPath("$.availableTags[0].values.length()").value(3));

        mockMvc.perform(get("/actuator/metrics/failureintel.events.backlog")
                        .param("tag", "status:received"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.measurements[0].statistic").value("VALUE"))
                .andExpect(jsonPath("$.measurements[0].value").isNumber());

        mockMvc.perform(get("/actuator/health"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.status").value("UP"));
    }

    @Test
    void shouldReturnNotFoundWhenFailureEventDoesNotExist() throws Exception {
        mockMvc.perform(get("/api/v1/failure-events/{eventId}", "8e013f83-e5df-474e-9c18-378a63da0678"))
                .andExpect(status().isNotFound());
    }

    @Test
    void shouldReturnNotFoundForUnmappedRoute() throws Exception {
        mockMvc.perform(get("/"))
                .andExpect(status().isNotFound())
                .andExpect(jsonPath("$.status").value(404))
                .andExpect(jsonPath("$.error").value("RESOURCE_NOT_FOUND"))
                .andExpect(jsonPath("$.path").value("/"));
    }

    @Test
    void shouldReturnFailureEventByTraceIdFromDatabase() throws Exception {
        String eventId = ingestionService.ingestFailureEvent(validRequest());

        mockMvc.perform(get("/api/v1/failure-events/by-trace-id/{traceId}", "trace-controller-001"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.eventId").value(eventId))
                .andExpect(jsonPath("$.traceId").value("trace-controller-001"))
                .andExpect(jsonPath("$.serviceName").value("Payment-Service"))
                .andExpect(jsonPath("$.environment").value("production"))
                .andExpect(jsonPath("$.severity").value("HIGH"));
    }

    @Test
    void shouldReturnNotFoundWhenTraceIdDoesNotExist() throws Exception {
        mockMvc.perform(get("/api/v1/failure-events/by-trace-id/{traceId}", "missing-trace"))
                .andExpect(status().isNotFound());
    }

    @Test
    void shouldSearchFailureEventsByNormalizedFieldsFromDatabase() throws Exception {
        String eventId = ingestionService.ingestFailureEvent(validRequest());
        persistNormalizedSearchFixture(UUID.fromString(eventId));

        mockMvc.perform(get("/api/v1/failure-events/search")
                .param("serviceName", "payment-service")
                .param("environment", "prod")
                .param("severity", "high"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.content[0].eventId").value(eventId))
                .andExpect(jsonPath("$.content[0].traceId").value("trace-controller-001"))
                .andExpect(jsonPath("$.content[0].serviceName").value("payment-service"))
                .andExpect(jsonPath("$.content[0].environment").value("prod"))
                .andExpect(jsonPath("$.content[0].severity").value("high"));
    }

    @Test
    void shouldRejectInvalidIngestionRequestWithoutWritingRows() throws Exception {
        String invalidRequest = """
                {
                  "serverName": "datadog",
                  "serviceName": null,
                  "environment": " ",
                  "eventType": "",
                  "errorMessage": "",
                  "occurredAt": null,
                  "rawPayload": {"message": "not accepted"}
                }
                """;

        mockMvc.perform(post("/api/v1/failure-events")
                        .contentType(APPLICATION_JSON)
                        .content(invalidRequest))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.error").value("VALIDATION_ERROR"));

        assertEquals(0, failureEventRepository.count());
    }

    @Test
    void shouldRejectOversizedJsonRequestWithoutWritingRows() throws Exception {
        String oversizedRequest = """
                {
                  "serverName": "datadog",
                  "serviceName": "Payment-Service",
                  "environment": "production",
                  "eventType": "ERROR",
                  "errorMessage": "Connection timeout",
                  "occurredAt": "2026-08-03T20:00:00Z",
                  "rawPayload": {"details": "%s"}
                }
                """.formatted("x".repeat(5000));

        mockMvc.perform(post("/api/v1/failure-events")
                        .contentType(APPLICATION_JSON)
                        .content(oversizedRequest))
                .andExpect(status().isPayloadTooLarge());

        assertEquals(0, failureEventRepository.count());
    }

    @Test
    void shouldRejectIngestionPathParametersWithoutWritingRows() throws Exception {
        mockMvc.perform(post("/api/v1/failure-events;probe=1")
                        .contentType(APPLICATION_JSON)
                        .content(validRequestJson()))
                .andExpect(status().isBadRequest());

        assertEquals(0, failureEventRepository.count());
    }

    @Test
    void shouldAcceptJsonRequestAtConfiguredSizeLimit() throws Exception {
        String validRequest = validRequestJson();
        String requestAtLimit = validRequest + " ".repeat(4096 - validRequest.length());

        mockMvc.perform(post("/api/v1/failure-events")
                        .contentType(APPLICATION_JSON)
                        .content(requestAtLimit))
                .andExpect(status().isAccepted());

        assertEquals(1, failureEventRepository.count());
    }

    private FailureEventIngestionRequest validRequest() {
        FailureEventIngestionRequest request = new FailureEventIngestionRequest();
        request.setServerName("datadog");
        request.setServiceName("Payment-Service");
        request.setEnvironment("production");
        request.setEventType("ERROR");
        request.setErrorType("PSQLException");
        request.setErrorMessage("Connection timeout after 5000ms");
        request.setDependencyTarget("payment-database");
        request.setTraceId("trace-controller-001");
        request.setSeverityHint("HIGH");
        request.setOccurredAt(Instant.parse("2026-08-03T20:00:00Z"));
        request.setRawPayload(Map.of(
                "host", "payment-prod-01",
                "region", "us-east-1"));
        return request;
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void shouldEnforceSizeLimitForEveryEncodedPathCharacterOverRealHttp(boolean chunked)
            throws Exception {
        String path = "/api/v1/failure-events";
        byte[] oversizedBody = paddedJsonBody(4097);
        for (int index = 0; index < path.length(); index++) {
            if (path.charAt(index) == '/') {
                continue;
            }
            String encodedPath = path.substring(0, index)
                    + "%%%02x".formatted((int) path.charAt(index)) + path.substring(index + 1);
            assertEquals(413, postOverHttp(encodedPath, oversizedBody, chunked), encodedPath);
            assertEquals(0, failureEventRepository.count(), encodedPath);
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void shouldPreserveEncodedRouteByteBoundaryAndRejectCombinedPathParameters(boolean chunked)
            throws Exception {
        String encodedPath = "/%61pi/v%31/%66ailure%2devents";
        assertEquals(202, postOverHttp(encodedPath, paddedJsonBody(4096), chunked));
        assertEquals(1, failureEventRepository.count());

        assertEquals(413, postOverHttp(encodedPath + "?probe=1", paddedJsonBody(4097), chunked));
        for (String path : new String[] {
                encodedPath + ";probe=1", encodedPath + "%3bprobe=1",
                "/%61pi;tenant=one/v%31/%66ailure%2devents"}) {
            assertEquals(400, postOverHttp(path, paddedJsonBody(4097), chunked), path);
        }
        assertEquals(1, failureEventRepository.count());
    }

    private byte[] paddedJsonBody(int sizeBytes) {
        String json = validRequestJson().replace("Connection timeout after 5000ms", "Failure λ € 😀");
        return (json + " ".repeat(sizeBytes - json.getBytes(StandardCharsets.UTF_8).length))
                .getBytes(StandardCharsets.UTF_8);
    }

    private int postOverHttp(String path, byte[] body, boolean chunked) throws Exception {
        HttpRequest.BodyPublisher publisher = chunked
                ? HttpRequest.BodyPublishers.ofInputStream(() -> new ByteArrayInputStream(body))
                : HttpRequest.BodyPublishers.ofByteArray(body);
        HttpRequest request = HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + port + path))
                .timeout(Duration.ofSeconds(15))
                .header("Content-Type", "application/json;charset=UTF-8")
                .POST(publisher)
                .build();
        return httpClient.send(request, HttpResponse.BodyHandlers.discarding()).statusCode();
    }

    private HttpResponse<String> postOverHttpForBody(String path, String body) throws Exception {
        HttpRequest request = HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + port + path))
                .timeout(Duration.ofSeconds(15))
                .header("Content-Type", "application/json;charset=UTF-8")
                .POST(HttpRequest.BodyPublishers.ofString(body, StandardCharsets.UTF_8))
                .build();
        return httpClient.send(request, HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
    }

    private void installDeferredFailureEventTrigger(String functionName, String triggerName, String traceId) {
        jdbcTemplate.execute("""
                CREATE FUNCTION %s() RETURNS trigger
                LANGUAGE plpgsql
                AS $$
                BEGIN
                    IF NEW.trace_id = '%s' THEN
                        RAISE EXCEPTION 'designated integration-test commit rejection'
                            USING ERRCODE = '23514';
                    END IF;
                    RETURN NEW;
                END;
                $$
                """.formatted(functionName, traceId));
        jdbcTemplate.execute("""
                CREATE CONSTRAINT TRIGGER %s
                AFTER INSERT ON failure_event
                DEFERRABLE INITIALLY DEFERRED
                FOR EACH ROW EXECUTE FUNCTION %s()
                """.formatted(triggerName, functionName));
    }

    private void removeDeferredFailureEventTrigger(String functionName, String triggerName) {
        jdbcTemplate.execute("DROP TRIGGER IF EXISTS " + triggerName + " ON failure_event");
        jdbcTemplate.execute("DROP FUNCTION IF EXISTS " + functionName + "()");
    }

    private String validRequestJson() {
        return """
                {
                  "serverName": "datadog",
                  "serviceName": "Payment-Service",
                  "environment": "production",
                  "eventType": "ERROR",
                  "errorType": "PSQLException",
                  "errorMessage": "Connection timeout after 5000ms",
                  "dependencyTarget": "payment-database",
                  "traceId": "trace-controller-001",
                  "severityHint": "HIGH",
                  "occurredAt": "2026-08-03T20:00:00Z",
                  "rawPayload": {
                    "host": "payment-prod-01",
                    "region": "us-east-1"
                  }
                }
                """;
    }

    private FailureEventEntity persistEventWithStatus(String suffix, ProcessingStatus processingStatus) {
        FailureEventIngestionRequest request = validRequest();
        request.setTraceId("trace-query-" + suffix);
        UUID eventId = UUID.fromString(ingestionService.ingestFailureEvent(request));
        FailureEventEntity event = failureEventRepository.findById(eventId).orElseThrow();
        if (processingStatus != ProcessingStatus.RECEIVED) {
            Instant claimedAt = Instant.parse("2026-08-03T20:00:01Z");
            event.claimForProcessing(claimedAt);
            switch (processingStatus) {
                case PROCESSING -> { }
                case RETRYABLE -> event.markRetryable(
                        "DATABASE_TIMEOUT",
                        "internal database exception must not be exposed",
                        Instant.parse("2026-08-03T20:05:01Z"));
                case FAILED -> event.markFailed(
                        "MALFORMED_EVENT",
                        "internal parser exception must not be exposed");
                case NORMALIZED -> event.markNormalized();
                case RECEIVED -> throw new IllegalStateException("Unexpected RECEIVED transition");
            }
            event = failureEventRepository.saveAndFlush(event);
        }
        return event;
    }

    private void persistNormalizedEvent(FailureEventEntity rawEvent) {
        normalizedFailureEventRepository.saveAndFlush(normalizedEvent(rawEvent));
    }

    private NormalizedFailureEventEntity normalizedEvent(FailureEventEntity rawEvent) {
        NormalizedFailureEventEntity normalizedEvent = new NormalizedFailureEventEntity();
        normalizedEvent.setFailureEvent(rawEvent);
        normalizedEvent.setNormalizedPayload(Map.of("test", "normalized"));
        normalizedEvent.setNormalizedServiceName("normalized-service");
        normalizedEvent.setNormalizedEnvironment("normalized-environment");
        normalizedEvent.setNormalizedEventType("normalized-event");
        normalizedEvent.setNormalizedErrorType("NormalizedError");
        normalizedEvent.setNormalizedErrorMessage("normalized message");
        normalizedEvent.setNormalizedDependencyTarget("normalized-dependency");
        normalizedEvent.setNormalizedTraceId(rawEvent.getTraceId());
        normalizedEvent.setNormalizedSeverity("high");
        normalizedEvent.setNormalizedOccurredAt(Instant.parse("2026-08-03T20:00:00Z"));
        normalizedEvent.setNormalizationStatus(NormalizationStatus.FULLY_NORMALIZED);
        normalizedEvent.setNormalizationMetadata(Map.of());
        normalizedEvent.setNormalizedAt(Instant.parse("2026-08-03T20:00:02Z"));
        return normalizedEvent;
    }

    private void persistNormalizedSearchFixture(UUID eventId) {
        FailureEventEntity rawEvent = failureEventRepository.findById(eventId).orElseThrow();
        Instant processingStartedAt = Instant.parse("2026-08-03T20:00:01Z");
        rawEvent.claimForProcessing(processingStartedAt);
        rawEvent.markNormalized();
        failureEventRepository.saveAndFlush(rawEvent);

        NormalizedFailureEventEntity normalizedEvent = new NormalizedFailureEventEntity();
        normalizedEvent.setFailureEvent(rawEvent);
        normalizedEvent.setNormalizedPayload(Map.of(
                "host", "payment-prod-01",
                "region", "us-east-1"));
        normalizedEvent.setNormalizedServiceName("payment-service");
        normalizedEvent.setNormalizedEnvironment("prod");
        normalizedEvent.setNormalizedEventType("exception");
        normalizedEvent.setNormalizedErrorType("PSQLException");
        normalizedEvent.setNormalizedErrorMessage("Connection timeout after 5000ms");
        normalizedEvent.setNormalizedDependencyTarget("payment-database");
        normalizedEvent.setNormalizedTraceId("trace-controller-001");
        normalizedEvent.setNormalizedSeverity("high");
        normalizedEvent.setNormalizedOccurredAt(Instant.parse("2026-08-03T20:00:00Z"));
        normalizedEvent.setNormalizationStatus(NormalizationStatus.FULLY_NORMALIZED);
        normalizedEvent.setNormalizationMetadata(Map.of());
        normalizedEvent.setNormalizedAt(Instant.parse("2026-08-03T20:00:02Z"));
        normalizedFailureEventRepository.saveAndFlush(normalizedEvent);
    }
}
