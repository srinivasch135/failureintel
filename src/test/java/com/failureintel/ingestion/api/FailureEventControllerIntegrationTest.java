package com.failureintel.ingestion.api;

import com.failureintel.infrastructure.persistence.failureevent.repository.FailureEventRepository;
import com.failureintel.infrastructure.persistence.failureevent.entity.FailureEventEntity;
import com.failureintel.infrastructure.persistence.failureevent.entity.ProcessingStatus;
import com.failureintel.infrastructure.persistence.normalizedFailureEvent.entity.NormalizedFailureEventEntity;
import com.failureintel.infrastructure.persistence.normalizedFailureEvent.repository.NormalizedFailureEventRepository;
import com.failureintel.ingestion.api.dto.FailureEventIngestionRequest;
import com.failureintel.ingestion.application.service.FailureEventIngestionService;
import com.failureintel.ingestion.domain.normalization.NormalizationStatus;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.boot.webmvc.test.autoconfigure.AutoConfigureMockMvc;
import org.springframework.test.context.TestPropertySource;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.time.Instant;
import java.util.Map;
import java.util.UUID;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;
import static org.springframework.http.MediaType.APPLICATION_JSON;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@AutoConfigureMockMvc
@Testcontainers
@TestPropertySource(properties = {
        "spring.jpa.hibernate.ddl-auto=validate"
})
class FailureEventControllerIntegrationTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(FailureEventControllerIntegrationTest.class);

    @Container
    @ServiceConnection
    static final PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>(
            DockerImageName.parse("postgres:16-alpine"))
            .withDatabaseName("failureintel_controller_test")
            .withUsername("testuser")
            .withPassword("testpassword")
            .withLogConsumer(new Slf4jLogConsumer(LOGGER));

    @Autowired
    private MockMvc mockMvc;

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
    void shouldAcceptAndDurablyCaptureRawFailureEvent() throws Exception {
        mockMvc.perform(post("/api/v1/failure-events")
                        .contentType(APPLICATION_JSON)
                        .content(validRequestJson()))
                .andExpect(status().isAccepted())
                .andExpect(jsonPath("$.status").value("RECEIVED"))
                .andExpect(jsonPath("$.message").value("Failure event accepted for processing"));

        FailureEventEntity persisted = failureEventRepository
                .findByTraceId("trace-controller-001")
                .orElseThrow();

        assertEquals(ProcessingStatus.RECEIVED, persisted.getProcessingStatus());
        assertEquals(0, persisted.getAttemptCount());
        assertNull(persisted.getFailureReason());
        assertFalse(normalizedFailureEventRepository.existsById(persisted.getEventId()));
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
    void shouldReturnNotFoundWhenFailureEventDoesNotExist() throws Exception {
        mockMvc.perform(get("/api/v1/failure-events/{eventId}", "8e013f83-e5df-474e-9c18-378a63da0678"))
                .andExpect(status().isNotFound());
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
