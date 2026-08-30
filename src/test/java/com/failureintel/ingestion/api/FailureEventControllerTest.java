package com.failureintel.ingestion.api;

import com.failureintel.infrastructure.web.exception.GlobalExceptionHandler;
import com.failureintel.ingestion.api.dto.FailureEventIngestionRequest;
import com.failureintel.ingestion.api.dto.FailureEventResponse;
import com.failureintel.ingestion.application.exception.DuplicateFailureEventException;
import com.failureintel.ingestion.application.exception.FailureEventNotFoundException;
import com.failureintel.ingestion.application.service.FailureEventQueryService;
import com.failureintel.ingestion.application.useCase.IngestFailureEventUseCase;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.data.domain.PageImpl;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.data.web.PageableHandlerMethodArgumentResolver;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;
import static org.springframework.http.MediaType.APPLICATION_JSON;

class FailureEventControllerTest {

    private FailureEventQueryService failureEventQueryService;
    private IngestFailureEventUseCase ingestFailureEventUseCase;
    private MockMvc mockMvc;

    @BeforeEach
    void setUp() {
        ingestFailureEventUseCase = mock(IngestFailureEventUseCase.class);
        failureEventQueryService = mock(FailureEventQueryService.class);

        FailureEventController controller = new FailureEventController(
                ingestFailureEventUseCase,
                failureEventQueryService);

        mockMvc = MockMvcBuilders.standaloneSetup(controller)
                .setControllerAdvice(new GlobalExceptionHandler())
                .setCustomArgumentResolvers(new PageableHandlerMethodArgumentResolver())
                .build();
    }

    @Test
    void shouldReturnConflictForDuplicateTraceId() throws Exception {
        when(ingestFailureEventUseCase.ingestFailureEvent(isA(FailureEventIngestionRequest.class)))
                .thenThrow(new DuplicateFailureEventException("trace-duplicate-001"));

        mockMvc.perform(post("/api/v1/failure-events")
                .contentType(APPLICATION_JSON)
                .content("""
                        {
                          "occurredAt": "2026-08-24T10:15:30Z",
                          "serviceName": "payment-service",
                          "serverName": "datadog",
                          "environment": "prod",
                          "eventType": "error",
                          "errorMessage": "Connection timeout",
                          "traceId": "trace-duplicate-001",
                          "rawPayload": {"message": "Connection timeout"}
                        }
                        """))
                .andExpect(status().isConflict())
                .andExpect(jsonPath("$.error").value("DUPLICATE_FAILURE_EVENT"))
                .andExpect(jsonPath("$.message")
                        .value("Failure event already exists for traceId: trace-duplicate-001"));
    }

    @Test
    void shouldReturnFailureEventById() throws Exception {
        UUID eventId = UUID.randomUUID();
        FailureEventResponse response = new FailureEventResponse(
                eventId,
                "trace-123",
                Instant.parse("2026-08-24T10:15:30Z"),
                "NORMALIZED",
                "checkout-service",
                "prod",
                "DEPENDENCY_FAILURE",
                "TIMEOUT",
                "Payment provider timed out",
                "payment-provider",
                "HIGH",
                Instant.parse("2026-08-24T10:14:30Z"));

        when(failureEventQueryService.getFailureEvent(eventId)).thenReturn(response);

        mockMvc.perform(get("/api/v1/failure-events/{eventId}", eventId))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.eventId").value(eventId.toString()))
                .andExpect(jsonPath("$.serviceName").value("checkout-service"))
                .andExpect(jsonPath("$.errorType").value("TIMEOUT"));

        verify(failureEventQueryService).getFailureEvent(eventId);
    }

    @Test
    void shouldReturnNotFoundWhenFailureEventDoesNotExist() throws Exception {
        UUID eventId = UUID.randomUUID();
        when(failureEventQueryService.getFailureEvent(eventId))
                .thenThrow(new FailureEventNotFoundException(eventId));

        mockMvc.perform(get("/api/v1/failure-events/{eventId}", eventId))
                .andExpect(status().isNotFound());

        verify(failureEventQueryService).getFailureEvent(eventId);
    }

    @Test
    void shouldReturnFailureEventByTraceId() throws Exception {
        UUID eventId = UUID.randomUUID();
        String traceId = "trace-123";
        FailureEventResponse response = new FailureEventResponse(
                eventId,
                traceId,
                Instant.parse("2026-08-24T10:15:30Z"),
                "NORMALIZED",
                "checkout-service",
                "prod",
                "DEPENDENCY_FAILURE",
                "TIMEOUT",
                "Payment provider timed out",
                "payment-provider",
                "HIGH",
                Instant.parse("2026-08-24T10:14:30Z"));

        when(failureEventQueryService.getFailureEventByTraceId(traceId)).thenReturn(response);

        mockMvc.perform(get("/api/v1/failure-events/by-trace-id/{traceId}", traceId))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.eventId").value(eventId.toString()))
                .andExpect(jsonPath("$.traceId").value(traceId))
                .andExpect(jsonPath("$.serviceName").value("checkout-service"));

        verify(failureEventQueryService).getFailureEventByTraceId(traceId);
    }

    @Test
    void shouldReturnNotFoundWhenTraceIdDoesNotExist() throws Exception {
        String traceId = "missing-trace";
        when(failureEventQueryService.getFailureEventByTraceId(traceId))
                .thenThrow(new FailureEventNotFoundException(traceId));

        mockMvc.perform(get("/api/v1/failure-events/by-trace-id/{traceId}", traceId))
                .andExpect(status().isNotFound());

        verify(failureEventQueryService).getFailureEventByTraceId(traceId);
    }

    @Test
    void shouldSearchFailureEventsByNormalizedFields() throws Exception {
        UUID eventId = UUID.randomUUID();
        FailureEventResponse response = new FailureEventResponse(
                eventId,
                "trace-123",
                Instant.parse("2026-08-24T10:15:30Z"),
                "NORMALIZED",
                "checkout-service",
                "prod",
                "DEPENDENCY_FAILURE",
                "TIMEOUT",
                "Payment provider timed out",
                "payment-provider",
                "HIGH",
                Instant.parse("2026-08-24T10:14:30Z"));

        when(failureEventQueryService.searchFailureEvents(
                eq("checkout-service"),
                eq("prod"),
                eq("HIGH"),
                isA(Pageable.class)))
                .thenReturn(new PageImpl<>(List.of(response), PageRequest.of(0, 20), 1));

        mockMvc.perform(get("/api/v1/failure-events/search")
                .param("serviceName", "checkout-service")
                .param("environment", "prod")
                .param("severity", "HIGH"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.content[0].eventId").value(eventId.toString()))
                .andExpect(jsonPath("$.content[0].serviceName").value("checkout-service"))
                .andExpect(jsonPath("$.content[0].environment").value("prod"))
                .andExpect(jsonPath("$.totalElements").value(1));

        verify(failureEventQueryService).searchFailureEvents(
                eq("checkout-service"),
                eq("prod"),
                eq("HIGH"),
                isA(Pageable.class));
    }
}
