package com.failureintel.ingestion.application.service;

import com.failureintel.ingestion.api.dto.FailureEventResponse;
import com.failureintel.ingestion.application.exception.FailureEventNotFoundException;
import com.failureintel.infrastructure.persistence.failureevent.entity.FailureEventEntity;
import com.failureintel.infrastructure.persistence.failureevent.entity.ProcessingStatus;
import com.failureintel.infrastructure.persistence.failureevent.repository.FailureEventRepository;
import com.failureintel.infrastructure.persistence.normalizedFailureEvent.entity.NormalizedFailureEventEntity;
import com.failureintel.infrastructure.persistence.normalizedFailureEvent.repository.NormalizedFailureEventRepository;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageImpl;
import org.springframework.data.domain.PageRequest;

import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class FailureEventQueryServiceTest {

    @Mock
    private FailureEventRepository failureEventRepository;

    @Mock
    private NormalizedFailureEventRepository normalizedFailureEventRepository;

    @InjectMocks
    private FailureEventQueryService queryService;

    @Test
    void shouldReturnNormalizedValuesWhenNormalizedEventExists() {
        UUID eventId = UUID.randomUUID();
        FailureEventEntity rawEvent = rawEvent(eventId);
        NormalizedFailureEventEntity normalizedEvent = normalizedEvent(eventId);
        when(failureEventRepository.findById(eventId)).thenReturn(Optional.of(rawEvent));
        when(normalizedFailureEventRepository.findById(eventId)).thenReturn(Optional.of(normalizedEvent));

        FailureEventResponse response = queryService.getFailureEvent(eventId);

        assertEquals(eventId, response.eventId());
        assertEquals("normalized-service", response.serviceName());
        assertEquals("normalized message", response.message());
        verify(failureEventRepository).findById(eventId);
        verify(normalizedFailureEventRepository).findById(eventId);
    }

    @Test
    void shouldReturnRawValuesWhenNormalizedEventDoesNotExist() {
        UUID eventId = UUID.randomUUID();
        when(failureEventRepository.findById(eventId)).thenReturn(Optional.of(rawEvent(eventId)));
        when(normalizedFailureEventRepository.findById(eventId)).thenReturn(Optional.empty());

        FailureEventResponse response = queryService.getFailureEvent(eventId);

        assertEquals(eventId, response.eventId());
        assertEquals("raw-service", response.serviceName());
        assertEquals("raw message", response.message());
        verify(failureEventRepository).findById(eventId);
        verify(normalizedFailureEventRepository).findById(eventId);
    }

    @Test
    void shouldThrowWhenRawEventDoesNotExist() {
        UUID eventId = UUID.randomUUID();
        when(failureEventRepository.findById(eventId)).thenReturn(Optional.empty());

        FailureEventNotFoundException exception = assertThrows(
                FailureEventNotFoundException.class,
                () -> queryService.getFailureEvent(eventId));

        assertEquals("FailureEvent not found for eventID: " + eventId, exception.getMessage());
        verify(failureEventRepository).findById(eventId);
        verifyNoInteractions(normalizedFailureEventRepository);
    }

    @Test
    void shouldReturnFailureEventByTraceId() {
        UUID eventId = UUID.randomUUID();
        String traceId = "raw-trace";
        FailureEventEntity rawEvent = rawEvent(eventId);
        NormalizedFailureEventEntity normalizedEvent = normalizedEvent(eventId);
        when(failureEventRepository.findByTraceId(traceId)).thenReturn(Optional.of(rawEvent));
        when(normalizedFailureEventRepository.findById(eventId)).thenReturn(Optional.of(normalizedEvent));

        FailureEventResponse response = queryService.getFailureEventByTraceId(traceId);

        assertEquals(eventId, response.eventId());
        assertEquals("raw-trace", response.traceId());
        assertEquals("normalized-service", response.serviceName());
        verify(failureEventRepository).findByTraceId(traceId);
        verify(normalizedFailureEventRepository).findById(eventId);
    }

    @Test
    void shouldThrowWhenTraceIdDoesNotExist() {
        String traceId = "missing-trace";
        when(failureEventRepository.findByTraceId(traceId)).thenReturn(Optional.empty());

        FailureEventNotFoundException exception = assertThrows(
                FailureEventNotFoundException.class,
                () -> queryService.getFailureEventByTraceId(traceId));

        assertEquals("FailureEvent not found for traceId: " + traceId, exception.getMessage());
        verify(failureEventRepository).findByTraceId(traceId);
        verifyNoInteractions(normalizedFailureEventRepository);
    }

    @Test
    void shouldSearchFailureEventsByNormalizedFields() {
        UUID eventId = UUID.randomUUID();
        FailureEventEntity rawEvent = rawEvent(eventId);
        NormalizedFailureEventEntity normalizedEvent = normalizedEvent(eventId);
        normalizedEvent.setFailureEvent(rawEvent);
        PageRequest pageable = PageRequest.of(0, 20);

        when(normalizedFailureEventRepository.searchByFailureDetails(
                "payment-service",
                "prod",
                "high",
                pageable))
                .thenReturn(new PageImpl<>(List.of(normalizedEvent), pageable, 1));

        Page<FailureEventResponse> response = queryService.searchFailureEvents(
                " Payment-Service ",
                "PROD",
                "HIGH",
                pageable);

        assertEquals(1, response.getTotalElements());
        assertEquals(eventId, response.getContent().get(0).eventId());
        assertEquals("normalized-service", response.getContent().get(0).serviceName());
        verify(normalizedFailureEventRepository).searchByFailureDetails(
                "payment-service",
                "prod",
                "high",
                pageable);
        verifyNoInteractions(failureEventRepository);
    }

    private static FailureEventEntity rawEvent(UUID eventId) {
        FailureEventEntity event = new FailureEventEntity();
        event.setEventId(eventId);
        event.setTraceId("raw-trace");
        event.setIngestedAt(Instant.parse("2026-08-03T20:00:05Z"));
        event.setProcessingStatus(ProcessingStatus.NORMALIZED);
        event.setServiceName("raw-service");
        event.setEnvironment("raw-environment");
        event.setEventType("raw-event-type");
        event.setErrorType("raw-error-type");
        event.setMessage("raw message");
        event.setDependencyTarget("raw-dependency");
        event.setSeverityHint("raw-severity");
        event.setOccurredAt(Instant.parse("2026-08-03T20:00:00Z"));
        return event;
    }

    private static NormalizedFailureEventEntity normalizedEvent(UUID eventId) {
        NormalizedFailureEventEntity event = new NormalizedFailureEventEntity();
        event.setEventId(eventId);
        event.setNormalizedServiceName("normalized-service");
        event.setNormalizedErrorMessage("normalized message");
        return event;
    }
}
