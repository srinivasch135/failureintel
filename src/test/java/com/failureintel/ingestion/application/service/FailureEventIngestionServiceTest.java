package com.failureintel.ingestion.application.service;

import com.failureintel.ingestion.application.exception.DuplicateFailureEventException;
import com.failureintel.infrastructure.persistence.failureevent.entity.FailureEventEntity;
import com.failureintel.infrastructure.persistence.failureevent.entity.ProcessingStatus;
import com.failureintel.infrastructure.persistence.failureevent.repository.FailureEventRepository;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.dao.DataIntegrityViolationException;

import java.sql.SQLException;

import static com.failureintel.test.support.FailureEventTestFixtures.validRequest;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class FailureEventIngestionServiceTest {

    @Mock
    private FailureEventRepository failureEventRepository;

    @Test
    void shouldPersistRawFailureEventAsReceived() {
        FailureEventIngestionService service = new FailureEventIngestionService(failureEventRepository);
        when(failureEventRepository.saveAndFlush(any(FailureEventEntity.class)))
                .thenAnswer(invocation -> invocation.getArgument(0));

        String eventId = service.ingestFailureEvent(validRequest("trace-capture-001"));

        ArgumentCaptor<FailureEventEntity> captor = ArgumentCaptor.forClass(FailureEventEntity.class);
        verify(failureEventRepository).saveAndFlush(captor.capture());

        FailureEventEntity rawEvent = captor.getValue();
        assertNotNull(eventId);
        assertEquals(rawEvent.getEventId().toString(), eventId);
        assertEquals(ProcessingStatus.RECEIVED, rawEvent.getProcessingStatus());
        assertEquals("trace-capture-001", rawEvent.getTraceId());
        assertNull(rawEvent.getFailureReason());
    }

    @Test
    void shouldTrimTraceIdBeforePersistence() {
        FailureEventIngestionService service = new FailureEventIngestionService(failureEventRepository);
        when(failureEventRepository.saveAndFlush(any(FailureEventEntity.class)))
                .thenAnswer(invocation -> invocation.getArgument(0));

        service.ingestFailureEvent(validRequest("  trace-trimmed-001  "));

        ArgumentCaptor<FailureEventEntity> captor = ArgumentCaptor.forClass(FailureEventEntity.class);
        verify(failureEventRepository).saveAndFlush(captor.capture());
        assertEquals("trace-trimmed-001", captor.getValue().getTraceId());
    }

    @Test
    void shouldStoreBlankTraceIdAsNullAndSkipDuplicateCheck() {
        FailureEventIngestionService service = new FailureEventIngestionService(failureEventRepository);
        when(failureEventRepository.saveAndFlush(any(FailureEventEntity.class)))
                .thenAnswer(invocation -> invocation.getArgument(0));

        service.ingestFailureEvent(validRequest("   "));

        ArgumentCaptor<FailureEventEntity> captor = ArgumentCaptor.forClass(FailureEventEntity.class);
        verify(failureEventRepository, never()).existsByTraceId(any());
        verify(failureEventRepository).saveAndFlush(captor.capture());
        assertNull(captor.getValue().getTraceId());
    }

    @Test
    void shouldTranslateConcurrentTraceIdConstraintViolation() {
        FailureEventIngestionService service = new FailureEventIngestionService(failureEventRepository);
        SQLException uniqueViolation = new SQLException(
                "duplicate key violates unique constraint uq_failure_event_trace_id",
                "23505");
        when(failureEventRepository.saveAndFlush(any(FailureEventEntity.class)))
                .thenThrow(new DataIntegrityViolationException("duplicate trace ID", uniqueViolation));

        DuplicateFailureEventException exception = assertThrows(
                DuplicateFailureEventException.class,
                () -> service.ingestFailureEvent(validRequest("trace-race-001")));

        assertEquals(
                "Failure event already exists for traceId: trace-race-001",
                exception.getMessage());
    }
}
