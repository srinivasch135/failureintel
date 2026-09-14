package com.failureintel.ingestion.application.service;

import com.failureintel.ingestion.application.exception.DuplicateFailureEventException;
import com.failureintel.infrastructure.persistence.failureevent.entity.FailureEventEntity;
import com.failureintel.infrastructure.persistence.failureevent.entity.ProcessingStatus;
import com.failureintel.infrastructure.persistence.failureevent.mapper.FailureEventEntityMapper;
import com.failureintel.infrastructure.persistence.failureevent.repository.FailureEventRepository;
import com.failureintel.ingestion.domain.model.RawFailureEvent;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.dao.DataIntegrityViolationException;

import java.sql.SQLException;
import java.util.Optional;

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
    void shouldResolveConcurrentEquivalentIdempotencyKeyViolation() {
        FailureEventIngestionService service = new FailureEventIngestionService(failureEventRepository);
        var request = validRequest("trace-race-001");
        request.setIdempotencyKey("idempotency-race-001");
        var existingRaw = new RawFailureEvent(
                java.util.UUID.randomUUID(),
                request.getServerName(),
                request.getServiceName(),
                request.getEnvironment(),
                request.getEventType(),
                request.getErrorType(),
                request.getErrorMessage(),
                request.getDependencyTarget(),
                request.getTraceId(),
                request.getSeverityHint(),
                request.getOccurredAt(),
                java.time.Instant.now(),
                request.getRawPayload(),
                java.util.Map.of());
        FailureEventEntity existing = FailureEventEntityMapper.fromRaw(existingRaw);
        existing.setIdempotencyKey("key:idempotency-race-001");
        existing.setIngestionFingerprint(FailureEventFingerprint.calculate(existingRaw));
        when(failureEventRepository.findByIdempotencyKey("key:idempotency-race-001"))
                .thenReturn(Optional.empty())
                .thenReturn(Optional.of(existing));
        SQLException uniqueViolation = new SQLException(
                "duplicate key violates unique constraint uq_failure_event_idempotency_key",
                "23505");
        when(failureEventRepository.saveAndFlush(any(FailureEventEntity.class)))
                .thenThrow(new DataIntegrityViolationException("duplicate trace ID", uniqueViolation));

        assertEquals(existing.getEventId().toString(), service.ingestFailureEvent(request));
    }

    @Test
    void shouldRejectExplicitIdempotencyKeyWhenContentDiffers() {
        FailureEventIngestionService service = new FailureEventIngestionService(failureEventRepository);
        var request = validRequest("trace-conflict-001");
        request.setIdempotencyKey("idempotency-conflict-001");
        FailureEventEntity existing = FailureEventEntityMapper.fromRaw(new RawFailureEvent(
                java.util.UUID.randomUUID(),
                request.getServerName(),
                request.getServiceName(),
                request.getEnvironment(),
                request.getEventType(),
                request.getErrorType(),
                "different message",
                request.getDependencyTarget(),
                request.getTraceId(),
                request.getSeverityHint(),
                request.getOccurredAt(),
                java.time.Instant.now(),
                request.getRawPayload(),
                java.util.Map.of()));
        existing.setIdempotencyKey("key:idempotency-conflict-001");
        existing.setIngestionFingerprint(FailureEventFingerprint.calculate(
                FailureEventEntityMapper.toRaw(existing)));
        when(failureEventRepository.findByIdempotencyKey("key:idempotency-conflict-001"))
                .thenReturn(Optional.of(existing));

        DuplicateFailureEventException exception = assertThrows(
                DuplicateFailureEventException.class,
                () -> service.ingestFailureEvent(request));

        assertEquals(
                "Failure event already exists for idempotency key: idempotency-conflict-001",
                exception.getMessage());
    }
}
