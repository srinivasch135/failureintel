package com.failureintel.ingestion.application.service;

import com.failureintel.ingestion.application.exception.DuplicateFailureEventException;
import com.failureintel.ingestion.domain.model.RawFailureEvent;
import com.failureintel.ingestion.domain.normalization.FailureEventNormalizer;
import com.failureintel.ingestion.domain.parser.FailureEventParser;
import com.failureintel.infrastructure.persistence.failureevent.entity.FailureEventEntity;
import com.failureintel.infrastructure.persistence.failureevent.entity.ProcessingStatus;
import com.failureintel.infrastructure.persistence.failureevent.repository.FailureEventRepository;
import com.failureintel.infrastructure.persistence.normalizedFailureEvent.repository.NormalizedFailureEventRepository;
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

    @Mock
    private NormalizedFailureEventRepository normalizedFailureEventRepository;

    @Mock
    private FailureEventParser parser;

    @Mock
    private FailureEventNormalizer normalizer;

    @Test
    void shouldPersistParserFailureAsFailedRawEventWithoutNormalizedRow() {
        FailureEventIngestionService service = new FailureEventIngestionService(
                failureEventRepository,
                normalizedFailureEventRepository,
                parser,
                normalizer);
        when(parser.supports(any(RawFailureEvent.class))).thenReturn(true);
        when(parser.parse(any(RawFailureEvent.class)))
                .thenThrow(new IllegalArgumentException("invalid source format"));
        when(failureEventRepository.saveAndFlush(any(FailureEventEntity.class)))
                .thenAnswer(invocation -> invocation.getArgument(0));

        String eventId = service.ingestFailureEvent(validRequest("trace-parser-failure-001"));

        ArgumentCaptor<FailureEventEntity> captor = ArgumentCaptor.forClass(FailureEventEntity.class);
        verify(failureEventRepository).saveAndFlush(captor.capture());
        verifyNoInteractions(normalizedFailureEventRepository);
        verifyNoInteractions(normalizer);

        FailureEventEntity failedEvent = captor.getValue();
        assertNotNull(eventId);
        assertEquals(failedEvent.getEventId().toString(), eventId);
        assertEquals(ProcessingStatus.FAILED, failedEvent.getProcessingStatus());
        assertEquals(
                "Parsing/normalization failed: invalid source format",
                failedEvent.getFailureReason());
    }

    @Test
    void shouldTrimTraceIdBeforeDuplicateCheckAndPersistence() {
        FailureEventIngestionService service = new FailureEventIngestionService(
                failureEventRepository,
                normalizedFailureEventRepository,
                parser,
                normalizer);
        when(parser.supports(any(RawFailureEvent.class))).thenReturn(false);
        when(failureEventRepository.saveAndFlush(any(FailureEventEntity.class)))
                .thenAnswer(invocation -> invocation.getArgument(0));

        service.ingestFailureEvent(validRequest("  trace-trimmed-001  "));

        ArgumentCaptor<FailureEventEntity> captor = ArgumentCaptor.forClass(FailureEventEntity.class);
        verify(failureEventRepository).existsByTraceId("trace-trimmed-001");
        verify(failureEventRepository).saveAndFlush(captor.capture());
        assertEquals("trace-trimmed-001", captor.getValue().getTraceId());
    }

    @Test
    void shouldStoreBlankTraceIdAsNullAndSkipDuplicateCheck() {
        FailureEventIngestionService service = new FailureEventIngestionService(
                failureEventRepository,
                normalizedFailureEventRepository,
                parser,
                normalizer);
        when(parser.supports(any(RawFailureEvent.class))).thenReturn(false);
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
        FailureEventIngestionService service = new FailureEventIngestionService(
                failureEventRepository,
                normalizedFailureEventRepository,
                parser,
                normalizer);
        when(parser.supports(any(RawFailureEvent.class))).thenReturn(false);
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
