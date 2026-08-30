package com.failureintel.ingestion.application.service;

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

import static com.failureintel.test.support.FailureEventTestFixtures.validRequest;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
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
        when(failureEventRepository.save(any(FailureEventEntity.class)))
                .thenAnswer(invocation -> invocation.getArgument(0));

        String eventId = service.ingestFailureEvent(validRequest("trace-parser-failure-001"));

        ArgumentCaptor<FailureEventEntity> captor = ArgumentCaptor.forClass(FailureEventEntity.class);
        verify(failureEventRepository).save(captor.capture());
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
}
