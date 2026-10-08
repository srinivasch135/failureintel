package com.failureintel.ingestion.application.service;

import com.failureintel.ingestion.api.dto.FailureEventResponse;
import com.failureintel.ingestion.api.mapper.FailureEventResponseMapper;
import com.failureintel.ingestion.application.exception.FailureEventNotFoundException;
import com.failureintel.infrastructure.persistence.failureevent.entity.FailureEventEntity;
import com.failureintel.infrastructure.persistence.failureevent.entity.ProcessingStatus;
import com.failureintel.infrastructure.persistence.failureevent.repository.FailureEventRepository;
import com.failureintel.infrastructure.persistence.normalizedFailureEvent.entity.NormalizedFailureEventEntity;
import com.failureintel.infrastructure.persistence.normalizedFailureEvent.repository.NormalizedFailureEventRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Isolation;
import org.springframework.transaction.annotation.Transactional;

import java.util.Objects;
import java.util.UUID;

@Service
public class FailureEventQueryService {
    private static final Logger logger = LoggerFactory.getLogger(FailureEventQueryService.class);

    private final FailureEventRepository failureEventRepository;
    private final NormalizedFailureEventRepository normalizedFailureEventRepository;

    public FailureEventQueryService(FailureEventRepository failureEventRepository,
            NormalizedFailureEventRepository normalizedFailureEventRepository) {
        this.failureEventRepository = failureEventRepository;
        this.normalizedFailureEventRepository = normalizedFailureEventRepository;
    }

    @Transactional(readOnly = true, isolation = Isolation.REPEATABLE_READ)
    public FailureEventResponse getFailureEvent(UUID eventId) {
        Objects.requireNonNull(eventId, "eventId must not be null");
        FailureEventEntity failureEvent = failureEventRepository.findById(eventId)
                .orElseThrow(() -> new FailureEventNotFoundException(eventId));
        return toResponse(failureEvent);
    }

    @Transactional(readOnly = true, isolation = Isolation.REPEATABLE_READ)
    public FailureEventResponse getFailureEventByTraceId(String traceId) {
        Objects.requireNonNull(traceId, "traceId must not be null");
        FailureEventEntity failureEvent = failureEventRepository
                .findFirstByTraceIdOrderByIngestedAtDescEventIdDesc(traceId)
                .orElseThrow(() -> new FailureEventNotFoundException(traceId));
        return toResponse(failureEvent);
    }

    public Page<FailureEventResponse> searchFailureEvents(
            String serviceName,
            String environment,
            String severity,
            Pageable pageable) {
        Objects.requireNonNull(pageable, "pageable must not be null");

        return normalizedFailureEventRepository.searchByFailureDetails(
                normalizeFilter(serviceName),
                normalizeFilter(environment),
                normalizeFilter(severity),
                pageable)
                .map(this::toResponse);
    }

    private FailureEventResponse toResponse(FailureEventEntity failureEvent) {
        UUID eventId = failureEvent.getEventId();
        NormalizedFailureEventEntity normalizedFailureEvent = normalizedFailureEventRepository.findById(eventId)
                .orElse(null);
        if (failureEvent.getProcessingStatus() == ProcessingStatus.NORMALIZED
                && normalizedFailureEvent == null) {
            logger.warn("Normalized failure event is missing for raw event with NORMALIZED status. eventId={}",
                    eventId);
        }
        return FailureEventResponseMapper.fromEntities(failureEvent, normalizedFailureEvent);
    }

    private FailureEventResponse toResponse(NormalizedFailureEventEntity normalizedFailureEvent) {
        return FailureEventResponseMapper.fromEntities(
                normalizedFailureEvent.getFailureEvent(),
                normalizedFailureEvent);
    }

    private String normalizeFilter(String value) {
        if (value == null || value.isBlank()) {
            return null;
        }
        return value.trim().toLowerCase();
    }
}
