package com.failureintel.ingestion.application.service;

import com.failureintel.ingestion.api.dto.FailureEventResponse;
import com.failureintel.ingestion.api.mapper.FailureEventResponseMapper;
import com.failureintel.ingestion.application.exception.FailureEventNotFoundException;
import com.failureintel.infrastructure.persistence.failureevent.entity.FailureEventEntity;
import com.failureintel.infrastructure.persistence.failureevent.repository.FailureEventRepository;
import com.failureintel.infrastructure.persistence.normalizedFailureEvent.entity.NormalizedFailureEventEntity;
import com.failureintel.infrastructure.persistence.normalizedFailureEvent.repository.NormalizedFailureEventRepository;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.stereotype.Service;

import java.util.Objects;
import java.util.UUID;

@Service
public class FailureEventQueryService {
    private final FailureEventRepository failureEventRepository;
    private final NormalizedFailureEventRepository normalizedFailureEventRepository;

    public FailureEventQueryService(FailureEventRepository failureEventRepository,
            NormalizedFailureEventRepository normalizedFailureEventRepository) {
        this.failureEventRepository = failureEventRepository;
        this.normalizedFailureEventRepository = normalizedFailureEventRepository;
    }

    public FailureEventResponse getFailureEvent(UUID eventId) {
        Objects.requireNonNull(eventId, "eventId must not be null");
        FailureEventEntity failureEvent = failureEventRepository.findById(eventId)
                .orElseThrow(() -> new FailureEventNotFoundException(eventId));
        return toResponse(failureEvent);
    }

    public FailureEventResponse getFailureEventByTraceId(String traceId) {
        Objects.requireNonNull(traceId, "traceId must not be null");
        FailureEventEntity failureEvent = failureEventRepository.findByTraceId(traceId)
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
