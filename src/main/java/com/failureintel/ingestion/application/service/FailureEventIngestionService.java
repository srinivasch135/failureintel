package com.failureintel.ingestion.application.service;

import com.failureintel.ingestion.api.dto.FailureEventIngestionRequest;
import com.failureintel.ingestion.application.useCase.IngestFailureEventUseCase;
import com.failureintel.ingestion.domain.model.RawFailureEvent;
import com.failureintel.ingestion.domain.model.NormalizedFailureEvent;
import com.failureintel.ingestion.domain.parser.FailureEventParser;
import com.failureintel.ingestion.domain.normalization.FailureEventNormalizer;
import com.failureintel.infrastructure.persistence.failureevent.entity.FailureEventEntity;
import com.failureintel.infrastructure.persistence.failureevent.entity.ProcessingStatus;
import com.failureintel.infrastructure.persistence.failureevent.repository.FailureEventRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import java.time.Instant;

@Service
public class FailureEventIngestionService implements IngestFailureEventUseCase {
    private static final Logger logger = LoggerFactory.getLogger(FailureEventIngestionService.class);

    private final FailureEventRepository failureEventRepository;
    private final FailureEventParser failureEventParser;
    private final FailureEventNormalizer failureEventNormalizer;

    public FailureEventIngestionService(FailureEventRepository failureEventRepository,
            FailureEventParser failureEventParser,
            FailureEventNormalizer failureEventNormalizer) {
        this.failureEventRepository = failureEventRepository;
        this.failureEventParser = failureEventParser;
        this.failureEventNormalizer = failureEventNormalizer;
    }

    @Override
    @Transactional
    public String ingestFailureEvent(FailureEventIngestionRequest request) {
        // Since eventId is not in request, and is generated in entity,
        // we'll check traceId for duplicates if provided.
        if (request.getTraceId() != null && !request.getTraceId().isBlank()) {
            boolean duplicateExists = failureEventRepository.existsByTraceId(request.getTraceId());
            if (duplicateExists) {
                logger.warn("Duplicate failure event detected for traceId={}", request.getTraceId());
                throw new IllegalArgumentException("Failure event already exists for traceId: " + request.getTraceId());
            }
        }
        RawFailureEvent rawFailureEvent = mapRequestToRawFailureEvent(request);
        if (!failureEventParser.supports(rawFailureEvent)) {
            logger.warn(
                    "Unsupported failure event received. sourceSystem={} serviceName={} eventType={}",
                    rawFailureEvent.getSourceSystem(),
                    rawFailureEvent.getServiceName(),
                    rawFailureEvent.getEventType());

            throw new IllegalArgumentException(
                    "Unsupported failure event source: " + rawFailureEvent.getSourceSystem());
        }
        ParsedFailureEvent parsedFailureEvent = failureEventParser.parse(rawFailureEvent);
        NormalizedFailureEvent normalizedFailureEvent = failureEventNormalizer.normalize(parsedFailureEvent);
        FailureEventEntity entity = mapToFailureEventEntity(rawFailureEvent,
                parsedFailureEvent,
                normalizedFailureEvent);

        FailureEventEntity savedEntity = failureEventRepository.save(entity);
        logger.info(
                "Accepted and saved failure event. eventId={} service={} environment={} eventType={} normalizationStatus={}",
                savedEntity.getEventId(),
                savedEntity.getServiceName(),
                savedEntity.getEnvironment(),
                savedEntity.getEventType(),
                savedEntity.getNormalizationStatus());
        return savedEntity.getEventId().toString();
    }

    private FailureEventEntity mapToFailureEventEntity(RawFailureEvent rawFailureEvent,
            ParsedFailureEvent parsedFailureEvent,
            NormalizedFailureEvent normalizedFailureEvent) {
        FailureEventEntity entity = new FailureEventEntity();
        entity.setOccurredAt(rawFailureEvent.getOccuredAt());
        entity.setIngestedAt(rawFailureEvent.getReceivedAt());

        entity.setServiceName(parsedFailureEvent.getServiceName());
        entity.setEnvironment(parsedFailureEvent.getEnvironment());
        entity.setEventType(parsedFailureEvent.getEventType());
        entity.setErrorType(parsedFailureEvent.getErrorType());
        entity.setMessage(parsedFailureEvent.getErrorMessage());
        entity.setDependencyTarget(parsedFailureEvent.getDependencyTarget());
        entity.setTraceId(parsedFailureEvent.getTraceId());
        entity.setSeverityHint(parsedFailureEvent.getSeverityHint());

        entity.setNormalizedServiceName(normalizedFailureEvent.getServiceName());
        entity.setNormalizedEnvironment(normalizedFailureEvent.getEnvironment());
        entity.setNormalizedEventType(normalizedFailureEvent.getEventType());
        entity.setNormalizedErrorType(normalizedFailureEvent.getErrorType());
        entity.setNormalizedErrorMessage(normalizedFailureEvent.getErrorMessage());
        entity.setNormalizedDependencyTarget(normalizedFailureEvent.getDependencyTarget());
        entity.setNormalizedTraceId(normalizedFailureEvent.getTraceId());
        entity.setNormalizedSeverity(normalizedFailureEvent.getSeverity());
        entity.setNormalizedOccurredAt(normalizedFailureEvent.getOccurredAt());
        entity.setNormalizationStatus(normalizedFailureEvent.getNormalizationStatus());

        entity.setProcessingStatus(ProcessingStatus.RECEIVED);
        return entity;
    }

    private RawFailureEvent mapRequestToRawFailureEvent(FailureEventIngestionRequest request) {
        return new RawFailureEvent(
                UUID.randomUUID(),
                request.getSourceSystem(),
                request.getServiceName(),
                request.getEnvironment(),
                request.getEventType(),
                request.getErrorType(),
                request.getErrorMessage(),
                request.getDependencyTarget(),
                request.getTraceId(),
                request.getSeverityHint(),
                request.getOccurredAt(),
                Instant.now(),
                request.getRawPayload() != null ? request.getRawPayload() : Map.of(),
                Map.of());
    }
}
