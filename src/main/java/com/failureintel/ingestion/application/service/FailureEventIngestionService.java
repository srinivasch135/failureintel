package com.failureintel.ingestion.application.service;

import com.failureintel.ingestion.api.dto.FailureEventIngestionRequest;
import com.failureintel.ingestion.application.useCase.IngestFailureEventUseCase;
import com.failureintel.ingestion.domain.model.NormalizedFailureEvent;
import com.failureintel.ingestion.domain.model.ParsedFailureEvent;
import com.failureintel.ingestion.domain.model.RawFailureEvent;
import com.failureintel.ingestion.domain.normalization.FailureEventNormalizer;
import com.failureintel.ingestion.domain.normalization.NormalizationStatus;
import com.failureintel.ingestion.domain.parser.FailureEventParser;
import com.failureintel.infrastructure.persistence.failureevent.entity.FailureEventEntity;
import com.failureintel.infrastructure.persistence.failureevent.mapper.FailureEventEntityMapper;
import com.failureintel.infrastructure.persistence.failureevent.repository.FailureEventRepository;
import com.failureintel.infrastructure.persistence.normalizedFailureEvent.mapper.NormalizedFailureEventEntityMapper;
import com.failureintel.infrastructure.persistence.normalizedFailureEvent.repository.NormalizedFailureEventRepository;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.Map;
import java.util.UUID;

@Service
public class FailureEventIngestionService implements IngestFailureEventUseCase {

        private static final Logger logger = LoggerFactory.getLogger(FailureEventIngestionService.class);

        private final FailureEventRepository failureEventRepository;
        private final NormalizedFailureEventRepository normalizedFailureEventRepository;
        private final FailureEventParser failureEventParser;
        private final FailureEventNormalizer failureEventNormalizer;

        public FailureEventIngestionService(
                        FailureEventRepository failureEventRepository,
                        NormalizedFailureEventRepository normalizedFailureEventRepository,
                        FailureEventParser failureEventParser,
                        FailureEventNormalizer failureEventNormalizer) {
                this.failureEventRepository = failureEventRepository;
                this.normalizedFailureEventRepository = normalizedFailureEventRepository;
                this.failureEventParser = failureEventParser;
                this.failureEventNormalizer = failureEventNormalizer;
        }

        @Override
        @Transactional
        public String ingestFailureEvent(FailureEventIngestionRequest request) {

                if (request.getTraceId() != null && !request.getTraceId().isBlank()) {
                        boolean duplicateExists = failureEventRepository.existsByTraceId(request.getTraceId());

                        if (duplicateExists) {
                                logger.warn(
                                                "Duplicate failure event detected for traceId={}",
                                                request.getTraceId());

                                throw new IllegalArgumentException(
                                                "Failure event already exists for traceId: " + request.getTraceId());
                        }
                }

                RawFailureEvent rawFailureEvent = mapRequestToRawFailureEvent(request);

                if (!failureEventParser.supports(rawFailureEvent)) {
                        logger.warn(
                                        "Unsupported failure event received. Saving as failed. sourceSystem={} serviceName={} eventType={}",
                                        rawFailureEvent.getSourceSystem(),
                                        rawFailureEvent.getServiceName(),
                                        rawFailureEvent.getEventType());

                        return saveFailedEvent(
                                        rawFailureEvent,
                                        "Unsupported or empty raw payload for source: "
                                                        + rawFailureEvent.getSourceSystem());
                }

                ParsedFailureEvent parsedFailureEvent;
                NormalizedFailureEvent normalizedFailureEvent;

                try {
                        parsedFailureEvent = failureEventParser.parse(rawFailureEvent);
                        normalizedFailureEvent = failureEventNormalizer.normalize(parsedFailureEvent);
                } catch (Exception ex) {
                        logger.warn(
                                        "Parsing/normalization failed. Saving failure event as failed. sourceSystem={} reason={}",
                                        rawFailureEvent.getSourceSystem(),
                                        ex.getMessage());

                        return saveFailedEvent(
                                        rawFailureEvent,
                                        "Parsing/normalization failed: " + ex.getMessage());
                }

                if (normalizedFailureEvent.getNormalizationStatus() == NormalizationStatus.MALFORMED) {
                        logger.warn(
                                        "Failure event contains no minimum useful data. Saving as failed. sourceSystem={}",
                                        rawFailureEvent.getSourceSystem());

                        return saveFailedEvent(
                                        rawFailureEvent,
                                        "Event does not contain minimum useful failure data");
                }

                FailureEventEntity entity = FailureEventEntityMapper.fromDomain(
                                rawFailureEvent,
                                parsedFailureEvent);

                FailureEventEntity savedEntity = failureEventRepository.save(entity);
                normalizedFailureEventRepository.save(
                                NormalizedFailureEventEntityMapper.fromDomain(
                                                normalizedFailureEvent,
                                                savedEntity));

                logger.info(
                                "Accepted and saved failure event. eventId={} service={} environment={} eventType={} normalizationStatus={}",
                                savedEntity.getEventId(),
                                savedEntity.getServiceName(),
                                savedEntity.getEnvironment(),
                                savedEntity.getEventType(),
                                normalizedFailureEvent.getNormalizationStatus());

                return savedEntity.getEventId().toString();
        }

        private String saveFailedEvent(
                        RawFailureEvent rawFailureEvent,
                        String failureReason) {
                FailureEventEntity failedEntity = FailureEventEntityMapper.failedFromRaw(
                                rawFailureEvent,
                                failureReason);
                FailureEventEntity savedEntity = failureEventRepository.save(failedEntity);

                logger.info(
                                "Saved failed failure event. eventId={} sourceSystem={} processingStatus={} reason={}",
                                savedEntity.getEventId(),
                                rawFailureEvent.getSourceSystem(),
                                savedEntity.getProcessingStatus(),
                                failureReason);

                return savedEntity.getEventId().toString();
        }

        private RawFailureEvent mapRequestToRawFailureEvent(
                        FailureEventIngestionRequest request) {
                return new RawFailureEvent(
                                UUID.randomUUID(),
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
                                Instant.now(),
                                request.getRawPayload() != null ? request.getRawPayload() : Map.of(),
                                Map.of());
        }
}
