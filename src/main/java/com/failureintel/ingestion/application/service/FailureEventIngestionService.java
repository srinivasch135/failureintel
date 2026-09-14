package com.failureintel.ingestion.application.service;

import com.failureintel.ingestion.api.dto.FailureEventIngestionRequest;
import com.failureintel.ingestion.application.exception.DuplicateFailureEventException;
import com.failureintel.ingestion.application.useCase.IngestFailureEventUseCase;
import com.failureintel.ingestion.domain.model.RawFailureEvent;
import com.failureintel.infrastructure.persistence.failureevent.entity.FailureEventEntity;
import com.failureintel.infrastructure.persistence.failureevent.mapper.FailureEventEntityMapper;
import com.failureintel.infrastructure.persistence.failureevent.repository.FailureEventRepository;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.sql.SQLException;
import java.time.Instant;
import java.util.Map;
import java.util.UUID;

@Service
public class FailureEventIngestionService implements IngestFailureEventUseCase {

        private static final Logger logger = LoggerFactory.getLogger(FailureEventIngestionService.class);
        private static final String TRACE_ID_UNIQUE_INDEX = "uq_failure_event_trace_id";
        private static final String POSTGRES_UNIQUE_VIOLATION = "23505";

        private final FailureEventRepository failureEventRepository;

        public FailureEventIngestionService(FailureEventRepository failureEventRepository) {
                this.failureEventRepository = failureEventRepository;
        }

        @Override
        @Transactional
        public String ingestFailureEvent(FailureEventIngestionRequest request) {

                RawFailureEvent rawFailureEvent = mapRequestToRawFailureEvent(request);
                FailureEventEntity entity = FailureEventEntityMapper.fromRaw(rawFailureEvent);
                FailureEventEntity savedEntity = saveRawEntity(entity);

                logger.info(
                                "Persisted raw failure event for processing. eventId={} traceId={} processingStatus={}",
                                savedEntity.getEventId(),
                                savedEntity.getTraceId(),
                                savedEntity.getProcessingStatus());

                return savedEntity.getEventId().toString();
        }

        private FailureEventEntity saveRawEntity(FailureEventEntity entity) {
                try {
                        return failureEventRepository.saveAndFlush(entity);
                } catch (DataIntegrityViolationException exception) {
                        if (entity.getTraceId() != null && isTraceIdUniqueViolation(exception)) {
                                throw new DuplicateFailureEventException(entity.getTraceId());
                        }
                        throw exception;
                }
        }

        private boolean isTraceIdUniqueViolation(Throwable failure) {
                Throwable current = failure;
                while (current != null) {
                        if (current instanceof SQLException sqlException
                                        && POSTGRES_UNIQUE_VIOLATION.equals(sqlException.getSQLState())
                                        && containsTraceIdIndex(sqlException.getMessage())) {
                                return true;
                        }
                        current = current.getCause();
                }
                return false;
        }

        private boolean containsTraceIdIndex(String message) {
                return message != null && message.contains(TRACE_ID_UNIQUE_INDEX);
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
                                normalizeTraceId(request.getTraceId()),
                                request.getSeverityHint(),
                                request.getOccurredAt(),
                                Instant.now(),
                                request.getRawPayload() != null ? request.getRawPayload() : Map.of(),
                                Map.of());
        }

        private String normalizeTraceId(String traceId) {
                if (traceId == null || traceId.isBlank()) {
                        return null;
                }
                return traceId.trim();
        }
}
