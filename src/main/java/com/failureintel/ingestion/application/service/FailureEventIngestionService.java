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
import org.hibernate.exception.ConstraintViolationException;
import java.sql.SQLException;
import java.time.Instant;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;

@Service
public class FailureEventIngestionService implements IngestFailureEventUseCase {

        private static final Logger logger = LoggerFactory.getLogger(FailureEventIngestionService.class);
        private static final String IDEMPOTENCY_KEY_UNIQUE_INDEX = "uq_failure_event_idempotency_key";
        private static final String POSTGRES_UNIQUE_VIOLATION = "23505";

        private final FailureEventRepository failureEventRepository;

        public FailureEventIngestionService(FailureEventRepository failureEventRepository) {
                this.failureEventRepository = failureEventRepository;
        }

        @Override
        public String ingestFailureEvent(FailureEventIngestionRequest request) {

                RawFailureEvent rawFailureEvent = mapRequestToRawFailureEvent(request);
                String idempotencyKey = resolveIdempotencyKey(request, rawFailureEvent);
                String fingerprint = idempotencyKey == null
                                ? null
                                : FailureEventFingerprint.calculate(rawFailureEvent);

                if (idempotencyKey != null) {
                        Optional<FailureEventEntity> existing = failureEventRepository
                                        .findByIdempotencyKey(idempotencyKey);
                        if (existing.isPresent()) {
                                return resolveExisting(existing.get(), rawFailureEvent, fingerprint, request);
                        }
                }

                FailureEventEntity entity = FailureEventEntityMapper.fromRaw(rawFailureEvent);
                entity.setIdempotencyKey(idempotencyKey);
                entity.setIngestionFingerprint(fingerprint);

                try {
                        FailureEventEntity savedEntity = failureEventRepository.saveAndFlush(entity);

                        logger.info(
                                        "Persisted raw failure event for processing. eventId={} traceId={} idempotencyKeyPresent={} processingStatus={}",
                                        savedEntity.getEventId(),
                                        savedEntity.getTraceId(),
                                        idempotencyKey != null,
                                        savedEntity.getProcessingStatus());

                        return savedEntity.getEventId().toString();
                } catch (DataIntegrityViolationException exception) {
                        if (idempotencyKey == null || !isIdempotencyKeyUniqueViolation(exception)) {
                                throw exception;
                        }

                        // The failed insert transaction has ended. A fresh repository read
                        // is required because PostgreSQL aborts the transaction that saw the
                        // unique-key violation.
                        FailureEventEntity existing = failureEventRepository
                                        .findByIdempotencyKey(idempotencyKey)
                                        .orElseThrow(() -> exception);
                        return resolveExisting(existing, rawFailureEvent, fingerprint, request);
                }
        }

        private String resolveExisting(
                        FailureEventEntity existing,
                        RawFailureEvent incoming,
                        String incomingFingerprint,
                        FailureEventIngestionRequest request) {
                String existingFingerprint = existing.getIngestionFingerprint();
                if (existingFingerprint == null) {
                        existingFingerprint = FailureEventFingerprint.calculate(
                                        FailureEventEntityMapper.toRaw(existing));
                        // Legacy rows are compared without a write-back. This keeps the
                        // idempotency lookup side-effect free and avoids a migration backfill.
                }

                if (!Objects.equals(existingFingerprint, incomingFingerprint)) {
                        throw duplicateException(request);
                }

                logger.info(
                                "Idempotent failure-event retry resolved to existing event. eventId={} traceId={} idempotencyKeyPresent={}",
                                existing.getEventId(),
                                incoming.getTraceId(),
                                request.getIdempotencyKey() != null
                                                && !request.getIdempotencyKey().isBlank());
                return existing.getEventId().toString();
        }

        private DuplicateFailureEventException duplicateException(FailureEventIngestionRequest request) {
                if (request.getIdempotencyKey() != null && !request.getIdempotencyKey().isBlank()) {
                        return new DuplicateFailureEventException(
                                        "idempotency key",
                                        request.getIdempotencyKey().trim());
                }
                return new DuplicateFailureEventException(normalizeKey(request.getTraceId()));
        }

        private boolean isIdempotencyKeyUniqueViolation(Throwable failure) {
                Throwable current = failure;
                while (current != null) {
                        if (current instanceof ConstraintViolationException constraintViolation
                                        && IDEMPOTENCY_KEY_UNIQUE_INDEX.equals(
                                                        constraintViolation.getConstraintName())) {
                                return true;
                        }
                        if (current instanceof SQLException sqlException
                                        && POSTGRES_UNIQUE_VIOLATION.equals(sqlException.getSQLState())
                                        && containsIdempotencyKeyIndex(sqlException.getMessage())) {
                                return true;
                        }
                        current = current.getCause();
                }
                return false;
        }

        private boolean containsIdempotencyKeyIndex(String message) {
                return message != null && message.contains(IDEMPOTENCY_KEY_UNIQUE_INDEX);
        }

        private String resolveIdempotencyKey(
                        FailureEventIngestionRequest request,
                        RawFailureEvent rawFailureEvent) {
                String explicitKey = normalizeKey(request.getIdempotencyKey());
                if (explicitKey != null) {
                        return "key:" + explicitKey;
                }

                String legacyTraceId = rawFailureEvent.getTraceId();
                return legacyTraceId == null ? null : "trace:" + legacyTraceId;
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
                                normalizeKey(request.getTraceId()),
                                request.getSeverityHint(),
                                request.getOccurredAt(),
                                Instant.now(),
                                request.getRawPayload() != null ? request.getRawPayload() : Map.of(),
                                Map.of());
        }

        private String normalizeKey(String value) {
                if (value == null || value.isBlank()) {
                        return null;
                }
                return value.trim();
        }
}
