package com.failureintel.infrastructure.persistence.failureevent.mapper;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.failureintel.ingestion.domain.model.RawFailureEvent;
import com.failureintel.infrastructure.persistence.failureevent.entity.FailureEventEntity;
import com.failureintel.infrastructure.persistence.failureevent.entity.ProcessingStatus;

import java.util.Map;
import java.util.Objects;

public final class FailureEventEntityMapper {

    public static final class MalformedRawPayloadException extends RuntimeException {
        public MalformedRawPayloadException(String message, Throwable cause) {
            super(message, cause);
        }
    }

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private FailureEventEntityMapper() {
    }

    public static FailureEventEntity fromRaw(RawFailureEvent rawFailureEvent) {
        Objects.requireNonNull(rawFailureEvent, "rawFailureEvent must not be null");

        FailureEventEntity entity = mapRawFields(rawFailureEvent);
        entity.setProcessingStatus(ProcessingStatus.RECEIVED);

        return entity;
    }

    public static RawFailureEvent toRaw(FailureEventEntity entity) {
        Objects.requireNonNull(entity, "entity must not be null");

        return new RawFailureEvent(
                entity.getEventId(),
                entity.getSourceSystem(),
                entity.getServiceName(),
                entity.getEnvironment(),
                entity.getEventType(),
                entity.getErrorType(),
                entity.getMessage(),
                entity.getDependencyTarget(),
                entity.getTraceId(),
                entity.getSeverityHint(),
                entity.getOccurredAt(),
                entity.getIngestedAt(),
                fromJson(entity.getRawPayload()),
                entity.getSourceMetadata() == null ? Map.of() : entity.getSourceMetadata());
    }

    public static FailureEventEntity failedFromRaw(
            RawFailureEvent rawFailureEvent,
            String failureReason) {
        Objects.requireNonNull(rawFailureEvent, "rawFailureEvent must not be null");

        FailureEventEntity entity = mapRawFields(rawFailureEvent);
        entity.setFailureReason(failureReason);
        entity.setProcessingStatus(ProcessingStatus.FAILED);

        return entity;
    }

    private static FailureEventEntity mapRawFields(RawFailureEvent rawFailureEvent) {
        FailureEventEntity entity = new FailureEventEntity();
        entity.setEventId(rawFailureEvent.getRawEventId());
        entity.setSourceSystem(rawFailureEvent.getSourceSystem());
        entity.setServiceName(rawFailureEvent.getServiceName());
        entity.setEnvironment(rawFailureEvent.getEnvironment());
        entity.setEventType(rawFailureEvent.getEventType());
        entity.setErrorType(rawFailureEvent.getErrorType());
        entity.setMessage(rawFailureEvent.getErrorMessage());
        entity.setDependencyTarget(rawFailureEvent.getDependencyTarget());
        entity.setTraceId(rawFailureEvent.getTraceId());
        entity.setSeverityHint(rawFailureEvent.getSeverityHint());
        entity.setOccurredAt(rawFailureEvent.getOccuredAt());
        entity.setIngestedAt(rawFailureEvent.getReceivedAt());
        entity.setRawPayload(toJson(rawFailureEvent.getRawPayload()));
        entity.setSourceMetadata(rawFailureEvent.getMetaData());
        return entity;
    }

    private static String toJson(Map<String, Object> payload) {
        if (payload == null || payload.isEmpty()) {
            return "{}";
        }

        try {
            return OBJECT_MAPPER.writeValueAsString(payload);
        } catch (JsonProcessingException exception) {
            throw new IllegalArgumentException("Unable to serialize failure event payload", exception);
        }
    }

    private static Map<String, Object> fromJson(String rawPayload) {
        if (rawPayload == null || rawPayload.isBlank()) {
            return Map.of();
        }

        try {
            return OBJECT_MAPPER.readValue(
                    rawPayload,
                    new TypeReference<Map<String, Object>>() {});
        } catch (JsonProcessingException exception) {
            throw new MalformedRawPayloadException(
                    "Unable to deserialize persisted failure event payload",
                    exception);
        }
    }
}
