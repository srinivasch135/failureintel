package com.failureintel.infrastructure.persistence.failureevent.mapper;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.failureintel.ingestion.domain.model.ParsedFailureEvent;
import com.failureintel.ingestion.domain.model.RawFailureEvent;
import com.failureintel.infrastructure.persistence.failureevent.entity.FailureEventEntity;
import com.failureintel.infrastructure.persistence.failureevent.entity.ProcessingStatus;

import java.util.Map;
import java.util.Objects;
import java.util.UUID;

public final class FailureEventEntityMapper {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private FailureEventEntityMapper() {
    }

    public static FailureEventEntity fromDomain(
            RawFailureEvent rawFailureEvent,
            ParsedFailureEvent parsedFailureEvent) {
        Objects.requireNonNull(rawFailureEvent, "rawFailureEvent must not be null");
        Objects.requireNonNull(parsedFailureEvent, "parsedFailureEvent must not be null");

        FailureEventEntity entity = new FailureEventEntity();

        entity.setEventId(rawFailureEvent.getRawEventId());
        entity.setRawPayload(toJson(rawFailureEvent.getRawPayload()));
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
        entity.setProcessingStatus(ProcessingStatus.NORMALIZED);

        return entity;
    }

    public static FailureEventEntity failedFromRaw(
            RawFailureEvent rawFailureEvent,
            String failureReason) {
        Objects.requireNonNull(rawFailureEvent, "rawFailureEvent must not be null");

        FailureEventEntity entity = new FailureEventEntity();

        entity.setEventId(UUID.randomUUID());
        entity.setRawPayload(toJson(rawFailureEvent.getRawPayload()));
        entity.setOccurredAt(rawFailureEvent.getOccuredAt());
        entity.setIngestedAt(rawFailureEvent.getReceivedAt());

        entity.setServiceName(rawFailureEvent.getServiceName());
        entity.setEnvironment(rawFailureEvent.getEnvironment());
        entity.setEventType(rawFailureEvent.getEventType());
        entity.setErrorType(rawFailureEvent.getErrorType());
        entity.setMessage(rawFailureEvent.getErrorMessage());
        entity.setDependencyTarget(rawFailureEvent.getDependencyTarget());
        entity.setTraceId(rawFailureEvent.getTraceId());
        entity.setSeverityHint(rawFailureEvent.getSeverityHint());
        entity.setFailureReason(failureReason);
        entity.setProcessingStatus(ProcessingStatus.FAILED);

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
}
