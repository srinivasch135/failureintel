package com.failureintel.ingestion.domain.parser;

import com.failureintel.ingestion.domain.model.ParsedFailureEvent;
import com.failureintel.ingestion.domain.model.RawFailureEvent;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;

@Component
public class GenericJsonFailureEventParser implements FailureEventParser {

    @Override
    public boolean supports(RawFailureEvent rawFailureEvent) {
        return rawFailureEvent != null
                && rawFailureEvent.getRawPayload() != null
                && !rawFailureEvent.getRawPayload().isEmpty();
    }

    @Override
    public ParsedFailureEvent parse(RawFailureEvent rawFailureEvent) {

        if (rawFailureEvent == null) {
            throw new IllegalArgumentException("rawFailureEvent cannot be null");
        }

        if (!supports(rawFailureEvent)) {
            return ParsedFailureEvent.malformed(
                    rawFailureEvent,
                    "Raw payload is null or empty");
        }

        Map<String, Object> payload = rawFailureEvent.getRawPayload();

        String serviceName = firstNonBlank(
                rawFailureEvent.getServiceName(),
                findString(payload, List.of("serviceName", "service", "application", "app")));

        String environment = firstNonBlank(
                rawFailureEvent.getEnvironment(),
                findString(payload, List.of("environment", "env", "stage")));

        String eventType = firstNonBlank(
                rawFailureEvent.getEventType(),
                findString(payload, List.of("eventType", "type", "kind")));

        String errorType = firstNonBlank(
                rawFailureEvent.getErrorType(),
                findString(payload, List.of("errorType", "exceptionType", "exception", "error")));

        String errorMessage = firstNonBlank(
                rawFailureEvent.getErrorMessage(),
                findString(payload, List.of("errorMessage", "message", "msg", "detail")));

        String dependencyTarget = firstNonBlank(
                rawFailureEvent.getDependencyTarget(),
                findString(payload, List.of("dependencyTarget", "target", "dependency", "downstreamService")));

        String traceId = firstNonBlank(
                rawFailureEvent.getTraceId(),
                findString(payload, List.of("traceId", "trace_id", "correlationId", "requestId")));

        String severityHint = firstNonBlank(
                rawFailureEvent.getSeverityHint(),
                findString(payload, List.of("severityHint", "severity", "level")));

        Instant occurredAt = firstNonNull(
                rawFailureEvent.getOccurredAt(),
                findInstant(payload, List.of("occurredAt", "timestamp", "time")));

        return ParsedFailureEvent.parsed(
                rawFailureEvent,
                serviceName,
                environment,
                eventType,
                errorType,
                errorMessage,
                dependencyTarget,
                traceId,
                severityHint,
                occurredAt);
    }

    private String findString(Map<String, Object> payload, List<String> possibleKeys) {
        for (String key : possibleKeys) {
            Object value = findValueIgnoreCase(payload, key);

            if (value instanceof String text && !text.isBlank()) {
                return text.trim();
            }

            if (value != null && !(value instanceof Map<?, ?>) && !(value instanceof List<?>)) {
                String text = String.valueOf(value).trim();

                if (!text.isBlank()) {
                    return text;
                }
            }
        }

        return null;
    }

    private Instant findInstant(Map<String, Object> payload, List<String> possibleKeys) {
        String value = findString(payload, possibleKeys);

        if (value == null || value.isBlank()) {
            return null;
        }

        try {
            return Instant.parse(value);
        } catch (Exception exception) {
            return null;
        }
    }

    private Object findValueIgnoreCase(Map<String, Object> payload, String expectedKey) {
        for (Map.Entry<String, Object> entry : payload.entrySet()) {
            if (entry.getKey() != null && entry.getKey().equalsIgnoreCase(expectedKey)) {
                return entry.getValue();
            }

            if (entry.getValue() instanceof Map<?, ?> nestedMap) {
                Optional<Object> nestedValue = findNestedValue(nestedMap, expectedKey);

                if (nestedValue.isPresent()) {
                    return nestedValue.get();
                }
            }
        }

        return null;
    }

    private Optional<Object> findNestedValue(Map<?, ?> payload, String expectedKey) {
        for (Map.Entry<?, ?> entry : payload.entrySet()) {
            if (entry.getKey() != null && entry.getKey().toString().equalsIgnoreCase(expectedKey)) {
                return Optional.ofNullable(entry.getValue());
            }

            if (entry.getValue() instanceof Map<?, ?> nestedMap) {
                Optional<Object> nestedValue = findNestedValue(nestedMap, expectedKey);

                if (nestedValue.isPresent()) {
                    return nestedValue;
                }
            }
        }

        return Optional.empty();
    }

    private String firstNonBlank(String primaryValue, String fallbackValue) {
        if (primaryValue != null && !primaryValue.isBlank()) {
            return primaryValue.trim();
        }

        return fallbackValue;
    }

    private Instant firstNonNull(Instant primaryValue, Instant fallbackValue) {
        return primaryValue != null ? primaryValue : fallbackValue;
    }
}