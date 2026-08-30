package com.failureintel.ingestion.domain.normalization;

import com.failureintel.ingestion.domain.model.ParsedFailureEvent;
import com.failureintel.ingestion.domain.model.NormalizedFailureEvent;
import org.springframework.stereotype.Component;

import java.time.Clock;
import java.time.Instant;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;

@Component
public class FailureEventNormalizer {

    private static final String UNKNOWN_SERVICE = "unknown_service";
    private static final String UNKNOWN_ENVIRONMENT = "unknown";
    private static final String UNKNOWN_EVENT_TYPE = "unknown_event";
    private static final String UNKNOWN_ERROR_TYPE = "UNKNOWN_FAILURE";
    private static final String UNKNOWN_SEVERITY = "unknown";
    private final Clock clock;

    public FailureEventNormalizer() {
        this(Clock.systemUTC());
    }

    public FailureEventNormalizer(Clock clock) {
        this.clock = Objects.requireNonNull(
                clock,
                "clock cannot be null");
    }

    public NormalizedFailureEvent normalize(ParsedFailureEvent parsedEvent) {
        Objects.requireNonNull(parsedEvent, "parsedEvent cannot be null");
        Instant normalizedAt = Instant.now(clock);
        Map<String, Object> normalizationMetadata = new HashMap<>();
        String serviceName = normalizeServiceName(parsedEvent.getServiceName(), normalizationMetadata);
        String environment = normalizeEnvironment(parsedEvent.getEnvironment(), normalizationMetadata);
        String eventType = normalizeEventType(parsedEvent.getEventType(), normalizationMetadata);
        String errorType = normalizeErrorType(parsedEvent.getErrorType(), normalizationMetadata);
        String errorMessage = sanitizeErrorMessage(parsedEvent.getErrorMessage(), normalizationMetadata);
        String dependencyTarget = normalizeOptionalText(parsedEvent.getDependencyTarget());
        String traceId = normalizeOptionalText(parsedEvent.getTraceId());
        String severityHint = normalizeSeverity(parsedEvent.getSeverityHint(), normalizationMetadata);

        Instant occurredAt = parsedEvent.getOccurredAt();
        if (occurredAt == null) {
            occurredAt = normalizedAt;
            normalizationMetadata.put("occurredAtFallbackApplied", true);
        }
        NormalizationStatus status = determineStatus(parsedEvent, normalizationMetadata);
        return new NormalizedFailureEvent(
                UUID.randomUUID(),
                serviceName,
                environment,
                eventType,
                errorType,
                errorMessage,
                dependencyTarget,
                traceId,
                severityHint,
                occurredAt,
                normalizedAt,
                status,
                parsedEvent.getExtractedFields(),
                parsedEvent.getSourceMetadata(),
                normalizationMetadata);

    }

    private String normalizeServiceName(String serviceName, Map<String, Object> normalizationMetadata) {
        String value = normalizeRequiredText(serviceName);
        if (value == null) {
            normalizationMetadata.put("serviceNameDefaulted", true);
            return UNKNOWN_SERVICE;
        }
        return value.toLowerCase(Locale.ROOT);
    }

    private String normalizeEnvironment(String environment, Map<String, Object> normalizationMetadata) {
        String value = normalizeRequiredText(environment);
        if (value == null) {
            normalizationMetadata.put("environmentDefaulted", true);
            return UNKNOWN_ENVIRONMENT;
        }
        value = value.toLowerCase(Locale.ROOT);
        return switch (value) {
            case "production", "prod" -> "prod";
            case "development", "dev" -> "dev";
            case "staging", "stage" -> "staging";
            case "qa", "test", "testing" -> "qa";
            default -> {
                normalizationMetadata.put("unknownEnvironmentValue", true);
                yield UNKNOWN_ENVIRONMENT;
            }
        };
    }

    private String normalizeEventType(String eventType, Map<String, Object> normalizationMetadata) {
        String value = normalizeRequiredText(eventType);
        if (value == null) {
            normalizationMetadata.put("eventTypeDefaulted", true);
            return UNKNOWN_EVENT_TYPE;
        }
        value = value.toLowerCase(Locale.ROOT);

        return switch (value) {
            case "exception", "error", "application_error" -> "exception";
            case "timeout", "request_timeout" -> "timeout";
            case "dependency", "dependency_failure", "external_service_failure" -> "dependency_failure";
            case "validation", "validation_error", "bad_request" -> "validation_error";
            default -> {
                normalizationMetadata.put("unknownEventTypeValue", true);
                yield value;
            }

        };

    }

    private String normalizeErrorType(String errorType, Map<String, Object> normalizationMetadata) {
        String value = normalizeRequiredText(errorType);
        if (value == null) {
            normalizationMetadata.put("errorTypeDefaulted", true);
            return UNKNOWN_ERROR_TYPE;
        }
        value = value.trim();
        return switch (value.toLowerCase(Locale.ROOT)) {
            case "psqlexception",
                    "postgres_exception",
                    "postgresql_error" ->
                "PSQLException";

            case "timeoutexception",
                    "timeout_exception",
                    "request_timeout_exception" ->
                "TimeoutException";

            default -> value;
        };
    }

    private String sanitizeErrorMessage(String errorMessage, Map<String, Object> normalizationMetadata) {
        String value = normalizeRequiredText(errorMessage);
        if (value == null) {
            normalizationMetadata.put("errorMessageDefaulted", true);
            return "";
        }
        String sanitized = value
                .replaceAll("(?i)(password=)[^\\s&]+", "$1****")
                .replaceAll("(?i)(token=)[^\\s&]+", "$1****")
                .replaceAll("(?i)(apiKey=)[^\\s&]+", "$1****")
                .replaceAll("(?i)(secret=)[^\\s&]+", "$1****");
        if (!sanitized.equals(value)) {
            normalizationMetadata.put("errorMessageSanitized", true);
        }
        return sanitized;
    }

    private String normalizeSeverity(String severityHint, Map<String, Object> normalizationMetadata) {
        String value = normalizeRequiredText(severityHint);
        if (value == null) {
            normalizationMetadata.put("severityDefaulted", true);
            return UNKNOWN_SEVERITY;
        }
        value = value.toLowerCase(Locale.ROOT);
        return switch (value) {
            case "critical", "fatal", "sev1", "p0" -> "critical";
            case "high", "error", "sev2", "p1" -> "high";
            case "medium", "warn", "warning", "sev3", "p2" -> "medium";
            case "low", "info", "sev4", "p3" -> "low";
            default -> {
                normalizationMetadata.put("unknownSeverityValue", value);
                yield UNKNOWN_SEVERITY;
            }
        };
    }

    private NormalizationStatus determineStatus(ParsedFailureEvent parsedEvent,
            Map<String, Object> normalizationMetadata) {
        if (parsedEvent.isMalformed() || !parsedEvent.hasMinimumUsefulData()) {
            return NormalizationStatus.MALFORMED;
        }

        if (hasRequiredCanonicalFields(normalizationMetadata)) {
            return NormalizationStatus.FULLY_NORMALIZED;
        }

        return NormalizationStatus.PARTIALLY_NORMALIZED;

    }

    private boolean hasRequiredCanonicalFields(Map<String, Object> normalizationMetadata) {
        return !Boolean.TRUE.equals(normalizationMetadata.get("serviceNameDefaulted"))
                && !Boolean.TRUE.equals(normalizationMetadata.get("environmentDefaulted"))
                && !Boolean.TRUE.equals(normalizationMetadata.get("unknownEnvironmentValue"))
                && !Boolean.TRUE.equals(normalizationMetadata.get("eventTypeDefaulted"))
                && !Boolean.TRUE.equals(normalizationMetadata.get("unknownEventTypeValue"))
                && !Boolean.TRUE.equals(normalizationMetadata.get("errorTypeDefaulted"))
                && !Boolean.TRUE.equals(normalizationMetadata.get("errorMessageDefaulted"))
                && !Boolean.TRUE.equals(normalizationMetadata.get("severityDefaulted"))
                && !normalizationMetadata.containsKey("unknownSeverityValue")
                && !Boolean.TRUE.equals(
                        normalizationMetadata.get("occurredAtFallbackApplied"));
    }

    private String normalizeRequiredText(String value) {
        if (value == null || value.isBlank()) {
            return null;
        }
        return value.trim();
    }

    private String normalizeOptionalText(String value) {
        return value == null || value.isBlank() ? null : value.trim();
    }
}
