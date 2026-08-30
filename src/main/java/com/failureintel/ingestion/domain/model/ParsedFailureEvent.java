package com.failureintel.ingestion.domain.model;

import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class ParsedFailureEvent {
    private final String source;
    private final String serviceName;
    private final String environment;
    private final String eventType;
    private final String errorType;
    private final String errorMessage;
    private final String dependencyTarget;
    private final String traceId;
    private final String severityHint;
    private final Instant occurredAt;
    private final Map<String, Object> extractedFields;
    private final Map<String, Object> sourceMetaData;
    private final List<String> parsingWarnings;
    private final boolean malformed;

    private ParsedFailureEvent(Builder builder) {
        this.source = builder.source;
        this.serviceName = builder.serviceName;
        this.environment = builder.environment;
        this.eventType = builder.eventType;
        this.errorType = builder.errorType;
        this.errorMessage = builder.errorMessage;
        this.dependencyTarget = builder.dependencyTarget;
        this.traceId = builder.traceId;
        this.severityHint = builder.severityHint;
        this.occurredAt = builder.occurredAt;
        this.extractedFields = builder.extractedFields == null ? Collections.emptyMap()
                : Collections.unmodifiableMap(builder.extractedFields);
        this.sourceMetaData = builder.sourceMetaData == null ? Collections.emptyMap()
                : Collections.unmodifiableMap(builder.sourceMetaData);
        this.parsingWarnings = builder.parsingWarnings == null ? Collections.emptyList()
                : Collections.unmodifiableList(builder.parsingWarnings);
        this.malformed = builder.malformed;
    }

    public boolean isMalformed() {
        return malformed;
    }

    public static ParsedFailureEvent malformed(RawFailureEvent rawFailureEvent, String reason) {
        Map<String, Object> sourceMeta = rawFailureEvent != null ? rawFailureEvent.getMetaData()
                : Collections.emptyMap();
        return ParsedFailureEvent.builder()
                .source(rawFailureEvent != null ? rawFailureEvent.getSourceSystem() : null)
                .serviceName(rawFailureEvent != null ? rawFailureEvent.getServiceName() : null)
                .environment(rawFailureEvent != null ? rawFailureEvent.getEnvironment() : null)
                .eventType(rawFailureEvent != null ? rawFailureEvent.getEventType() : null)
                .errorMessage(reason)
                .traceId(rawFailureEvent != null ? rawFailureEvent.getTraceId() : null)
                .sourceMetadata(sourceMeta)
                .parsingWarnings(Collections.singletonList(reason))
                .malformed(true)
                .build();
    }

    public static ParsedFailureEvent parsed(
            RawFailureEvent rawFailureEvent,
            String serviceName,
            String environment,
            String eventType,
            String errorType,
            String errorMessage,
            String dependencyTarget,
            String traceId,
            String severityHint,
            Instant occurredAt) {

        return ParsedFailureEvent.builder()
                .source(rawFailureEvent != null ? rawFailureEvent.getSourceSystem() : null)
                .serviceName(serviceName)
                .environment(environment)
                .eventType(eventType)
                .errorType(errorType)
                .errorMessage(errorMessage)
                .dependencyTarget(dependencyTarget)
                .traceId(traceId)
                .severityHint(severityHint)
                .occurredAt(occurredAt)
                .sourceMetadata(rawFailureEvent != null ? rawFailureEvent.getMetaData() : null)
                .extractedFields(rawFailureEvent != null ? rawFailureEvent.getRawPayload() : null)
                .malformed(false)
                .build();
    }

    public static Builder builder() {
        return new Builder();
    }

    public boolean hasMinimumUsefulData() {
        return hasText(serviceName)
                || hasText(errorType)
                || hasText(errorMessage)
                || hasText(traceId);
    }

    public boolean hasParsingWarnings() {
        return !parsingWarnings.isEmpty();
    }

    private boolean hasText(String value) {
        return value != null && !value.isBlank();
    }

    public String getSource() {
        return source;
    }

    public String getServiceName() {
        return serviceName;
    }

    public String getEnvironment() {
        return environment;
    }

    public String getEventType() {
        return eventType;
    }

    public String getErrorType() {
        return errorType;
    }

    public String getErrorMessage() {
        return errorMessage;
    }

    public String getDependencyTarget() {
        return dependencyTarget;
    }

    public String getTraceId() {
        return traceId;
    }

    public String getSeverityHint() {
        return severityHint;
    }

    public Instant getOccurredAt() {
        return occurredAt;
    }

    public Instant getOccuredAt() {
        return occurredAt;
    }

    public Map<String, Object> getExtractedFields() {
        return extractedFields;
    }

    public Map<String, Object> getSourceMetadata() {
        return sourceMetaData;
    }

    public List<String> getParsingWarnings() {
        return parsingWarnings;
    }

    public static class Builder {
        private String source;
        private String serviceName;
        private String environment;
        private String eventType;
        private String errorType;
        private String errorMessage;
        private String dependencyTarget;
        private String traceId;
        private String severityHint;
        private Instant occurredAt;
        private Map<String, Object> extractedFields;
        private Map<String, Object> sourceMetaData;
        private List<String> parsingWarnings;
        private boolean malformed = false;

        public Builder malformed(boolean malformed) {
            this.malformed = malformed;
            return this;
        }

        public Builder source(String source) {
            this.source = source;
            return this;
        }

        public Builder serviceName(String serviceName) {
            this.serviceName = serviceName;
            return this;
        }

        public Builder environment(String environment) {
            this.environment = environment;
            return this;
        }

        public Builder eventType(String eventType) {
            this.eventType = eventType;
            return this;
        }

        public Builder errorType(String errorType) {
            this.errorType = errorType;
            return this;
        }

        public Builder errorMessage(String errorMessage) {
            this.errorMessage = errorMessage;
            return this;
        }

        public Builder dependencyTarget(String dependencyTarget) {
            this.dependencyTarget = dependencyTarget;
            return this;
        }

        public Builder traceId(String traceId) {
            this.traceId = traceId;
            return this;
        }

        public Builder severityHint(String severityHint) {
            this.severityHint = severityHint;
            return this;
        }

        public Builder occurredAt(Instant occurredAt) {
            this.occurredAt = occurredAt;
            return this;
        }

        public Builder extractedFields(Map<String, Object> extractedFields) {
            this.extractedFields = extractedFields;
            return this;
        }

        public Builder sourceMetadata(Map<String, Object> sourceMetadata) {
            this.sourceMetaData = sourceMetadata;
            return this;
        }

        public Builder parsingWarnings(List<String> parsingWarnings) {
            this.parsingWarnings = parsingWarnings;
            return this;
        }

        public ParsedFailureEvent build() {
            return new ParsedFailureEvent(this);
        }
    }
}
