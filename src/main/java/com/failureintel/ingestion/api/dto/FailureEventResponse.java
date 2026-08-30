package com.failureintel.ingestion.api.dto;

import java.time.Instant;
import java.util.UUID;

public record FailureEventResponse(
        UUID eventId,
        String traceId,
        Instant ingestedAt,
        String processingStatus,
        String serviceName,
        String environment,
        String eventType,
        String errorType,
        String message,
        String dependencyTarget,
        String severity,
        Instant occurredAt) {
}
