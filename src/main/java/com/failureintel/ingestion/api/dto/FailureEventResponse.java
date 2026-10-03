package com.failureintel.ingestion.api.dto;

import com.failureintel.ingestion.domain.normalization.NormalizationStatus;

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
        Instant occurredAt,
        Integer attemptCount,
        Instant lastAttemptAt,
        Instant nextAttemptAt,
        String failureCode,
        String failureReason,
        boolean normalizedAvailable,
        NormalizationStatus normalizationStatus,
        Instant normalizedAt) {
}
