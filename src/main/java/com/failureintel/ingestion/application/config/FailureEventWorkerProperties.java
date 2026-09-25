package com.failureintel.ingestion.application.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.time.Duration;
import java.util.Objects;

/** Settings for scheduling and bounding asynchronous failure-event processing. */
@ConfigurationProperties(prefix = "failure-event.processing.worker")
public record FailureEventWorkerProperties(
        boolean enabled,
        Duration fixedDelay,
        // The worker executor uses this value as its bounded queue capacity,
        // ensuring a complete claimed batch can be submitted.
        int batchSize,
        int concurrency,
        Duration shutdownAwait) {

    public FailureEventWorkerProperties {
        Objects.requireNonNull(fixedDelay, "fixedDelay must not be null");
        Objects.requireNonNull(shutdownAwait, "shutdownAwait must not be null");

        if (fixedDelay.isZero() || fixedDelay.isNegative()) {
            throw new IllegalArgumentException("fixedDelay must be greater than zero");
        }
        if (batchSize <= 0) {
            throw new IllegalArgumentException("batchSize must be greater than zero");
        }
        if (concurrency <= 0) {
            throw new IllegalArgumentException("concurrency must be greater than zero");
        }
        if (shutdownAwait.isNegative()) {
            throw new IllegalArgumentException("shutdownAwait must not be negative");
        }
    }
}
