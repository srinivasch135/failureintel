package com.failureintel.ingestion.application.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.time.Duration;
import java.util.Objects;

@ConfigurationProperties(prefix = "failure-event.grouping.worker")
public record GroupingWorkerProperties(
        boolean enabled,
        Duration fixedDelay,
        Duration processingLeaseTimeout,
        int batchSize,
        int concurrency,
        Duration shutdownAwait) {

    public GroupingWorkerProperties {
        Objects.requireNonNull(fixedDelay, "fixedDelay must not be null");
        Objects.requireNonNull(processingLeaseTimeout, "processingLeaseTimeout must not be null");
        Objects.requireNonNull(shutdownAwait, "shutdownAwait must not be null");
        if (fixedDelay.isZero() || fixedDelay.isNegative()) {
            throw new IllegalArgumentException("fixedDelay must be greater than zero");
        }
        if (processingLeaseTimeout.isZero() || processingLeaseTimeout.isNegative()) {
            throw new IllegalArgumentException("processingLeaseTimeout must be greater than zero");
        }
        if (batchSize <= 0 || concurrency <= 0) {
            throw new IllegalArgumentException("batchSize and concurrency must be greater than zero");
        }
        if (shutdownAwait.isNegative()) {
            throw new IllegalArgumentException("shutdownAwait must not be negative");
        }
    }
}
