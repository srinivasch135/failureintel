package com.failureintel.ingestion.application.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.time.Duration;
import java.util.Objects;

@ConfigurationProperties(prefix = "failure-event.grouping.historical-correction")
public record HistoricalCorrectionProperties(Duration window) {
    public HistoricalCorrectionProperties {
        Objects.requireNonNull(window, "window must not be null");
        if (window.isZero() || window.isNegative()) {
            throw new IllegalArgumentException("window must be greater than zero");
        }
    }
}
