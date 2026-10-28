package com.failureintel.ingestion.application.service;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/** Provides grouping-only attempt limits and bounded retry delays. */
@Component
public class GroupingRetryPolicy {
    private final int maxAttempts;
    private final List<Duration> delays;

    @Autowired
    public GroupingRetryPolicy(
            @Value("${failure-event.grouping.retry.max-attempts:5}") int maxAttempts,
            @Value("${failure-event.grouping.retry.delays:PT1M,PT5M,PT30M,PT2H}") String delays) {
        this(maxAttempts, Arrays.stream(Objects.requireNonNull(delays).split(","))
                .map(String::trim).map(Duration::parse).toList());
    }

    GroupingRetryPolicy(int maxAttempts, List<Duration> delays) {
        if (maxAttempts <= 0 || delays == null || delays.isEmpty()
                || delays.stream().anyMatch(delay -> delay == null || delay.isNegative() || delay.isZero())) {
            throw new IllegalArgumentException("grouping retry settings must be positive");
        }
        this.maxAttempts = maxAttempts;
        this.delays = List.copyOf(delays);
    }

    public int maxAttempts() {
        return maxAttempts;
    }

    public Duration delayAfter(int failedAttempt) {
        if (failedAttempt <= 0 || failedAttempt >= maxAttempts) {
            throw new IllegalArgumentException("failed attempt must have a remaining retry");
        }
        return delays.get(Math.min(failedAttempt - 1, delays.size() - 1));
    }
}
