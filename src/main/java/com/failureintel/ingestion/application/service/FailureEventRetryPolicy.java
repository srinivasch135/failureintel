package com.failureintel.ingestion.application.service;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.DoubleSupplier;

/** Calculates bounded retry times for already-claimed processing attempts. */
@Component
public class FailureEventRetryPolicy {
    private final int maxAttempts;
    private final List<Duration> retryDelays;
    private final double jitterFraction;
    private final Clock clock;
    private final DoubleSupplier randomValue;

    public FailureEventRetryPolicy(
            @Value("${failure-event.processing.retry.max-attempts:5}") int maxAttempts,
            @Value("${failure-event.processing.retry.delays:PT1M,PT5M,PT30M,PT2H}") String retryDelays,
            @Value("${failure-event.processing.retry.jitter-fraction:0.2}") double jitterFraction) {
        this(
                maxAttempts,
                parseRetryDelays(retryDelays),
                jitterFraction,
                Clock.systemUTC(),
                () -> ThreadLocalRandom.current().nextDouble());
    }

    FailureEventRetryPolicy(
            int maxAttempts,
            List<Duration> retryDelays,
            double jitterFraction,
            Clock clock,
            DoubleSupplier randomValue) {
        if (maxAttempts <= 0) {
            throw new IllegalArgumentException("maxAttempts must be greater than zero");
        }
        if (retryDelays == null || retryDelays.isEmpty()) {
            throw new IllegalArgumentException("retryDelays must contain at least one delay");
        }
        if (retryDelays.stream().anyMatch(
                delay -> delay == null || delay.isNegative() || delay.toMillis() <= 0)) {
            throw new IllegalArgumentException("retryDelays must contain durations of at least one millisecond");
        }
        if (!Double.isFinite(jitterFraction) || jitterFraction < 0 || jitterFraction >= 1) {
            throw new IllegalArgumentException("jitterFraction must be between zero and one");
        }

        this.maxAttempts = maxAttempts;
        this.retryDelays = List.copyOf(retryDelays);
        this.jitterFraction = jitterFraction;
        this.clock = Objects.requireNonNull(clock, "clock must not be null");
        this.randomValue = Objects.requireNonNull(randomValue, "randomValue must not be null");
    }

    /**
     * Returns empty when the failed attempt consumed the configured attempt
     * limit; otherwise returns the scheduled time for the next attempt.
     */
    public Optional<Instant> nextAttemptAt(int failedAttemptNumber) {
        if (failedAttemptNumber <= 0 || failedAttemptNumber > maxAttempts) {
            throw new IllegalArgumentException("failedAttemptNumber must be between 1 and maxAttempts");
        }
        if (failedAttemptNumber == maxAttempts) {
            return Optional.empty();
        }

        Duration baseDelay = retryDelays.get(Math.min(failedAttemptNumber - 1, retryDelays.size() - 1));
        double random = randomValue.getAsDouble();
        if (!Double.isFinite(random) || random < 0 || random >= 1) {
            throw new IllegalStateException("random value must be in the range [0, 1)");
        }

        long jitterMillis = Math.round(
                baseDelay.toMillis() * jitterFraction * ((2 * random) - 1));
        Duration actualDelay = baseDelay.plusMillis(jitterMillis);
        return Optional.of(clock.instant().plus(actualDelay));
    }

    public int getMaxAttempts() {
        return maxAttempts;
    }

    private static List<Duration> parseRetryDelays(String retryDelays) {
        Objects.requireNonNull(retryDelays, "retryDelays must not be null");
        return Arrays.stream(retryDelays.split(","))
                .map(String::trim)
                .map(Duration::parse)
                .toList();
    }
}
