package com.failureintel.ingestion.application.service;

import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class FailureEventRetryPolicyTest {
    private static final List<Duration> APPROVED_DELAYS = List.of(
            Duration.ofMinutes(1),
            Duration.ofMinutes(5),
            Duration.ofMinutes(30),
            Duration.ofHours(2));

    @Test
    void shouldUseApprovedDelayForEachRetryAttemptWithoutJitter() {
        FailureEventRetryPolicy policy = policy(0.5);

        for (int attempt = 1; attempt <= APPROVED_DELAYS.size(); attempt++) {
            Instant beforeCalculation = Instant.now();
            Instant retryAt = policy.nextAttemptAt(attempt).orElseThrow();
            Instant afterCalculation = Instant.now();
            Duration delay = APPROVED_DELAYS.get(attempt - 1);

            assertFalse(retryAt.isBefore(beforeCalculation.plus(delay)));
            assertFalse(retryAt.isAfter(afterCalculation.plus(delay)));
        }
    }

    @Test
    void shouldApplyMinimumJitterAndKeepRetryTimeInTheFuture() {
        FailureEventRetryPolicy policy = policy(0.0);

        Instant beforeCalculation = Instant.now();
        Instant retryAt = policy.nextAttemptAt(1).orElseThrow();
        Instant afterCalculation = Instant.now();

        assertFalse(retryAt.isBefore(beforeCalculation.plusSeconds(48)));
        assertFalse(retryAt.isAfter(afterCalculation.plusSeconds(48)));
        assertTrue(retryAt.isAfter(afterCalculation));
    }

    @Test
    void shouldKeepMaximumJitterWithinApprovedTwentyPercentBound() {
        FailureEventRetryPolicy policy = policy(Math.nextDown(1.0));

        Instant beforeCalculation = Instant.now();
        Instant retryAt = policy.nextAttemptAt(1).orElseThrow();
        Instant afterCalculation = Instant.now();

        assertFalse(retryAt.isBefore(beforeCalculation.plusSeconds(48)));
        assertFalse(retryAt.isAfter(afterCalculation.plusSeconds(72)));
    }

    @Test
    void shouldStopSchedulingAfterTheMaximumClaim() {
        AtomicInteger randomCalls = new AtomicInteger();
        FailureEventRetryPolicy policy = policy(() -> {
            randomCalls.incrementAndGet();
            return 0.5;
        });

        assertEquals(Optional.empty(), policy.nextAttemptAt(5));
        assertEquals(0, randomCalls.get());
    }

    @Test
    void shouldRejectAttemptNumbersOutsideTheConfiguredRange() {
        FailureEventRetryPolicy policy = policy(0.5);

        assertThrows(IllegalArgumentException.class, () -> policy.nextAttemptAt(0));
        assertThrows(IllegalArgumentException.class, () -> policy.nextAttemptAt(6));
    }

    private FailureEventRetryPolicy policy(double random) {
        return policy(() -> random);
    }

    private FailureEventRetryPolicy policy(java.util.function.DoubleSupplier random) {
        return new FailureEventRetryPolicy(
                5,
                APPROVED_DELAYS,
                0.2,
                Clock.systemUTC(),
                random);
    }
}
