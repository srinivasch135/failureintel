package com.failureintel.ingestion.domain.normalization;

import com.failureintel.ingestion.domain.model.NormalizedFailureEvent;
import com.failureintel.ingestion.domain.model.ParsedFailureEvent;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;

import static org.junit.jupiter.api.Assertions.assertEquals;

class ShouldDefaultMissingErrorTypeAndReturnPartialStatusTest {

    private FailureEventNormalizer failureEventNormalizer;

    @BeforeEach
    void setUp() {
        failureEventNormalizer = new FailureEventNormalizer();
    }

    @Test
    void shouldDefaultMissingErrorTypeAndReturnPartialStatus() {
        Instant fixedTime = Instant.parse("2026-07-29T20:00:00Z");
        Clock fixedClock = Clock.fixed(
                fixedTime,
                ZoneOffset.UTC);
        failureEventNormalizer = new FailureEventNormalizer(fixedClock);
        ParsedFailureEvent parsedEvent = ParsedFailureEvent.builder()
                .serviceName("payment-service")
                .environment("prod")
                .eventType("exception")
                .errorType(null)
                .errorMessage("Payment gateway timed out")
                .severityHint("high")
                .occurredAt(fixedTime)
                .malformed(false)
                .build();

        NormalizedFailureEvent result = failureEventNormalizer.normalize(parsedEvent);

        assertEquals("UNKNOWN_FAILURE", result.getErrorType());
        assertEquals(NormalizationStatus.PARTIALLY_NORMALIZED, result.getNormalizationStatus());
    }
}
