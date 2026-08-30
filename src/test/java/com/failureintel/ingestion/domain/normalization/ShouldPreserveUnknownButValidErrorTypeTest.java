package com.failureintel.ingestion.domain.normalization;

import com.failureintel.ingestion.domain.model.NormalizedFailureEvent;
import com.failureintel.ingestion.domain.model.ParsedFailureEvent;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Instant;

import static org.junit.jupiter.api.Assertions.assertEquals;

class ShouldPreserveUnknownButValidErrorTypeTest {

    private FailureEventNormalizer failureEventNormalizer;

    @BeforeEach
    void setUp() {
        failureEventNormalizer = new FailureEventNormalizer();
    }

    @Test
    void shouldPreserveUnknownButValidErrorType() {
        ParsedFailureEvent parsedEvent = ParsedFailureEvent.builder()
                .serviceName("payment-service")
                .environment("prod")
                .eventType("exception")
                .errorType("PaymentGatewayTimeoutException")
                .errorMessage("Payment gateway timed out")
                .severityHint("high")
                .occurredAt(Instant.parse("2026-07-29T20:00:00Z"))
                .malformed(false)
                .build();

        NormalizedFailureEvent result = failureEventNormalizer.normalize(parsedEvent);

        assertEquals("PaymentGatewayTimeoutException", result.getErrorType());
        assertEquals(NormalizationStatus.FULLY_NORMALIZED, result.getNormalizationStatus());
    }
}
