package com.failureintel.ingestion.domain.normalization;

import com.failureintel.ingestion.domain.model.NormalizedFailureEvent;
import com.failureintel.ingestion.domain.model.ParsedFailureEvent;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ShouldPreserveNormalizationMetadataTest {

    private FailureEventNormalizer failureEventNormalizer;

    @BeforeEach
    void setUp() {
        failureEventNormalizer = new FailureEventNormalizer();
    }

    @Test
    void shouldPreserveNormalizationMetadata() {
        ParsedFailureEvent parsedEvent =
                ParsedFailureEvent.builder()
                        .serviceName("payment-service")
                        .environment("prod")
                        .eventType("timeout")
                        .errorType("PSQLException")
                        .errorMessage("Request failed token=abc123")
                        .severityHint("extreme")
                        .occurredAt(Instant.parse("2026-07-29T20:00:00Z"))
                        .malformed(false)
                        .build();

        NormalizedFailureEvent result = failureEventNormalizer.normalize(parsedEvent);
        Map<String, Object> metadata = result.getNormalizationMetadata();

        assertEquals("extreme", metadata.get("unknownSeverityValue"));
        assertTrue(Boolean.TRUE.equals(metadata.get("errorMessageSanitized")));
        assertEquals(NormalizationStatus.PARTIALLY_NORMALIZED, result.getNormalizationStatus());
    }
}
