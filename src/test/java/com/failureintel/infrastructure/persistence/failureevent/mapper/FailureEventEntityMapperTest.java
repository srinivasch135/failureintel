package com.failureintel.infrastructure.persistence.failureevent.mapper;

import com.failureintel.ingestion.domain.model.RawFailureEvent;
import com.failureintel.infrastructure.persistence.failureevent.entity.FailureEventEntity;
import com.failureintel.infrastructure.persistence.failureevent.entity.ProcessingStatus;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static com.failureintel.test.support.FailureEventTestFixtures.OCCURRED_AT;
import static com.failureintel.test.support.FailureEventTestFixtures.RECEIVED_AT;
import static com.failureintel.test.support.FailureEventTestFixtures.rawEvent;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class FailureEventEntityMapperTest {

    @Test
    void shouldPreserveRawIdentityAndReceivedValues() {
        Map<String, Object> sourceMetadata = Map.of(
                "region", "us-east-1",
                "collector", "datadog");
        RawFailureEvent rawEvent = rawEvent(
                "Payment-Service",
                "PRODUCTION",
                "ERROR",
                "postgres_exception",
                "Connection timeout",
                "payment-database",
                "trace-raw-001",
                "SEV2",
                OCCURRED_AT,
                Map.of("environment", "payload-environment"),
                sourceMetadata);

        FailureEventEntity entity = FailureEventEntityMapper.fromRaw(rawEvent);

        assertEquals(rawEvent.getRawEventId(), entity.getEventId());
        assertEquals("test-source", entity.getSourceSystem());
        assertEquals("Payment-Service", entity.getServiceName());
        assertEquals("PRODUCTION", entity.getEnvironment());
        assertEquals("ERROR", entity.getEventType());
        assertEquals("postgres_exception", entity.getErrorType());
        assertEquals("Connection timeout", entity.getMessage());
        assertEquals("payment-database", entity.getDependencyTarget());
        assertEquals("trace-raw-001", entity.getTraceId());
        assertEquals("SEV2", entity.getSeverityHint());
        assertEquals(OCCURRED_AT, entity.getOccurredAt());
        assertEquals(RECEIVED_AT, entity.getIngestedAt());
        assertTrue(entity.getRawPayload().contains("payload-environment"));
        assertEquals(sourceMetadata, entity.getSourceMetadata());
        assertEquals(ProcessingStatus.RECEIVED, entity.getProcessingStatus());
    }

    @Test
    void shouldRoundTripRawFailureEventWithoutChangingValues() {
        Map<String, Object> payload = Map.of(
                "Environment", "Payload-Environment",
                "nested", Map.of("attempt", 3));
        Map<String, Object> metadata = Map.of(
                "collector", "datadog",
                "region", "us-east-1");
        RawFailureEvent original = rawEvent(
                "Payment-Service",
                "PRODUCTION",
                "ERROR",
                "postgres_exception",
                "Connection timeout",
                "payment-database",
                "trace-raw-roundtrip-001",
                "SEV2",
                OCCURRED_AT,
                payload,
                metadata);

        FailureEventEntity entity = FailureEventEntityMapper.fromRaw(original);
        RawFailureEvent reconstructed = FailureEventEntityMapper.toRaw(entity);

        assertEquals(original.getRawEventId(), reconstructed.getRawEventId());
        assertEquals(original.getSourceSystem(), reconstructed.getSourceSystem());
        assertEquals(original.getServiceName(), reconstructed.getServiceName());
        assertEquals(original.getEnvironment(), reconstructed.getEnvironment());
        assertEquals(original.getEventType(), reconstructed.getEventType());
        assertEquals(original.getErrorType(), reconstructed.getErrorType());
        assertEquals(original.getErrorMessage(), reconstructed.getErrorMessage());
        assertEquals(original.getDependencyTarget(), reconstructed.getDependencyTarget());
        assertEquals(original.getTraceId(), reconstructed.getTraceId());
        assertEquals(original.getSeverityHint(), reconstructed.getSeverityHint());
        assertEquals(original.getOccurredAt(), reconstructed.getOccurredAt());
        assertEquals(original.getReceivedAt(), reconstructed.getReceivedAt());
        assertEquals(original.getRawPayload(), reconstructed.getRawPayload());
        assertEquals(original.getMetaData(), reconstructed.getMetaData());
    }

    @Test
    void shouldUseTheSameRawMappingForFailedEvents() {
        RawFailureEvent rawEvent = rawEvent(
                "Payment-Service",
                "PRODUCTION",
                "ERROR",
                null,
                "Connection timeout",
                null,
                "trace-raw-failed-001",
                null,
                OCCURRED_AT,
                Map.of(),
                Map.of());

        FailureEventEntity entity = FailureEventEntityMapper.failedFromRaw(rawEvent, "unsupported");

        assertEquals(rawEvent.getRawEventId(), entity.getEventId());
        assertEquals("test-source", entity.getSourceSystem());
        assertEquals("Payment-Service", entity.getServiceName());
        assertEquals("PRODUCTION", entity.getEnvironment());
        assertEquals("trace-raw-failed-001", entity.getTraceId());
        assertEquals("unsupported", entity.getFailureReason());
        assertEquals(ProcessingStatus.FAILED, entity.getProcessingStatus());
    }
}
