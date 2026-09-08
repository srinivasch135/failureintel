package com.failureintel.infrastructure.persistence.failureevent.entity;

import org.junit.jupiter.api.Test;

import java.time.Instant;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

class FailureEventEntityTest {

    @Test
    void shouldApplyReceivedAndZeroAttemptDefaultsBeforePersist() {
        FailureEventEntity entity = new FailureEventEntity();

        entity.OnCreate();

        assertNotNull(entity.getIngestedAt());
        assertEquals(ProcessingStatus.RECEIVED, entity.getProcessingStatus());
        assertEquals(0, entity.getAttemptCount());
    }

    @Test
    void shouldPreserveExplicitLifecycleValuesBeforePersist() {
        Instant ingestedAt = Instant.parse("2026-09-04T12:00:00Z");
        FailureEventEntity entity = new FailureEventEntity();
        entity.setIngestedAt(ingestedAt);
        entity.setProcessingStatus(ProcessingStatus.PROCESSING);
        entity.setAttemptCount(2);

        entity.OnCreate();

        assertEquals(ingestedAt, entity.getIngestedAt());
        assertEquals(ProcessingStatus.PROCESSING, entity.getProcessingStatus());
        assertEquals(2, entity.getAttemptCount());
    }
}
