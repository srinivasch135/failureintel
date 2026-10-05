package com.failureintel.infrastructure.monitoring.metrics;

import com.failureintel.infrastructure.persistence.failureevent.entity.ProcessingStatus;
import com.failureintel.infrastructure.persistence.failureevent.repository.FailureEventRepository;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

class FailureEventMetricsTest {

    private static final String PREFIX = "failureintel.events.";

    private SimpleMeterRegistry registry;
    private FailureEventRepository failureEventRepository;
    private FailureEventMetrics metrics;

    @BeforeEach
    void setUp() {
        registry = new SimpleMeterRegistry();
        failureEventRepository = mock(FailureEventRepository.class);
        metrics = new FailureEventMetrics(registry, failureEventRepository);
    }

    @AfterEach
    void tearDown() {
        registry.close();
    }

    @Test
    void shouldRegisterExpectedNamesAndInstrumentTypes() {
        assertTrue(registry.find(PREFIX + "raw.accepted").counter() != null);
        assertTrue(registry.find(PREFIX + "capture.failures").counter() != null);
        assertTrue(registry.find(PREFIX + "processing.duration").timer() != null);
        assertTrue(registry.find(PREFIX + "normalized").counter() != null);
        assertTrue(registry.find(PREFIX + "backlog").tag("status", "received").gauge() != null);
        assertTrue(registry.find(PREFIX + "backlog").tag("status", "processing").gauge() != null);
        assertTrue(registry.find(PREFIX + "backlog").tag("status", "retryable").gauge() != null);
        assertTrue(registry.find(PREFIX + "backlog.oldest.age").gauge() != null);
        assertTrue(registry.find(PREFIX + "normalization.success.ratio").gauge() != null);

        for (String source : Set.of("processing", "recovery")) {
            assertTrue(registry.find(PREFIX + "retry.scheduled").tag("source", source).counter() != null);
        }
        for (String source : Set.of("processing", "claim", "recovery")) {
            assertTrue(registry.find(PREFIX + "failed").tag("source", source).counter() != null);
        }
        for (String result : Set.of("retryable", "failed")) {
            assertTrue(registry.find(PREFIX + "lease.recovered").tag("result", result).counter() != null);
        }

        Set<String> names = registry.getMeters().stream()
                .map(Meter::getId)
                .map(Meter.Id::getName)
                .collect(java.util.stream.Collectors.toSet());
        assertEquals(Set.of(
                PREFIX + "raw.accepted",
                PREFIX + "capture.failures",
                PREFIX + "backlog",
                PREFIX + "backlog.oldest.age",
                PREFIX + "processing.duration",
                PREFIX + "normalized",
                PREFIX + "retry.scheduled",
                PREFIX + "failed",
                PREFIX + "lease.recovered",
                PREFIX + "normalization.success.ratio"), names);

        for (Meter meter : registry.getMeters()) {
            for (Tag tag : meter.getId().getTags()) {
                assertTrue(Set.of("status", "source", "result").contains(tag.getKey()));
                assertFalse(tag.getValue().isBlank());
            }
        }
        verifyNoInteractions(failureEventRepository);
    }

    @Test
    void shouldKeepTagValuesWithinTheContract() {
        Set<String> backlogStatuses = registry.find(PREFIX + "backlog").meters().stream()
                .map(meter -> meter.getId().getTag("status"))
                .collect(java.util.stream.Collectors.toSet());
        Set<String> retrySources = registry.find(PREFIX + "retry.scheduled").meters().stream()
                .map(meter -> meter.getId().getTag("source"))
                .collect(java.util.stream.Collectors.toSet());
        Set<String> failureSources = registry.find(PREFIX + "failed").meters().stream()
                .map(meter -> meter.getId().getTag("source"))
                .collect(java.util.stream.Collectors.toSet());
        Set<String> leaseResults = registry.find(PREFIX + "lease.recovered").meters().stream()
                .map(meter -> meter.getId().getTag("result"))
                .collect(java.util.stream.Collectors.toSet());

        assertEquals(Set.of("received", "processing", "retryable"), backlogStatuses);
        assertEquals(Set.of("processing", "recovery"), retrySources);
        assertEquals(Set.of("processing", "claim", "recovery"), failureSources);
        assertEquals(Set.of("retryable", "failed"), leaseResults);
    }

    @Test
    void shouldDeriveSuccessRatioFromProcessingOutcomeCountersOnly() {
        assertTrue(metrics.normalizationSuccessRatio().isEmpty());
        assertTrue(Double.isNaN(registry.get(PREFIX + "normalization.success.ratio").gauge().value()));

        metrics.recordNormalized();
        metrics.recordNormalized();
        metrics.recordRetryScheduled(FailureEventMetrics.RetrySource.PROCESSING);
        metrics.recordRetryScheduled(FailureEventMetrics.RetrySource.RECOVERY);
        metrics.recordFailed(FailureEventMetrics.FailureSource.PROCESSING);
        metrics.recordFailed(FailureEventMetrics.FailureSource.CLAIM);
        metrics.recordFailed(FailureEventMetrics.FailureSource.RECOVERY);
        metrics.recordLeaseRecovered(FailureEventMetrics.LeaseRecoveryResult.RETRYABLE);
        metrics.recordLeaseRecovered(FailureEventMetrics.LeaseRecoveryResult.FAILED);

        assertEquals(0.5, metrics.normalizationSuccessRatio().orElseThrow());
        assertEquals(0.5, registry.get(PREFIX + "normalization.success.ratio").gauge().value());
    }

    @Test
    void shouldReadDatabaseBackedGaugeValuesFromRawEventRepository() {
        when(failureEventRepository.countByProcessingStatus(ProcessingStatus.RECEIVED)).thenReturn(4L);
        when(failureEventRepository.countByProcessingStatus(ProcessingStatus.PROCESSING)).thenReturn(2L);
        when(failureEventRepository.countByProcessingStatus(ProcessingStatus.RETRYABLE)).thenReturn(1L);
        when(failureEventRepository.findOldestUnfinishedEventAgeSeconds()).thenReturn(BigDecimal.valueOf(12.5));

        assertEquals(4.0, registry.get(PREFIX + "backlog").tag("status", "received").gauge().value());
        assertEquals(2.0, registry.get(PREFIX + "backlog").tag("status", "processing").gauge().value());
        assertEquals(1.0, registry.get(PREFIX + "backlog").tag("status", "retryable").gauge().value());
        assertEquals(12.5, registry.get(PREFIX + "backlog.oldest.age").gauge().value());

        verify(failureEventRepository).countByProcessingStatus(ProcessingStatus.RECEIVED);
        verify(failureEventRepository).countByProcessingStatus(ProcessingStatus.PROCESSING);
        verify(failureEventRepository).countByProcessingStatus(ProcessingStatus.RETRYABLE);
        verify(failureEventRepository).findOldestUnfinishedEventAgeSeconds();
    }

    @Test
    void shouldContainRegistryFailuresWhenRecordingMetrics() {
        FailingNormalizedCounterRegistry failingRegistry = new FailingNormalizedCounterRegistry();
        try {
            FailureEventMetrics metrics = new FailureEventMetrics(failingRegistry, failureEventRepository);

            assertDoesNotThrow(metrics::recordNormalized);
            assertDoesNotThrow(() -> metrics.recordFailed(FailureEventMetrics.FailureSource.PROCESSING));
        } finally {
            failingRegistry.close();
        }
    }

    private static final class FailingNormalizedCounterRegistry extends SimpleMeterRegistry {

        private final Counter failingCounter = mock(Counter.class);

        private FailingNormalizedCounterRegistry() {
            doThrow(new IllegalStateException("test registry failure"))
                    .when(failingCounter)
                    .increment();
        }

        @Override
        protected Counter newCounter(Meter.Id id) {
            if ((PREFIX + "normalized").equals(id.getName())) {
                return failingCounter;
            }
            return super.newCounter(id);
        }
    }
}
