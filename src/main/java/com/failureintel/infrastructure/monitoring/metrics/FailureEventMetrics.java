package com.failureintel.infrastructure.monitoring.metrics;

import com.failureintel.infrastructure.persistence.failureevent.entity.ProcessingStatus;
import com.failureintel.infrastructure.persistence.failureevent.repository.FailureEventRepository;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.EnumMap;
import java.util.Map;
import java.util.Objects;
import java.util.OptionalDouble;

@Component
public class FailureEventMetrics {

    private static final Logger LOGGER = LoggerFactory.getLogger(FailureEventMetrics.class);

    private static final String PREFIX = "failureintel.events.";
    private static final String RAW_ACCEPTED = PREFIX + "raw.accepted";
    private static final String CAPTURE_FAILURES = PREFIX + "capture.failures";
    private static final String BACKLOG = PREFIX + "backlog";
    private static final String OLDEST_BACKLOG_AGE = PREFIX + "backlog.oldest.age";
    private static final String PROCESSING_DURATION = PREFIX + "processing.duration";
    private static final String NORMALIZED = PREFIX + "normalized";
    private static final String RETRY_SCHEDULED = PREFIX + "retry.scheduled";
    private static final String FAILED = PREFIX + "failed";
    private static final String LEASE_RECOVERED = PREFIX + "lease.recovered";
    private static final String NORMALIZATION_SUCCESS_RATIO = PREFIX + "normalization.success.ratio";

    private final MeterRegistry meterRegistry;
    private final FailureEventRepository failureEventRepository;
    private final Counter rawAccepted;
    private final Counter captureFailures;
    private final Timer processingDuration;
    private final Counter normalized;
    private final Map<RetrySource, Counter> retryScheduled;
    private final Map<FailureSource, Counter> failed;
    private final Map<LeaseRecoveryResult, Counter> leaseRecovered;

    public FailureEventMetrics(MeterRegistry meterRegistry, FailureEventRepository failureEventRepository) {
        this.meterRegistry = Objects.requireNonNull(meterRegistry, "meterRegistry must not be null");
        this.failureEventRepository = Objects.requireNonNull(
                failureEventRepository, "failureEventRepository must not be null");

        rawAccepted = Counter.builder(RAW_ACCEPTED)
                .description("New raw failure-event rows committed during ingestion; existing idempotent rows excluded")
                .register(meterRegistry);
        captureFailures = Counter.builder(CAPTURE_FAILURES)
                .description("Unexpected raw capture failures; validation, conflicts, resolved races, and normalization excluded")
                .register(meterRegistry);
        processingDuration = Timer.builder(PROCESSING_DURATION)
                .description("Duration of one processing invocation through normalization and any retry-state recording")
                .register(meterRegistry);
        normalized = Counter.builder(NORMALIZED)
                .description("Committed normalized outcomes, including fully and partially normalized events")
                .register(meterRegistry);

        retryScheduled = new EnumMap<>(RetrySource.class);
        for (RetrySource source : RetrySource.values()) {
            retryScheduled.put(source, Counter.builder(RETRY_SCHEDULED)
                    .tag("source", source.tagValue)
                    .description("Committed transitions to RETRYABLE by processing or recovery source")
                    .register(meterRegistry));
        }

        failed = new EnumMap<>(FailureSource.class);
        for (FailureSource source : FailureSource.values()) {
            failed.put(source, Counter.builder(FAILED)
                    .tag("source", source.tagValue)
                    .description("Committed transitions to terminal FAILED by processing, claim, or recovery source")
                    .register(meterRegistry));
        }

        leaseRecovered = new EnumMap<>(LeaseRecoveryResult.class);
        for (LeaseRecoveryResult result : LeaseRecoveryResult.values()) {
            leaseRecovered.put(result, Counter.builder(LEASE_RECOVERED)
                    .tag("result", result.tagValue)
                    .description("Expired processing claims successfully transitioned during recovery")
                    .register(meterRegistry));
        }

        registerBacklogGauge(ProcessingStatus.RECEIVED, "received");
        registerBacklogGauge(ProcessingStatus.PROCESSING, "processing");
        registerBacklogGauge(ProcessingStatus.RETRYABLE, "retryable");

        Gauge.builder(OLDEST_BACKLOG_AGE, this, FailureEventMetrics::oldestBacklogAgeSeconds)
                .description("Database time minus ingested_at for the oldest unfinished failure event")
                .baseUnit("seconds")
                .register(meterRegistry);
        Gauge.builder(NORMALIZATION_SUCCESS_RATIO, this, FailureEventMetrics::normalizationSuccessRatioValue)
                .description("Committed normalized outcomes divided by processing-attempt outcomes")
                .register(meterRegistry);
    }

    public void recordRawEventAccepted() {
        recordSafely("raw acceptance", rawAccepted::increment);
    }

    public void recordCaptureFailure() {
        recordSafely("capture failure", captureFailures::increment);
    }

    public Timer.Sample startProcessingTimer() {
        try {
            return Timer.start(meterRegistry);
        } catch (RuntimeException metricsFailure) {
            LOGGER.warn("Unable to start failure-event processing timer", metricsFailure);
            return null;
        }
    }

    public void stopProcessingTimer(Timer.Sample sample) {
        if (sample == null) {
            return;
        }
        recordSafely("processing duration", () -> sample.stop(processingDuration));
    }

    public void recordNormalized() {
        recordSafely("normalized outcome", normalized::increment);
    }

    public void recordRetryScheduled(RetrySource source) {
        Counter counter = retryScheduled.get(Objects.requireNonNull(source, "source must not be null"));
        recordSafely("scheduled retry", counter::increment);
    }

    public void recordFailed(FailureSource source) {
        Counter counter = failed.get(Objects.requireNonNull(source, "source must not be null"));
        recordSafely("terminal failure", counter::increment);
    }

    public void recordLeaseRecovered(LeaseRecoveryResult result) {
        Counter counter = leaseRecovered.get(Objects.requireNonNull(result, "result must not be null"));
        recordSafely("lease recovery", counter::increment);
    }

    private void recordSafely(String operation, Runnable recording) {
        try {
            recording.run();
        } catch (RuntimeException metricsFailure) {
            LOGGER.warn("Unable to record failure-event {} metric", operation, metricsFailure);
        }
    }

    public OptionalDouble normalizationSuccessRatio() {
        double normalizedOutcomes = normalized.count();
        double processingRetries = retryScheduled.get(RetrySource.PROCESSING).count();
        double processingFailures = failed.get(FailureSource.PROCESSING).count();
        double processingAttemptOutcomes = normalizedOutcomes + processingRetries + processingFailures;

        return processingAttemptOutcomes == 0
                ? OptionalDouble.empty()
                : OptionalDouble.of(normalizedOutcomes / processingAttemptOutcomes);
    }

    private void registerBacklogGauge(ProcessingStatus status, String tagValue) {
        Gauge.builder(BACKLOG, this, metrics -> metrics.backlogCount(status))
                .tag("status", tagValue)
                .description("Current number of unfinished failure events by processing status")
                .baseUnit("events")
                .register(meterRegistry);
    }

    private double backlogCount(ProcessingStatus status) {
        return failureEventRepository.countByProcessingStatus(status);
    }

    private double oldestBacklogAgeSeconds() {
        var ageSeconds = failureEventRepository.findOldestUnfinishedEventAgeSeconds();
        return ageSeconds == null ? Double.NaN : ageSeconds.doubleValue();
    }

    private double normalizationSuccessRatioValue() {
        return normalizationSuccessRatio().orElse(Double.NaN);
    }

    public enum RetrySource {
        PROCESSING("processing"),
        RECOVERY("recovery");

        private final String tagValue;

        RetrySource(String tagValue) {
            this.tagValue = tagValue;
        }
    }

    public enum FailureSource {
        PROCESSING("processing"),
        CLAIM("claim"),
        RECOVERY("recovery");

        private final String tagValue;

        FailureSource(String tagValue) {
            this.tagValue = tagValue;
        }
    }

    public enum LeaseRecoveryResult {
        RETRYABLE("retryable"),
        FAILED("failed");

        private final String tagValue;

        LeaseRecoveryResult(String tagValue) {
            this.tagValue = tagValue;
        }
    }
}
