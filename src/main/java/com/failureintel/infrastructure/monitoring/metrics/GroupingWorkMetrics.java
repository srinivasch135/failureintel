package com.failureintel.infrastructure.monitoring.metrics;

import com.failureintel.ingestion.application.service.GroupingRetryPolicy;
import com.failureintel.ingestion.domain.grouping.IncidentGroupingEligibilityEvaluator.UnassignedReason;
import com.failureintel.infrastructure.persistence.incidentgrouping.repository.GroupingWorkRepository;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Counter;
import java.util.EnumMap;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;

import java.util.Locale;

/** Read-only gauges backed by the durable grouping work table. */
@Component
public class GroupingWorkMetrics {
    private static final Logger LOGGER = LoggerFactory.getLogger(GroupingWorkMetrics.class);
    private static final String PREFIX = "failureintel.grouping.work.";
    private final GroupingWorkRepository repository;
    private final int maxAttempts;
    private final Counter correctionsAccepted;
    private final Map<RecoveryResult, Counter> leaseRecovered = new EnumMap<>(RecoveryResult.class);

    public GroupingWorkMetrics(MeterRegistry registry, GroupingWorkRepository repository,
            GroupingRetryPolicy retryPolicy) {
        this.repository = repository;
        this.maxAttempts = retryPolicy.maxAttempts();
        correctionsAccepted = Counter.builder("failureintel.grouping.historical.correction.accepted")
                .description("Accepted historical corrections committed since process start")
                .register(registry);
        for (RecoveryResult result : RecoveryResult.values()) {
            leaseRecovered.put(result, Counter.builder("failureintel.grouping.lease.recovered")
                    .tag("result", result.tag)
                    .description("Expired grouping claims committed to retryable or failed since process start")
                    .register(registry));
        }
        for (String status : new String[] {"PENDING", "PROCESSING", "RETRYABLE", "COMPLETED", "FAILED"}) {
            register(registry, "state", status.toLowerCase(Locale.ROOT), () -> repository.countWorkByStatus(status));
        }
        register(registry, "unfinished", null, repository::countUnfinishedWork);
        register(registry, "claimable", null, () -> repository.countClaimableWork(maxAttempts));
        register(registry, "outcome", "associated", () -> repository.countCompletedOutcome("ASSOCIATED"));
        register(registry, "outcome", "unassigned", () -> repository.countCompletedOutcome("UNASSIGNED"));
        for (UnassignedReason reason : UnassignedReason.values()) {
            register(registry, "unassigned.reason", reason.name().toLowerCase(Locale.ROOT),
                    () -> repository.countUnassignedReason(reason.name()));
        }
        register(registry, "retry.exhausted", null, repository::countRetryExhausted);
        register(registry, "failure.permanent", null, repository::countPermanentFailures);
        register(registry, "historical.correction.expired", null, repository::countExpiredCorrections);
    }

    public void recordCorrectionAccepted() {
        afterCommit(correctionsAccepted);
    }

    public void recordLeaseRecovered(RecoveryResult result, long count) {
        if (count > 0) {
            afterCommit(leaseRecovered.get(result), count);
        }
    }

    private void afterCommit(Counter counter) {
        afterCommit(counter, 1);
    }

    private void afterCommit(Counter counter, long amount) {
        if (!TransactionSynchronizationManager.isSynchronizationActive()) {
            counter.increment(amount);
            return;
        }
        TransactionSynchronizationManager.registerSynchronization(new TransactionSynchronization() {
            @Override
            public void afterCommit() {
                counter.increment(amount);
            }
        });
    }

    private void register(MeterRegistry registry, String name, String tag, CountSupplier supplier) {
        Gauge.Builder<GroupingWorkMetrics> builder = Gauge.builder(PREFIX + name, this,
                ignored -> count(supplier));
        if (tag != null) {
            String key = switch (name) {
                case "state" -> "status";
                case "outcome" -> "outcome";
                default -> "reason";
            };
            builder.tag(key, tag);
        }
        builder.description("Current persisted grouping work count").baseUnit("work").register(registry);
    }

    private double count(CountSupplier supplier) {
        try {
            return supplier.get();
        } catch (RuntimeException failure) {
            LOGGER.warn("Unable to observe grouping work count; failureType={}",
                    failure.getClass().getSimpleName());
            return Double.NaN;
        }
    }

    @FunctionalInterface
    private interface CountSupplier { long get(); }

    public enum RecoveryResult {
        RETRYABLE("retryable"),
        FAILED("failed");

        private final String tag;

        RecoveryResult(String tag) { this.tag = tag; }
    }
}
