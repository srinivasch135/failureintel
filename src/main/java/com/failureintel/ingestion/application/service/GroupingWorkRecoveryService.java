package com.failureintel.ingestion.application.service;

import com.failureintel.ingestion.application.config.GroupingWorkerProperties;
import com.failureintel.infrastructure.persistence.incidentgrouping.entity.GroupingWorkEntity;
import com.failureintel.infrastructure.persistence.incidentgrouping.repository.GroupingWorkRepository;
import com.failureintel.infrastructure.monitoring.metrics.GroupingWorkMetrics;
import com.failureintel.infrastructure.monitoring.metrics.GroupingWorkMetrics.RecoveryResult;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.List;

/** Recovers abandoned grouping claims in a transaction independent of normalization recovery. */
@Service
public class GroupingWorkRecoveryService {
    private final GroupingWorkRepository repository;
    private final GroupingRetryPolicy retryPolicy;
    private final Duration leaseTimeout;
    private final GroupingWorkMetrics groupingMetrics;

    public GroupingWorkRecoveryService(
            GroupingWorkRepository repository,
            GroupingRetryPolicy retryPolicy,
            GroupingWorkerProperties workerProperties,
            GroupingWorkMetrics groupingMetrics) {
        this.repository = repository;
        this.retryPolicy = retryPolicy;
        this.leaseTimeout = workerProperties.processingLeaseTimeout();
        this.groupingMetrics = groupingMetrics;
    }

    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public int recoverExpiredClaims(int batchSize) {
        if (batchSize <= 0) {
            throw new IllegalArgumentException("batchSize must be greater than zero");
        }

        LocalDateTime now = repository.currentDatabaseTime();
        List<GroupingWorkEntity> expired = repository.lockExpiredProcessingClaims(
                now.minus(leaseTimeout), batchSize);
        long exhaustedCount = expired.stream()
                .filter(work -> work.getAttemptCount() >= retryPolicy.maxAttempts()).count();
        for (GroupingWorkEntity work : expired) {
            int attempt = work.getAttemptCount();
            boolean exhausted = attempt >= retryPolicy.maxAttempts();
            LocalDateTime retryAt = exhausted ? null
                    : now.plus(retryPolicy.delayAfter(attempt));
            work.recoverExpiredLease(retryAt, exhausted);
        }
        groupingMetrics.recordLeaseRecovered(RecoveryResult.RETRYABLE, expired.size() - exhaustedCount);
        groupingMetrics.recordLeaseRecovered(RecoveryResult.FAILED, exhaustedCount);
        return expired.size();
    }
}
