package com.failureintel.ingestion.application.service;

import com.failureintel.ingestion.application.config.GroupingWorkerProperties;
import com.failureintel.infrastructure.persistence.incidentgrouping.entity.GroupingWorkEntity;
import com.failureintel.infrastructure.persistence.incidentgrouping.repository.GroupingWorkRepository;
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

    public GroupingWorkRecoveryService(
            GroupingWorkRepository repository,
            GroupingRetryPolicy retryPolicy,
            GroupingWorkerProperties workerProperties) {
        this.repository = repository;
        this.retryPolicy = retryPolicy;
        this.leaseTimeout = workerProperties.processingLeaseTimeout();
    }

    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public int recoverExpiredClaims(int batchSize) {
        if (batchSize <= 0) {
            throw new IllegalArgumentException("batchSize must be greater than zero");
        }

        LocalDateTime now = repository.currentDatabaseTime();
        List<GroupingWorkEntity> expired = repository.lockExpiredProcessingClaims(
                now.minus(leaseTimeout), batchSize);
        for (GroupingWorkEntity work : expired) {
            int attempt = work.getAttemptCount();
            boolean exhausted = attempt >= retryPolicy.maxAttempts();
            LocalDateTime retryAt = exhausted ? null
                    : now.plus(retryPolicy.delayAfter(attempt));
            work.recoverExpiredLease(retryAt, exhausted);
        }
        return expired.size();
    }
}
