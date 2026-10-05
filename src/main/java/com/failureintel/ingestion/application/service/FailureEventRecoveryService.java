package com.failureintel.ingestion.application.service;

import com.failureintel.ingestion.application.config.FailureEventWorkerProperties;
import com.failureintel.infrastructure.monitoring.metrics.FailureEventMetrics;
import com.failureintel.infrastructure.persistence.failureevent.entity.FailureEventEntity;
import com.failureintel.infrastructure.persistence.failureevent.repository.FailureEventRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;

import java.time.Duration;
import java.time.Instant;
import java.util.List;

@Service
public class FailureEventRecoveryService {

    private static final Logger LOGGER = LoggerFactory.getLogger(FailureEventRecoveryService.class);

    private final FailureEventRepository failureEventRepository;
    private final FailureEventRetryPolicy retryPolicy;
    private final Duration processingLeaseTimeout;
    private final FailureEventMetrics failureEventMetrics;

    public FailureEventRecoveryService(
            FailureEventRepository failureEventRepository,
            FailureEventRetryPolicy retryPolicy,
            FailureEventWorkerProperties workerProperties,
            FailureEventMetrics failureEventMetrics) {
        this.failureEventRepository = failureEventRepository;
        this.retryPolicy = retryPolicy;
        this.processingLeaseTimeout = workerProperties.processingLeaseTimeout();
        this.failureEventMetrics = failureEventMetrics;
    }

    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public void recoverExpiredClaims(int batchSize) {
        if (batchSize <= 0) {
            throw new IllegalArgumentException("batchSize must be greater than zero");
        }

        Instant now = failureEventRepository.currentDatabaseTime();
        int maxAttempts = retryPolicy.getMaxAttempts();
        Instant expiredBefore = now.minus(processingLeaseTimeout);
        List<FailureEventEntity> expiredClaims = failureEventRepository
                .lockExpiredProcessingClaims(expiredBefore, batchSize);

        for (FailureEventEntity event : expiredClaims) {
            int attemptCount = event.getAttemptCount();
            if (attemptCount >= maxAttempts) {
                event.markRetryExhausted(
                        "WORKER_LEASE_EXPIRED",
                        "Worker did not complete the claimed processing attempt");
                recordRecoveryAfterCommit(FailureEventMetrics.LeaseRecoveryResult.FAILED);
                continue;
            }

            Instant nextAttemptAt = retryPolicy.nextAttemptAt(attemptCount)
                    .orElseThrow(() -> new IllegalStateException(
                            "A non-exhausted attempt must have a retry time"));
            event.recoverExpiredLease(
                    "WORKER_LEASE_EXPIRED",
                    "Worker did not complete the claimed processing attempt",
                    nextAttemptAt);
            recordRecoveryAfterCommit(FailureEventMetrics.LeaseRecoveryResult.RETRYABLE);
        }
    }

    private void recordRecoveryAfterCommit(FailureEventMetrics.LeaseRecoveryResult result) {
        if (!TransactionSynchronizationManager.isSynchronizationActive()) {
            return;
        }
        try {
            TransactionSynchronizationManager.registerSynchronization(new TransactionSynchronization() {
                @Override
                public void afterCommit() {
                    failureEventMetrics.recordLeaseRecovered(result);
                    if (result == FailureEventMetrics.LeaseRecoveryResult.RETRYABLE) {
                        failureEventMetrics.recordRetryScheduled(FailureEventMetrics.RetrySource.RECOVERY);
                    } else {
                        failureEventMetrics.recordFailed(FailureEventMetrics.FailureSource.RECOVERY);
                    }
                }
            });
        } catch (RuntimeException metricsFailure) {
            LOGGER.warn("Unable to register committed lease recovery metrics", metricsFailure);
        }
    }
}
