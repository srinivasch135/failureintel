package com.failureintel.ingestion.application.service;

import com.failureintel.ingestion.application.config.FailureEventWorkerProperties;
import com.failureintel.infrastructure.persistence.failureevent.entity.FailureEventEntity;
import com.failureintel.infrastructure.persistence.failureevent.repository.FailureEventRepository;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.time.Duration;
import java.time.Instant;
import java.util.List;

@Service
public class FailureEventRecoveryService {

    private final FailureEventRepository failureEventRepository;
    private final FailureEventRetryPolicy retryPolicy;
    private final Duration processingLeaseTimeout;

    public FailureEventRecoveryService(
            FailureEventRepository failureEventRepository,
            FailureEventRetryPolicy retryPolicy,
            FailureEventWorkerProperties workerProperties) {
        this.failureEventRepository = failureEventRepository;
        this.retryPolicy = retryPolicy;
        this.processingLeaseTimeout = workerProperties.processingLeaseTimeout();
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
                continue;
            }

            Instant nextAttemptAt = retryPolicy.nextAttemptAt(attemptCount)
                    .orElseThrow(() -> new IllegalStateException(
                            "A non-exhausted attempt must have a retry time"));
            event.recoverExpiredLease(
                    "WORKER_LEASE_EXPIRED",
                    "Worker did not complete the claimed processing attempt",
                    nextAttemptAt);
        }
    }
}
