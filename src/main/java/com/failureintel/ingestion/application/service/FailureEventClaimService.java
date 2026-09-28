package com.failureintel.ingestion.application.service;

import com.failureintel.ingestion.application.config.FailureEventWorkerProperties;
import com.failureintel.infrastructure.persistence.failureevent.entity.FailureEventEntity;
import com.failureintel.infrastructure.persistence.failureevent.entity.ProcessingStatus;
import com.failureintel.infrastructure.persistence.failureevent.repository.FailureEventRepository;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

@Service
public class FailureEventClaimService {

    private final FailureEventRepository failureEventRepository;
    private final FailureEventRetryPolicy retryPolicy;
    private final Duration processingLeaseTimeout;

    public FailureEventClaimService(
            FailureEventRepository failureEventRepository,
            FailureEventRetryPolicy retryPolicy,
            FailureEventWorkerProperties workerProperties) {
        this.failureEventRepository = failureEventRepository;
        this.retryPolicy = retryPolicy;
        Duration processingLeaseTimeout = workerProperties.processingLeaseTimeout();
        if (processingLeaseTimeout.isZero() || processingLeaseTimeout.isNegative()) {
            throw new IllegalArgumentException("processingLeaseTimeout must be greater than zero");
        }
        this.processingLeaseTimeout = processingLeaseTimeout;
    }

    /**
     * Claims a bounded batch and commits the claim before returning its claim
     * identities.
     * Parsing and normalization must happen after this transaction has ended.
     */
    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public List<ClaimedFailureEvent> claimNextEligibleForProcessing(int batchSize) {
        if (batchSize <= 0) {
            throw new IllegalArgumentException("batchSize must be greater than zero");
        }

        Instant claimedAt = Instant.now();
        int maxAttempts = retryPolicy.getMaxAttempts();
        recoverExpiredClaims(claimedAt, batchSize, maxAttempts);

        List<FailureEventEntity> candidates = failureEventRepository
                .lockNextEligibleForProcessing(claimedAt, batchSize, maxAttempts);

        List<ClaimedFailureEvent> claims = new ArrayList<>(candidates.size());
        for (FailureEventEntity event : candidates) {
            if (event.getProcessingStatus() == ProcessingStatus.RETRYABLE
                    && event.getAttemptCount() >= maxAttempts) {
                event.markRetryExhausted(event.getFailureCode(), event.getFailureReason());
                continue;
            }

            event.claimForProcessing(claimedAt);
            claims.add(new ClaimedFailureEvent(event.getEventId(), event.getAttemptCount()));
        }

        return List.copyOf(claims);
    }

    private void recoverExpiredClaims(Instant now, int batchSize, int maxAttempts) {
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
