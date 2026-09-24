package com.failureintel.ingestion.application.service;

import com.failureintel.infrastructure.persistence.failureevent.entity.FailureEventEntity;
import com.failureintel.infrastructure.persistence.failureevent.entity.ProcessingStatus;
import com.failureintel.infrastructure.persistence.failureevent.repository.FailureEventRepository;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

@Service
public class FailureEventClaimService {

    private final FailureEventRepository failureEventRepository;
    private final FailureEventRetryPolicy retryPolicy;

    public FailureEventClaimService(
            FailureEventRepository failureEventRepository,
            FailureEventRetryPolicy retryPolicy) {
        this.failureEventRepository = failureEventRepository;
        this.retryPolicy = retryPolicy;
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
}
