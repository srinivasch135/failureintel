package com.failureintel.ingestion.application.service;

import com.failureintel.infrastructure.monitoring.metrics.FailureEventMetrics;
import com.failureintel.infrastructure.persistence.failureevent.entity.FailureEventEntity;
import com.failureintel.infrastructure.persistence.failureevent.entity.ProcessingStatus;
import com.failureintel.infrastructure.persistence.failureevent.repository.FailureEventRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

@Service
public class FailureEventClaimService {

    private static final Logger LOGGER = LoggerFactory.getLogger(FailureEventClaimService.class);

    private final FailureEventRepository failureEventRepository;
    private final FailureEventRetryPolicy retryPolicy;
    private final FailureEventMetrics failureEventMetrics;

    public FailureEventClaimService(
            FailureEventRepository failureEventRepository,
            FailureEventRetryPolicy retryPolicy,
            FailureEventMetrics failureEventMetrics) {
        this.failureEventRepository = failureEventRepository;
        this.retryPolicy = retryPolicy;
        this.failureEventMetrics = failureEventMetrics;
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

        Instant claimedAt = failureEventRepository.currentDatabaseTime();
        int maxAttempts = retryPolicy.getMaxAttempts();

        List<FailureEventEntity> candidates = failureEventRepository
                .lockNextEligibleForProcessing(claimedAt, batchSize, maxAttempts);

        List<ClaimedFailureEvent> claims = new ArrayList<>(candidates.size());
        for (FailureEventEntity event : candidates) {
            if (event.getProcessingStatus() == ProcessingStatus.RETRYABLE
                    && event.getAttemptCount() >= maxAttempts) {
                event.markRetryExhausted(event.getFailureCode(), event.getFailureReason());
                recordAfterCommit(() -> failureEventMetrics.recordFailed(
                        FailureEventMetrics.FailureSource.CLAIM));
                continue;
            }

            event.claimForProcessing(claimedAt);
            claims.add(new ClaimedFailureEvent(event.getEventId(), event.getAttemptCount()));
        }

        return List.copyOf(claims);
    }

    private void recordAfterCommit(Runnable metricRecording) {
        if (!TransactionSynchronizationManager.isSynchronizationActive()) {
            return;
        }
        try {
            TransactionSynchronizationManager.registerSynchronization(new TransactionSynchronization() {
                @Override
                public void afterCommit() {
                    metricRecording.run();
                }
            });
        } catch (RuntimeException metricsFailure) {
            LOGGER.warn("Unable to register committed claim failure metric", metricsFailure);
        }
    }
}
