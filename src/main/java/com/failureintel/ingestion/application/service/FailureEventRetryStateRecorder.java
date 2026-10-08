package com.failureintel.ingestion.application.service;

import com.failureintel.ingestion.application.exception.FailureEventNotFoundException;
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
import java.util.Objects;
import java.util.Optional;

/** Persists the outcome of a failed processing attempt independently. */
@Service
public class FailureEventRetryStateRecorder {

    private static final Logger LOGGER = LoggerFactory.getLogger(FailureEventRetryStateRecorder.class);

    private final FailureEventRepository failureEventRepository;
    private final FailureEventMetrics failureEventMetrics;

    public FailureEventRetryStateRecorder(
            FailureEventRepository failureEventRepository,
            FailureEventMetrics failureEventMetrics) {
        this.failureEventRepository = failureEventRepository;
        this.failureEventMetrics = failureEventMetrics;
    }

    /**
     * Records a failure only while this claim still owns the processing attempt.
     * The transaction commits before this method returns to its caller.
     */
    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public boolean recordFailure(
            ClaimedFailureEvent claim,
            FailureEventRetryableFailure failure,
            Optional<Instant> nextAttemptAt) {
        Objects.requireNonNull(claim, "claim must not be null");
        Objects.requireNonNull(failure, "failure must not be null");
        Objects.requireNonNull(nextAttemptAt, "nextAttemptAt must not be null");

        FailureEventEntity event = failureEventRepository.findById(claim.eventId())
                .orElseThrow(() -> new FailureEventNotFoundException(claim.eventId()));

        if (event.getProcessingStatus() != ProcessingStatus.PROCESSING
                || !Objects.equals(event.getAttemptCount(), claim.attemptNumber())) {
            return false;
        }

        if (nextAttemptAt.isPresent()) {
            event.markRetryable(
                    failure.getCode(),
                    failure.getReason(),
                    nextAttemptAt.get());
        } else {
            event.markRetryExhausted(failure.getCode(), failure.getReason());
        }

        // save() participates in this service transaction; do not use the
        // repository's saveAndFlush(), which is explicitly REQUIRES_NEW.
        failureEventRepository.save(event);
        Runnable metricRecording = nextAttemptAt.isPresent()
                ? () -> failureEventMetrics.recordRetryScheduled(FailureEventMetrics.RetrySource.PROCESSING)
                : () -> failureEventMetrics.recordFailed(FailureEventMetrics.FailureSource.PROCESSING);
        recordAfterCommit(metricRecording);
        return true;
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
            LOGGER.warn("Unable to register committed retry outcome metric", metricsFailure);
        }
    }
}
