package com.failureintel.ingestion.application.service;

import com.failureintel.ingestion.application.exception.FailureEventNotFoundException;
import com.failureintel.infrastructure.persistence.failureevent.entity.FailureEventEntity;
import com.failureintel.infrastructure.persistence.failureevent.entity.ProcessingStatus;
import com.failureintel.infrastructure.persistence.failureevent.repository.FailureEventRepository;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.Objects;
import java.util.Optional;

/** Persists the outcome of a failed processing attempt independently. */
@Service
public class FailureEventRetryStateRecorder {

    private final FailureEventRepository failureEventRepository;

    public FailureEventRetryStateRecorder(FailureEventRepository failureEventRepository) {
        this.failureEventRepository = failureEventRepository;
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
        return true;
    }
}
