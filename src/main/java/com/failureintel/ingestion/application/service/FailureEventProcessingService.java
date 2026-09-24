package com.failureintel.ingestion.application.service;

import com.failureintel.ingestion.application.exception.StaleFailureEventClaimException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.Optional;

/** Runs one claimed normalization attempt and records technical failures afterward. */
@Service
public class FailureEventProcessingService {

    private static final Logger LOGGER = LoggerFactory.getLogger(FailureEventProcessingService.class);

    private final FailureEventNormalizationProcessor normalizationProcessor;
    private final FailureEventRetryStateRecorder retryStateRecorder;
    private final FailureEventRetryPolicy retryPolicy;

    public FailureEventProcessingService(
            FailureEventNormalizationProcessor normalizationProcessor,
            FailureEventRetryStateRecorder retryStateRecorder,
            FailureEventRetryPolicy retryPolicy) {
        this.normalizationProcessor = normalizationProcessor;
        this.retryStateRecorder = retryStateRecorder;
        this.retryPolicy = retryPolicy;
    }

    /**
     * Invokes the transactional processor through its Spring proxy. No
     * transaction spans processing and retry-state recording.
     */
    public void process(ClaimedFailureEvent claim) {
        try {
            normalizationProcessor.process(claim);
        } catch (StaleFailureEventClaimException staleClaim) {
            LOGGER.debug(
                    "Skipping stale failure-event claim: eventId={}, attempt={}",
                    claim.eventId(),
                    claim.attemptNumber());
        } catch (RuntimeException processingFailure) {
            FailureEventRetryableFailure failure =
                    FailureEventRetryableFailure.classify(processingFailure);
            Optional<Instant> nextAttemptAt = retryPolicy.nextAttemptAt(claim.attemptNumber());
            boolean recorded = retryStateRecorder.recordFailure(claim, failure, nextAttemptAt);

            if (recorded) {
                LOGGER.warn(
                        "Failure-event processing attempt failed: eventId={}, attempt={}, failureCode={}, reason={}",
                        claim.eventId(),
                        claim.attemptNumber(),
                        failure.getCode(),
                        failure.getReason(),
                        processingFailure);
            } else {
                LOGGER.debug(
                        "Failure-event attempt failure was not recorded because the claim is no longer current: "
                                + "eventId={}, attempt={}, failureCode={}",
                        claim.eventId(),
                        claim.attemptNumber(),
                        failure.getCode());
            }
        }
    }
}
