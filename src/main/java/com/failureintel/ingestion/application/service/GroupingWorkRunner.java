package com.failureintel.ingestion.application.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

/** Runs completion attempts and records failures only after completion rolls back. */
@Service
public class GroupingWorkRunner implements GroupingWorkProcessor {
    private static final Logger LOGGER = LoggerFactory.getLogger(GroupingWorkRunner.class);
    private final GroupingWorkCompletionProcessor completionProcessor;
    private final GroupingRetryStateRecorder retryRecorder;
    private final GroupingFailureClassifier classifier;

    public GroupingWorkRunner(GroupingWorkCompletionProcessor completionProcessor,
            GroupingRetryStateRecorder retryRecorder, GroupingFailureClassifier classifier) {
        this.completionProcessor = completionProcessor;
        this.retryRecorder = retryRecorder;
        this.classifier = classifier;
    }

    @Override
    public void process(ClaimedGroupingWork claim) {
        try {
            completionProcessor.process(claim);
        } catch (RuntimeException failure) {
            GroupingFailureClassifier.Failure classified = classifier.classify(failure);
            boolean recorded = retryRecorder.record(claim, classified);
            if (recorded) {
                LOGGER.warn("Grouping attempt failed: eventId={}, attempt={}, failureCode={}",
                        claim.eventId(), claim.attemptNumber(), classified.code(), failure);
            } else {
                LOGGER.debug("Skipping failure from stale grouping claim: eventId={}, attempt={}",
                        claim.eventId(), claim.attemptNumber());
            }
        }
    }
}
