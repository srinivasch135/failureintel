package com.failureintel.ingestion.application.service;

import com.failureintel.ingestion.application.config.FailureEventWorkerProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
@ConditionalOnProperty(
        prefix = "failure-event.processing.worker",
        name = "enabled",
        havingValue = "true")
public class FailureEventProcessingWorker {

    private static final Logger LOGGER = LoggerFactory.getLogger(FailureEventProcessingWorker.class);

    private final FailureEventClaimService claimService;
    private final FailureEventProcessingService processingService;
    private final FailureEventWorkerProperties workerProperties;

    public FailureEventProcessingWorker(
            FailureEventClaimService claimService,
            FailureEventProcessingService processingService,
            FailureEventWorkerProperties workerProperties) {
        this.claimService = claimService;
        this.processingService = processingService;
        this.workerProperties = workerProperties;
    }

    @Scheduled(fixedDelayString = "${failure-event.processing.worker.fixed-delay}")
    public void processNextBatch() {
        List<ClaimedFailureEvent> claims = claimService
                .claimNextEligibleForProcessing(workerProperties.batchSize());

        for (ClaimedFailureEvent claim : claims) {
            try {
                processingService.process(claim);
            } catch (RuntimeException processingFailure) {
                LOGGER.error(
                        "Failure-event worker could not complete claim: eventId={}, attempt={}",
                        claim.eventId(),
                        claim.attemptNumber(),
                        processingFailure);
            }
        }
    }
}
