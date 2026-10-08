package com.failureintel.ingestion.application.service;

import com.failureintel.ingestion.application.config.FailureEventWorkerProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.RejectedExecutionException;

@Component
@ConditionalOnProperty(
        prefix = "failure-event.processing.worker",
        name = "enabled",
        havingValue = "true")
public class FailureEventProcessingWorker {

    private static final Logger LOGGER = LoggerFactory.getLogger(FailureEventProcessingWorker.class);

    private final FailureEventRecoveryService recoveryService;
    private final FailureEventClaimService claimService;
    private final FailureEventProcessingService processingService;
    private final FailureEventWorkerProperties workerProperties;
    private final ThreadPoolTaskExecutor normalizationExecutor;

    public FailureEventProcessingWorker(
            FailureEventRecoveryService recoveryService,
            FailureEventClaimService claimService,
            FailureEventProcessingService processingService,
            FailureEventWorkerProperties workerProperties,
            @Qualifier("failureEventNormalizationExecutor") ThreadPoolTaskExecutor normalizationExecutor) {
        this.recoveryService = recoveryService;
        this.claimService = claimService;
        this.processingService = processingService;
        this.workerProperties = workerProperties;
        this.normalizationExecutor = normalizationExecutor;
    }

    @Scheduled(fixedDelayString = "${failure-event.processing.worker.fixed-delay}")
    public void processNextBatch() {
        List<ClaimedFailureEvent> claims;
        try {
            claims = claimService.claimNextEligibleForProcessing(workerProperties.batchSize());
        } catch (RuntimeException claimFailure) {
            LOGGER.error(
                    "Failure-event worker could not claim a batch: batchSize={}",
                    workerProperties.batchSize(),
                    claimFailure);
            return;
        }

        if (claims.isEmpty()) {
            return;
        }

        List<CompletableFuture<Void>> processingTasks = new ArrayList<>(claims.size());
        List<ClaimedFailureEvent> submittedClaims = new ArrayList<>(claims.size());
        for (ClaimedFailureEvent claim : claims) {
            try {
                processingTasks.add(CompletableFuture.runAsync(
                        () -> processingService.process(claim),
                        normalizationExecutor));
                submittedClaims.add(claim);
            } catch (RejectedExecutionException executorClosing) {
                LOGGER.warn(
                        "Failure-event worker stopped submitting its claimed batch because the executor is closing; "
                                + "unsubmitted claims will be recovered after their processing lease expires: "
                                + "eventId={}, attempt={}, unsubmittedCount={}",
                        claim.eventId(),
                        claim.attemptNumber(),
                        claims.size() - submittedClaims.size(),
                        executorClosing);
                break;
            }
        }

        CompletableFuture.allOf(processingTasks.toArray(CompletableFuture<?>[]::new))
                .handle((ignored, batchFailure) -> null)
                .join();

        for (int i = 0; i < processingTasks.size(); i++) {
            try {
                processingTasks.get(i).join();
            } catch (CompletionException | CancellationException processingFailure) {
                LOGGER.error(
                        "Failure-event worker could not complete claim: eventId={}, attempt={}",
                        submittedClaims.get(i).eventId(),
                        submittedClaims.get(i).attemptNumber(),
                        processingFailure);
            }
        }
    }

    @Scheduled(
            initialDelayString = "${failure-event.processing.worker.recovery-scan-interval}",
            fixedDelayString = "${failure-event.processing.worker.recovery-scan-interval}")
    public void recoverExpiredClaims() {
        try {
            recoveryService.recoverExpiredClaims(workerProperties.batchSize());
        } catch (RuntimeException recoveryFailure) {
            LOGGER.error(
                    "Failure-event worker could not recover expired claims: batchSize={}",
                    workerProperties.batchSize(),
                    recoveryFailure);
        }
    }
}
