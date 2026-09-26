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
    private final ThreadPoolTaskExecutor normalizationExecutor;

    public FailureEventProcessingWorker(
            FailureEventClaimService claimService,
            FailureEventProcessingService processingService,
            FailureEventWorkerProperties workerProperties,
            @Qualifier("failureEventNormalizationExecutor") ThreadPoolTaskExecutor normalizationExecutor) {
        this.claimService = claimService;
        this.processingService = processingService;
        this.workerProperties = workerProperties;
        this.normalizationExecutor = normalizationExecutor;
    }

    @Scheduled(fixedDelayString = "${failure-event.processing.worker.fixed-delay}")
    public void processNextBatch() {
        List<ClaimedFailureEvent> claims = claimService
                .claimNextEligibleForProcessing(workerProperties.batchSize());
        if (claims.isEmpty()) {
            return;
        }

        List<CompletableFuture<Void>> processingTasks = new ArrayList<>(claims.size());
        for (ClaimedFailureEvent claim : claims) {
            processingTasks.add(CompletableFuture.runAsync(
                    () -> processingService.process(claim),
                    normalizationExecutor));
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
                        claims.get(i).eventId(),
                        claims.get(i).attemptNumber(),
                        processingFailure);
            }
        }
    }
}
