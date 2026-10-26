package com.failureintel.ingestion.application.service;

import com.failureintel.ingestion.application.config.GroupingWorkerProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import java.util.List;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicInteger;

public class GroupingWorkProcessingWorker {
    private static final Logger LOGGER = LoggerFactory.getLogger(GroupingWorkProcessingWorker.class);

    private final GroupingWorkClaimService claimService;
    private final GroupingWorkProcessor processor;
    private final GroupingWorkerProperties properties;
    private final ThreadPoolTaskExecutor executor;
    private final AtomicInteger inFlight = new AtomicInteger();

    public GroupingWorkProcessingWorker(
            GroupingWorkClaimService claimService,
            GroupingWorkProcessor processor,
            GroupingWorkerProperties properties,
            @Qualifier("groupingWorkExecutor") ThreadPoolTaskExecutor executor) {
        this.claimService = claimService;
        this.processor = processor;
        this.properties = properties;
        this.executor = executor;
    }

    @Scheduled(fixedDelayString = "${failure-event.grouping.worker.fixed-delay}")
    public synchronized void processNextBatch() {
        int capacity = properties.concurrency() - inFlight.get();
        int claimLimit = Math.min(properties.batchSize(), capacity);
        if (claimLimit <= 0) {
            return;
        }

        final List<ClaimedGroupingWork> claims;
        try {
            // The proxied REQUIRES_NEW claim call returns only after Transaction A commits.
            claims = claimService.claimNextEligibleForProcessing(claimLimit);
        } catch (RuntimeException claimFailure) {
            LOGGER.error("Grouping worker could not claim work: batchSize={}", claimLimit, claimFailure);
            return;
        }

        for (ClaimedGroupingWork claim : claims.stream().limit(claimLimit).toList()) {
            inFlight.incrementAndGet();
            try {
                executor.execute(() -> {
                    try {
                        processor.process(claim);
                    } catch (RuntimeException processingFailure) {
                        LOGGER.error("Grouping work processing failed: eventId={}, attempt={}",
                                claim.eventId(), claim.attemptNumber(), processingFailure);
                    } finally {
                        inFlight.decrementAndGet();
                    }
                });
            } catch (RejectedExecutionException executorUnavailable) {
                inFlight.decrementAndGet();
                LOGGER.warn("Grouping executor rejected committed claim; it remains durable for recovery: "
                                + "eventId={}, attempt={}",
                        claim.eventId(), claim.attemptNumber(), executorUnavailable);
                break;
            }
        }
    }
}
