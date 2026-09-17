package com.failureintel.ingestion.application.service;

import com.failureintel.infrastructure.persistence.failureevent.entity.FailureEventEntity;
import com.failureintel.infrastructure.persistence.failureevent.repository.FailureEventRepository;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

@Service
public class FailureEventClaimService {

    private final FailureEventRepository failureEventRepository;

    public FailureEventClaimService(FailureEventRepository failureEventRepository) {
        this.failureEventRepository = failureEventRepository;
    }

    /**
     * Claims a bounded batch and commits the claim before returning its IDs.
     * Parsing and normalization must happen after this transaction has ended.
     */
    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public List<UUID> claimNextEligibleForProcessing(int batchSize) {
        if (batchSize <= 0) {
            throw new IllegalArgumentException("batchSize must be greater than zero");
        }

        Instant claimedAt = Instant.now();
        List<FailureEventEntity> candidates = failureEventRepository
                .lockNextEligibleForProcessing(claimedAt, batchSize);

        candidates.forEach(event -> event.claimForProcessing(claimedAt));

        return candidates.stream()
                .map(FailureEventEntity::getEventId)
                .toList();
    }
}
