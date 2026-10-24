package com.failureintel.ingestion.application.service;

import com.failureintel.infrastructure.persistence.incidentgrouping.entity.GroupingWorkEntity;
import com.failureintel.infrastructure.persistence.incidentgrouping.repository.GroupingWorkRepository;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;

/** Claims grouping work in a short transaction that completes before execution is dispatched. */
@Service
public class GroupingWorkClaimService {
    private final GroupingWorkRepository groupingWorkRepository;
    private final FailureEventRetryPolicy retryPolicy;

    public GroupingWorkClaimService(
            GroupingWorkRepository groupingWorkRepository,
            FailureEventRetryPolicy retryPolicy) {
        this.groupingWorkRepository = groupingWorkRepository;
        this.retryPolicy = retryPolicy;
    }

    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public List<ClaimedGroupingWork> claimNextEligibleForProcessing(int batchSize) {
        if (batchSize <= 0) {
            throw new IllegalArgumentException("batchSize must be greater than zero");
        }

        LocalDateTime claimedAt = groupingWorkRepository.currentDatabaseTime();
        List<GroupingWorkEntity> eligible = groupingWorkRepository.lockNextEligibleForProcessing(
                claimedAt, batchSize, retryPolicy.getMaxAttempts());
        List<ClaimedGroupingWork> claims = new ArrayList<>(eligible.size());
        for (GroupingWorkEntity work : eligible) {
            work.claimForProcessing(claimedAt);
            claims.add(new ClaimedGroupingWork(work.getEventId(), work.getAttemptCount()));
        }
        return List.copyOf(claims);
    }
}
