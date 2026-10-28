package com.failureintel.ingestion.application.service;

import com.failureintel.infrastructure.persistence.incidentgrouping.entity.GroupingWorkEntity;
import com.failureintel.infrastructure.persistence.incidentgrouping.repository.GroupingWorkRepository;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;

/** Persists a grouping failure after the completion transaction has rolled back. */
@Service
public class GroupingRetryStateRecorder {
    private final GroupingWorkRepository repository;
    private final GroupingRetryPolicy retryPolicy;

    public GroupingRetryStateRecorder(GroupingWorkRepository repository, GroupingRetryPolicy retryPolicy) {
        this.repository = repository;
        this.retryPolicy = retryPolicy;
    }

    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public boolean record(ClaimedGroupingWork claim, GroupingFailureClassifier.Failure failure) {
        GroupingWorkEntity work = repository.lockCurrentClaim(claim.eventId(), claim.attemptNumber()).orElse(null);
        if (work == null) {
            return false;
        }
        if (failure.transientFailure() && claim.attemptNumber() < retryPolicy.maxAttempts()) {
            LocalDateTime dueAt = repository.currentDatabaseTime().plus(retryPolicy.delayAfter(claim.attemptNumber()));
            work.markRetryable(dueAt, failure.code(), failure.reason());
        } else {
            work.markFailed(failure.transientFailure() ? "GROUPING_RETRY_EXHAUSTED" : failure.code(),
                    failure.transientFailure() ? "Grouping retry attempts exhausted" : failure.reason());
        }
        repository.save(work);
        return true;
    }
}
