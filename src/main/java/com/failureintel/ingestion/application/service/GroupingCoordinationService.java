package com.failureintel.ingestion.application.service;

import com.failureintel.ingestion.domain.grouping.CanonicalGroupingSignature;
import com.failureintel.ingestion.domain.model.NormalizedFailureEvent;
import com.failureintel.infrastructure.persistence.incidentgrouping.entity.IncidentEntity;
import com.failureintel.infrastructure.persistence.incidentgrouping.repository.GroupingWorkRepository;
import com.failureintel.infrastructure.persistence.incidentgrouping.repository.IncidentMembershipRepository;
import com.failureintel.infrastructure.persistence.incidentgrouping.repository.IncidentRepository;
import com.failureintel.infrastructure.persistence.normalizedFailureEvent.entity.NormalizedFailureEventEntity;
import com.failureintel.infrastructure.persistence.normalizedFailureEvent.repository.NormalizedFailureEventRepository;
import org.springframework.jdbc.core.ConnectionCallback;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Isolation;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.sql.PreparedStatement;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.function.Consumer;

/** Runs candidate discovery and all caller-supplied grouping work under one fenced transaction. */
@Service
public class GroupingCoordinationService {
    private final GroupingWorkRepository groupingWorkRepository;
    private final NormalizedFailureEventRepository normalizedEventRepository;
    private final IncidentMembershipRepository membershipRepository;
    private final IncidentRepository incidentRepository;
    private final JdbcTemplate jdbcTemplate;

    public GroupingCoordinationService(
            GroupingWorkRepository groupingWorkRepository,
            NormalizedFailureEventRepository normalizedEventRepository,
            IncidentMembershipRepository membershipRepository,
            IncidentRepository incidentRepository,
            JdbcTemplate jdbcTemplate) {
        this.groupingWorkRepository = groupingWorkRepository;
        this.normalizedEventRepository = normalizedEventRepository;
        this.membershipRepository = membershipRepository;
        this.incidentRepository = incidentRepository;
        this.jdbcTemplate = jdbcTemplate;
    }

    /** Returns false for stale claims; the callback runs only after all coordination reads are current. */
    @Transactional(propagation = Propagation.REQUIRES_NEW, isolation = Isolation.READ_COMMITTED)
    public boolean withCoordination(ClaimedGroupingWork claim, Consumer<CoordinationSnapshot> operation) {
        Objects.requireNonNull(claim, "claim must not be null");
        Objects.requireNonNull(operation, "operation must not be null");

        if (groupingWorkRepository.lockCurrentClaim(claim.eventId(), claim.attemptNumber()).isEmpty()) {
            return false;
        }

        NormalizedFailureEventEntity entity = normalizedEventRepository.findById(claim.eventId())
                .orElseThrow(() -> new IllegalStateException("Claimed grouping event has no normalized event"));
        NormalizedFailureEvent event = toDomain(entity);
        CanonicalGroupingSignature signature = CanonicalGroupingSignature.from(event)
                .orElseThrow(() -> new IllegalStateException("Claimed grouping event has no canonical signature"));

        jdbcTemplate.execute((ConnectionCallback<Void>) connection -> {
            try (PreparedStatement statement = connection.prepareStatement("SELECT pg_advisory_xact_lock(?)")) {
                statement.setLong(1, signature.advisoryLockKey());
                statement.execute();
            }
            return null;
        });

        boolean alreadyAssociated = membershipRepository.existsById(claim.eventId());
        List<IncidentEntity> candidates = incidentRepository.findHistoryBySignature(
                signature.serviceName(), signature.environment(), signature.errorType(),
                signature.dependencyTarget().orElse(null));
        List<IncidentEntity> matchingCandidates = candidates.stream()
                .filter(candidate -> signature.serviceName().equals(candidate.getServiceName())
                        && signature.environment().equals(candidate.getEnvironment())
                        && signature.errorType().equals(candidate.getErrorType())
                        && Objects.equals(signature.dependencyTarget().orElse(null), candidate.getDependencyTarget()))
                .toList();

        operation.accept(new CoordinationSnapshot(event, signature, alreadyAssociated, matchingCandidates));
        return true;
    }

    private static NormalizedFailureEvent toDomain(NormalizedFailureEventEntity entity) {
        return new NormalizedFailureEvent(
                entity.getEventId(), entity.getNormalizedServiceName(), entity.getNormalizedEnvironment(),
                entity.getNormalizedEventType(), entity.getNormalizedErrorType(), entity.getNormalizedErrorMessage(),
                entity.getNormalizedDependencyTarget(), entity.getNormalizedTraceId(), entity.getNormalizedSeverity(),
                entity.getNormalizedOccurredAt(), entity.getNormalizedAt(), entity.getNormalizationStatus(),
                entity.getNormalizedPayload(), Map.of(), entity.getNormalizationMetadata());
    }

    public record CoordinationSnapshot(
            NormalizedFailureEvent event,
            CanonicalGroupingSignature signature,
            boolean alreadyAssociated,
            List<IncidentEntity> candidates) {
        public CoordinationSnapshot {
            candidates = List.copyOf(candidates);
        }
    }
}
