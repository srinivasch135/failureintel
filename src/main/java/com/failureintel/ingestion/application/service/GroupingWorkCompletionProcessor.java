package com.failureintel.ingestion.application.service;

import com.failureintel.ingestion.application.config.HistoricalCorrectionProperties;
import com.failureintel.ingestion.domain.grouping.CanonicalGroupingSignature;
import com.failureintel.ingestion.domain.grouping.IncidentGroupingEligibilityEvaluator;
import com.failureintel.ingestion.domain.grouping.IncidentGroupingEligibilityEvaluator.Decision;
import com.failureintel.ingestion.domain.grouping.IncidentGroupingEligibilityEvaluator.IncidentCandidate;
import com.failureintel.ingestion.domain.grouping.IncidentGroupingEligibilityEvaluator.IncidentStatus;
import com.failureintel.ingestion.domain.grouping.IncidentGroupingEligibilityEvaluator.Outcome;
import com.failureintel.infrastructure.persistence.incidentgrouping.entity.GroupingWorkEntity;
import com.failureintel.infrastructure.persistence.incidentgrouping.entity.IncidentEntity;
import com.failureintel.infrastructure.persistence.incidentgrouping.entity.IncidentMembershipEntity;
import com.failureintel.infrastructure.persistence.incidentgrouping.repository.GroupingWorkRepository;
import com.failureintel.infrastructure.persistence.incidentgrouping.repository.IncidentMembershipRepository;
import com.failureintel.infrastructure.persistence.incidentgrouping.repository.IncidentRepository;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.List;
import java.util.UUID;

/** Applies eligibility outcomes through the existing proxied grouping completion transaction. */
@Service
public class GroupingWorkCompletionProcessor {
    private final GroupingCoordinationService coordinationService;
    private final GroupingWorkRepository groupingWorkRepository;
    private final IncidentMembershipRepository membershipRepository;
    private final IncidentRepository incidentRepository;
    private final JdbcTemplate jdbcTemplate;
    private final HistoricalCorrectionProperties correctionProperties;
    private final IncidentGroupingEligibilityEvaluator eligibilityEvaluator =
            new IncidentGroupingEligibilityEvaluator();

    public GroupingWorkCompletionProcessor(
            GroupingCoordinationService coordinationService,
            GroupingWorkRepository groupingWorkRepository,
            IncidentMembershipRepository membershipRepository,
            IncidentRepository incidentRepository,
            JdbcTemplate jdbcTemplate,
            HistoricalCorrectionProperties correctionProperties) {
        this.coordinationService = coordinationService;
        this.groupingWorkRepository = groupingWorkRepository;
        this.membershipRepository = membershipRepository;
        this.incidentRepository = incidentRepository;
        this.jdbcTemplate = jdbcTemplate;
        this.correctionProperties = correctionProperties;
    }

    public void process(ClaimedGroupingWork claim) {
        coordinationService.withCoordination(claim, snapshot -> {
            if (snapshot.alreadyAssociated()) {
                throw new IllegalStateException("Processing grouping work already has incident membership");
            }

            Instant evaluatedAt = currentDatabaseTime();
            List<IncidentCandidate> candidates = snapshot.signature()
                    .map(signature -> snapshot.candidates().stream()
                            .map(candidate -> toCandidate(candidate, signature))
                            .toList())
                    .orElseGet(List::of);
            Decision decision = eligibilityEvaluator.evaluate(snapshot.event(), candidates, evaluatedAt,
                    new IncidentGroupingEligibilityEvaluator.Configuration(
                            IncidentGroupingEligibilityEvaluator.DEFAULT_INACTIVITY_THRESHOLD,
                            correctionProperties.window()));
            GroupingWorkEntity work = groupingWorkRepository.findById(claim.eventId())
                    .orElseThrow(() -> new IllegalStateException("Claimed grouping work disappeared"));

            if (decision.outcome() == Outcome.UNASSIGNED) {
                work.completeUnassigned(decision.reason().orElseThrow().name());
                return;
            }

            Instant occurredAt = snapshot.event().getOccurredAt();
            LocalDateTime occurrence = toDatabaseTime(occurredAt);
            IncidentEntity incident;
            if (decision.outcome() == Outcome.CREATE_INCIDENT) {
                CanonicalGroupingSignature signature = snapshot.signature().orElseThrow(
                        () -> new IllegalStateException("Incident creation requires a canonical signature"));
                LocalDateTime expiresAt = toDatabaseTime(occurredAt.plus(
                        IncidentGroupingEligibilityEvaluator.DEFAULT_INACTIVITY_THRESHOLD));
                incident = incidentRepository.saveAndFlush(new IncidentEntity(UUID.randomUUID(),
                        signature.serviceName(), signature.environment(), signature.errorType(),
                        signature.dependencyTarget().orElse(null), "ACTIVE", occurrence, occurrence,
                        expiresAt, null, 1));
            } else {
                incident = snapshot.candidates().stream()
                        .filter(candidate -> candidate.getIncidentId().equals(decision.incidentId()))
                        .findFirst()
                        .orElseThrow(() -> new IllegalStateException("Eligible incident candidate disappeared"));
                if (decision.outcome() == Outcome.ASSOCIATED) {
                    LocalDateTime lastOccurrence = occurrence.isAfter(incident.getLastOccurredAt())
                            ? occurrence : incident.getLastOccurredAt();
                    incident.associateOccurrence(occurrence, toDatabaseTime(
                            lastOccurrence.toInstant(ZoneOffset.UTC).plus(
                                    IncidentGroupingEligibilityEvaluator.DEFAULT_INACTIVITY_THRESHOLD)));
                } else if (decision.outcome() == Outcome.HISTORICAL_CORRECTION
                        && !currentDatabaseTime().isBefore(decision.commitBefore().orElseThrow())) {
                    throw new IllegalStateException(
                            "Historical correction eligibility expired before completion");
                } else if (decision.outcome() == Outcome.HISTORICAL_CORRECTION) {
                    jdbcTemplate.queryForObject("SELECT set_config(?, ?, true)", String.class,
                            "failureintel.historical_correction_deadline",
                            toDatabaseTime(decision.commitBefore().orElseThrow()).toString());
                    incident.recordCorrection();
                }
            }

            membershipRepository.saveAndFlush(new IncidentMembershipEntity(claim.eventId(), incident.getIncidentId()));
            work.completeAssociated();
        });
    }

    private Instant currentDatabaseTime() {
        return jdbcTemplate.queryForObject("SELECT clock_timestamp() AT TIME ZONE 'UTC'", LocalDateTime.class)
                .toInstant(ZoneOffset.UTC);
    }

    private static IncidentCandidate toCandidate(
            IncidentEntity entity, CanonicalGroupingSignature signature) {
        return new IncidentCandidate(entity.getIncidentId(), signature,
                "CLOSED".equals(entity.getStatus()) ? IncidentStatus.CLOSED : IncidentStatus.ACTIVE,
                entity.getFirstOccurredAt().toInstant(ZoneOffset.UTC),
                entity.getLastOccurredAt().toInstant(ZoneOffset.UTC),
                entity.getClosedAt() == null ? null : entity.getClosedAt().toInstant(ZoneOffset.UTC));
    }

    private static LocalDateTime toDatabaseTime(Instant instant) {
        return LocalDateTime.ofInstant(instant, ZoneOffset.UTC);
    }
}
