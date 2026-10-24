package com.failureintel.ingestion.domain.grouping;

import com.failureintel.ingestion.domain.model.NormalizedFailureEvent;

import java.time.Duration;
import java.time.Instant;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

/** Evaluates grouping eligibility without consulting or changing persistence state. */
public final class IncidentGroupingEligibilityEvaluator {

    public static final Duration DEFAULT_INACTIVITY_THRESHOLD = Duration.ofMinutes(30);
    public static final Duration DEFAULT_HISTORICAL_CORRECTION_WINDOW = Duration.ofHours(24);

    /**
     * Evaluates a normalized event against a snapshot of candidate incidents.
     * The evaluation time is used only for historical-correction expiry; event occurrence rules use occurredAt.
     */
    public Decision evaluate(
            NormalizedFailureEvent event,
            List<IncidentCandidate> candidates,
            Instant evaluatedAt,
            Configuration configuration) {
        Objects.requireNonNull(event, "event must not be null");
        Objects.requireNonNull(candidates, "candidates must not be null");
        Objects.requireNonNull(evaluatedAt, "evaluatedAt must not be null");
        Objects.requireNonNull(configuration, "configuration must not be null");

        Optional<CanonicalGroupingSignature> signature = CanonicalGroupingSignature.from(event);
        if (signature.isEmpty()) {
            return Decision.unassigned(UnassignedReason.MISSING_GROUPING_ATTRIBUTES);
        }
        Instant occurredAt = event.getOccurredAt();
        if (occurredAt == null) {
            return Decision.unassigned(UnassignedReason.MISSING_OCCURRED_AT);
        }

        List<IncidentCandidate> candidateSnapshot = List.copyOf(candidates);
        requireUniqueIncidentIds(candidateSnapshot);

        List<IncidentCandidate> matchingCandidates = candidateSnapshot.stream()
                .filter(candidate -> candidate.signature().equals(signature.get()))
                .toList();

        List<CandidateAssessment> assessments = matchingCandidates.stream()
                .map(candidate -> assess(candidate, matchingCandidates, occurredAt, evaluatedAt, configuration))
                .toList();
        List<CandidateAssessment> conflictFree = assessments.stream()
                .filter(assessment -> assessment.status() == AssessmentStatus.ELIGIBLE)
                .toList();

        if (conflictFree.size() > 1) {
            return Decision.unassigned(UnassignedReason.AMBIGUOUS_INCIDENT_MATCH);
        }
        if (conflictFree.size() == 1) {
            return decisionFor(conflictFree.get(0));
        }

        if (assessments.stream().anyMatch(
                assessment -> assessment.status() == AssessmentStatus.OVERLAP_CONFLICT)) {
            return Decision.unassigned(UnassignedReason.TEMPORAL_OVERLAP_CONFLICT);
        }
        if (assessments.stream().anyMatch(
                assessment -> assessment.status() == AssessmentStatus.HISTORICAL_CORRECTION_EXPIRED)) {
            return Decision.unassigned(UnassignedReason.HISTORICAL_CORRECTION_EXPIRED);
        }

        return Decision.createIncident();
    }

    private static CandidateAssessment assess(
            IncidentCandidate candidate,
            List<IncidentCandidate> matchingCandidates,
            Instant occurredAt,
            Instant evaluatedAt,
            Configuration configuration) {
        if (candidate.status() == IncidentStatus.CLOSED && !isWithin(candidate, occurredAt)) {
            return CandidateAssessment.ineligible(candidate);
        }

        if (candidate.status() == IncidentStatus.CLOSED) {
            Instant deadline = correctionDeadline(candidate, configuration);
            if (!evaluatedAt.isBefore(deadline)) {
                return CandidateAssessment.expiredHistoricalCorrection(candidate);
            }
            return CandidateAssessment.historicalCorrection(candidate, new OccurrenceBounds(
                    candidate.firstOccurredAt(), candidate.lastOccurredAt()), deadline);
        }

        if (!isActiveEligible(candidate, occurredAt, configuration.inactivityThreshold())) {
            return CandidateAssessment.ineligible(candidate);
        }

        OccurrenceBounds proposedBounds = proposedBounds(candidate, occurredAt);
        if (extendsBounds(candidate, proposedBounds)
                && overlapsAnother(proposedBounds, candidate, matchingCandidates)) {
            return CandidateAssessment.overlapConflict(candidate, proposedBounds);
        }
        return CandidateAssessment.eligible(candidate, proposedBounds);
    }

    private static boolean isActiveEligible(
            IncidentCandidate candidate, Instant occurredAt, Duration inactivityThreshold) {
        Instant expiresAt = candidate.lastOccurredAt().plus(inactivityThreshold);
        if (!occurredAt.isBefore(expiresAt)) {
            return false;
        }
        if (!occurredAt.isBefore(candidate.firstOccurredAt())) {
            return true;
        }
        return Duration.between(occurredAt, candidate.firstOccurredAt()).compareTo(inactivityThreshold) <= 0;
    }

    private static Decision decisionFor(CandidateAssessment assessment) {
        IncidentCandidate candidate = assessment.candidate();
        if (candidate.status() == IncidentStatus.CLOSED) {
            return Decision.historicalCorrection(candidate.incidentId(), assessment.correctionDeadline());
        }

        OccurrenceBounds proposedBounds = assessment.proposedBounds();
        return Decision.associate(candidate.incidentId(),
                !proposedBounds.firstOccurredAt().equals(candidate.firstOccurredAt()),
                !proposedBounds.lastOccurredAt().equals(candidate.lastOccurredAt()));
    }

    private static boolean extendsBounds(IncidentCandidate candidate, OccurrenceBounds proposedBounds) {
        return !proposedBounds.firstOccurredAt().equals(candidate.firstOccurredAt())
                || !proposedBounds.lastOccurredAt().equals(candidate.lastOccurredAt());
    }

    private static boolean isWithin(IncidentCandidate candidate, Instant occurredAt) {
        return !occurredAt.isBefore(candidate.firstOccurredAt())
                && !occurredAt.isAfter(candidate.lastOccurredAt());
    }

    private static Instant correctionDeadline(IncidentCandidate candidate, Configuration configuration) {
        return candidate.closedAt().plus(configuration.historicalCorrectionWindow());
    }

    private static OccurrenceBounds proposedBounds(IncidentCandidate candidate, Instant occurredAt) {
        return new OccurrenceBounds(
                occurredAt.isBefore(candidate.firstOccurredAt()) ? occurredAt : candidate.firstOccurredAt(),
                occurredAt.isAfter(candidate.lastOccurredAt()) ? occurredAt : candidate.lastOccurredAt());
    }

    private static boolean overlapsAnother(
            OccurrenceBounds proposed,
            IncidentCandidate selected,
            List<IncidentCandidate> candidates) {
        return candidates.stream()
                .filter(other -> !other.incidentId().equals(selected.incidentId()))
                .anyMatch(other -> proposed.intersects(new OccurrenceBounds(
                        other.firstOccurredAt(), other.lastOccurredAt())));
    }

    private static void requireUniqueIncidentIds(List<IncidentCandidate> candidates) {
        Set<UUID> incidentIds = new HashSet<>();
        for (IncidentCandidate candidate : candidates) {
            if (!incidentIds.add(candidate.incidentId())) {
                throw new IllegalArgumentException("candidate incident IDs must be unique");
            }
        }
    }

    private enum AssessmentStatus {
        ELIGIBLE,
        OVERLAP_CONFLICT,
        HISTORICAL_CORRECTION_EXPIRED,
        INELIGIBLE
    }

    private record CandidateAssessment(
            IncidentCandidate candidate,
            AssessmentStatus status,
            OccurrenceBounds proposedBounds,
            Instant correctionDeadline) {
        private static CandidateAssessment eligible(
                IncidentCandidate candidate, OccurrenceBounds proposedBounds) {
            return new CandidateAssessment(candidate, AssessmentStatus.ELIGIBLE, proposedBounds, null);
        }

        private static CandidateAssessment historicalCorrection(
                IncidentCandidate candidate, OccurrenceBounds bounds, Instant correctionDeadline) {
            return new CandidateAssessment(candidate, AssessmentStatus.ELIGIBLE, bounds, correctionDeadline);
        }

        private static CandidateAssessment overlapConflict(
                IncidentCandidate candidate, OccurrenceBounds proposedBounds) {
            return new CandidateAssessment(candidate, AssessmentStatus.OVERLAP_CONFLICT, proposedBounds, null);
        }

        private static CandidateAssessment expiredHistoricalCorrection(IncidentCandidate candidate) {
            return new CandidateAssessment(candidate, AssessmentStatus.HISTORICAL_CORRECTION_EXPIRED,
                    null, null);
        }

        private static CandidateAssessment ineligible(IncidentCandidate candidate) {
            return new CandidateAssessment(candidate, AssessmentStatus.INELIGIBLE, null, null);
        }
    }

    public enum IncidentStatus {
        ACTIVE,
        CLOSED
    }

    public enum Outcome {
        ASSOCIATED,
        HISTORICAL_CORRECTION,
        CREATE_INCIDENT,
        UNASSIGNED
    }

    public enum UnassignedReason {
        MISSING_GROUPING_ATTRIBUTES,
        MISSING_OCCURRED_AT,
        AMBIGUOUS_INCIDENT_MATCH,
        TEMPORAL_OVERLAP_CONFLICT,
        HISTORICAL_CORRECTION_EXPIRED
    }

    /** The time windows used by this evaluation. */
    public record Configuration(Duration inactivityThreshold, Duration historicalCorrectionWindow) {
        public static final Configuration DEFAULT = new Configuration(
                DEFAULT_INACTIVITY_THRESHOLD, DEFAULT_HISTORICAL_CORRECTION_WINDOW);

        public Configuration {
            requirePositive(inactivityThreshold, "inactivityThreshold");
            requirePositive(historicalCorrectionWindow, "historicalCorrectionWindow");
        }

        private static void requirePositive(Duration value, String name) {
            Objects.requireNonNull(value, name + " must not be null");
            if (value.isZero() || value.isNegative()) {
                throw new IllegalArgumentException(name + " must be greater than zero");
            }
        }
    }

    /** Immutable incident state supplied by the caller; occurrence bounds are inclusive. */
    public record IncidentCandidate(
            UUID incidentId,
            CanonicalGroupingSignature signature,
            IncidentStatus status,
            Instant firstOccurredAt,
            Instant lastOccurredAt,
            Instant closedAt) {
        public IncidentCandidate {
            Objects.requireNonNull(incidentId, "incidentId must not be null");
            Objects.requireNonNull(signature, "signature must not be null");
            Objects.requireNonNull(status, "status must not be null");
            Objects.requireNonNull(firstOccurredAt, "firstOccurredAt must not be null");
            Objects.requireNonNull(lastOccurredAt, "lastOccurredAt must not be null");
            if (lastOccurredAt.isBefore(firstOccurredAt)) {
                throw new IllegalArgumentException("lastOccurredAt must not precede firstOccurredAt");
            }
            if (status == IncidentStatus.CLOSED) {
                Objects.requireNonNull(closedAt, "closedAt must not be null for CLOSED incidents");
            } else if (closedAt != null) {
                throw new IllegalArgumentException("closedAt must be null for ACTIVE incidents");
            }
        }
    }

    /** Immutable business outcome. Historical corrections include a strict commit-before deadline. */
    public record Decision(
            Outcome outcome,
            UUID incidentId,
            UnassignedReason unassignedReason,
            boolean extendsOccurrenceStart,
            boolean extendsOccurrenceEnd,
            Instant historicalCorrectionDeadline) {
        public Decision {
            Objects.requireNonNull(outcome, "outcome must not be null");
            if ((outcome == Outcome.UNASSIGNED) != (unassignedReason != null)) {
                throw new IllegalArgumentException("Only UNASSIGNED decisions have an unassigned reason");
            }
            if ((outcome == Outcome.ASSOCIATED || outcome == Outcome.HISTORICAL_CORRECTION)
                    != (incidentId != null)) {
                throw new IllegalArgumentException("Association decisions require exactly one incident ID");
            }
            if ((outcome == Outcome.HISTORICAL_CORRECTION) != (historicalCorrectionDeadline != null)) {
                throw new IllegalArgumentException("Only historical corrections have a correction deadline");
            }
            if (outcome != Outcome.ASSOCIATED && (extendsOccurrenceStart || extendsOccurrenceEnd)) {
                throw new IllegalArgumentException("Only ACTIVE associations can extend occurrence bounds");
            }
        }

        public Optional<UUID> associatedIncidentId() {
            return Optional.ofNullable(incidentId);
        }

        public Optional<UnassignedReason> reason() {
            return Optional.ofNullable(unassignedReason);
        }

        public Optional<Instant> commitBefore() {
            return Optional.ofNullable(historicalCorrectionDeadline);
        }

        private static Decision associate(UUID incidentId, boolean startExtended, boolean endExtended) {
            return new Decision(Outcome.ASSOCIATED, incidentId, null,
                    startExtended, endExtended, null);
        }

        private static Decision historicalCorrection(UUID incidentId, Instant deadline) {
            return new Decision(Outcome.HISTORICAL_CORRECTION, incidentId, null, false, false, deadline);
        }

        private static Decision createIncident() {
            return new Decision(Outcome.CREATE_INCIDENT, null, null, false, false, null);
        }

        private static Decision unassigned(UnassignedReason reason) {
            return new Decision(Outcome.UNASSIGNED, null, reason, false, false, null);
        }
    }

    private record OccurrenceBounds(Instant firstOccurredAt, Instant lastOccurredAt) {
        private boolean intersects(OccurrenceBounds other) {
            return !lastOccurredAt.isBefore(other.firstOccurredAt)
                    && !other.lastOccurredAt.isBefore(firstOccurredAt);
        }
    }
}
