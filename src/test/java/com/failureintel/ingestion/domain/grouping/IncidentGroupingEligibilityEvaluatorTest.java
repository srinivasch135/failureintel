package com.failureintel.ingestion.domain.grouping;

import com.failureintel.ingestion.domain.grouping.IncidentGroupingEligibilityEvaluator.Configuration;
import com.failureintel.ingestion.domain.grouping.IncidentGroupingEligibilityEvaluator.Decision;
import com.failureintel.ingestion.domain.grouping.IncidentGroupingEligibilityEvaluator.IncidentCandidate;
import com.failureintel.ingestion.domain.grouping.IncidentGroupingEligibilityEvaluator.IncidentStatus;
import com.failureintel.ingestion.domain.grouping.IncidentGroupingEligibilityEvaluator.Outcome;
import com.failureintel.ingestion.domain.grouping.IncidentGroupingEligibilityEvaluator.UnassignedReason;
import com.failureintel.ingestion.domain.model.NormalizedFailureEvent;
import com.failureintel.ingestion.domain.normalization.NormalizationStatus;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class IncidentGroupingEligibilityEvaluatorTest {

    private static final Instant T0 = Instant.parse("2026-01-01T12:00:00Z");
    private static final Duration THIRTY_MINUTES = Duration.ofMinutes(30);
    private static final CanonicalGroupingSignature SIGNATURE = signature("service", "prod", "Failure", "db");
    private static final CanonicalGroupingSignature OTHER_SIGNATURE = signature("service", "prod", "Other", "db");
    private static final UUID INCIDENT_A = UUID.fromString("00000000-0000-0000-0000-000000000001");
    private static final UUID INCIDENT_B = UUID.fromString("00000000-0000-0000-0000-000000000002");

    private final IncidentGroupingEligibilityEvaluator evaluator = new IncidentGroupingEligibilityEvaluator();

    @Test
    void defaultsAndSignatureComparisonUseAllFourCanonicalAttributes() {
        assertEquals(Duration.ofMinutes(30), Configuration.DEFAULT.inactivityThreshold());
        assertEquals(Duration.ofHours(24), Configuration.DEFAULT.historicalCorrectionWindow());

        IncidentCandidate candidate = active(INCIDENT_A, T0, T0.plusSeconds(60));
        assertEquals(Outcome.ASSOCIATED, evaluate(event(" service ", "prod", "Failure", " db ", T0),
                List.of(candidate), T0).outcome());
        assertEquals(Outcome.CREATE_INCIDENT, evaluate(event("service", "prod", "Failure", "other", T0),
                List.of(candidate), T0).outcome());
        assertEquals(Outcome.CREATE_INCIDENT, evaluate(event("service", "prod", "failure", "db", T0),
                List.of(candidate), T0).outcome());
    }

    @Test
    void missingRequiredIdentityOrOccurrenceTimeIsExplicitlyUnassigned() {
        IncidentCandidate candidate = active(INCIDENT_A, T0, T0.plusSeconds(60));
        assertUnassigned(evaluate(event(null, "prod", "Failure", "db", T0), List.of(candidate), T0),
                UnassignedReason.MISSING_GROUPING_ATTRIBUTES);
        assertUnassigned(evaluate(event("service", "prod", "Failure", "db", null), List.of(candidate), T0),
                UnassignedReason.MISSING_OCCURRED_AT);
    }

    @Test
    void activeCandidateAllowsForwardAssociationBeforeStrictExpiry() {
        IncidentCandidate candidate = active(INCIDENT_A, T0, T0.plusSeconds(600));

        Decision decision = evaluate(eventAt(T0.plus(Duration.ofMinutes(29))), List.of(candidate), T0);

        assertEquals(Outcome.ASSOCIATED, decision.outcome());
        assertEquals(INCIDENT_A, decision.incidentId());
        assertFalse(decision.extendsOccurrenceStart());
        assertTrue(decision.extendsOccurrenceEnd());
    }

    @Test
    void exactExpiryCannotExtendEvenWhenActiveStatusHasNotTransitioned() {
        IncidentCandidate candidate = active(INCIDENT_A, T0, T0);

        Decision decision = evaluate(eventAt(T0.plus(Duration.ofMinutes(30))), List.of(candidate),
                T0.plus(Duration.ofDays(2)));

        assertEquals(Outcome.CREATE_INCIDENT, decision.outcome());
    }

    @Test
    void eventAfterExpiredActiveWindowCreatesAnotherOccurrence() {
        IncidentCandidate candidate = active(INCIDENT_A, T0, T0);

        assertEquals(Outcome.CREATE_INCIDENT,
                evaluate(eventAt(T0.plus(Duration.ofMinutes(31))), List.of(candidate), T0).outcome());
    }

    @Test
    void backwardExtensionWithinThresholdIsAllowedIncludingThresholdBoundary() {
        IncidentCandidate candidate = active(INCIDENT_A, T0, T0.plusSeconds(600));

        Decision decision = evaluate(eventAt(T0.minus(THIRTY_MINUTES)), List.of(candidate), T0);

        assertEquals(Outcome.ASSOCIATED, decision.outcome());
        assertTrue(decision.extendsOccurrenceStart());
        assertFalse(decision.extendsOccurrenceEnd());
    }

    @Test
    void backwardEventBeyondThresholdCreatesSeparateOccurrence() {
        IncidentCandidate candidate = active(INCIDENT_A, T0, T0.plusSeconds(60));
        assertEquals(Outcome.CREATE_INCIDENT,
                evaluate(eventAt(T0.minus(THIRTY_MINUTES).minusNanos(1)), List.of(candidate), T0).outcome());
    }

    @Test
    void multipleConflictFreeActiveCandidatesProduceDeterministicAmbiguity() {
        IncidentCandidate earlier = active(INCIDENT_A, T0, T0.plusSeconds(60));
        IncidentCandidate later = active(INCIDENT_B, T0.plus(Duration.ofMinutes(10)),
                T0.plus(Duration.ofMinutes(11)));
        NormalizedFailureEvent event = eventAt(T0.plus(Duration.ofMinutes(5)));

        Decision forwardOrder = evaluate(event, List.of(earlier, later), T0);
        Decision reverseOrder = evaluate(event, List.of(later, earlier), T0);

        assertUnassigned(forwardOrder, UnassignedReason.AMBIGUOUS_INCIDENT_MATCH);
        assertEquals(forwardOrder, reverseOrder);
    }

    @Test
    void overlapPreventsBackwardOrForwardExtension() {
        IncidentCandidate first = active(INCIDENT_A, T0, T0.plusSeconds(60));

        assertUnassigned(evaluate(eventAt(T0.minus(Duration.ofMinutes(10))), List.of(first,
                closed(INCIDENT_B, T0.minus(Duration.ofMinutes(5)), T0.plus(Duration.ofMinutes(20)),
                        T0.minus(Duration.ofDays(2)))), T0),
                UnassignedReason.TEMPORAL_OVERLAP_CONFLICT);
        IncidentCandidate expiredOverlap = closed(INCIDENT_B, T0.plus(Duration.ofMinutes(10)),
                T0.plus(Duration.ofMinutes(20)), T0.minus(Duration.ofDays(2)));
        assertUnassigned(evaluate(eventAt(T0.plus(Duration.ofMinutes(15))), List.of(first, expiredOverlap), T0),
                UnassignedReason.TEMPORAL_OVERLAP_CONFLICT);
    }

    @Test
    void creationCannotOverlapAnEstablishedOccurrenceAndEndpointsAreInclusive() {
        IncidentCandidate closed = closed(INCIDENT_A, T0, T0.plusSeconds(60), T0.minus(Duration.ofDays(2)));

        assertUnassigned(evaluate(eventAt(T0), List.of(closed), T0),
                UnassignedReason.HISTORICAL_CORRECTION_EXPIRED);
        assertUnassigned(evaluate(eventAt(T0.plusSeconds(60)), List.of(closed), T0),
                UnassignedReason.HISTORICAL_CORRECTION_EXPIRED);
    }

    @Test
    void closedCorrectionIncludesOccurrenceBoundariesButDoesNotChangeBounds() {
        Instant closedAt = T0.plus(Duration.ofHours(1));
        IncidentCandidate candidate = closed(INCIDENT_A, T0, T0.plusSeconds(60), closedAt);

        for (Instant occurrence : List.of(T0, T0.plusSeconds(60))) {
            Decision decision = evaluate(eventAt(occurrence), List.of(candidate), closedAt);
            assertEquals(Outcome.HISTORICAL_CORRECTION, decision.outcome());
            assertFalse(decision.extendsOccurrenceStart());
            assertFalse(decision.extendsOccurrenceEnd());
            assertEquals(closedAt.plus(Duration.ofHours(24)), decision.commitBefore().orElseThrow());
        }
    }

    @Test
    void closedCorrectionRequiresCommitStrictlyBeforeDeadline() {
        Instant closedAt = T0.plus(Duration.ofHours(1));
        Instant deadline = closedAt.plus(Duration.ofHours(24));
        IncidentCandidate candidate = closed(INCIDENT_A, T0, T0.plusSeconds(60), closedAt);
        NormalizedFailureEvent historicalEvent = eventAt(T0.plusSeconds(30));

        assertEquals(Outcome.HISTORICAL_CORRECTION,
                evaluate(historicalEvent, List.of(candidate), deadline.minusNanos(1)).outcome());
        assertUnassigned(evaluate(historicalEvent, List.of(candidate), deadline),
                UnassignedReason.HISTORICAL_CORRECTION_EXPIRED);
        assertUnassigned(evaluate(historicalEvent, List.of(candidate), deadline.plusNanos(1)),
                UnassignedReason.HISTORICAL_CORRECTION_EXPIRED);
    }

    @Test
    void closedIncidentCannotReopenAndExpiredCorrectionCannotCreateReplacement() {
        Instant closedAt = T0.plus(Duration.ofDays(1));
        IncidentCandidate candidate = closed(INCIDENT_A, T0, T0.plusSeconds(60), closedAt);

        assertUnassigned(evaluate(eventAt(T0.plusSeconds(30)), List.of(candidate),
                closedAt.plus(Duration.ofHours(24))), UnassignedReason.HISTORICAL_CORRECTION_EXPIRED);
        assertEquals(Outcome.CREATE_INCIDENT,
                evaluate(eventAt(T0.plusSeconds(61)), List.of(candidate), closedAt).outcome());
    }

    @Test
    void closedCorrectionNeverMergesMultipleHistoricalIncidents() {
        Instant closedAt = T0.plus(Duration.ofHours(1));
        IncidentCandidate first = closed(INCIDENT_A, T0, T0.plus(Duration.ofMinutes(10)), closedAt);
        IncidentCandidate second = closed(INCIDENT_B, T0.plus(Duration.ofMinutes(5)),
                T0.plus(Duration.ofMinutes(15)), closedAt);

        assertUnassigned(evaluate(eventAt(T0.plus(Duration.ofMinutes(5))), List.of(first, second), closedAt),
                UnassignedReason.AMBIGUOUS_INCIDENT_MATCH);
    }

    @Test
    void unrelatedSignatureDoesNotAffectCandidateChoice() {
        IncidentCandidate matching = active(INCIDENT_A, T0, T0.plusSeconds(60));
        IncidentCandidate unrelated = new IncidentCandidate(INCIDENT_B, OTHER_SIGNATURE, IncidentStatus.ACTIVE,
                T0, T0.plusSeconds(60), null);

        assertEquals(INCIDENT_A, evaluate(eventAt(T0), List.of(unrelated, matching), T0).incidentId());
    }

    @Test
    void inputCandidateOrderDoesNotChangeUniqueAssociation() {
        IncidentCandidate selected = active(INCIDENT_A, T0, T0.plusSeconds(60));
        IncidentCandidate distant = active(INCIDENT_B, T0.plus(Duration.ofHours(3)),
                T0.plus(Duration.ofHours(3)).plusSeconds(60));
        NormalizedFailureEvent event = eventAt(T0.plusSeconds(30));
        Instant originalOccurrence = event.getOccurredAt();
        Instant originalFirstOccurrence = selected.firstOccurredAt();

        assertEquals(evaluate(event, List.of(selected, distant), T0),
                evaluate(event, List.of(distant, selected), T0));
        assertEquals(originalOccurrence, event.getOccurredAt());
        assertEquals(originalFirstOccurrence, selected.firstOccurredAt());
        assertEquals(Outcome.ASSOCIATED, evaluate(event, List.of(selected), T0).outcome());
        assertEquals(originalOccurrence, event.getOccurredAt());
        assertEquals(originalFirstOccurrence, selected.firstOccurredAt());
    }

    @Test
    void validatesThresholdConfigurationAndOccurrenceBounds() {
        assertThrows(IllegalArgumentException.class, () -> new Configuration(Duration.ZERO, Duration.ofHours(1)));
        assertThrows(IllegalArgumentException.class, () -> new Configuration(Duration.ofMinutes(1), Duration.ofSeconds(-1)));
        assertThrows(IllegalArgumentException.class, () -> new IncidentCandidate(INCIDENT_A, SIGNATURE,
                IncidentStatus.ACTIVE, T0.plusSeconds(1), T0, null));
        assertThrows(IllegalArgumentException.class, () -> evaluate(eventAt(T0),
                List.of(active(INCIDENT_A, T0, T0), active(INCIDENT_A, T0, T0)), T0));
        assertThrows(IllegalArgumentException.class, () -> new Decision(
                Outcome.CREATE_INCIDENT, null, null, true, false, null));
    }

    private Decision evaluate(NormalizedFailureEvent event, List<IncidentCandidate> candidates, Instant evaluatedAt) {
        return evaluator.evaluate(event, candidates, evaluatedAt, Configuration.DEFAULT);
    }

    private static IncidentCandidate active(UUID id, Instant first, Instant last) {
        return new IncidentCandidate(id, SIGNATURE, IncidentStatus.ACTIVE, first, last, null);
    }

    private static IncidentCandidate closed(UUID id, Instant first, Instant last, Instant closedAt) {
        return new IncidentCandidate(id, SIGNATURE, IncidentStatus.CLOSED, first, last, closedAt);
    }

    private static void assertUnassigned(Decision decision, UnassignedReason reason) {
        assertEquals(Outcome.UNASSIGNED, decision.outcome());
        assertEquals(reason, decision.unassignedReason());
        assertTrue(decision.associatedIncidentId().isEmpty());
    }

    private static NormalizedFailureEvent eventAt(Instant occurredAt) {
        return event("service", "prod", "Failure", "db", occurredAt);
    }

    private static NormalizedFailureEvent event(
            String service, String environment, String errorType, String dependency, Instant occurredAt) {
        return new NormalizedFailureEvent(UUID.randomUUID(), service, environment, "exception", errorType,
                "message", dependency, "trace", "high", occurredAt, T0, NormalizationStatus.FULLY_NORMALIZED,
                Map.of(), Map.of(), Map.of());
    }

    private static CanonicalGroupingSignature signature(
            String service, String environment, String errorType, String dependency) {
        return CanonicalGroupingSignature.from(event(service, environment, errorType, dependency, T0)).orElseThrow();
    }
}
