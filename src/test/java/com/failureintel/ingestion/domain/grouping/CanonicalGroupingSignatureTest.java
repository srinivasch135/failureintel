package com.failureintel.ingestion.domain.grouping;

import com.failureintel.ingestion.domain.model.NormalizedFailureEvent;
import com.failureintel.ingestion.domain.normalization.NormalizationStatus;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.HexFormat;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class CanonicalGroupingSignatureTest {

    @Test
    void shouldTreatTrimmedEquivalentValuesAsTheSameIdentity() {
        assertEquals(signature(" service ", "prod", "PSQLException", " database "),
                signature("service", "prod", "PSQLException", "database"));
    }

    @Test
    void shouldTreatNullAndBlankDependencyTargetsAsNoDependency() {
        assertEquals(signature("service", "prod", "Failure", null),
                signature("service", "prod", "Failure", "  "));
    }

    @Test
    void shouldIncludeExactlyTheFourGroupingAttributes() {
        CanonicalGroupingSignature baseline = signature("service", "prod", "Failure", "database");

        Stream.of(
                signature("other", "prod", "Failure", "database"),
                signature("service", "dev", "Failure", "database"),
                signature("service", "prod", "OtherFailure", "database"),
                signature("service", "prod", "Failure", "other"))
                .forEach(candidate -> assertNotEquals(baseline, candidate));
    }

    @Test
    void shouldPreserveCaseOfCanonicalIdentifiers() {
        assertNotEquals(signature("Service", "prod", "Failure", "database"),
                signature("service", "prod", "Failure", "database"));
        assertNotEquals(signature("service", "prod", "Failure", "Database"),
                signature("service", "prod", "Failure", "database"));
        assertNotEquals(signature("service", "prod", "Failure", "database"),
                signature("service", "prod", "failure", "database"));
    }

    @Test
    void shouldRejectMissingOrDefaultedRequiredAttributes() {
        assertTrue(CanonicalGroupingSignature.from(event(null, "prod", "Failure", null, Map.of())).isEmpty());
        assertTrue(CanonicalGroupingSignature.from(event("service", " ", "Failure", null, Map.of())).isEmpty());
        assertTrue(CanonicalGroupingSignature.from(event("service", "prod", null, null, Map.of())).isEmpty());
        assertTrue(CanonicalGroupingSignature.from(event("service", "prod", "Failure", null,
                Map.of("serviceNameDefaulted", true))).isEmpty());
        assertTrue(CanonicalGroupingSignature.from(event("service", "prod", "Failure", null,
                Map.of("environmentDefaulted", true))).isEmpty());
        assertTrue(CanonicalGroupingSignature.from(event("service", "prod", "Failure", null,
                Map.of("errorTypeDefaulted", true))).isEmpty());
        assertTrue(CanonicalGroupingSignature.from(event("service", "unknown", "Failure", null,
                Map.of("unknownEnvironmentValue", true))).isEmpty());
    }

    @Test
    void shouldDistinguishNoDependencyFromTheLiteralSentinel() {
        CanonicalGroupingSignature absent = signature("service", "prod", "Failure", null);
        CanonicalGroupingSignature sentinel = signature("service", "prod", "Failure", "NO_DEPENDENCY");

        assertNotEquals(absent, sentinel);
        assertEquals(Optional.empty(), absent.dependencyTarget());
        assertEquals(Optional.of("NO_DEPENDENCY"), sentinel.dependencyTarget());
    }

    @Test
    void shouldUseUnambiguousTaggedLengthPrefixedUtf8Encoding() {
        CanonicalGroupingSignature left = signature("ab", "c", "Failure", null);
        CanonicalGroupingSignature right = signature("a", "bc", "Failure", null);

        assertNotEquals(left, right);
        assertNotEquals(HexFormat.of().formatHex(left.encodedSignature()),
                HexFormat.of().formatHex(right.encodedSignature()));
    }

    @Test
    void shouldProduceStableVersionedBytesAndSignedBigEndianSha256Key() {
        CanonicalGroupingSignature signature = signature("café", "prod", "E", "dep");

        assertEquals("010100000005636166c3a9020000000470726f64030000000145040100000003646570",
                HexFormat.of().formatHex(signature.encodedSignature()));
        assertEquals(-1047466816941529953L, signature.advisoryLockKey());
        assertArrayEquals(signature.encodedSignature(), signature.encodedSignature());
    }

    @Test
    void signatureEqualityUsesCompleteTupleRatherThanLockKey() {
        CanonicalGroupingSignature first = signature("service", "prod", "Failure", "database");
        CanonicalGroupingSignature second = signature("service", "prod", "Failure", "other");

        assertNotEquals(first, second);
        // The lock key is a coordination hint; equality always compares the complete tuple.
        assertNotEquals(first.encodedSignature(), second.encodedSignature());
    }

    private static CanonicalGroupingSignature signature(
            String service, String environment, String errorType, String dependencyTarget) {
        return CanonicalGroupingSignature.from(event(
                service, environment, errorType, dependencyTarget, Map.of())).orElseThrow();
    }

    private static NormalizedFailureEvent event(
            String service, String environment, String errorType, String dependencyTarget,
            Map<String, Object> metadata) {
        return new NormalizedFailureEvent(
                UUID.randomUUID(), service, environment, "exception", errorType, "message", dependencyTarget,
                "trace", "high", Instant.EPOCH, Instant.EPOCH, NormalizationStatus.FULLY_NORMALIZED,
                Map.of(), Map.of(), metadata);
    }
}
