package com.failureintel.infrastructure.persistence.incidentgrouping.repository;

import com.failureintel.infrastructure.persistence.incidentgrouping.entity.GroupingWorkEntity;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

public interface GroupingWorkRepository extends JpaRepository<GroupingWorkEntity, UUID> {

    @Query(value = "SELECT COUNT(*) FROM grouping_work WHERE status = :status", nativeQuery = true)
    long countWorkByStatus(@Param("status") String status);

    @Query(value = "SELECT COUNT(*) FROM grouping_work WHERE status IN ('PENDING', 'PROCESSING', 'RETRYABLE')", nativeQuery = true)
    long countUnfinishedWork();

    @Query(value = "SELECT COUNT(*) FROM grouping_work WHERE (status = 'PENDING' AND attempt_count < :maxAttempts) OR (status = 'RETRYABLE' AND next_attempt_at <= (clock_timestamp() AT TIME ZONE 'UTC') AND attempt_count < :maxAttempts)", nativeQuery = true)
    long countClaimableWork(@Param("maxAttempts") int maxAttempts);

    @Query(value = "SELECT COUNT(*) FROM grouping_work WHERE status = 'COMPLETED' AND terminal_outcome = :outcome", nativeQuery = true)
    long countCompletedOutcome(@Param("outcome") String outcome);

    @Query(value = "SELECT COUNT(*) FROM grouping_work WHERE status = 'COMPLETED' AND unassigned_reason = :reason", nativeQuery = true)
    long countUnassignedReason(@Param("reason") String reason);

    @Query(value = "SELECT COUNT(*) FROM grouping_work WHERE status = 'FAILED' AND failure_code = 'GROUPING_RETRY_EXHAUSTED'", nativeQuery = true)
    long countRetryExhausted();

    @Query(value = "SELECT COUNT(*) FROM grouping_work WHERE status = 'FAILED' AND failure_code IS DISTINCT FROM 'GROUPING_RETRY_EXHAUSTED'", nativeQuery = true)
    long countPermanentFailures();

    @Query(value = "SELECT COUNT(*) FROM grouping_work WHERE status = 'COMPLETED' AND unassigned_reason = 'HISTORICAL_CORRECTION_EXPIRED'", nativeQuery = true)
    long countExpiredCorrections();

    @Modifying
    @Query(value = "INSERT INTO grouping_work (event_id) VALUES (:eventId) "
            + "ON CONFLICT (event_id) DO NOTHING", nativeQuery = true)
    int insertPendingIfAbsent(@Param("eventId") UUID eventId);

    @Query(value = "SELECT LOCALTIMESTAMP", nativeQuery = true)
    LocalDateTime currentDatabaseTime();

    @Query(value = """
            SELECT *
            FROM grouping_work
            WHERE (status = 'PENDING' AND attempt_count < :maxAttempts)
               OR (status = 'RETRYABLE'
                   AND next_attempt_at <= :eligibleAt
                   AND attempt_count < :maxAttempts)
            ORDER BY created_at ASC, event_id ASC
            LIMIT :batchSize
            FOR UPDATE SKIP LOCKED
            """, nativeQuery = true)
    List<GroupingWorkEntity> lockNextEligibleForProcessing(
            @Param("eligibleAt") LocalDateTime eligibleAt,
            @Param("batchSize") int batchSize,
            @Param("maxAttempts") int maxAttempts);

    /** Locks and verifies the current claim in one statement for future completion transactions. */
    @Query(value = """
            SELECT * FROM grouping_work
            WHERE event_id = :eventId
              AND status = 'PROCESSING'
              AND attempt_count = :attemptNumber
            FOR UPDATE
            """, nativeQuery = true)
    Optional<GroupingWorkEntity> lockCurrentClaim(
            @Param("eventId") UUID eventId,
            @Param("attemptNumber") int attemptNumber);

    @Query(value = """
            SELECT * FROM grouping_work
            WHERE status = 'PROCESSING'
              AND processing_started_at <= :expiredBefore
            ORDER BY processing_started_at ASC, event_id ASC
            LIMIT :batchSize
            FOR UPDATE SKIP LOCKED
            """, nativeQuery = true)
    List<GroupingWorkEntity> lockExpiredProcessingClaims(
            @Param("expiredBefore") LocalDateTime expiredBefore,
            @Param("batchSize") int batchSize);
}
