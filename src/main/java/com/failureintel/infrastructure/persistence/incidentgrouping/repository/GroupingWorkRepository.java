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
}
