package com.failureintel.infrastructure.persistence.failureevent.repository;

import com.failureintel.infrastructure.persistence.failureevent.entity.FailureEventEntity;
import com.failureintel.infrastructure.persistence.failureevent.entity.ProcessingStatus;
import org.springframework.data.domain.*;
import org.springframework.data.jpa.repository.*;
import org.springframework.data.repository.query.Param;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;
import java.time.Instant;
import java.util.*;

public interface FailureEventRepository extends JpaRepository<FailureEventEntity, UUID> {
        Optional<FailureEventEntity> findFirstByTraceIdOrderByIngestedAtDescEventIdDesc(String traceId);

        @Transactional(readOnly = true, propagation = Propagation.REQUIRES_NEW)
        Optional<FailureEventEntity> findByIdempotencyKey(String idempotencyKey);

        @Override
        @Transactional(propagation = Propagation.REQUIRES_NEW)
        FailureEventEntity saveAndFlush(FailureEventEntity entity);

        boolean existsByTraceId(String traceId);

        Page<FailureEventEntity> findByProcessingStatus(ProcessingStatus processingStatus, Pageable pageable);

        @Query("SELECT fe FROM FailureEventEntity fe WHERE fe.processingStatus = :processingStatus ORDER BY fe.occurredAt ASC")
        List<FailureEventEntity> fetchNextBatchForProcessing(
                        @Param("processingStatus") ProcessingStatus processingStatus,
                        Pageable pageable);

        @Query("SELECT fe FROM FailureEventEntity fe WHERE fe.serviceName = :serviceName " +
                        "AND fe.environment = :environment AND fe.errorType = :errorType " +
                        "AND fe.occurredAt > :occurredAt ORDER BY fe.occurredAt DESC")
        List<FailureEventEntity> findRecentSimilarErrors(
                        @Param("serviceName") String serviceName,
                        @Param("environment") String environment,
                        @Param("errorType") String errorType,
                        @Param("occurredAt") Instant occurredAt);

        @Modifying(clearAutomatically = true, flushAutomatically = true)
        @Query("UPDATE FailureEventEntity fe SET fe.processingStatus=:processingStatus WHERE fe.eventId=:eventId")
        int updateProcessingStatus(@Param("eventId") UUID eventId,
                        @Param("processingStatus") ProcessingStatus processingStatus);

        @Query(value = """
                        SELECT fe.*
                        FROM failure_event fe
                        WHERE fe.processing_status = 'RECEIVED'
                           OR (
                                fe.processing_status = 'RETRYABLE'
                                AND fe.next_attempt_at <= :eligibleAt
                           )
                        ORDER BY fe.ingested_at ASC, fe.event_id ASC
                        LIMIT :batchSize
                        FOR UPDATE SKIP LOCKED
                        """, nativeQuery = true)
        List<FailureEventEntity> lockNextEligibleForProcessing(
                @Param("eligibleAt") Instant eligibleAt,
                @Param("batchSize") int batchSize);

        Page<FailureEventEntity> findByOccurredAtBetween(
                        Instant start,
                        Instant end,
                        Pageable pageable);

}
