package com.failureintel.infrastructure.persistence.normalizedFailureEvent.repository;

import com.failureintel.ingestion.domain.normalization.NormalizationStatus;
import com.failureintel.infrastructure.persistence.normalizedFailureEvent.entity.NormalizedFailureEventEntity;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.time.Instant;
import java.util.UUID;

public interface NormalizedFailureEventRepository
                extends JpaRepository<NormalizedFailureEventEntity, UUID> {

        Page<NormalizedFailureEventEntity> findByNormalizationStatusOrderByNormalizedAtDesc(
                        NormalizationStatus normalizationStatus,
                        Pageable pageable);

        @Query("""
                        select n
                        from NormalizedFailureEventEntity n
                        where n.normalizedServiceName = :serviceName
                                and n.normalizedEnvironment = :environment
                                and n.normalizedOccurredAt between :start and :end
                        """)
        Page<NormalizedFailureEventEntity> findByServiceNameAndEnvironmentWithinOccurredAtRange(
                        @Param("serviceName") String normalizedServiceName,
                        @Param("environment") String normalizedEnvironment,
                        @Param("start") Instant start,
                        @Param("end") Instant end,
                        Pageable pageable);

        @Query("""
                        select n
                        from NormalizedFailureEventEntity n
                        where n.normalizedEventType = :eventType
                                and n.normalizedOccurredAt between :start and :end
                        """)
        Page<NormalizedFailureEventEntity> findByEventTypeWithinOccurredAtRange(
                        @Param("eventType") String normalizedEventType,
                        @Param("start") Instant start,
                        @Param("end") Instant end,
                        Pageable pageable);

        @Query(value = """
                        select n
                        from NormalizedFailureEventEntity n
                        join fetch n.failureEvent
                        where (:serviceName is null or n.normalizedServiceName = :serviceName)
                                and (:environment is null or n.normalizedEnvironment = :environment)
                                and (:severity is null or n.normalizedSeverity = :severity)
                        """,
                        countQuery = """
                                        select count(n)
                                        from NormalizedFailureEventEntity n
                                        where (:serviceName is null or n.normalizedServiceName = :serviceName)
                                                and (:environment is null or n.normalizedEnvironment = :environment)
                                                and (:severity is null or n.normalizedSeverity = :severity)
                                        """)
        Page<NormalizedFailureEventEntity> searchByFailureDetails(
                        @Param("serviceName") String normalizedServiceName,
                        @Param("environment") String normalizedEnvironment,
                        @Param("severity") String normalizedSeverity,
                        Pageable pageable);
}
