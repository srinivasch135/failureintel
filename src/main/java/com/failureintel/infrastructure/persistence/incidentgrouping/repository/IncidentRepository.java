package com.failureintel.infrastructure.persistence.incidentgrouping.repository;

import com.failureintel.infrastructure.persistence.incidentgrouping.entity.IncidentEntity;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.util.List;
import java.util.UUID;

public interface IncidentRepository extends JpaRepository<IncidentEntity, UUID> {
    @Query("""
            select i from IncidentEntity i
            where i.serviceName = :serviceName
              and i.environment = :environment
              and i.errorType = :errorType
              and ((i.dependencyTarget is null and :dependencyTarget is null)
                   or i.dependencyTarget = :dependencyTarget)
            order by i.firstOccurredAt desc, i.incidentId
            """)
    List<IncidentEntity> findHistoryBySignature(
            @Param("serviceName") String serviceName,
            @Param("environment") String environment,
            @Param("errorType") String errorType,
            @Param("dependencyTarget") String dependencyTarget);
}
