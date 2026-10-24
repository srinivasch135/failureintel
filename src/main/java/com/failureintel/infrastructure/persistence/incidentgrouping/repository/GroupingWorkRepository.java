package com.failureintel.infrastructure.persistence.incidentgrouping.repository;

import com.failureintel.infrastructure.persistence.incidentgrouping.entity.GroupingWorkEntity;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.util.UUID;

public interface GroupingWorkRepository extends JpaRepository<GroupingWorkEntity, UUID> {

    @Modifying
    @Query(value = "INSERT INTO grouping_work (event_id) VALUES (:eventId) "
            + "ON CONFLICT (event_id) DO NOTHING", nativeQuery = true)
    int insertPendingIfAbsent(@Param("eventId") UUID eventId);
}
