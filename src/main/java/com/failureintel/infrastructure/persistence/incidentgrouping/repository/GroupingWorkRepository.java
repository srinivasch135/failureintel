package com.failureintel.infrastructure.persistence.incidentgrouping.repository;

import com.failureintel.infrastructure.persistence.incidentgrouping.entity.GroupingWorkEntity;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.UUID;

public interface GroupingWorkRepository extends JpaRepository<GroupingWorkEntity, UUID> {
}
