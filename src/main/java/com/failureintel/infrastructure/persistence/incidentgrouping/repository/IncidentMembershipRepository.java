package com.failureintel.infrastructure.persistence.incidentgrouping.repository;

import com.failureintel.infrastructure.persistence.incidentgrouping.entity.IncidentMembershipEntity;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;
import java.util.UUID;

public interface IncidentMembershipRepository extends JpaRepository<IncidentMembershipEntity, UUID> {
    List<IncidentMembershipEntity> findByIncidentId(UUID incidentId);
}
