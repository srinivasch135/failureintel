package com.failureintel.infrastructure.persistence.incidentgrouping.entity;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

import java.util.UUID;

@Entity
@Table(name = "incident_membership")
public class IncidentMembershipEntity {
    @Id
    @Column(name = "event_id", nullable = false)
    private UUID eventId;

    @Column(name = "incident_id", nullable = false)
    private UUID incidentId;

    protected IncidentMembershipEntity() {}

    public IncidentMembershipEntity(UUID eventId, UUID incidentId) {
        this.eventId = eventId;
        this.incidentId = incidentId;
    }

    public UUID getEventId() { return eventId; }
    public UUID getIncidentId() { return incidentId; }
}
