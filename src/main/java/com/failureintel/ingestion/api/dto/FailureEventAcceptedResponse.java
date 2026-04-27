package com.failureintel.ingestion.api.dto;

public class FailureEventAcceptedResponse {
    private String eventId;
    private String status;
    private String message;

    public FailureEventAcceptedResponse(String eventId, String status, String message) {
        this.eventId = eventId;
        this.status = status;
        this.message = message;

    }

    public String getEventId() {
        return eventId;
    }

    public String getStatus() {
        return status;
    }

    public String getMessage() {
        return message;
    }

}
