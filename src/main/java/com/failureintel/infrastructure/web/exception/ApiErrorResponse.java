package com.failureintel.infrastructure.web.exception;

import java.time.Instant;

public class ApiErrorResponse {
    private final Instant timestamp;
    private final int status;
    private final String error;
    private final String message;
    private final String path;
    private final String correlationId;

    public ApiErrorResponse(Instant timestamp, int status, String error, String message, String path,
            String correlationId) {
        this.timestamp = timestamp;
        this.status = status;
        this.error = error;
        this.message = message;
        this.path = path;
        this.correlationId = correlationId;
    }

}
