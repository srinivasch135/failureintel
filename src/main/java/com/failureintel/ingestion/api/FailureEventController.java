package com.failureintel.ingestion.api;

import com.failureintel.ingestion.api.dto.FailureEventAcceptedResponse;
import com.failureintel.ingestion.api.dto.FailureEventIngestionRequest;
import com.failureintel.ingestion.application.IngestFailureEventUseCase;
import jakarta.validation.Valid;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping(path = "/api/v1/failure-events", produces = MediaType.APPLICATION_JSON_VALUE)
public class FailureEventController {
    private static final Logger logger = LoggerFactory.getLogger(FailureEventController.class);
    private final IngestFailureEventUseCase ingestFailureEventUseCase;

    public FailureEventController(IngestFailureEventUseCase ingestFailureEventUseCase) {
        this.ingestFailureEventUseCase = ingestFailureEventUseCase;
    }

    @PostMapping(consumes = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<FailureEventAcceptedResponse> ingestFailureEvent(
            @Valid @RequestBody FailureEventIngestionRequest request) {
        logger.info("Received failure event ingestion request for service={} environment={} eventType={}",
                request.getServiceName(), request.getEnvironment(), request.getEventType());
        String eventId = ingestFailureEventUseCase.ingestFailureEvent(request);
        FailureEventAcceptedResponse response = new FailureEventAcceptedResponse(eventId, "ACCEPTED",
                "Failure event accepted");
        return ResponseEntity.status(HttpStatus.ACCEPTED).body(response);
    }
}