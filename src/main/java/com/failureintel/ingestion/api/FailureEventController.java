package com.failureintel.ingestion.api;

import com.failureintel.ingestion.api.dto.FailureEventAcceptedResponse;
import com.failureintel.ingestion.api.dto.FailureEventIngestionRequest;
import com.failureintel.ingestion.api.dto.FailureEventResponse;
import com.failureintel.ingestion.api.dto.PageResponse;
import com.failureintel.ingestion.application.service.FailureEventQueryService;
import com.failureintel.ingestion.application.useCase.IngestFailureEventUseCase;
import jakarta.validation.Valid;
import jakarta.validation.constraints.Size;
import org.springframework.data.domain.Pageable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.validation.annotation.Validated;

import java.util.UUID;

@RestController
@Validated
@RequestMapping(path = "/api/v1/failure-events", produces = MediaType.APPLICATION_JSON_VALUE)
public class FailureEventController {
    private static final Logger logger = LoggerFactory.getLogger(FailureEventController.class);
    private static final String ACCEPTED_STATUS = "RECEIVED";
    private static final String ACCEPTED_MESSAGE = "Failure event accepted for processing";

    private final IngestFailureEventUseCase ingestFailureEventUseCase;
    private final FailureEventQueryService failureEventQueryService;

    public FailureEventController(
            IngestFailureEventUseCase ingestFailureEventUseCase,
            FailureEventQueryService failureEventQueryService) {
        this.ingestFailureEventUseCase = ingestFailureEventUseCase;
        this.failureEventQueryService = failureEventQueryService;
    }

    @PostMapping(consumes = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<FailureEventAcceptedResponse> ingestFailureEvent(
            @Valid @RequestBody FailureEventIngestionRequest request,
            @RequestHeader(value = "Idempotency-Key", required = false)
            @Size(max = FailureEventIngestionRequest.MAX_IDEMPOTENCY_KEY_LENGTH,
                    message = "Idempotency key must not exceed 251 characters") String idempotencyKey) {
        if (idempotencyKey != null && !idempotencyKey.isBlank()) {
            request.setIdempotencyKey(idempotencyKey);
        }
        logger.info("Received failure event ingestion request for service={} environment={} eventType={}",
                request.getServiceName(), request.getEnvironment(), request.getEventType());
        String eventId = ingestFailureEventUseCase.ingestFailureEvent(request);
        FailureEventAcceptedResponse response = new FailureEventAcceptedResponse(
                eventId,
                ACCEPTED_STATUS,
                ACCEPTED_MESSAGE);
        return ResponseEntity.status(HttpStatus.ACCEPTED).body(response);
    }

    @GetMapping("/{eventId}")
    public ResponseEntity<FailureEventResponse> getFailureEvent(@PathVariable UUID eventId) {
        return ResponseEntity.ok(failureEventQueryService.getFailureEvent(eventId));
    }

    @GetMapping("/by-trace-id/{traceId}")
    public ResponseEntity<FailureEventResponse> getFailureEventByTraceId(@PathVariable String traceId) {
        return ResponseEntity.ok(failureEventQueryService.getFailureEventByTraceId(traceId));
    }

    @GetMapping("/search")
    public ResponseEntity<PageResponse<FailureEventResponse>> searchFailureEvents(
            @RequestParam(required = false) String serviceName,
            @RequestParam(required = false) String environment,
            @RequestParam(required = false) String severity,
            Pageable pageable) {
        return ResponseEntity.ok(PageResponse.from(
                failureEventQueryService.searchFailureEvents(
                        serviceName,
                        environment,
                        severity,
                        pageable)));
    }
}
