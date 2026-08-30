package com.failureintel.ingestion.application.useCase;

import com.failureintel.ingestion.api.dto.FailureEventIngestionRequest;

public interface IngestFailureEventUseCase {
    String ingestFailureEvent(FailureEventIngestionRequest request);
}