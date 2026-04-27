package com.failureintel.ingestion.application;

import com.failureintel.ingestion.api.dto.FailureEventIngestionRequest;

public interface IngestFailureEventUseCase {
    String ingestFailureEvent(FailureEventIngestionRequest request);
}