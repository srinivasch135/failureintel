package com.failureintel.ingestion.application;

import com.failureintel.ingestion.api.dto.FailureEventIngestionRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import java.util.UUID;

@Service
public class FailureEventIngestionService implements IngestFailureEventUseCase {
    private static final Logger logger = LoggerFactory.getLogger(FailureEventIngestionService.class);

    @Override
    public String ingestFailureEvent(FailureEventIngestionRequest request) {
        String eventId = UUID.randomUUID().toString();
        logger.info("Accepted failure event for service={} environment={} eventType={} eventId={}",
                request.getServiceName(),
                request.getEnvironment(),
                request.getEventType(),
                eventId);
        return eventId;

    }

}
