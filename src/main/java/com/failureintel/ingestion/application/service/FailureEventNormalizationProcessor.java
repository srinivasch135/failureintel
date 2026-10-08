package com.failureintel.ingestion.application.service;

import com.failureintel.ingestion.application.exception.FailureEventNotFoundException;
import com.failureintel.ingestion.application.exception.StaleFailureEventClaimException;
import com.failureintel.ingestion.domain.model.NormalizedFailureEvent;
import com.failureintel.ingestion.domain.model.ParsedFailureEvent;
import com.failureintel.ingestion.domain.model.RawFailureEvent;
import com.failureintel.ingestion.domain.normalization.FailureEventNormalizer;
import com.failureintel.ingestion.domain.normalization.NormalizationStatus;
import com.failureintel.ingestion.domain.parser.FailureEventParser;
import com.failureintel.infrastructure.monitoring.metrics.FailureEventMetrics;
import com.failureintel.infrastructure.persistence.failureevent.entity.FailureEventEntity;
import com.failureintel.infrastructure.persistence.failureevent.entity.ProcessingStatus;
import com.failureintel.infrastructure.persistence.failureevent.mapper.FailureEventEntityMapper;
import com.failureintel.infrastructure.persistence.failureevent.mapper.FailureEventEntityMapper.MalformedRawPayloadException;
import com.failureintel.infrastructure.persistence.failureevent.repository.FailureEventRepository;
import com.failureintel.infrastructure.persistence.normalizedFailureEvent.entity.NormalizedFailureEventEntity;
import com.failureintel.infrastructure.persistence.normalizedFailureEvent.mapper.NormalizedFailureEventEntityMapper;
import com.failureintel.infrastructure.persistence.normalizedFailureEvent.repository.NormalizedFailureEventRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;

import java.util.Objects;

@Service
public class FailureEventNormalizationProcessor {

    private static final Logger LOGGER = LoggerFactory.getLogger(FailureEventNormalizationProcessor.class);

    private static final String UNSUPPORTED_PAYLOAD_CODE = "UNSUPPORTED_PAYLOAD";
    private static final String MALFORMED_EVENT_CODE = "MALFORMED_EVENT";
    private static final String INSUFFICIENT_FAILURE_DATA_CODE = "INSUFFICIENT_FAILURE_DATA";

    private final FailureEventRepository failureEventRepository;
    private final NormalizedFailureEventRepository normalizedFailureEventRepository;
    private final FailureEventParser failureEventParser;
    private final FailureEventNormalizer failureEventNormalizer;
    private final FailureEventMetrics failureEventMetrics;

    public FailureEventNormalizationProcessor(
            FailureEventRepository failureEventRepository,
            NormalizedFailureEventRepository normalizedFailureEventRepository,
            FailureEventParser failureEventParser,
            FailureEventNormalizer failureEventNormalizer,
            FailureEventMetrics failureEventMetrics) {
        this.failureEventRepository = failureEventRepository;
        this.normalizedFailureEventRepository = normalizedFailureEventRepository;
        this.failureEventParser = failureEventParser;
        this.failureEventNormalizer = failureEventNormalizer;
        this.failureEventMetrics = failureEventMetrics;
    }

    /**
     * Processes one already-claimed event. The claim transaction has ended
     * before this method is called; this transaction owns only this event's
     * parse, normalization, and persistence attempt.
     */
    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public void process(ClaimedFailureEvent claim) {
        Objects.requireNonNull(claim, "claim must not be null");

        FailureEventEntity failureEvent = failureEventRepository.findById(claim.eventId())
                .orElseThrow(() -> new FailureEventNotFoundException(claim.eventId()));

        verifyClaimOwnership(claim, failureEvent);

        RawFailureEvent rawFailureEvent;
        try {
            rawFailureEvent = FailureEventEntityMapper.toRaw(failureEvent);
        } catch (MalformedRawPayloadException malformedPayload) {
            markPermanentlyFailed(failureEvent,
                    MALFORMED_EVENT_CODE,
                    "Persisted raw payload could not be decoded");
            return;
        }

        if (!failureEventParser.supports(rawFailureEvent)) {
            markPermanentlyFailed(failureEvent,
                    UNSUPPORTED_PAYLOAD_CODE,
                    "No parser supports the persisted failure event");
            return;
        }

        ParsedFailureEvent parsedFailureEvent = failureEventParser.parse(rawFailureEvent);
        if (parsedFailureEvent.isMalformed()) {
            markPermanentlyFailed(failureEvent,
                    MALFORMED_EVENT_CODE,
                    "Persisted failure event is malformed");
            return;
        }

        if (!parsedFailureEvent.hasMinimumUsefulData()) {
            markPermanentlyFailed(failureEvent,
                    INSUFFICIENT_FAILURE_DATA_CODE,
                    "Event does not contain minimum useful failure data");
            return;
        }

        NormalizedFailureEvent normalizedFailureEvent =
                failureEventNormalizer.normalize(parsedFailureEvent);

        if (normalizedFailureEvent.getNormalizationStatus() == NormalizationStatus.MALFORMED) {
            markPermanentlyFailed(failureEvent,
                    MALFORMED_EVENT_CODE,
                    "Normalization determined the parsed failure event is malformed");
            return;
        }

        NormalizedFailureEventEntity normalizedEntity = normalizedFailureEventRepository
                .findById(failureEvent.getEventId())
                .map(existingEntity -> {
                    existingEntity.applyNormalizedEvent(normalizedFailureEvent);
                    return existingEntity;
                })
                .orElseGet(() -> NormalizedFailureEventEntityMapper.fromDomain(
                        normalizedFailureEvent,
                        failureEvent));

        normalizedEntity.setFailureReason(null);
        normalizedFailureEventRepository.save(normalizedEntity);

        failureEvent.markNormalized();

        // Surface deferred normalized persistence errors inside this
        // transaction while retaining atomic rollback with the raw status.
        normalizedFailureEventRepository.flush();
        recordAfterCommit(failureEventMetrics::recordNormalized);
    }

    private void markPermanentlyFailed(FailureEventEntity failureEvent, String code, String reason) {
        failureEvent.markFailed(code, reason);
        recordAfterCommit(() -> failureEventMetrics.recordFailed(
                FailureEventMetrics.FailureSource.PROCESSING));
    }

    private void recordAfterCommit(Runnable metricRecording) {
        if (!TransactionSynchronizationManager.isSynchronizationActive()) {
            return;
        }
        try {
            TransactionSynchronizationManager.registerSynchronization(new TransactionSynchronization() {
                @Override
                public void afterCommit() {
                    metricRecording.run();
                }
            });
        } catch (RuntimeException metricsFailure) {
            LOGGER.warn("Unable to register committed normalization outcome metric", metricsFailure);
        }
    }

    private void verifyClaimOwnership(
            ClaimedFailureEvent claim,
            FailureEventEntity failureEvent) {
        if (failureEvent.getProcessingStatus() != ProcessingStatus.PROCESSING
                || !Objects.equals(failureEvent.getAttemptCount(), claim.attemptNumber())) {
            throw new StaleFailureEventClaimException(claim, failureEvent);
        }
    }
}
