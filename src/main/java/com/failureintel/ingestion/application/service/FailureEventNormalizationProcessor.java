package com.failureintel.ingestion.application.service;

import com.failureintel.ingestion.application.exception.FailureEventNotFoundException;
import com.failureintel.ingestion.application.exception.StaleFailureEventClaimException;
import com.failureintel.ingestion.domain.model.NormalizedFailureEvent;
import com.failureintel.ingestion.domain.model.ParsedFailureEvent;
import com.failureintel.ingestion.domain.model.RawFailureEvent;
import com.failureintel.ingestion.domain.normalization.FailureEventNormalizer;
import com.failureintel.ingestion.domain.normalization.NormalizationStatus;
import com.failureintel.ingestion.domain.parser.FailureEventParser;
import com.failureintel.infrastructure.persistence.failureevent.entity.FailureEventEntity;
import com.failureintel.infrastructure.persistence.failureevent.entity.ProcessingStatus;
import com.failureintel.infrastructure.persistence.failureevent.mapper.FailureEventEntityMapper;
import com.failureintel.infrastructure.persistence.failureevent.repository.FailureEventRepository;
import com.failureintel.infrastructure.persistence.normalizedFailureEvent.entity.NormalizedFailureEventEntity;
import com.failureintel.infrastructure.persistence.normalizedFailureEvent.mapper.NormalizedFailureEventEntityMapper;
import com.failureintel.infrastructure.persistence.normalizedFailureEvent.repository.NormalizedFailureEventRepository;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.util.Objects;

@Service
public class FailureEventNormalizationProcessor {

    private static final String UNSUPPORTED_PAYLOAD_CODE = "UNSUPPORTED_PAYLOAD";
    private static final String MALFORMED_EVENT_CODE = "MALFORMED_EVENT";

    private final FailureEventRepository failureEventRepository;
    private final NormalizedFailureEventRepository normalizedFailureEventRepository;
    private final FailureEventParser failureEventParser;
    private final FailureEventNormalizer failureEventNormalizer;

    public FailureEventNormalizationProcessor(
            FailureEventRepository failureEventRepository,
            NormalizedFailureEventRepository normalizedFailureEventRepository,
            FailureEventParser failureEventParser,
            FailureEventNormalizer failureEventNormalizer) {
        this.failureEventRepository = failureEventRepository;
        this.normalizedFailureEventRepository = normalizedFailureEventRepository;
        this.failureEventParser = failureEventParser;
        this.failureEventNormalizer = failureEventNormalizer;
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

        RawFailureEvent rawFailureEvent = FailureEventEntityMapper.toRaw(failureEvent);
        if (!failureEventParser.supports(rawFailureEvent)) {
            failureEvent.markFailed(
                    UNSUPPORTED_PAYLOAD_CODE,
                    "No parser supports the persisted failure event");
            return;
        }

        ParsedFailureEvent parsedFailureEvent = failureEventParser.parse(rawFailureEvent);
        NormalizedFailureEvent normalizedFailureEvent =
                failureEventNormalizer.normalize(parsedFailureEvent);

        if (normalizedFailureEvent.getNormalizationStatus() == NormalizationStatus.MALFORMED) {
            failureEvent.markFailed(
                    MALFORMED_EVENT_CODE,
                    "Event does not contain minimum useful failure data");
            return;
        }

        NormalizedFailureEventEntity normalizedEntity = normalizedFailureEventRepository
                .findById(failureEvent.getEventId())
                .orElseGet(() -> NormalizedFailureEventEntityMapper.fromDomain(
                        normalizedFailureEvent,
                        failureEvent));

        normalizedEntity.applyNormalizedEvent(normalizedFailureEvent);
        normalizedEntity.setFailureReason(null);
        normalizedFailureEventRepository.save(normalizedEntity);

        failureEvent.markNormalized();

        // Surface deferred normalized persistence errors inside this
        // transaction while retaining atomic rollback with the raw status.
        normalizedFailureEventRepository.flush();
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
