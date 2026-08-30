package com.failureintel.ingestion.domain.parser;

import com.failureintel.ingestion.domain.model.ParsedFailureEvent;
import com.failureintel.ingestion.domain.model.RawFailureEvent;

public interface FailureEventParser {
    boolean supports(RawFailureEvent rawEvent);

    ParsedFailureEvent parse(RawFailureEvent rawFailureEvent);
}