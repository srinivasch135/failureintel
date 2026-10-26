package com.failureintel.ingestion.application.service;

/** Processing boundary for Task 06; implementations must fence work with the supplied claim generation. */
@FunctionalInterface
public interface GroupingWorkProcessor {
    void process(ClaimedGroupingWork claim);
}
