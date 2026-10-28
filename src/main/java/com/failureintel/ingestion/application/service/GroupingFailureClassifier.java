package com.failureintel.ingestion.application.service;

import org.springframework.dao.DataAccessResourceFailureException;
import org.springframework.dao.PessimisticLockingFailureException;
import org.springframework.dao.QueryTimeoutException;
import org.springframework.dao.TransientDataAccessException;
import org.springframework.stereotype.Component;

import java.sql.SQLException;

/** Classifies technical grouping failures without exposing exception text. */
@Component
public class GroupingFailureClassifier {
    public Failure classify(Throwable failure) {
        boolean transientFailure = false;
        String code = "GROUPING_FAILURE";
        for (Throwable cause = failure; cause != null; cause = cause.getCause()) {
            if (cause instanceof QueryTimeoutException) {
                transientFailure = true;
                code = "GROUPING_DATABASE_TIMEOUT";
            } else if (cause instanceof PessimisticLockingFailureException) {
                transientFailure = true;
                code = "GROUPING_DATABASE_CONFLICT";
            } else if (cause instanceof DataAccessResourceFailureException) {
                transientFailure = true;
                code = "GROUPING_DATABASE_UNAVAILABLE";
            } else if (cause instanceof TransientDataAccessException) {
                transientFailure = true;
                code = "GROUPING_TRANSIENT_FAILURE";
            }
            if (cause instanceof SQLException sqlException && isTransientState(sqlException.getSQLState())) {
                transientFailure = true;
                code = "GROUPING_DATABASE_TRANSIENT";
            }
        }
        return new Failure(transientFailure, code,
                transientFailure ? "Transient technical grouping failure" : "Permanent technical grouping failure");
    }

    private boolean isTransientState(String state) {
        return state != null && (state.startsWith("08") || state.startsWith("40")
                || state.equals("53300") || state.equals("53400") || state.equals("55P03")
                || state.equals("57014") || state.equals("57P01") || state.equals("57P02")
                || state.equals("57P03"));
    }

    public record Failure(boolean transientFailure, String code, String reason) {}
}
