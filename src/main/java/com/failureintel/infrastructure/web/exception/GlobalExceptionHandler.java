package com.failureintel.infrastructure.web.exception;

import jakarta.servlet.http.HttpServletRequest;

import java.time.Instant;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import org.springframework.dao.DataAccessException;
import org.springframework.http.*;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.MissingServletRequestParameterException;
import org.springframework.web.HttpRequestMethodNotSupportedException;

@RestControllerAdvice
public class GlobalExceptionHandler {

        private static final Logger log = LoggerFactory.getLogger(GlobalExceptionHandler.class);

        @ExceptionHandler(DataAccessException.class)
        public ResponseEntity<ApiErrorResponse> handleDataAccessException(
                        DataAccessException ex,
                        HttpServletRequest request) {

                log.error("DATABASE_ERROR | Path: {} | Method: {}",
                                request.getRequestURI(),
                                request.getMethod(),
                                ex);

                return build(HttpStatus.INTERNAL_SERVER_ERROR,
                                "DATABASE_ERROR",
                                "Database operation failed",
                                request);
        }

        @ExceptionHandler(MethodArgumentNotValidException.class)
        public ResponseEntity<ApiErrorResponse> handleValidationException(
                        MethodArgumentNotValidException ex,
                        HttpServletRequest request) {

                String errorMessage = ex.getBindingResult().getFieldErrors().stream()
                                .map(error -> error.getField() + ": " + error.getDefaultMessage())
                                .collect(Collectors.joining("; "));

                log.warn("VALIDATION_ERROR | {} | Path: {} | Method: {}",
                                errorMessage,
                                request.getRequestURI(),
                                request.getMethod());

                return build(HttpStatus.BAD_REQUEST,
                                "VALIDATION_ERROR",
                                errorMessage,
                                request);
        }

        @ExceptionHandler(MissingServletRequestParameterException.class)
        public ResponseEntity<ApiErrorResponse> handleMissingParameterException(
                        MissingServletRequestParameterException ex,
                        HttpServletRequest request) {

                String errorMessage = "Missing required parameter: " + ex.getParameterName();

                log.warn("MISSING_PARAMETER | {} | Path: {} | Method: {}",
                                errorMessage,
                                request.getRequestURI(),
                                request.getMethod());

                return build(HttpStatus.BAD_REQUEST,
                                "MISSING_PARAMETER",
                                errorMessage,
                                request);
        }

        @ExceptionHandler(HttpRequestMethodNotSupportedException.class)
        public ResponseEntity<ApiErrorResponse> handleMethodNotSupportedException(
                        HttpRequestMethodNotSupportedException ex,
                        HttpServletRequest request) {

                String errorMessage = "HTTP method not supported: " + ex.getMethod();

                log.warn("METHOD_NOT_SUPPORTED | {} | Path: {} | Method: {}",
                                errorMessage,
                                request.getRequestURI(),
                                request.getMethod());

                return build(HttpStatus.METHOD_NOT_ALLOWED,
                                "METHOD_NOT_SUPPORTED",
                                errorMessage,
                                request);
        }

        @ExceptionHandler(Exception.class)
        public ResponseEntity<ApiErrorResponse> handleGenericException(
                        Exception ex,
                        HttpServletRequest request) {

                log.error("INTERNAL_SERVER_ERROR | Path: {} | Method: {}",
                                request.getRequestURI(),
                                request.getMethod(),
                                ex);

                return build(HttpStatus.INTERNAL_SERVER_ERROR,
                                "INTERNAL_SERVER_ERROR",
                                "Something went wrong",
                                request);
        }

        private ResponseEntity<ApiErrorResponse> build(
                        HttpStatus status,
                        String error,
                        String message,
                        HttpServletRequest request) {

                ApiErrorResponse response = new ApiErrorResponse(
                                Instant.now(),
                                status.value(),
                                error,
                                message,
                                request.getRequestURI(),
                                MDC.get("correlationId"));

                return new ResponseEntity<>(response, status);
        }
}