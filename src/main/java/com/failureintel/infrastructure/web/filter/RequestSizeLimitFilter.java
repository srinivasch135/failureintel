package com.failureintel.infrastructure.web.filter;

import jakarta.servlet.FilterChain;
import jakarta.servlet.ReadListener;
import jakarta.servlet.ServletException;
import jakarta.servlet.ServletInputStream;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletRequestWrapper;
import jakarta.servlet.http.HttpServletResponse;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Component;
import org.springframework.web.filter.OncePerRequestFilter;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Locale;

@Component
public class RequestSizeLimitFilter extends OncePerRequestFilter {

    private static final String INGESTION_PATH = "/api/v1/failure-events";

    private final int maxRequestSizeBytes;

    public RequestSizeLimitFilter(
            @Value("${failure-event.ingestion.max-request-size-bytes}") int maxRequestSizeBytes) {
        if (maxRequestSizeBytes <= 0 || maxRequestSizeBytes == Integer.MAX_VALUE) {
            throw new IllegalArgumentException("Request size limit must be positive and below Integer.MAX_VALUE");
        }
        this.maxRequestSizeBytes = maxRequestSizeBytes;
    }

    @Override
    protected void doFilterInternal(
            HttpServletRequest request,
            HttpServletResponse response,
            FilterChain filterChain) throws ServletException, IOException {
        if (!isJsonIngestionRequest(request)) {
            filterChain.doFilter(request, response);
            return;
        }

        if (request.getContentLengthLong() > maxRequestSizeBytes) {
            reject(response);
            return;
        }

        byte[] requestBody = request.getInputStream().readNBytes(maxRequestSizeBytes + 1);
        if (requestBody.length > maxRequestSizeBytes) {
            reject(response);
            return;
        }

        filterChain.doFilter(new BufferedRequest(request, requestBody), response);
    }

    private boolean isJsonIngestionRequest(HttpServletRequest request) {
        String contentType = request.getContentType();
        return "POST".equalsIgnoreCase(request.getMethod())
                && (request.getContextPath() + INGESTION_PATH).equals(request.getRequestURI())
                && contentType != null
                && contentType.toLowerCase(Locale.ROOT).startsWith("application/json");
    }

    private void reject(HttpServletResponse response) throws IOException {
        response.sendError(
                HttpStatus.PAYLOAD_TOO_LARGE.value(),
                "Failure event request body exceeds the configured size limit");
    }

    private static final class BufferedRequest extends HttpServletRequestWrapper {
        private final byte[] body;
        private ServletInputStream inputStream;

        private BufferedRequest(HttpServletRequest request, byte[] body) {
            super(request);
            this.body = body;
        }

        @Override
        public int getContentLength() {
            return body.length;
        }

        @Override
        public long getContentLengthLong() {
            return body.length;
        }

        @Override
        public ServletInputStream getInputStream() {
            if (inputStream == null) {
                ByteArrayInputStream bodyStream = new ByteArrayInputStream(body);
                inputStream = new ServletInputStream() {
                    @Override
                    public int read() {
                        return bodyStream.read();
                    }

                    @Override
                    public int read(byte[] buffer, int offset, int length) {
                        return bodyStream.read(buffer, offset, length);
                    }

                    @Override
                    public boolean isFinished() {
                        return bodyStream.available() == 0;
                    }

                    @Override
                    public boolean isReady() {
                        return true;
                    }

                    @Override
                    public void setReadListener(ReadListener readListener) {
                        throw new IllegalStateException("Asynchronous body reads are not supported");
                    }
                };
            }
            return inputStream;
        }

        @Override
        public BufferedReader getReader() throws IOException {
            String encoding = getCharacterEncoding();
            Charset charset = encoding == null
                    ? StandardCharsets.ISO_8859_1
                    : Charset.forName(encoding);
            return new BufferedReader(new InputStreamReader(getInputStream(), charset));
        }
    }
}
