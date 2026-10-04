package com.failureintel.infrastructure.web.filter;

import jakarta.servlet.FilterChain;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletRequestWrapper;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.mock.web.MockHttpServletResponse;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RequestSizeLimitFilterTest {

    private static final int MAX_REQUEST_SIZE_BYTES = 4096;

    private final RequestSizeLimitFilter filter = new RequestSizeLimitFilter(MAX_REQUEST_SIZE_BYTES);

    @ParameterizedTest
    @ValueSource(strings = {
            "/api/v1/failure-events;probe=1",
            "/api/v1/failure-events;",
            "/api/v1/failure-events;probe=1;mode=strict",
            "/api;tenant=one/v1/failure-events",
            "/api/v1;tenant=one/failure-events",
            "/api;one=1/v1;two=2/failure-events;three=3",
            "/api/v1/failure-events%3Bprobe=1",
            "/api/v1/failure-events%3bprobe=1",
            "/api/v1/%66ailure-events;probe=1",
            "/%61pi;tenant=one/v1/failure-events%3Bprobe=1"
    })
    void shouldRejectPathParameterVariantsBeforeCallingTheFilterChain(String requestUri) throws Exception {
        MockHttpServletRequest request = jsonPost(requestUri, "{}");
        MockHttpServletResponse response = new MockHttpServletResponse();
        AtomicBoolean filterChainCalled = new AtomicBoolean();

        filter.doFilter(request, response, recordingChain(filterChainCalled));

        assertEquals(400, response.getStatus());
        assertFalse(filterChainCalled.get());
        assertTrue(response.getErrorMessage().contains("Path parameters are not supported"));
    }

    @Test
    void shouldRejectPathParameterVariantWhenApplicationUsesAContextPath() throws Exception {
        MockHttpServletRequest request = new MockHttpServletRequest(
                "POST",
                "/failureintel/api/v1/failure-events;probe=1");
        request.setContextPath("/failureintel");
        MockHttpServletResponse response = new MockHttpServletResponse();
        AtomicBoolean filterChainCalled = new AtomicBoolean();

        filter.doFilter(request, response, recordingChain(filterChainCalled));

        assertEquals(400, response.getStatus());
        assertFalse(filterChainCalled.get());
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "/api/v1/failure-events-extra;probe=1",
            "/api/v1/failure-events/child;probe=1",
            "/api/v1/other;probe=1"
    })
    void shouldNotTreatSimilarPathsAsTheIngestionEndpoint(String requestUri) throws Exception {
        MockHttpServletRequest request = jsonPost(
                requestUri,
                "x".repeat(MAX_REQUEST_SIZE_BYTES + 1));
        MockHttpServletResponse response = new MockHttpServletResponse();
        AtomicBoolean filterChainCalled = new AtomicBoolean();

        filter.doFilter(request, response, recordingChain(filterChainCalled));

        assertEquals(200, response.getStatus());
        assertTrue(filterChainCalled.get());
    }

    @Test
    void shouldAllowCanonicalJsonBodyExactlyAtTheConfiguredLimitAndReplayIt() throws Exception {
        String body = "x".repeat(MAX_REQUEST_SIZE_BYTES);
        MockHttpServletRequest request = jsonPost("/api/v1/failure-events", body);
        MockHttpServletResponse response = new MockHttpServletResponse();
        AtomicReference<String> replayedBody = new AtomicReference<>();
        AtomicReference<Long> replayedContentLength = new AtomicReference<>();
        FilterChain filterChain = (requestInChain, responseInChain) -> {
            HttpServletRequest replayableRequest = (HttpServletRequest) requestInChain;
            replayedBody.set(new String(
                    replayableRequest.getInputStream().readAllBytes(),
                    StandardCharsets.UTF_8));
            replayedContentLength.set(replayableRequest.getContentLengthLong());
        };

        filter.doFilter(request, response, filterChain);

        assertEquals(200, response.getStatus());
        assertEquals(body, replayedBody.get());
        assertEquals((long) MAX_REQUEST_SIZE_BYTES, replayedContentLength.get());
    }

    @Test
    void shouldRejectDeclaredOversizedBodyBeforeCallingTheFilterChain() throws Exception {
        MockHttpServletRequest request = jsonPost(
                "/api/v1/failure-events",
                "x".repeat(MAX_REQUEST_SIZE_BYTES + 1));
        MockHttpServletResponse response = new MockHttpServletResponse();
        AtomicBoolean filterChainCalled = new AtomicBoolean();

        filter.doFilter(request, response, recordingChain(filterChainCalled));

        assertEquals(413, response.getStatus());
        assertFalse(filterChainCalled.get());
    }

    @Test
    void shouldLeaveNonPostPathParameterRequestOutsideIngestionFiltering() throws Exception {
        MockHttpServletRequest request = new MockHttpServletRequest(
                "GET",
                "/api/v1/failure-events;probe=1");
        MockHttpServletResponse response = new MockHttpServletResponse();
        AtomicBoolean filterChainCalled = new AtomicBoolean();

        filter.doFilter(request, response, recordingChain(filterChainCalled));

        assertEquals(200, response.getStatus());
        assertTrue(filterChainCalled.get());
    }

    @Test
    void shouldLeaveCanonicalPostWithUnsupportedContentTypeForMvcToReject() throws Exception {
        MockHttpServletRequest request = new MockHttpServletRequest("POST", "/api/v1/failure-events");
        request.setContentType("text/plain");
        request.setContent("x".repeat(MAX_REQUEST_SIZE_BYTES + 1).getBytes(StandardCharsets.UTF_8));
        MockHttpServletResponse response = new MockHttpServletResponse();
        AtomicBoolean filterChainCalled = new AtomicBoolean();

        filter.doFilter(request, response, recordingChain(filterChainCalled));

        assertEquals(200, response.getStatus());
        assertTrue(filterChainCalled.get());
    }

    @Test
    void shouldRejectOversizedJsonBodyWhenContentLengthIsUnknown() throws Exception {
        MockHttpServletRequest request = jsonPost(
                "/api/v1/failure-events",
                "x".repeat(MAX_REQUEST_SIZE_BYTES + 1));

        HttpServletRequest requestWithoutContentLength = new HttpServletRequestWrapper(request) {
            @Override
            public int getContentLength() {
                return -1;
            }

            @Override
            public long getContentLengthLong() {
                return -1;
            }
        };
        MockHttpServletResponse response = new MockHttpServletResponse();
        AtomicBoolean filterChainCalled = new AtomicBoolean();
        FilterChain filterChain = (requestInChain, responseInChain) -> filterChainCalled.set(true);

        filter.doFilter(requestWithoutContentLength, response, filterChain);

        assertEquals(413, response.getStatus());
        assertFalse(filterChainCalled.get());
        assertTrue(response.getErrorMessage().contains("configured size limit"));
    }

    private MockHttpServletRequest jsonPost(String requestUri, String body) {
        MockHttpServletRequest request = new MockHttpServletRequest("POST", requestUri);
        request.setContentType("application/json;charset=UTF-8");
        request.setContent(body.getBytes(StandardCharsets.UTF_8));
        return request;
    }

    @ParameterizedTest
    @ValueSource(strings = {"", "/failureintel"})
    void shouldEnforceTheByteLimitOnEncodedPathsWithOrWithoutAContextPath(String contextPath)
            throws Exception {
        MockHttpServletRequest request = jsonPost(
                contextPath + "/%61pi/v%31/%66ailure%2devents",
                "x".repeat(MAX_REQUEST_SIZE_BYTES + 1));
        request.setContextPath(contextPath);
        MockHttpServletResponse response = new MockHttpServletResponse();
        AtomicBoolean filterChainCalled = new AtomicBoolean();

        filter.doFilter(request, response, recordingChain(filterChainCalled));

        assertEquals(413, response.getStatus());
        assertFalse(filterChainCalled.get());
    }

    private FilterChain recordingChain(AtomicBoolean called) {
        return (request, response) -> called.set(true);
    }
}
