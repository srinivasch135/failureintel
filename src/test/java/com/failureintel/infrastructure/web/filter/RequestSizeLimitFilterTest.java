package com.failureintel.infrastructure.web.filter;

import jakarta.servlet.FilterChain;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletRequestWrapper;
import org.junit.jupiter.api.Test;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.mock.web.MockHttpServletResponse;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RequestSizeLimitFilterTest {

    private static final int MAX_REQUEST_SIZE_BYTES = 4096;

    private final RequestSizeLimitFilter filter = new RequestSizeLimitFilter(MAX_REQUEST_SIZE_BYTES);

    @Test
    void shouldRejectOversizedJsonBodyWhenContentLengthIsUnknown() throws Exception {
        MockHttpServletRequest request = new MockHttpServletRequest("POST", "/api/v1/failure-events");
        request.setContentType("application/json");
        request.setContent("x".repeat(MAX_REQUEST_SIZE_BYTES + 1).getBytes(StandardCharsets.UTF_8));

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
}
