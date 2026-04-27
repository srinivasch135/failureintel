package com.failureintel.infrastructure.observability.filter;

import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.springframework.web.filter.OncePerRequestFilter;
import java.io.IOException;
import java.util.UUID;

@Component
public class HttpLoggingFilter extends OncePerRequestFilter {
    private static final Logger log = LoggerFactory.getLogger(HttpLoggingFilter.class);

    @Override
    protected void doFilterInternal(HttpServletRequest request, HttpServletResponse response, FilterChain filterChain)
            throws ServletException, IOException {
        String method = request.getMethod();
        String URI = request.getRequestURI();
        long startTime = System.currentTimeMillis();
        log.info("HTTP Request started at {}: {} {}", startTime, method, URI);
        try {
            filterChain.doFilter(request, response);
        } finally {
            long duration = System.currentTimeMillis() - startTime;
            int status = response.getStatus();
            log.info("HTTP Request completed at {}: {} {}, Status: {}, Duration: {} ms",
                    System.currentTimeMillis(), method, URI, status, duration);
            if (status >= 500) {
                log.error("HTTP Request resulted in error at {}: {} {}, Status: {}, Duration: {} ms",
                        System.currentTimeMillis(), method, URI, status, duration);
            } else {
                log.info("HTTP Request completed successfully at {}: {} {}, Status: {}, Duration: {} ms",
                        System.currentTimeMillis(), method, URI, status, duration);
            }
        }

    }

}
