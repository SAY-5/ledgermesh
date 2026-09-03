package io.ledgermesh.common.correlation;

import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.io.IOException;
import org.springframework.web.filter.OncePerRequestFilter;

/** Reads or mints a correlation id per request and echoes it back on the response. */
public class CorrelationIdFilter extends OncePerRequestFilter {

  @Override
  protected void doFilterInternal(
      HttpServletRequest request, HttpServletResponse response, FilterChain chain)
      throws ServletException, IOException {
    String id = CorrelationId.orGenerate(request.getHeader(CorrelationId.HEADER));
    CorrelationId.bind(id);
    response.setHeader(CorrelationId.HEADER, id);
    try {
      chain.doFilter(request, response);
    } finally {
      CorrelationId.clear();
    }
  }
}
