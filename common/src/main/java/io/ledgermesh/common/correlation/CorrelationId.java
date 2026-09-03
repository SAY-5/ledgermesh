package io.ledgermesh.common.correlation;

import java.nio.charset.StandardCharsets;
import java.util.UUID;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.slf4j.MDC;

/** Correlation id propagation across HTTP, Kafka headers and log lines. */
public final class CorrelationId {

  public static final String HEADER = "x-correlation-id";
  public static final String MDC_KEY = "correlationId";

  private CorrelationId() {}

  public static String generate() {
    return UUID.randomUUID().toString();
  }

  public static String current() {
    String value = MDC.get(MDC_KEY);
    return value == null ? generate() : value;
  }

  public static String orGenerate(String candidate) {
    return candidate == null || candidate.isBlank() ? generate() : candidate;
  }

  public static void bind(String value) {
    MDC.put(MDC_KEY, value);
  }

  public static void clear() {
    MDC.remove(MDC_KEY);
  }

  public static String fromHeaders(Headers headers, String fallback) {
    Header header = headers.lastHeader(HEADER);
    if (header == null || header.value() == null) {
      return orGenerate(fallback);
    }
    return new String(header.value(), StandardCharsets.UTF_8);
  }
}
