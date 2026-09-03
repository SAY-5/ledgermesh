package io.ledgermesh.common.correlation;

import static org.assertj.core.api.Assertions.assertThat;

import org.apache.kafka.common.header.internals.RecordHeaders;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

class CorrelationIdTest {

  @AfterEach
  void tearDown() {
    CorrelationId.clear();
  }

  @Test
  void readsHeaderWhenPresent() {
    RecordHeaders headers = new RecordHeaders();
    headers.add(CorrelationId.HEADER, "abc".getBytes());

    assertThat(CorrelationId.fromHeaders(headers, "fallback")).isEqualTo("abc");
  }

  @Test
  void fallsBackToPayloadValueOrMintsOne() {
    assertThat(CorrelationId.fromHeaders(new RecordHeaders(), "from-payload")).isEqualTo("from-payload");
    assertThat(CorrelationId.fromHeaders(new RecordHeaders(), null)).hasSize(36);
  }

  @Test
  void bindsToMdc() {
    CorrelationId.bind("xyz");
    assertThat(CorrelationId.current()).isEqualTo("xyz");
    CorrelationId.clear();
    assertThat(CorrelationId.current()).isNotEqualTo("xyz");
  }
}
