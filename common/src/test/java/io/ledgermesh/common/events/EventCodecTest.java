package io.ledgermesh.common.events;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;
import org.junit.jupiter.api.Test;

class EventCodecTest {

  private final EventCodec codec =
      new EventCodec(new ObjectMapper().registerModule(new JavaTimeModule()));

  @Test
  void roundTripsAnOrderCreatedEvent() {
    OrderCreated event =
        new OrderCreated(
            "e1",
            "o1",
            "c1",
            Instant.parse("2026-01-01T00:00:00Z"),
            "cust-7",
            List.of(new OrderLine("sku-1", 2)),
            new BigDecimal("19.90"));

    String json = codec.encode(event);
    OrderCreated back = codec.decode(json, OrderCreated.class);

    assertThat(back).isEqualTo(event);
    assertThat(back.topic()).isEqualTo(Topics.ORDER_CREATED);
  }

  @Test
  void rejectsMalformedPayloadAsNonRetryable() {
    assertThatThrownBy(() -> codec.decode("{not json", PaymentFailed.class))
        .isInstanceOf(IllegalArgumentException.class);
  }
}
