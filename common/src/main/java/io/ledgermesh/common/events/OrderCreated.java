package io.ledgermesh.common.events;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;

public record OrderCreated(
    String eventId,
    String orderId,
    String correlationId,
    Instant occurredAt,
    String customerId,
    List<OrderLine> lines,
    BigDecimal amount)
    implements DomainEvent {

  @Override
  public String topic() {
    return Topics.ORDER_CREATED;
  }
}
