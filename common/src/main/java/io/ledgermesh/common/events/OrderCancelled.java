package io.ledgermesh.common.events;

import java.time.Instant;
import java.util.List;

/** Compensation event: inventory releases the reservation for the listed lines. */
public record OrderCancelled(
    String eventId,
    String orderId,
    String correlationId,
    Instant occurredAt,
    String reason,
    List<OrderLine> lines)
    implements DomainEvent {

  @Override
  public String topic() {
    return Topics.ORDER_CANCELLED;
  }
}
