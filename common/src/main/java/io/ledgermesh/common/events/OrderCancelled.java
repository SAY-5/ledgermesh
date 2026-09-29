package io.ledgermesh.common.events;

import java.time.Instant;
import java.util.List;

/**
 * Compensation event: inventory releases the reservation for the listed lines, and payment voids
 * the order's payment and answers with {@code payment.voided}. The order service sends it again,
 * under a fresh event id, until that answer arrives.
 */
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
