package io.ledgermesh.common.events;

import java.math.BigDecimal;
import java.time.Instant;

/**
 * Re-drive signal from the order service: a reserved order has waited past its payment deadline.
 * The payment service answers with the outcome it already has, or attempts the payment now.
 */
public record PaymentRequested(
    String eventId,
    String orderId,
    String correlationId,
    Instant occurredAt,
    String customerId,
    BigDecimal amount,
    int redrive)
    implements DomainEvent {

  @Override
  public String topic() {
    return Topics.PAYMENT_REQUESTED;
  }
}
