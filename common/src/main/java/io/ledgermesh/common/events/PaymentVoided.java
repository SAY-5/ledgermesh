package io.ledgermesh.common.events;

import java.time.Instant;

/**
 * The payment service's answer to {@code order.cancelled}: nothing is charged for the order any
 * more. {@code previous} is the state the payment was in when the cancellation reached it ({@code
 * NONE} when there was no payment yet), so a declined or already voided payment is acknowledged
 * too, and a cancellation the order service sends again is answered again.
 */
public record PaymentVoided(
    String eventId, String orderId, String correlationId, Instant occurredAt, String previous)
    implements DomainEvent {

  @Override
  public String topic() {
    return Topics.PAYMENT_VOIDED;
  }
}
