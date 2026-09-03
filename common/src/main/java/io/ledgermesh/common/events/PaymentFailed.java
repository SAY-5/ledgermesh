package io.ledgermesh.common.events;

import java.time.Instant;

public record PaymentFailed(
    String eventId, String orderId, String correlationId, Instant occurredAt, String reason)
    implements DomainEvent {

  @Override
  public String topic() {
    return Topics.PAYMENT_FAILED;
  }
}
