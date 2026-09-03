package io.ledgermesh.common.events;

import java.time.Instant;

public record PaymentCompleted(
    String eventId,
    String orderId,
    String correlationId,
    Instant occurredAt,
    String authorizationCode)
    implements DomainEvent {

  @Override
  public String topic() {
    return Topics.PAYMENT_COMPLETED;
  }
}
