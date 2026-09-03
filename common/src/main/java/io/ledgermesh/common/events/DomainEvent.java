package io.ledgermesh.common.events;

import java.time.Instant;

/**
 * Every message on the bus carries a globally unique event id (used by idempotent consumers), the
 * order it belongs to (used as the Kafka key so one order is always handled in sequence) and the
 * correlation id of the originating request.
 */
public sealed interface DomainEvent
    permits OrderCreated,
        OrderCancelled,
        InventoryReserved,
        InventoryRejected,
        PaymentCompleted,
        PaymentFailed {

  String eventId();

  String orderId();

  String correlationId();

  Instant occurredAt();

  String topic();
}
