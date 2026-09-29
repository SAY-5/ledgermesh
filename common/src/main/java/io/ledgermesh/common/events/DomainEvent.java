package io.ledgermesh.common.events;

import java.time.Instant;

/**
 * Every message on the bus carries a globally unique event id (used by idempotent consumers), the
 * order it belongs to (used as the Kafka key, so one order's events on one topic are handled in
 * sequence; its events on different topics are not, since each topic has listener threads of its
 * own) and the correlation id of the originating request.
 */
public sealed interface DomainEvent
    permits OrderCreated,
        OrderCancelled,
        PaymentRequested,
        InventoryReserved,
        InventoryRejected,
        PaymentCompleted,
        PaymentFailed,
        PaymentVoided {

  String eventId();

  String orderId();

  String correlationId();

  Instant occurredAt();

  String topic();
}
