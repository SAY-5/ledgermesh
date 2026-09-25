package io.ledgermesh.inventory.messaging;

import io.ledgermesh.common.correlation.CorrelationId;
import io.ledgermesh.common.events.EventCodec;
import io.ledgermesh.common.events.InventoryRejected;
import io.ledgermesh.common.events.InventoryReserved;
import io.ledgermesh.common.events.OrderCancelled;
import io.ledgermesh.common.events.OrderCreated;
import io.ledgermesh.common.events.Topics;
import io.ledgermesh.common.idempotency.IdempotentConsumer;
import io.ledgermesh.common.outbox.OutboxWriter;
import io.ledgermesh.inventory.stock.ReservationService;
import io.ledgermesh.inventory.stock.ReservationService.Outcome;
import io.ledgermesh.inventory.stock.ReservationService.Rejected;
import io.micrometer.core.instrument.MeterRegistry;
import java.time.Clock;
import java.util.UUID;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * Reserves stock for new orders and releases it for cancelled ones. The reservation, the outcome
 * event and the processed marker share one transaction.
 */
@Component
public class InventoryEventListener {

  public static final String CONSUMER = "inventory-service";

  private static final Logger log = LoggerFactory.getLogger(InventoryEventListener.class);

  private final EventCodec codec;
  private final IdempotentConsumer idempotent;
  private final ReservationService reservations;
  private final OutboxWriter outbox;
  private final Clock clock;
  private final MeterRegistry meters;

  public InventoryEventListener(
      EventCodec codec,
      IdempotentConsumer idempotent,
      ReservationService reservations,
      OutboxWriter outbox,
      Clock clock,
      MeterRegistry meters) {
    this.codec = codec;
    this.idempotent = idempotent;
    this.reservations = reservations;
    this.outbox = outbox;
    this.clock = clock;
    this.meters = meters;
  }

  @KafkaListener(id = "inventory-orders", topics = Topics.ORDER_CREATED, groupId = CONSUMER)
  public void onOrderCreated(ConsumerRecord<String, String> record) {
    OrderCreated event = codec.decode(record.value(), OrderCreated.class);
    String correlationId = CorrelationId.fromHeaders(record.headers(), event.correlationId());
    CorrelationId.bind(correlationId);
    try {
      idempotent.once(CONSUMER, event.eventId(), () -> reserve(event, correlationId));
    } finally {
      CorrelationId.clear();
    }
  }

  @KafkaListener(id = "inventory-cancels", topics = Topics.ORDER_CANCELLED, groupId = CONSUMER)
  public void onOrderCancelled(ConsumerRecord<String, String> record) {
    OrderCancelled event = codec.decode(record.value(), OrderCancelled.class);
    CorrelationId.bind(CorrelationId.fromHeaders(record.headers(), event.correlationId()));
    try {
      idempotent.once(
          CONSUMER,
          event.eventId(),
          () -> {
            int released = reservations.release(event.orderId(), event.lines());
            meters
                .counter(
                    "ledgermesh.inventory.releases", "result", released > 0 ? "released" : "noop")
                .increment();
            log.info(
                "released {} unit(s) for order {} ({})", released, event.orderId(), event.reason());
          });
    } finally {
      CorrelationId.clear();
    }
  }

  private void reserve(OrderCreated event, String correlationId) {
    Outcome outcome = reservations.reserve(event.orderId(), event.lines());
    if (outcome instanceof Rejected rejected) {
      outbox.append(
          new InventoryRejected(
              UUID.randomUUID().toString(),
              event.orderId(),
              correlationId,
              clock.instant(),
              rejected.reason()));
      meters.counter("ledgermesh.inventory.reservations", "result", "rejected").increment();
      log.info("order {} rejected: {}", event.orderId(), rejected.reason());
      return;
    }
    outbox.append(
        new InventoryReserved(
            UUID.randomUUID().toString(),
            event.orderId(),
            correlationId,
            clock.instant(),
            event.customerId(),
            event.lines(),
            event.amount()));
    meters.counter("ledgermesh.inventory.reservations", "result", "reserved").increment();
    log.info("order {} reserved", event.orderId());
  }
}
