package io.ledgermesh.order.saga;

import io.ledgermesh.common.correlation.CorrelationId;
import io.ledgermesh.common.events.DomainEvent;
import io.ledgermesh.common.events.EventCodec;
import io.ledgermesh.common.events.InventoryRejected;
import io.ledgermesh.common.events.InventoryReserved;
import io.ledgermesh.common.events.PaymentCompleted;
import io.ledgermesh.common.events.PaymentFailed;
import io.ledgermesh.common.events.Topics;
import io.ledgermesh.common.idempotency.IdempotentConsumer;
import io.ledgermesh.order.domain.SagaEvent;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * Consumes reservation and payment outcomes. Offsets are committed per record after the handler
 * returns, so a process killed mid-flight sees the record again and the idempotent consumer
 * decides whether it still needs applying.
 */
@Component
public class SagaEventListener {

  public static final String CONSUMER = "order-service";

  private final EventCodec codec;
  private final IdempotentConsumer idempotent;
  private final OrderSagaService saga;

  public SagaEventListener(EventCodec codec, IdempotentConsumer idempotent, OrderSagaService saga) {
    this.codec = codec;
    this.idempotent = idempotent;
    this.saga = saga;
  }

  @KafkaListener(
      id = "order-saga",
      topics = {
        Topics.INVENTORY_RESERVED,
        Topics.INVENTORY_REJECTED,
        Topics.PAYMENT_COMPLETED,
        Topics.PAYMENT_FAILED
      },
      groupId = CONSUMER)
  public void on(ConsumerRecord<String, String> record) {
    DomainEvent event = decode(record);
    SagaEvent signal = signal(record.topic());
    String correlationId = CorrelationId.fromHeaders(record.headers(), event.correlationId());
    CorrelationId.bind(correlationId);
    try {
      idempotent.once(
          CONSUMER, event.eventId(), () -> saga.apply(event.orderId(), signal, correlationId));
    } finally {
      CorrelationId.clear();
    }
  }

  private DomainEvent decode(ConsumerRecord<String, String> record) {
    return switch (record.topic()) {
      case Topics.INVENTORY_RESERVED -> codec.decode(record.value(), InventoryReserved.class);
      case Topics.INVENTORY_REJECTED -> codec.decode(record.value(), InventoryRejected.class);
      case Topics.PAYMENT_COMPLETED -> codec.decode(record.value(), PaymentCompleted.class);
      case Topics.PAYMENT_FAILED -> codec.decode(record.value(), PaymentFailed.class);
      default -> throw new IllegalArgumentException("unexpected topic " + record.topic());
    };
  }

  private static SagaEvent signal(String topic) {
    return switch (topic) {
      case Topics.INVENTORY_RESERVED -> SagaEvent.INVENTORY_RESERVED;
      case Topics.INVENTORY_REJECTED -> SagaEvent.INVENTORY_REJECTED;
      case Topics.PAYMENT_COMPLETED -> SagaEvent.PAYMENT_COMPLETED;
      case Topics.PAYMENT_FAILED -> SagaEvent.PAYMENT_FAILED;
      default -> throw new IllegalArgumentException("unexpected topic " + topic);
    };
  }
}
