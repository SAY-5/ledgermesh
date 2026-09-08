package io.ledgermesh.payment.messaging;

import io.ledgermesh.common.correlation.CorrelationId;
import io.ledgermesh.common.events.EventCodec;
import io.ledgermesh.common.events.InventoryReserved;
import io.ledgermesh.common.events.PaymentRequested;
import io.ledgermesh.common.events.Topics;
import io.ledgermesh.common.idempotency.IdempotentConsumer;
import io.ledgermesh.payment.authorize.PaymentService;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * Records the payment idempotently, then authorizes it. Re-drive requests from the order service
 * are answered with the outcome on file or a fresh attempt.
 */
@Component
public class PaymentEventListener {

  public static final String CONSUMER = "payment-service";

  private final EventCodec codec;
  private final IdempotentConsumer idempotent;
  private final PaymentService payments;

  public PaymentEventListener(
      EventCodec codec, IdempotentConsumer idempotent, PaymentService payments) {
    this.codec = codec;
    this.idempotent = idempotent;
    this.payments = payments;
  }

  @KafkaListener(id = "payment-reserved", topics = Topics.INVENTORY_RESERVED, groupId = CONSUMER)
  public void onInventoryReserved(ConsumerRecord<String, String> record) {
    InventoryReserved event = codec.decode(record.value(), InventoryReserved.class);
    String correlationId = CorrelationId.fromHeaders(record.headers(), event.correlationId());
    CorrelationId.bind(correlationId);
    try {
      boolean recorded =
          idempotent.once(CONSUMER, event.eventId(), () -> payments.record(event, correlationId));
      if (recorded) {
        payments.attempt(event.orderId());
      }
    } finally {
      CorrelationId.clear();
    }
  }

  @KafkaListener(id = "payment-requested", topics = Topics.PAYMENT_REQUESTED, groupId = CONSUMER)
  public void onPaymentRequested(ConsumerRecord<String, String> record) {
    PaymentRequested event = codec.decode(record.value(), PaymentRequested.class);
    String correlationId = CorrelationId.fromHeaders(record.headers(), event.correlationId());
    CorrelationId.bind(correlationId);
    try {
      idempotent.once(CONSUMER, event.eventId(), () -> payments.requestAgain(event, correlationId));
    } finally {
      CorrelationId.clear();
    }
  }
}
