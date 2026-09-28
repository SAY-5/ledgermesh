package io.ledgermesh.order.saga;

import static org.assertj.core.api.Assertions.assertThat;

import io.ledgermesh.common.events.Topics;
import io.ledgermesh.common.idempotency.IdempotentConsumer;
import io.ledgermesh.common.outbox.OutboxEvent;
import io.ledgermesh.common.outbox.OutboxEventRepository;
import io.ledgermesh.order.domain.Order;
import io.ledgermesh.order.domain.OrderEvent;
import io.ledgermesh.order.domain.OrderEventRepository;
import io.ledgermesh.order.domain.OrderItem;
import io.ledgermesh.order.domain.OrderRepository;
import io.ledgermesh.order.domain.OrderStatus;
import io.ledgermesh.order.domain.SagaEvent;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import java.math.BigDecimal;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.system.CapturedOutput;
import org.springframework.boot.test.system.OutputCaptureExtension;
import org.springframework.transaction.support.TransactionTemplate;

@SpringBootTest
@ExtendWith(OutputCaptureExtension.class)
class OrderSagaServiceTest {

  @Autowired private OrderSagaService saga;
  @Autowired private OrderRepository orders;
  @Autowired private OutboxEventRepository outbox;
  @Autowired private OrderEventRepository timeline;
  @Autowired private IdempotentConsumer idempotent;
  @Autowired private TransactionTemplate tx;
  @Autowired private MeterRegistry meters;

  @BeforeEach
  void clean() {
    outbox.deleteAll();
    timeline.deleteAll();
    orders.deleteAll();
  }

  @Test
  void createWritesOrderAndOutboxRowTogether() {
    Order order = saga.create("cust-1", List.of(new OrderItem("sku-1", 2, new BigDecimal("5.00"))));

    assertThat(order.getStatus()).isEqualTo(OrderStatus.PENDING);
    assertThat(order.getAmount()).isEqualByComparingTo("10.00");
    assertThat(order.getDeadlineAt()).isEqualTo(order.getCreatedAt().plusSeconds(30));
    List<OutboxEvent> rows = outbox.findAll();
    assertThat(rows).hasSize(1);
    assertThat(rows.get(0).getTopic()).isEqualTo(Topics.ORDER_CREATED);
    assertThat(rows.get(0).getMessageKey()).isEqualTo(order.getId());
    assertThat(rows.get(0).getPayload()).contains("\"sku\":\"sku-1\"");
  }

  @Test
  void reservationThenPaymentConfirmsTheOrder() {
    Order order = saga.create("cust-1", List.of(new OrderItem("sku-1", 1, new BigDecimal("5.00"))));

    saga.apply(order.getId(), SagaEvent.INVENTORY_RESERVED, "c");
    assertThat(orders.findById(order.getId()).orElseThrow().getStatus())
        .isEqualTo(OrderStatus.RESERVED);

    saga.apply(order.getId(), SagaEvent.PAYMENT_COMPLETED, "c");
    Order confirmed = orders.findById(order.getId()).orElseThrow();
    assertThat(confirmed.getStatus()).isEqualTo(OrderStatus.CONFIRMED);
    assertThat(outbox.findAll())
        .extracting(OutboxEvent::getTopic)
        .containsExactly(Topics.ORDER_CREATED);
  }

  @Test
  void declinedPaymentCancelsAndEmitsCompensation() {
    Order order = saga.create("cust-1", List.of(new OrderItem("sku-1", 3, new BigDecimal("5.00"))));
    saga.apply(order.getId(), SagaEvent.INVENTORY_RESERVED, "c");

    saga.apply(order.getId(), SagaEvent.PAYMENT_FAILED, "c");

    Order cancelled = orders.findById(order.getId()).orElseThrow();
    assertThat(cancelled.getStatus()).isEqualTo(OrderStatus.CANCELLED);
    assertThat(cancelled.getReason()).isEqualTo("PAYMENT_DECLINED");
    assertThat(outbox.findAll())
        .extracting(OutboxEvent::getTopic)
        .containsExactly(Topics.ORDER_CREATED, Topics.ORDER_CANCELLED);
    assertThat(outbox.findAll().get(1).getPayload()).contains("\"quantity\":3");
  }

  @Test
  void redeliveredEventIsAppliedOnlyOnce() {
    Order order = saga.create("cust-1", List.of(new OrderItem("sku-1", 1, new BigDecimal("5.00"))));
    saga.apply(order.getId(), SagaEvent.INVENTORY_RESERVED, "c");

    boolean first =
        idempotent.once(
            "order-service",
            "evt-9",
            () -> saga.apply(order.getId(), SagaEvent.PAYMENT_FAILED, "c"));
    boolean second =
        idempotent.once(
            "order-service",
            "evt-9",
            () -> saga.apply(order.getId(), SagaEvent.PAYMENT_FAILED, "c"));

    assertThat(first).isTrue();
    assertThat(second).isFalse();
    assertThat(outbox.findAll())
        .filteredOn(r -> r.getTopic().equals(Topics.ORDER_CANCELLED))
        .hasSize(1);
  }

  @Test
  void eventsForUnknownOrdersAreIgnored() {
    assertThat(saga.apply("missing", SagaEvent.PAYMENT_COMPLETED, "c")).isEmpty();
    assertThat(saga.timeline("missing")).isEmpty();
  }

  @Test
  void timelineRecordsEveryStepIncludingIgnoredEvents() {
    Order order = saga.create("cust-1", List.of(new OrderItem("sku-1", 1, new BigDecimal("5.00"))));
    saga.apply(order.getId(), SagaEvent.INVENTORY_RESERVED, "c-1");
    saga.apply(order.getId(), SagaEvent.INVENTORY_RESERVED, "c-2");
    saga.apply(order.getId(), SagaEvent.PAYMENT_COMPLETED, "c-3");

    List<OrderEvent> events = saga.timeline(order.getId()).orElseThrow();

    assertThat(events)
        .extracting(OrderEvent::getType)
        .containsExactly(
            "CREATED", "INVENTORY_RESERVED", "INVENTORY_RESERVED", "PAYMENT_COMPLETED");
    assertThat(events)
        .extracting(OrderEvent::getToStatus)
        .containsExactly(OrderStatus.PENDING, OrderStatus.RESERVED, null, OrderStatus.CONFIRMED);
    assertThat(events.get(2).getFromStatus()).isEqualTo(OrderStatus.RESERVED);
    assertThat(events.get(3).getCorrelationId()).isEqualTo("c-3");
    assertThat(events).allSatisfy(e -> assertThat(e.getOccurredAt()).isNotNull());
    assertThat(orders.findById(order.getId()).orElseThrow().getDeadlineAt()).isNull();
  }

  @Test
  void aChangeThatRollsBackIsNeitherCountedNorLogged(CapturedOutput output) {
    List<OrderItem> items = List.of(new OrderItem("sku-1", 1, new BigDecimal("5.00")));
    double created = transitions(OrderStatus.PENDING);
    double reserved = transitions(OrderStatus.RESERVED);
    AtomicReference<String> rolledBack = new AtomicReference<>();

    // rolled back after the order was placed and reserved, the way the loser of a race on one
    // idempotency key rolls back the order it placed
    tx.executeWithoutResult(
        status -> {
          Order order = saga.create("cust-rolled-back", items);
          saga.apply(order.getId(), SagaEvent.INVENTORY_RESERVED, "c");
          rolledBack.set(order.getId());
          status.setRollbackOnly();
        });

    assertThat(orders.count()).isZero();
    assertThat(transitions(OrderStatus.PENDING)).isEqualTo(created);
    assertThat(transitions(OrderStatus.RESERVED)).isEqualTo(reserved);
    assertThat(output).doesNotContain("order " + rolledBack.get());

    Order kept = saga.create("cust-committed", items);
    saga.apply(kept.getId(), SagaEvent.INVENTORY_RESERVED, "c");

    assertThat(transitions(OrderStatus.PENDING)).isEqualTo(created + 1);
    assertThat(transitions(OrderStatus.RESERVED)).isEqualTo(reserved + 1);
    assertThat(output)
        .contains("order " + kept.getId() + " created for cust-committed")
        .contains("order " + kept.getId() + " moved to RESERVED");
  }

  private double transitions(OrderStatus to) {
    Counter counter =
        meters.find("ledgermesh.orders.transitions").tag("to", to.name()).counter();
    return counter == null ? 0 : counter.count();
  }
}
