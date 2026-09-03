package io.ledgermesh.order.saga;

import static org.assertj.core.api.Assertions.assertThat;

import io.ledgermesh.common.events.Topics;
import io.ledgermesh.common.idempotency.IdempotentConsumer;
import io.ledgermesh.common.outbox.OutboxEvent;
import io.ledgermesh.common.outbox.OutboxEventRepository;
import io.ledgermesh.order.domain.Order;
import io.ledgermesh.order.domain.OrderItem;
import io.ledgermesh.order.domain.OrderRepository;
import io.ledgermesh.order.domain.OrderStatus;
import io.ledgermesh.order.domain.SagaEvent;
import java.math.BigDecimal;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

@SpringBootTest
class OrderSagaServiceTest {

  @Autowired private OrderSagaService saga;
  @Autowired private OrderRepository orders;
  @Autowired private OutboxEventRepository outbox;
  @Autowired private IdempotentConsumer idempotent;

  @BeforeEach
  void clean() {
    outbox.deleteAll();
    orders.deleteAll();
  }

  @Test
  void createWritesOrderAndOutboxRowTogether() {
    Order order = saga.create("cust-1", List.of(new OrderItem("sku-1", 2, new BigDecimal("5.00"))));

    assertThat(order.getStatus()).isEqualTo(OrderStatus.PENDING);
    assertThat(order.getAmount()).isEqualByComparingTo("10.00");
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
    assertThat(orders.findById(order.getId()).orElseThrow().getStatus()).isEqualTo(OrderStatus.RESERVED);

    saga.apply(order.getId(), SagaEvent.PAYMENT_COMPLETED, "c");
    Order confirmed = orders.findById(order.getId()).orElseThrow();
    assertThat(confirmed.getStatus()).isEqualTo(OrderStatus.CONFIRMED);
    assertThat(outbox.findAll()).extracting(OutboxEvent::getTopic).containsExactly(Topics.ORDER_CREATED);
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

    boolean first = idempotent.once("order-service", "evt-9", () -> saga.apply(order.getId(), SagaEvent.PAYMENT_FAILED, "c"));
    boolean second = idempotent.once("order-service", "evt-9", () -> saga.apply(order.getId(), SagaEvent.PAYMENT_FAILED, "c"));

    assertThat(first).isTrue();
    assertThat(second).isFalse();
    assertThat(outbox.findAll()).filteredOn(r -> r.getTopic().equals(Topics.ORDER_CANCELLED)).hasSize(1);
  }

  @Test
  void eventsForUnknownOrdersAreIgnored() {
    assertThat(saga.apply("missing", SagaEvent.PAYMENT_COMPLETED, "c")).isEmpty();
  }
}
