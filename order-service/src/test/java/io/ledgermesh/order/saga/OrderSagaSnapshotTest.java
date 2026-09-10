package io.ledgermesh.order.saga;

import static org.assertj.core.api.Assertions.assertThat;

import io.ledgermesh.common.ops.SagaSnapshot.Sagas;
import io.ledgermesh.common.outbox.OutboxEventRepository;
import io.ledgermesh.order.TestClock;
import io.ledgermesh.order.TestClockConfig;
import io.ledgermesh.order.domain.OrderEventRepository;
import io.ledgermesh.order.domain.OrderItem;
import io.ledgermesh.order.domain.OrderRepository;
import io.ledgermesh.order.domain.SagaEvent;
import java.math.BigDecimal;
import java.time.Clock;
import java.time.Duration;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;

@SpringBootTest
@Import(TestClockConfig.class)
class OrderSagaSnapshotTest {

  @Autowired private OrderSagaSnapshot snapshot;
  @Autowired private OrderSagaService saga;
  @Autowired private OrderRepository orders;
  @Autowired private OrderEventRepository timeline;
  @Autowired private OutboxEventRepository outbox;
  @Autowired private Clock clock;

  @BeforeEach
  void clean() {
    outbox.deleteAll();
    timeline.deleteAll();
    orders.deleteAll();
  }

  @Test
  void countsOpenOrdersAndTheOnesPastTheirDeadline() {
    String pending = place();
    String reserved = place();
    saga.apply(reserved, SagaEvent.INVENTORY_RESERVED, "c");

    Sagas open = snapshot.sagas();
    assertThat(open.inFlight()).isEqualTo(2);
    assertThat(open.stuck()).isZero();
    assertThat(open.byState()).containsEntry("PENDING", 1L).containsEntry("RESERVED", 1L);

    ((TestClock) clock).advance(Duration.ofSeconds(31));
    assertThat(snapshot.sagas().stuck()).isEqualTo(1);

    saga.apply(pending, SagaEvent.INVENTORY_REJECTED, "c");
    saga.apply(reserved, SagaEvent.PAYMENT_COMPLETED, "c");

    Sagas settled = snapshot.sagas();
    assertThat(settled.inFlight()).isZero();
    assertThat(settled.stuck()).isZero();
    assertThat(settled.byState()).containsEntry("CONFIRMED", 1L).containsEntry("CANCELLED", 1L);
  }

  private String place() {
    return saga.create("cust-s", List.of(new OrderItem("sku-1", 1, new BigDecimal("4.00"))))
        .getId();
  }
}
