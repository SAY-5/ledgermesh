package io.ledgermesh.order.saga;

import static org.assertj.core.api.Assertions.assertThat;

import io.ledgermesh.common.events.Topics;
import io.ledgermesh.common.outbox.OutboxEvent;
import io.ledgermesh.common.outbox.OutboxEventRepository;
import io.ledgermesh.order.TestClock;
import io.ledgermesh.order.TestClockConfig;
import io.ledgermesh.order.domain.Order;
import io.ledgermesh.order.domain.OrderEvent;
import io.ledgermesh.order.domain.OrderEventRepository;
import io.ledgermesh.order.domain.OrderItem;
import io.ledgermesh.order.domain.OrderRepository;
import io.ledgermesh.order.domain.OrderStatus;
import io.ledgermesh.order.domain.SagaEvent;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import java.math.BigDecimal;
import java.time.Clock;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;

@SpringBootTest(properties = "ledgermesh.saga.reaper=true")
@Import(TestClockConfig.class)
class StuckOrderReaperTest {

  @Autowired private StuckOrderReaper reaper;
  @Autowired private OrderSagaService saga;
  @Autowired private OrderRepository orders;
  @Autowired private OrderEventRepository timeline;
  @Autowired private OutboxEventRepository outbox;
  @Autowired private Clock clock;
  @Autowired private MeterRegistry meters;

  @BeforeEach
  void clean() {
    outbox.deleteAll();
    timeline.deleteAll();
    orders.deleteAll();
  }

  @Test
  void reservationThatNeverAnswersIsCancelledAndStockReleased() {
    Order order = saga.create("cust-1", List.of(new OrderItem("sku-1", 2, new BigDecimal("5.00"))));

    advance(Duration.ofSeconds(31));
    assertThat(reaper.reap()).isEqualTo(1);

    Order cancelled = orders.findById(order.getId()).orElseThrow();
    assertThat(cancelled.getStatus()).isEqualTo(OrderStatus.CANCELLED);
    assertThat(cancelled.getReason()).isEqualTo("RESERVATION_TIMEOUT");
    assertThat(cancelled.getDeadlineAt()).isNull();
    assertThat(outbox.findAll())
        .extracting(OutboxEvent::getTopic)
        .containsExactly(Topics.ORDER_CREATED, Topics.ORDER_CANCELLED);
    assertThat(outbox.findAll().get(1).getPayload()).contains("\"quantity\":2");
    assertThat(reaper.reap()).isZero();
  }

  @Test
  void paymentPastDeadlineIsRedrivenOnceThenCancelled() {
    Order order = saga.create("cust-1", List.of(new OrderItem("sku-1", 1, new BigDecimal("5.00"))));
    saga.apply(order.getId(), SagaEvent.INVENTORY_RESERVED, "c");
    double redrives = redrives();
    Timer latency = meters.get("ledgermesh.saga.latency").timer();
    long completed = latency.count();
    double seconds = latency.totalTime(TimeUnit.SECONDS);

    advance(Duration.ofSeconds(46));
    assertThat(reaper.reap()).isEqualTo(1);
    Order redriven = orders.findById(order.getId()).orElseThrow();
    assertThat(redriven.getStatus()).isEqualTo(OrderStatus.RESERVED);
    assertThat(redriven.getRedrives()).isEqualTo(1);
    assertThat(redriven.getDeadlineAt()).isEqualTo(clock.instant().plusSeconds(45));
    assertThat(outbox.findAll())
        .extracting(OutboxEvent::getTopic)
        .containsExactly(Topics.ORDER_CREATED, Topics.PAYMENT_REQUESTED);
    assertThat(outbox.findAll().get(1).getPayload()).contains("\"redrive\":1");
    assertThat(redrives()).isEqualTo(redrives + 1);
    assertThat(latency.count()).isEqualTo(completed);

    advance(Duration.ofSeconds(46));
    assertThat(reaper.reap()).isEqualTo(1);
    Order cancelled = orders.findById(order.getId()).orElseThrow();
    assertThat(cancelled.getStatus()).isEqualTo(OrderStatus.CANCELLED);
    assertThat(cancelled.getReason()).isEqualTo("PAYMENT_TIMEOUT");
    assertThat(outbox.findAll())
        .extracting(OutboxEvent::getTopic)
        .containsExactly(Topics.ORDER_CREATED, Topics.PAYMENT_REQUESTED, Topics.ORDER_CANCELLED);
    assertThat(redrives()).isEqualTo(redrives + 1);
    // the saga ended 92 seconds after the order was created
    assertThat(latency.count()).isEqualTo(completed + 1);
    assertThat(latency.totalTime(TimeUnit.SECONDS)).isEqualTo(seconds + 92);
  }

  @Test
  void ordersInsideTheirDeadlineAreLeftAlone() {
    Order pending =
        saga.create("cust-1", List.of(new OrderItem("sku-1", 1, new BigDecimal("5.00"))));
    Order reserved =
        saga.create("cust-2", List.of(new OrderItem("sku-1", 1, new BigDecimal("5.00"))));
    saga.apply(reserved.getId(), SagaEvent.INVENTORY_RESERVED, "c");

    advance(Duration.ofSeconds(29));
    assertThat(reaper.reap()).isZero();
    assertThat(orders.findById(pending.getId()).orElseThrow().getStatus())
        .isEqualTo(OrderStatus.PENDING);
    assertThat(orders.findById(reserved.getId()).orElseThrow().getStatus())
        .isEqualTo(OrderStatus.RESERVED);
  }

  @Test
  void paymentAnsweringDuringTheRedriveWindowStillConfirms() {
    Order order = saga.create("cust-1", List.of(new OrderItem("sku-1", 1, new BigDecimal("5.00"))));
    saga.apply(order.getId(), SagaEvent.INVENTORY_RESERVED, "c");
    advance(Duration.ofSeconds(46));
    reaper.reap();

    saga.apply(order.getId(), SagaEvent.PAYMENT_COMPLETED, "c");

    assertThat(orders.findById(order.getId()).orElseThrow().getStatus())
        .isEqualTo(OrderStatus.CONFIRMED);
    advance(Duration.ofSeconds(60));
    assertThat(reaper.reap()).isZero();
  }

  private double redrives() {
    return meters.counter("ledgermesh.saga.redrives").count();
  }

  @Test
  void aCancellationThePaymentServiceDoesNotAnswerIsSentAgainUntilItIs() {
    Order order = saga.create("cust-1", List.of(new OrderItem("sku-1", 2, new BigDecimal("5.00"))));
    advance(Duration.ofSeconds(31));
    assertThat(reaper.reap()).isEqualTo(1);
    Order cancelled = orders.findById(order.getId()).orElseThrow();
    assertThat(cancelled.getCompensationDueAt()).isEqualTo(clock.instant().plusSeconds(60));

    advance(Duration.ofSeconds(59));
    assertThat(reaper.reap()).isZero();
    advance(Duration.ofSeconds(1));
    assertThat(reaper.reap()).isEqualTo(1);
    advance(Duration.ofSeconds(60));
    assertThat(reaper.reap()).isEqualTo(1);

    List<OutboxEvent> cancellations =
        outbox.findAll().stream().filter(r -> r.getTopic().equals(Topics.ORDER_CANCELLED)).toList();
    assertThat(cancellations).hasSize(3);
    assertThat(cancellations).extracting(OutboxEvent::getEventId).doesNotHaveDuplicates();
    assertThat(cancellations).allMatch(r -> r.getPayload().contains("\"quantity\":2"));
    assertThat(timeline.findByOrderIdOrderByIdAsc(order.getId()))
        .extracting(OrderEvent::getType)
        .containsSubsequence(
            "RESERVATION_TIMEOUT", "CANCELLATION_SENT_AGAIN", "CANCELLATION_SENT_AGAIN");

    assertThat(saga.compensated(order.getId(), "AUTHORIZED", "c")).isTrue();
    advance(Duration.ofSeconds(600));
    assertThat(reaper.reap()).isZero();
    Order answered = orders.findById(order.getId()).orElseThrow();
    assertThat(answered.getCompensatedAt()).isNotNull();
    assertThat(answered.getCompensationDueAt()).isNull();
  }

  @Test
  void anOrderCancelledForStockWaitsForNoAnswer() {
    Order order = saga.create("cust-1", List.of(new OrderItem("sku-1", 1, new BigDecimal("5.00"))));
    saga.apply(order.getId(), SagaEvent.INVENTORY_REJECTED, "c");

    advance(Duration.ofSeconds(600));
    assertThat(reaper.reap()).isZero();
    assertThat(orders.findById(order.getId()).orElseThrow().getCompensationDueAt()).isNull();
    assertThat(saga.resendCancellation(order.getId())).isFalse();
  }

  private void advance(Duration by) {
    ((TestClock) clock).advance(by);
  }
}
