package io.ledgermesh.order.saga;

import io.ledgermesh.common.correlation.CorrelationId;
import io.ledgermesh.common.events.OrderCancelled;
import io.ledgermesh.common.events.OrderCreated;
import io.ledgermesh.common.events.OrderLine;
import io.ledgermesh.common.events.PaymentRequested;
import io.ledgermesh.common.outbox.OutboxWriter;
import io.ledgermesh.order.domain.Order;
import io.ledgermesh.order.domain.OrderEvent;
import io.ledgermesh.order.domain.OrderEventRepository;
import io.ledgermesh.order.domain.OrderItem;
import io.ledgermesh.order.domain.OrderRepository;
import io.ledgermesh.order.domain.OrderStateMachine;
import io.ledgermesh.order.domain.OrderStateMachine.Transition;
import io.ledgermesh.order.domain.OrderStatus;
import io.ledgermesh.order.domain.SagaEvent;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/**
 * Creates orders and applies saga events. Every state change, the event it emits and the timeline
 * row that records it are written in one transaction through the outbox.
 */
@Service
public class OrderSagaService {

  public static final String CREATED = "CREATED";
  public static final String PAYMENT_REDRIVEN = "PAYMENT_REDRIVEN";

  private static final Logger log = LoggerFactory.getLogger(OrderSagaService.class);

  private final OrderRepository orders;
  private final OrderEventRepository timeline;
  private final OutboxWriter outbox;
  private final SagaMetrics metrics;
  private final SagaTimeouts timeouts;
  private final Clock clock;

  public OrderSagaService(
      OrderRepository orders,
      OrderEventRepository timeline,
      OutboxWriter outbox,
      SagaMetrics metrics,
      SagaTimeouts timeouts,
      Clock clock) {
    this.orders = orders;
    this.timeline = timeline;
    this.outbox = outbox;
    this.metrics = metrics;
    this.timeouts = timeouts;
    this.clock = clock;
  }

  @Transactional
  public Order create(String customerId, List<OrderItem> items) {
    String correlationId = CorrelationId.current();
    Instant now = clock.instant();
    Order order =
        orders.save(
            new Order(
                UUID.randomUUID().toString(),
                customerId,
                items,
                correlationId,
                now,
                now.plus(timeouts.reservation())));
    outbox.append(
        new OrderCreated(
            UUID.randomUUID().toString(),
            order.getId(),
            correlationId,
            now,
            customerId,
            lines(order),
            order.getAmount()));
    timeline.save(
        new OrderEvent(
            order.getId(), CREATED, null, OrderStatus.PENDING, null, correlationId, now));
    metrics.transition(order.getStatus());
    log.info("order {} created for {} amount {}", order.getId(), customerId, order.getAmount());
    return order;
  }

  @Transactional(readOnly = true)
  public Optional<Order> find(String id) {
    return orders.findById(id);
  }

  @Transactional(readOnly = true)
  public Optional<List<OrderEvent>> timeline(String id) {
    if (!orders.existsById(id)) {
      return Optional.empty();
    }
    return Optional.of(timeline.findByOrderIdOrderByIdAsc(id));
  }

  /** Applies a saga event. Unknown orders are ignored; they cannot belong to this service. */
  @Transactional
  public Optional<Transition> apply(String orderId, SagaEvent event, String correlationId) {
    Optional<Order> found = orders.findById(orderId);
    if (found.isEmpty()) {
      log.warn("event {} for unknown order {} ignored", event, orderId);
      return Optional.empty();
    }
    Order order = found.get();
    Instant now = clock.instant();
    Optional<Transition> transition = OrderStateMachine.apply(order.getStatus(), event);
    if (transition.isEmpty()) {
      timeline.save(
          new OrderEvent(orderId, event.name(), order.getStatus(), null, null, correlationId, now));
      log.info("event {} ignored for order {} in state {}", event, orderId, order.getStatus());
      return Optional.empty();
    }
    Transition t = transition.get();
    OrderStatus from = order.getStatus();
    order.transition(t.to(), t.reason(), now, deadlineFor(t.to(), now));
    timeline.save(
        new OrderEvent(orderId, event.name(), from, t.to(), t.reason(), correlationId, now));
    metrics.transition(t.to());
    if (t.releaseInventory()) {
      outbox.append(
          new OrderCancelled(
              UUID.randomUUID().toString(), orderId, correlationId, now, t.reason(), lines(order)));
    }
    if (t.to().isTerminal()) {
      metrics.completed(Duration.between(order.getCreatedAt(), now));
    }
    log.info("order {} moved to {} on {}", orderId, t.to(), event);
    return transition;
  }

  /**
   * Asks the payment service again for a reserved order whose payment deadline passed. Returns
   * false when the order is not reserved any more or has used up its re-drives.
   */
  @Transactional
  public boolean redrivePayment(String orderId) {
    Order order = orders.findById(orderId).orElse(null);
    if (order == null
        || order.getStatus() != OrderStatus.RESERVED
        || order.getRedrives() >= timeouts.maxRedrives()) {
      return false;
    }
    Instant now = clock.instant();
    order.redriven(now, now.plus(timeouts.payment()));
    outbox.append(
        new PaymentRequested(
            UUID.randomUUID().toString(),
            orderId,
            order.getCorrelationId(),
            now,
            order.getCustomerId(),
            order.getAmount(),
            order.getRedrives()));
    timeline.save(
        new OrderEvent(
            orderId,
            PAYMENT_REDRIVEN,
            OrderStatus.RESERVED,
            OrderStatus.RESERVED,
            null,
            order.getCorrelationId(),
            now));
    metrics.redriven();
    log.warn(
        "order {} payment re-driven ({} of {})",
        orderId,
        order.getRedrives(),
        timeouts.maxRedrives());
    return true;
  }

  private Instant deadlineFor(OrderStatus status, Instant now) {
    return switch (status) {
      case PENDING -> now.plus(timeouts.reservation());
      case RESERVED -> now.plus(timeouts.payment());
      case CONFIRMED, CANCELLED -> null;
    };
  }

  private static List<OrderLine> lines(Order order) {
    return order.getItems().stream().map(i -> new OrderLine(i.getSku(), i.getQuantity())).toList();
  }
}
