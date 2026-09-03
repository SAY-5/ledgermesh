package io.ledgermesh.order.saga;

import io.ledgermesh.common.correlation.CorrelationId;
import io.ledgermesh.common.events.OrderCancelled;
import io.ledgermesh.common.events.OrderCreated;
import io.ledgermesh.common.events.OrderLine;
import io.ledgermesh.common.outbox.OutboxWriter;
import io.ledgermesh.order.domain.Order;
import io.ledgermesh.order.domain.OrderItem;
import io.ledgermesh.order.domain.OrderRepository;
import io.ledgermesh.order.domain.OrderStateMachine;
import io.ledgermesh.order.domain.OrderStateMachine.Transition;
import io.ledgermesh.order.domain.SagaEvent;
import java.time.Clock;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/**
 * Creates orders and applies saga events. Every state change and the event it emits are written
 * in one transaction through the outbox.
 */
@Service
public class OrderSagaService {

  private static final Logger log = LoggerFactory.getLogger(OrderSagaService.class);

  private final OrderRepository orders;
  private final OutboxWriter outbox;
  private final SagaMetrics metrics;
  private final Clock clock;

  public OrderSagaService(
      OrderRepository orders, OutboxWriter outbox, SagaMetrics metrics, Clock clock) {
    this.orders = orders;
    this.outbox = outbox;
    this.metrics = metrics;
    this.clock = clock;
  }

  @Transactional
  public Order create(String customerId, List<OrderItem> items) {
    String correlationId = CorrelationId.current();
    Order order =
        orders.save(
            new Order(
                UUID.randomUUID().toString(), customerId, items, correlationId, clock.instant()));
    outbox.append(
        new OrderCreated(
            UUID.randomUUID().toString(),
            order.getId(),
            correlationId,
            clock.instant(),
            customerId,
            lines(order),
            order.getAmount()));
    metrics.transition(order.getStatus());
    log.info("order {} created for {} amount {}", order.getId(), customerId, order.getAmount());
    return order;
  }

  @Transactional(readOnly = true)
  public Optional<Order> find(String id) {
    return orders.findById(id);
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
    Optional<Transition> transition = OrderStateMachine.apply(order.getStatus(), event);
    if (transition.isEmpty()) {
      log.info("event {} ignored for order {} in state {}", event, orderId, order.getStatus());
      return Optional.empty();
    }
    Transition t = transition.get();
    order.transition(t.to(), t.reason(), clock.instant());
    metrics.transition(t.to());
    if (t.releaseInventory()) {
      outbox.append(
          new OrderCancelled(
              UUID.randomUUID().toString(),
              order.getId(),
              correlationId,
              clock.instant(),
              t.reason(),
              lines(order)));
    }
    if (t.to().isTerminal()) {
      metrics.completed(Duration.between(order.getCreatedAt(), clock.instant()));
    }
    log.info("order {} moved to {} on {}", orderId, t.to(), event);
    return transition;
  }

  private static List<OrderLine> lines(Order order) {
    return order.getItems().stream().map(i -> new OrderLine(i.getSku(), i.getQuantity())).toList();
  }
}
