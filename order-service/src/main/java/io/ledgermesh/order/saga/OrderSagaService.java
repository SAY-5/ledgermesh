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
import org.springframework.data.domain.PageRequest;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;

/**
 * Creates orders and applies saga events. Every state change, the event it emits and the timeline
 * row that records it are written in one transaction through the outbox. The change is counted and
 * logged once that transaction has committed, so a change that rolls back, like the order of a call
 * that lost the race for its idempotency key, is neither.
 *
 * <p>A cancellation that emits {@code order.cancelled} waits for the payment service to answer it
 * with {@code payment.voided}. Until then the reaper sends it again every {@code
 * ledgermesh.saga.compensation}, so a cancellation that the payment service dead lettered, or never
 * saw, is still answered without an operator; the payment service handles every copy the same way.
 */
@Service
public class OrderSagaService {

  public static final String CREATED = "CREATED";
  public static final String PAYMENT_REDRIVEN = "PAYMENT_REDRIVEN";
  public static final String PAYMENT_VOIDED = "PAYMENT_VOIDED";
  public static final String CANCELLATION_SENT_AGAIN = "CANCELLATION_SENT_AGAIN";

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
    afterCommit(
        () -> {
          metrics.transition(OrderStatus.PENDING);
          log.info(
              "order {} created for {} amount {}", order.getId(), customerId, order.getAmount());
        });
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
    if (t.releaseInventory()) {
      outbox.append(
          new OrderCancelled(
              UUID.randomUUID().toString(), orderId, correlationId, now, t.reason(), lines(order)));
      order.awaitCompensation(now.plus(timeouts.compensation()));
    }
    Duration elapsed = Duration.between(order.getCreatedAt(), now);
    afterCommit(
        () -> {
          metrics.transition(t.to());
          if (t.to().isTerminal()) {
            metrics.completed(elapsed);
          }
          log.info("order {} moved to {} on {}", orderId, t.to(), event);
        });
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
    int redrives = order.getRedrives();
    afterCommit(
        () -> {
          metrics.redriven();
          log.warn(
              "order {} payment re-driven ({} of {})", orderId, redrives, timeouts.maxRedrives());
        });
    return true;
  }

  /**
   * Records the payment service's answer to the cancellation. Returns false for an unknown order,
   * an order that is not cancelled, or one whose cancellation was already answered, each of which
   * leaves an ignored step in the timeline.
   */
  @Transactional
  public boolean compensated(String orderId, String previous, String correlationId) {
    Order order = orders.findById(orderId).orElse(null);
    if (order == null) {
      log.warn("payment.voided for unknown order {} ignored", orderId);
      return false;
    }
    Instant now = clock.instant();
    if (order.getStatus() != OrderStatus.CANCELLED || order.getCompensatedAt() != null) {
      timeline.save(
          new OrderEvent(
              orderId, PAYMENT_VOIDED, order.getStatus(), null, previous, correlationId, now));
      return false;
    }
    order.compensated(now);
    timeline.save(
        new OrderEvent(
            orderId,
            PAYMENT_VOIDED,
            OrderStatus.CANCELLED,
            OrderStatus.CANCELLED,
            previous,
            correlationId,
            now));
    afterCommit(() -> log.info("order {} compensated, its payment was {}", orderId, previous));
    return true;
  }

  /**
   * Sends {@code order.cancelled} again, under a fresh event id, for a cancelled order whose
   * cancellation the payment service has not answered, and waits another {@code compensation} for
   * the answer. Returns false when there is nothing to send: the order is not cancelled, was
   * cancelled for stock, or has been answered.
   */
  @Transactional
  public boolean resendCancellation(String orderId) {
    Order order = orders.findById(orderId).orElse(null);
    if (order == null
        || order.getStatus() != OrderStatus.CANCELLED
        || !OrderStateMachine.COMPENSATED.contains(order.getReason())
        || order.getCompensatedAt() != null) {
      return false;
    }
    Instant now = clock.instant();
    outbox.append(
        new OrderCancelled(
            UUID.randomUUID().toString(),
            orderId,
            order.getCorrelationId(),
            now,
            order.getReason(),
            lines(order)));
    order.awaitCompensation(now.plus(timeouts.compensation()));
    timeline.save(
        new OrderEvent(
            orderId,
            CANCELLATION_SENT_AGAIN,
            OrderStatus.CANCELLED,
            OrderStatus.CANCELLED,
            order.getReason(),
            order.getCorrelationId(),
            now));
    afterCommit(
        () -> {
          metrics.cancellationSentAgain();
          log.warn("order {} cancellation sent again, payment has not answered it", orderId);
        });
    return true;
  }

  /**
   * Up to {@code max} cancelled orders whose cancellation was never answered and that are not
   * waiting for an answer, oldest first: orders cancelled before the order service waited for
   * answers at all. Sending one again makes it wait, so it is not returned twice; the ones already
   * waiting are the reaper's.
   */
  @Transactional(readOnly = true)
  public List<String> unansweredCancellations(int max) {
    return orders
        .findByStatusAndReasonInAndCompensatedAtIsNullAndCompensationDueAtIsNullOrderByUpdatedAtAsc(
            OrderStatus.CANCELLED, OrderStateMachine.COMPENSATED, PageRequest.of(0, max))
        .stream()
        .map(Order::getId)
        .toList();
  }

  private static void afterCommit(Runnable action) {
    TransactionSynchronizationManager.registerSynchronization(
        new TransactionSynchronization() {
          @Override
          public void afterCommit() {
            action.run();
          }
        });
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
