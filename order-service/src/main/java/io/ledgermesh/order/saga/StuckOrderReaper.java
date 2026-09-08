package io.ledgermesh.order.saga;

import io.ledgermesh.order.domain.Order;
import io.ledgermesh.order.domain.OrderRepository;
import io.ledgermesh.order.domain.OrderStatus;
import io.ledgermesh.order.domain.SagaEvent;
import java.time.Clock;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/**
 * Finds orders whose current saga step has passed its deadline and moves them on. A pending order
 * that never heard back from inventory is cancelled and its reservation released, in case it was
 * made. A reserved order that never heard back from payment is asked for again once; if the
 * deadline passes a second time it is cancelled and released. Every decision goes through the state
 * machine and the outbox, so a reaper tick is as durable and as idempotent as any other saga step.
 */
@Component
@ConditionalOnProperty(name = "ledgermesh.saga.reaper", havingValue = "true", matchIfMissing = true)
public class StuckOrderReaper {

  private static final Logger log = LoggerFactory.getLogger(StuckOrderReaper.class);
  private static final List<OrderStatus> OPEN = List.of(OrderStatus.PENDING, OrderStatus.RESERVED);

  private final OrderRepository orders;
  private final OrderSagaService saga;
  private final Clock clock;

  public StuckOrderReaper(OrderRepository orders, OrderSagaService saga, Clock clock) {
    this.orders = orders;
    this.saga = saga;
    this.clock = clock;
  }

  @Scheduled(fixedDelayString = "${ledgermesh.saga.reaper-ms:5000}")
  public void tick() {
    reap();
  }

  /** Handles every expired order once. Returns how many orders were acted on. */
  public int reap() {
    List<Order> expired =
        orders.findTop100ByStatusInAndDeadlineAtLessThanEqualOrderByDeadlineAtAsc(
            OPEN, clock.instant());
    int acted = 0;
    for (Order order : expired) {
      String id = order.getId();
      boolean moved =
          switch (order.getStatus()) {
            case PENDING ->
                saga.apply(id, SagaEvent.RESERVATION_TIMEOUT, order.getCorrelationId()).isPresent();
            case RESERVED ->
                saga.redrivePayment(id)
                    || saga.apply(id, SagaEvent.PAYMENT_TIMEOUT, order.getCorrelationId())
                        .isPresent();
            case CONFIRMED, CANCELLED -> false;
          };
      if (moved) {
        acted++;
        log.warn("order {} passed its {} deadline", id, order.getStatus());
      }
    }
    return acted;
  }
}
