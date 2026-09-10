package io.ledgermesh.order.saga;

import io.ledgermesh.common.ops.SagaSnapshot;
import io.ledgermesh.order.domain.OrderRepository;
import io.ledgermesh.order.domain.OrderStatus;
import io.micrometer.core.instrument.MeterRegistry;
import java.time.Clock;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import org.springframework.stereotype.Component;

/**
 * Open sagas and the ones that have already missed a deadline, for the ops overview and a gauge.
 */
@Component
public class OrderSagaSnapshot implements SagaSnapshot {

  private static final List<OrderStatus> OPEN = List.of(OrderStatus.PENDING, OrderStatus.RESERVED);

  private final OrderRepository orders;
  private final Clock clock;

  public OrderSagaSnapshot(OrderRepository orders, Clock clock, MeterRegistry meters) {
    this.orders = orders;
    this.clock = clock;
    meters.gauge("ledgermesh.saga.stuck", this, OrderSagaSnapshot::stuck);
  }

  @Override
  public Sagas sagas() {
    Map<String, Long> byState = new TreeMap<>();
    long inFlight = 0;
    for (OrderStatus status : OrderStatus.values()) {
      long count = orders.countByStatus(status);
      byState.put(status.name(), count);
      if (OPEN.contains(status)) {
        inFlight += count;
      }
    }
    return new Sagas(inFlight, stuck(), byState);
  }

  /** Open orders whose current step has already passed its deadline. */
  public long stuck() {
    return orders.countByStatusInAndDeadlineAtLessThanEqual(OPEN, clock.instant());
  }
}
