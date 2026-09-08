package io.ledgermesh.order.saga;

import io.ledgermesh.order.domain.OrderRepository;
import io.ledgermesh.order.domain.OrderStatus;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import java.time.Duration;
import org.springframework.stereotype.Component;

/** Orders by state, transition counts and end to end saga latency. */
@Component
public class SagaMetrics {

  private final MeterRegistry registry;
  private final Timer sagaLatency;

  public SagaMetrics(MeterRegistry registry, OrderRepository orders) {
    this.registry = registry;
    for (OrderStatus status : OrderStatus.values()) {
      registry.gauge(
          "ledgermesh.orders.by_state",
          java.util.List.of(io.micrometer.core.instrument.Tag.of("state", status.name())),
          orders,
          r -> r.countByStatus(status));
    }
    this.sagaLatency =
        Timer.builder("ledgermesh.saga.latency")
            .description("time from order creation to a terminal state")
            .publishPercentiles(0.5, 0.95, 0.99)
            .register(registry);
  }

  public void transition(OrderStatus to) {
    registry.counter("ledgermesh.orders.transitions", "to", to.name()).increment();
  }

  public void completed(Duration elapsed) {
    sagaLatency.record(elapsed);
  }

  public void redriven() {
    registry.counter("ledgermesh.saga.redrives").increment();
  }
}
