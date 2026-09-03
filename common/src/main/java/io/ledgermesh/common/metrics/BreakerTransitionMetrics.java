package io.ledgermesh.common.metrics;

import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import io.micrometer.core.instrument.MeterRegistry;

/**
 * Counts circuit breaker state transitions so a run can report how often a breaker opened and
 * recovered. Resilience4j exposes the current state as a gauge; a counter keeps the history.
 */
public class BreakerTransitionMetrics {

  public static final String METRIC = "ledgermesh.breaker.transitions";

  public BreakerTransitionMetrics(CircuitBreakerRegistry breakers, MeterRegistry meters) {
    breakers.getAllCircuitBreakers().forEach(cb -> attach(cb, meters));
    breakers.getEventPublisher().onEntryAdded(added -> attach(added.getAddedEntry(), meters));
  }

  private static void attach(CircuitBreaker cb, MeterRegistry meters) {
    cb.getEventPublisher()
        .onStateTransition(
            event ->
                meters
                    .counter(
                        METRIC,
                        "name",
                        cb.getName(),
                        "from",
                        event.getStateTransition().getFromState().name(),
                        "to",
                        event.getStateTransition().getToState().name())
                    .increment());
  }
}
