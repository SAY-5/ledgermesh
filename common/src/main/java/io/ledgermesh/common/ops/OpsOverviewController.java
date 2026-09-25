package io.ledgermesh.common.ops;

import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import io.ledgermesh.common.metrics.KafkaLagMetrics;
import io.ledgermesh.common.ops.SagaSnapshot.Sagas;
import java.util.Map;
import java.util.TreeMap;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.actuate.health.HealthEndpoint;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * One page an operator can read during an incident: whether this service is healthy, how far its
 * listener groups are behind, what is waiting in the dead letter topics and what was parked after
 * its replays ran out, which breakers are open, and, on the order service, how many sagas are still
 * open and how many have missed their deadline. The numbers come from the same gauges the metrics
 * endpoint publishes, so the page and the dashboards cannot disagree.
 */
@RestController
@RequestMapping("/ops")
public class OpsOverviewController {

  public record Overview(
      String service,
      String health,
      Map<String, Long> consumerLag,
      Map<String, Long> deadLetterDepth,
      Map<String, Long> parkedDepth,
      Map<String, String> breakers,
      Sagas sagas) {}

  private final String service;
  private final ObjectProvider<HealthEndpoint> health;
  private final ObjectProvider<KafkaLagMetrics> kafka;
  private final CircuitBreakerRegistry breakers;
  private final ObjectProvider<SagaSnapshot> sagas;

  public OpsOverviewController(
      @Value("${spring.application.name:ledgermesh}") String service,
      ObjectProvider<HealthEndpoint> health,
      ObjectProvider<KafkaLagMetrics> kafka,
      CircuitBreakerRegistry breakers,
      ObjectProvider<SagaSnapshot> sagas) {
    this.service = service;
    this.health = health;
    this.kafka = kafka;
    this.breakers = breakers;
    this.sagas = sagas;
  }

  @GetMapping("/overview")
  public Overview overview() {
    KafkaLagMetrics metrics = kafka.getIfAvailable();
    SagaSnapshot saga = sagas.getIfAvailable();
    return new Overview(
        service,
        status(),
        metrics == null ? Map.of() : metrics.consumerLag(),
        metrics == null ? Map.of() : metrics.dlqDepth(),
        metrics == null ? Map.of() : metrics.parkedDepth(),
        breakerStates(),
        saga == null ? null : saga.sagas());
  }

  private String status() {
    HealthEndpoint endpoint = health.getIfAvailable();
    return endpoint == null ? "UNKNOWN" : endpoint.health().getStatus().getCode();
  }

  private Map<String, String> breakerStates() {
    Map<String, String> states = new TreeMap<>();
    for (CircuitBreaker breaker : breakers.getAllCircuitBreakers()) {
      states.put(breaker.getName(), breaker.getState().name());
    }
    return states;
  }
}
