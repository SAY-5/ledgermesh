package io.ledgermesh.common.kafka;

import java.util.Map;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.stereotype.Component;

/**
 * Fails startup when the producer is configured in a way that would break the relay's delivery
 * guarantees. The outbox already makes every send at-least-once; the idempotent producer with
 * {@code acks=all} is what turns broker-side retries into exactly one copy per partition, and a
 * bounded in-flight window is what keeps one order's events in order. A missing setting is the
 * client default, which is already safe; an explicit weaker value is refused.
 */
@Component
public class ProducerSettingsGuard implements InitializingBean {

  private final ProducerFactory<?, ?> producers;

  public ProducerSettingsGuard(ProducerFactory<?, ?> producers) {
    this.producers = producers;
  }

  @Override
  public void afterPropertiesSet() {
    verify(producers.getConfigurationProperties());
  }

  public static void verify(Map<String, Object> config) {
    Object idempotence = config.get(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG);
    if (idempotence != null && !Boolean.parseBoolean(idempotence.toString())) {
      throw new IllegalStateException("enable.idempotence must be true for the outbox relay");
    }
    Object acks = config.get(ProducerConfig.ACKS_CONFIG);
    if (acks != null && !("all".equals(acks.toString()) || "-1".equals(acks.toString()))) {
      throw new IllegalStateException("acks must be all for the outbox relay, was " + acks);
    }
    Object inFlight = config.get(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION);
    if (inFlight != null && Integer.parseInt(inFlight.toString()) > 5) {
      throw new IllegalStateException(
          "max.in.flight.requests.per.connection must be at most 5 to keep per key ordering");
    }
  }
}
