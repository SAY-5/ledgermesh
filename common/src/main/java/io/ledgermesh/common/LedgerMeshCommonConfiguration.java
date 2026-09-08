package io.ledgermesh.common;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import io.ledgermesh.common.correlation.CorrelationIdFilter;
import io.ledgermesh.common.dlq.DlqReplayer;
import io.ledgermesh.common.events.EventCodec;
import io.ledgermesh.common.metrics.BreakerTransitionMetrics;
import io.ledgermesh.common.metrics.KafkaLagMetrics;
import io.ledgermesh.common.outbox.OutboxEventRepository;
import io.ledgermesh.common.outbox.OutboxRelay;
import io.ledgermesh.common.outbox.OutboxRelayScheduler;
import io.micrometer.core.instrument.MeterRegistry;
import java.time.Clock;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.web.servlet.FilterRegistrationBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.EnableScheduling;

/** Wiring shared by all three services. Each service pulls it in through component scanning. */
@Configuration
@EnableScheduling
public class LedgerMeshCommonConfiguration {

  @Bean
  public Clock clock() {
    return Clock.systemUTC();
  }

  @Bean
  public EventCodec eventCodec(ObjectMapper mapper) {
    return new EventCodec(mapper);
  }

  @Bean
  public FilterRegistrationBean<CorrelationIdFilter> correlationIdFilter() {
    FilterRegistrationBean<CorrelationIdFilter> bean =
        new FilterRegistrationBean<>(new CorrelationIdFilter());
    bean.setOrder(Integer.MIN_VALUE);
    return bean;
  }

  @Bean
  public OutboxRelay outboxRelay(
      OutboxEventRepository repository,
      KafkaTemplate<String, String> kafka,
      Clock clock,
      MeterRegistry meters,
      @Value("${ledgermesh.outbox.send-timeout-ms:5000}") long sendTimeoutMs) {
    return new OutboxRelay(repository, kafka, clock, meters, sendTimeoutMs);
  }

  @Bean
  @ConditionalOnProperty(
      name = "ledgermesh.outbox.enabled",
      havingValue = "true",
      matchIfMissing = true)
  public OutboxRelayScheduler outboxRelayScheduler(OutboxRelay relay) {
    return new OutboxRelayScheduler(relay);
  }

  @Bean
  public DlqReplayer dlqReplayer(
      ConsumerFactory<String, String> consumers,
      KafkaTemplate<String, String> kafka,
      Clock clock,
      MeterRegistry meters,
      @Value("${spring.application.name:ledgermesh}") String serviceName) {
    return new DlqReplayer(consumers, kafka, serviceName, clock, meters);
  }

  @Bean
  @ConditionalOnProperty(
      name = "ledgermesh.kafka.metrics",
      havingValue = "true",
      matchIfMissing = true)
  public KafkaLagMetrics kafkaLagMetrics(
      KafkaAdmin kafkaAdmin,
      KafkaListenerEndpointRegistry listeners,
      MeterRegistry meters,
      DlqReplayer replayer) {
    return new KafkaLagMetrics(kafkaAdmin, listeners, meters, replayer.group());
  }

  @Bean
  public BreakerTransitionMetrics breakerTransitionMetrics(
      CircuitBreakerRegistry breakers, MeterRegistry meters) {
    return new BreakerTransitionMetrics(breakers, meters);
  }
}
