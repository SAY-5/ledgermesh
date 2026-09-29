package io.ledgermesh.payment.processor;

import java.math.BigDecimal;
import java.time.Clock;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.PlatformTransactionManager;

/**
 * The synthetic processor and the table it keeps its authorizations in, on unless {@code
 * ledgermesh.processor.synthetic} is false, which is where a real processor's client would take its
 * place. Nothing of the synthetic processor, its table included, exists without it.
 */
@Configuration
@ConditionalOnProperty(
    name = "ledgermesh.processor.synthetic",
    havingValue = "true",
    matchIfMissing = true)
public class SyntheticProcessorConfiguration {

  @Bean
  public JdbcAuthorizationHolds authorizationHolds(
      JdbcTemplate jdbc, PlatformTransactionManager transactions, Clock clock) {
    return new JdbcAuthorizationHolds(jdbc, transactions, clock);
  }

  @Bean
  public SyntheticProcessor syntheticProcessor(
      @Value("${ledgermesh.processor.limit:10000}") BigDecimal limit,
      @Value("${ledgermesh.processor.transient-percent:5}") int transientPercent,
      @Value("${ledgermesh.processor.slow-percent:1}") int slowPercent,
      @Value("${ledgermesh.processor.slow-millis:3000}") long slowMillis,
      AuthorizationHolds holds) {
    return new SyntheticProcessor(limit, transientPercent, slowPercent, slowMillis, holds);
  }
}
