package io.ledgermesh.payment.processor;

import static org.assertj.core.api.Assertions.assertThat;

import java.math.BigDecimal;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.transaction.support.TransactionTemplate;

/** The synthetic processor's own table of authorizations, on a database of its own. */
class JdbcAuthorizationHoldsTest {

  private JdbcTemplate jdbc;
  private DataSourceTransactionManager transactions;
  private JdbcAuthorizationHolds holds;

  @BeforeEach
  void database() {
    DriverManagerDataSource source =
        new DriverManagerDataSource(
            "jdbc:h2:mem:holds-" + UUID.randomUUID() + ";DB_CLOSE_DELAY=-1", "sa", "");
    jdbc = new JdbcTemplate(source);
    transactions = new DataSourceTransactionManager(source);
    holds =
        new JdbcAuthorizationHolds(
            jdbc, transactions, Clock.fixed(Instant.parse("2026-09-29T00:00:00Z"), ZoneOffset.UTC));
  }

  @Test
  void everyGrantIsAHoldOfItsOwnUntilItIsReleased() {
    holds.grant("o1", "AUTH-A", new BigDecimal("12.50"));
    holds.grant("o1", "AUTH-B", new BigDecimal("12.50"));
    holds.grant("o2", "AUTH-C", new BigDecimal("3.00"));
    assertThat(holds.outstanding("o1")).isEqualTo(2);

    assertThat(holds.release("o1", "AUTH-A")).isEqualTo(1);
    assertThat(holds.release("o1", "AUTH-A")).isZero();
    assertThat(holds.outstanding("o1")).isEqualTo(1);
    assertThat(state("AUTH-A")).isEqualTo(JdbcAuthorizationHolds.OUTSTANDING);
    assertThat(state("AUTH-B")).isEqualTo(JdbcAuthorizationHolds.RELEASED);

    assertThat(holds.release("o1", null)).isEqualTo(1);
    assertThat(holds.release("o1", null)).isZero();
    assertThat(holds.outstanding("o1")).isZero();
    assertThat(holds.outstanding("o2")).isEqualTo(1);
  }

  @Test
  void theListingPagesInGrantOrderFromWhereTheLastPageEnded() {
    holds.grant("o5", "AUTH-F", BigDecimal.ONE);
    holds.grant("o5", "AUTH-G", BigDecimal.ONE);
    holds.grant("o6", "AUTH-H", BigDecimal.ONE);

    List<PaymentProcessor.Authorization> first = holds.outstanding(null, 2);
    List<PaymentProcessor.Authorization> second = holds.outstanding(first.get(1), 2);

    assertThat(first)
        .extracting(PaymentProcessor.Authorization::authorizationCode)
        .containsExactly("AUTH-F", "AUTH-G");
    assertThat(second)
        .extracting(PaymentProcessor.Authorization::authorizationCode)
        .containsExactly("AUTH-H");
    assertThat(holds.releaseCode("o5", "AUTH-G")).isTrue();
    assertThat(holds.releaseCode("o5", "AUTH-G")).isFalse();
    assertThat(holds.releaseCode("o6", "AUTH-F")).isFalse();
    assertThat(holds.outstanding(null, 10))
        .extracting(PaymentProcessor.Authorization::authorizationCode)
        .containsExactly("AUTH-F", "AUTH-H");
  }

  @Test
  void aGrantStandsWhenTheCallersTransactionRollsBack() {
    new TransactionTemplate(transactions)
        .executeWithoutResult(
            status -> {
              holds.grant("o3", "AUTH-D", BigDecimal.TEN);
              status.setRollbackOnly();
            });

    assertThat(holds.outstanding("o3")).isEqualTo(1);
  }

  @Test
  void theTableIsCreatedOnceAndKeptAcrossRestarts() {
    holds.grant("o4", "AUTH-E", BigDecimal.ONE);
    JdbcAuthorizationHolds again =
        new JdbcAuthorizationHolds(jdbc, transactions, Clock.systemUTC());

    assertThat(again.outstanding("o4")).isEqualTo(1);
  }

  private String state(String code) {
    return jdbc.queryForObject(
        "select state from processor_hold where authorization_code = ?", String.class, code);
  }
}
