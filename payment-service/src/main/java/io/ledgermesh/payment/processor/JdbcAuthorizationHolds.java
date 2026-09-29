package io.ledgermesh.payment.processor;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.time.Clock;
import java.util.List;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.support.TransactionTemplate;

/**
 * Keeps the synthetic processor's authorizations in a {@code processor_hold} table of their own,
 * one row per authorization. Every write commits in a transaction of its own, whatever the caller
 * is in the middle of, so what the processor granted stays granted when the payment service's
 * transaction rolls back or its process is killed, as it would at a real processor. Created only
 * with the synthetic processor, by {@link SyntheticProcessorConfiguration}.
 */
public class JdbcAuthorizationHolds implements AuthorizationHolds {

  public static final String OUTSTANDING = "OUTSTANDING";
  public static final String RELEASED = "RELEASED";

  private final JdbcTemplate jdbc;
  private final TransactionTemplate own;
  private final Clock clock;

  public JdbcAuthorizationHolds(
      JdbcTemplate jdbc, PlatformTransactionManager transactions, Clock clock) {
    this.jdbc = jdbc;
    this.own = new TransactionTemplate(transactions);
    this.own.setPropagationBehavior(TransactionDefinition.PROPAGATION_REQUIRES_NEW);
    this.clock = clock;
    jdbc.execute(
        "create table if not exists processor_hold ("
            + "authorization_code varchar(64) primary key, "
            + "order_id varchar(36) not null, "
            + "amount numeric(12,2) not null, "
            + "state varchar(16) not null, "
            + "granted_at timestamp with time zone not null, "
            + "released_at timestamp with time zone)");
    jdbc.execute("create index if not exists ix_processor_hold_order on processor_hold (order_id)");
  }

  @Override
  public void grant(String orderId, String authorizationCode, BigDecimal amount) {
    own.executeWithoutResult(
        status ->
            jdbc.update(
                "insert into processor_hold"
                    + " (authorization_code, order_id, amount, state, granted_at)"
                    + " values (?, ?, ?, ?, ?)",
                authorizationCode,
                orderId,
                amount,
                OUTSTANDING,
                Timestamp.from(clock.instant())));
  }

  @Override
  public int release(String orderId, String keep) {
    Integer released =
        own.execute(
            status ->
                jdbc.update(
                    "update processor_hold set state = ?, released_at = ?"
                        + " where order_id = ? and state = ? and authorization_code <> ?",
                    RELEASED,
                    Timestamp.from(clock.instant()),
                    orderId,
                    OUTSTANDING,
                    keep == null ? "" : keep));
    return released == null ? 0 : released;
  }

  @Override
  public boolean releaseCode(String orderId, String authorizationCode) {
    Integer released =
        own.execute(
            status ->
                jdbc.update(
                    "update processor_hold set state = ?, released_at = ?"
                        + " where authorization_code = ? and order_id = ? and state = ?",
                    RELEASED,
                    Timestamp.from(clock.instant()),
                    authorizationCode,
                    orderId,
                    OUTSTANDING));
    return released != null && released > 0;
  }

  @Override
  public List<PaymentProcessor.Authorization> outstanding(
      PaymentProcessor.Authorization after, int limit) {
    RowMapper<PaymentProcessor.Authorization> row =
        (rs, n) ->
            new PaymentProcessor.Authorization(
                rs.getString("order_id"),
                rs.getString("authorization_code"),
                rs.getBigDecimal("amount"),
                rs.getTimestamp("granted_at").toInstant());
    if (after == null) {
      return jdbc.query(
          "select order_id, authorization_code, amount, granted_at from processor_hold"
              + " where state = ? order by granted_at, authorization_code limit ?",
          row,
          OUTSTANDING,
          limit);
    }
    Timestamp at = Timestamp.from(after.grantedAt());
    return jdbc.query(
        "select order_id, authorization_code, amount, granted_at from processor_hold"
            + " where state = ? and (granted_at > ? or (granted_at = ? and authorization_code > ?))"
            + " order by granted_at, authorization_code limit ?",
        row,
        OUTSTANDING,
        at,
        at,
        after.authorizationCode(),
        limit);
  }

  @Override
  public int outstanding(String orderId) {
    Integer count =
        jdbc.queryForObject(
            "select count(*) from processor_hold where order_id = ? and state = ?",
            Integer.class,
            orderId,
            OUTSTANDING);
    return count == null ? 0 : count;
  }
}
