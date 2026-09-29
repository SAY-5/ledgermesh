package io.ledgermesh.payment.authorize;

import static org.assertj.core.api.Assertions.assertThat;

import io.ledgermesh.payment.domain.PaymentStatus;
import java.sql.DriverManager;
import java.time.Instant;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestInstance;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.postgresql.PostgreSQLContainer;

/** The same behavioral contract on PostgreSQL, booted over an existing pre-decision payment. */
@SpringBootTest(properties = "spring.jpa.hibernate.ddl-auto=update")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class AuthorizationOwnershipPostgresIT extends AuthorizationOwnershipTest {
  private static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:16");

  @DynamicPropertySource
  static void postgres(DynamicPropertyRegistry properties) throws Exception {
    POSTGRES.start();
    Runtime.getRuntime().addShutdownHook(new Thread(POSTGRES::stop));
    try (var connection =
            DriverManager.getConnection(
                POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword());
        var sql = connection.createStatement()) {
      // Old payment table, deliberately no authorization_decision table. Hibernate update must
      // add the new table without replacing or losing a previously authorized payment.
      sql.execute(
          """
          create table payment (
            order_id varchar(36) primary key, customer_id varchar(64) not null,
            amount numeric(12,2) not null, status varchar(16) not null,
            authorization_code varchar(64), reason varchar(64), attempts integer not null,
            next_attempt_at timestamp with time zone, correlation_id varchar(64) not null,
            created_at timestamp with time zone not null, updated_at timestamp with time zone not null,
            release_due_at timestamp with time zone, released_at timestamp with time zone,
            release_attempts integer not null default 0, version bigint not null
          )
          """);
      sql.execute(
          """
          insert into payment (order_id, customer_id, amount, status, authorization_code, attempts,
            correlation_id, created_at, updated_at, version)
          values ('legacy-upgrade', 'cust', 12.50, 'AUTHORIZED', 'AUTH-LEGACY', 1,
            'c', current_timestamp, current_timestamp, 7)
          """);
    }
    properties.add("spring.datasource.url", POSTGRES::getJdbcUrl);
    properties.add("spring.datasource.username", POSTGRES::getUsername);
    properties.add("spring.datasource.password", POSTGRES::getPassword);
  }

  @BeforeAll
  void additiveSchemaUpgradePreservesAndProtectsTheExistingAuthorization() {
    var legacy = payments.findById("legacy-upgrade").orElseThrow();
    assertThat(legacy.getStatus()).isEqualTo(PaymentStatus.AUTHORIZED);
    assertThat(legacy.getVersion()).isEqualTo(7);
    assertThat(legacy.getAmount()).isEqualByComparingTo("12.50");
    assertThat(decisionRows.count()).isZero();
    holds.grant("legacy-upgrade", "AUTH-LEGACY", legacy.getAmount());

    assertThat(reconciler.reconcile(Instant.now().plusSeconds(60))).isZero();

    assertThat(holds.outstanding("legacy-upgrade")).isEqualTo(1);
    assertThat(decisionRows.findById("AUTH-LEGACY")).isPresent();
    assertThat(payments.findById("legacy-upgrade").orElseThrow().getVersion()).isEqualTo(7);
  }
}
