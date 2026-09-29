package io.ledgermesh.payment.processor;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.ApplicationContext;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.bean.override.mockito.MockitoBean;

/** With the synthetic processor off, neither it nor its table exists. */
@SpringBootTest(
    properties = {
      "ledgermesh.processor.synthetic=false",
      "spring.datasource.url=jdbc:h2:mem:no-synthetic;DB_CLOSE_DELAY=-1"
    })
class SyntheticProcessorOffTest {

  @Autowired private ApplicationContext context;
  @Autowired private JdbcTemplate jdbc;
  @MockitoBean private PaymentProcessor processor;

  @Test
  void neitherTheSyntheticProcessorNorItsHoldsTableExist() {
    assertThat(context.getBeansOfType(SyntheticProcessor.class)).isEmpty();
    assertThat(context.getBeansOfType(AuthorizationHolds.class)).isEmpty();
    Integer tables =
        jdbc.queryForObject(
            "select count(*) from information_schema.tables"
                + " where lower(table_name) = 'processor_hold'",
            Integer.class);
    assertThat(tables).isZero();
  }
}
