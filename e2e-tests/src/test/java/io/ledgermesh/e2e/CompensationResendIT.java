package io.ledgermesh.e2e;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.fasterxml.jackson.databind.JsonNode;
import io.ledgermesh.common.events.Topics;
import io.ledgermesh.common.kafka.KafkaSupportConfiguration;
import io.ledgermesh.order.saga.StuckOrderReaper;
import io.micrometer.core.instrument.MeterRegistry;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.listener.MessageListenerContainer;

/**
 * A cancellation the payment service cannot apply until its retries run out is dead lettered, and
 * nobody replays it. The order service has not had its answer, so its reaper sends the cancellation
 * again once the compensation wait is over, and the payment is voided and its hold released without
 * an operator.
 */
class CompensationResendIT {

  private static final Duration TIMEOUT = Duration.ofSeconds(45);
  private static final String SKU = "E2E-COMPENSATION";
  private static final String BLOCK = "e2e_void_blocked";

  @BeforeAll
  static void start() {
    Stack.start();
  }

  @Test
  void aDeadLetteredCancellationIsSentAgainUntilThePaymentIsVoided() throws Exception {
    Stack.putStock(SKU, 10);
    MessageListenerContainer saga =
        Stack.order.getBean(KafkaListenerEndpointRegistry.class).getListenerContainer("order-saga");
    MeterRegistry paymentMeters = Stack.payment.getBean(MeterRegistry.class);
    double deadLettered = deadLettered(paymentMeters);
    String id;
    saga.stop();
    try {
      id = Stack.createOrder("cust-compensation", SKU, 1, new BigDecimal("7.00"));
      await()
          .atMost(TIMEOUT)
          .until(() -> ReservationTimeoutIT.paymentStatus(id).equals("AUTHORIZED"));

      // the payment service cannot void this payment until the block is lifted
      payments(
          "alter table payment add constraint "
              + BLOCK
              + " check (order_id <> '"
              + id
              + "' or status <> 'VOIDED') not valid");
      ReservationTimeoutIT.expireDeadline(id);
      Stack.order.getBean(StuckOrderReaper.class).reap();
      assertThat(Stack.orderStatus(id)).isEqualTo("CANCELLED");

      await().atMost(TIMEOUT).until(() -> deadLettered(paymentMeters) > deadLettered);
      assertThat(ReservationTimeoutIT.paymentStatus(id)).isEqualTo("AUTHORIZED");
      assertThat(ReservationTimeoutIT.holdsAtProcessor(id)).isEqualTo(1);
      assertThat(Stack.getOrder(id).get("compensatedAt").isNull()).isTrue();
    } finally {
      payments("alter table payment drop constraint if exists " + BLOCK);
      saga.start();
    }

    // no replay: the reaper sends the cancellation again after the compensation wait
    await().atMost(TIMEOUT).until(() -> ReservationTimeoutIT.paymentStatus(id).equals("VOIDED"));
    await().atMost(TIMEOUT).until(() -> ReservationTimeoutIT.holdsAtProcessor(id) == 0);
    await().atMost(TIMEOUT).until(() -> !Stack.getOrder(id).get("compensatedAt").isNull());
    assertThat(steps(id)).contains("CANCELLATION_SENT_AGAIN", "PAYMENT_VOIDED");
    assertThat(Stack.stock(SKU)).isEqualTo(10);
  }

  private static double deadLettered(MeterRegistry meters) {
    return meters
        .counter(KafkaSupportConfiguration.DLQ_PUBLISHED, "topic", Topics.ORDER_CANCELLED)
        .count();
  }

  private static List<String> steps(String orderId) {
    JsonNode timeline =
        Stack.send(
            "GET",
            "http://localhost:" + Stack.orderPort + "/orders/" + orderId + "/timeline",
            null);
    List<String> types = new ArrayList<>();
    for (JsonNode step : timeline) {
      types.add(step.get("type").asText());
    }
    return types;
  }

  private static void payments(String sql) throws Exception {
    try (Connection connection =
            DriverManager.getConnection(
                Stack.jdbcUrl("payments"),
                Stack.POSTGRES.getUsername(),
                Stack.POSTGRES.getPassword());
        Statement statement = connection.createStatement()) {
      statement.execute(sql);
    }
  }
}
