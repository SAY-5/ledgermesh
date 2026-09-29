package io.ledgermesh.e2e;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.fasterxml.jackson.databind.JsonNode;
import io.ledgermesh.order.saga.StuckOrderReaper;
import io.ledgermesh.payment.domain.PaymentRepository;
import io.ledgermesh.payment.processor.AuthorizationHolds;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.time.Duration;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.listener.MessageListenerContainer;

/**
 * An order whose reservation deadline passes while the answers to it sit unread in the order
 * service's topics, which is what a restarted order service meets when it was away for part of an
 * order's reservation window: inventory has reserved, the payment service has authorized off that
 * reservation, and the reaper cancels the order before the saga listener reads either answer.
 */
class ReservationTimeoutIT {

  private static final Duration TIMEOUT = Duration.ofSeconds(45);
  private static final String SKU = "E2E-DEADLINE";

  @BeforeAll
  static void start() {
    Stack.start();
  }

  @Test
  void anOrderCancelledAtItsDeadlineAfterItsPaymentWasAuthorizedIsNotCharged() throws Exception {
    Stack.putStock(SKU, 10);
    MessageListenerContainer saga =
        Stack.order.getBean(KafkaListenerEndpointRegistry.class).getListenerContainer("order-saga");
    String id;
    saga.stop();
    try {
      id = Stack.createOrder("cust-deadline", SKU, 2, new BigDecimal("4.00"));
      await().atMost(TIMEOUT).until(() -> paymentStatus(id).equals("AUTHORIZED"));
      assertThat(holdsAtProcessor(id)).isPositive();
      assertThat(Stack.stock(SKU)).isEqualTo(8);
      assertThat(Stack.orderStatus(id)).isEqualTo("PENDING");

      expireDeadline(id);
      Stack.order.getBean(StuckOrderReaper.class).reap();
      assertThat(Stack.orderStatus(id)).isEqualTo("CANCELLED");
      assertThat(Stack.getOrder(id).get("reason").asText()).isEqualTo("RESERVATION_TIMEOUT");
    } finally {
      saga.start();
    }

    await().atMost(TIMEOUT).until(() -> Stack.stock(SKU) == 10);
    await().atMost(TIMEOUT).until(() -> paymentStatus(id).equals("VOIDED"));
    await().atMost(TIMEOUT).until(() -> holdsAtProcessor(id) == 0);
    await().atMost(TIMEOUT).until(() -> !Stack.getOrder(id).get("compensatedAt").isNull());

    // the answers the listener had not read arrive after the cancellation and change nothing; they
    // come on two topics, so either may be applied first
    await()
        .atMost(TIMEOUT)
        .until(() -> ignored(id, "INVENTORY_RESERVED") && ignored(id, "PAYMENT_COMPLETED"));
    assertThat(Stack.orderStatus(id)).isEqualTo("CANCELLED");
    assertThat(paymentStatus(id)).isEqualTo("VOIDED");
    assertThat(Stack.stock(SKU)).isEqualTo(10);
  }

  /** Authorizations the processor still holds on the card for the order. */
  static int holdsAtProcessor(String orderId) {
    return Stack.payment.getBean(AuthorizationHolds.class).outstanding(orderId);
  }

  static String paymentStatus(String orderId) {
    return Stack.payment
        .getBean(PaymentRepository.class)
        .findById(orderId)
        .map(p -> p.getStatus().name())
        .orElse("none");
  }

  /** Whether the order's timeline records an event of this type that moved nothing. */
  static boolean ignored(String orderId, String type) {
    JsonNode timeline =
        Stack.send(
            "GET",
            "http://localhost:" + Stack.orderPort + "/orders/" + orderId + "/timeline",
            null);
    for (JsonNode step : timeline) {
      if (step.get("type").asText().equals(type) && step.get("to").isNull()) {
        return true;
      }
    }
    return false;
  }

  /** Puts the order's reservation deadline in the past, as a long enough absence would have. */
  static void expireDeadline(String orderId) throws Exception {
    try (Connection connection =
            DriverManager.getConnection(
                Stack.jdbcUrl("orders"),
                Stack.POSTGRES.getUsername(),
                Stack.POSTGRES.getPassword());
        PreparedStatement update =
            connection.prepareStatement(
                "update orders set deadline_at = now() - interval '1 second' where id = ?")) {
      update.setString(1, orderId);
      assertThat(update.executeUpdate()).isEqualTo(1);
    }
  }
}
