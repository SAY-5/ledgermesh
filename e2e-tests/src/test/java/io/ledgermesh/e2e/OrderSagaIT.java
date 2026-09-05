package io.ledgermesh.e2e;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.fasterxml.jackson.databind.JsonNode;
import java.math.BigDecimal;
import java.time.Duration;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

class OrderSagaIT {

  private static final Duration SAGA_TIMEOUT = Duration.ofSeconds(45);

  @BeforeAll
  static void start() {
    Stack.start();
  }

  @Test
  void orderFlowsToConfirmedAcrossTheThreeServices() {
    Stack.putStock("E2E-A", 100);

    String id = Stack.createOrder("cust-1", "E2E-A", 2, new BigDecimal("9.99"));
    assertThat(Stack.orderStatus(id)).isEqualTo("PENDING");

    await().atMost(SAGA_TIMEOUT).until(() -> Stack.orderStatus(id).equals("CONFIRMED"));
    JsonNode order = Stack.getOrder(id);
    assertThat(order.get("amount").decimalValue()).isEqualByComparingTo("19.98");
    assertThat(order.get("reason").isNull()).isTrue();
    assertThat(Stack.stock("E2E-A")).isEqualTo(98);

    JsonNode view = Stack.stockViaOrderService("E2E-A");
    assertThat(view.get("available").asInt()).isEqualTo(98);
    assertThat(view.get("source").asText()).isEqualTo("live");
  }

  @Test
  void outOfStockOrderIsCancelledWithoutTouchingStock() {
    Stack.putStock("E2E-B", 1);

    String id = Stack.createOrder("cust-2", "E2E-B", 5, new BigDecimal("1.00"));

    await().atMost(SAGA_TIMEOUT).until(() -> Stack.orderStatus(id).equals("CANCELLED"));
    assertThat(Stack.getOrder(id).get("reason").asText()).isEqualTo("OUT_OF_STOCK");
    assertThat(Stack.stock("E2E-B")).isEqualTo(1);
  }

  @Test
  void declinedPaymentCancelsTheOrderAndReleasesTheReservation() {
    Stack.putStock("E2E-C", 10);

    String id = Stack.createOrder("cust-declined", "E2E-C", 3, new BigDecimal("2.50"));

    await().atMost(SAGA_TIMEOUT).until(() -> Stack.orderStatus(id).equals("CANCELLED"));
    assertThat(Stack.getOrder(id).get("reason").asText()).isEqualTo("PAYMENT_DECLINED");
    await().atMost(SAGA_TIMEOUT).until(() -> Stack.stock("E2E-C") == 10);
  }
}
