package io.ledgermesh.e2e;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.fasterxml.jackson.databind.JsonNode;
import io.ledgermesh.inventory.stock.StockItem;
import io.ledgermesh.inventory.stock.StockRepository;
import io.ledgermesh.payment.domain.Payment;
import io.ledgermesh.payment.domain.PaymentRepository;
import io.micrometer.core.instrument.MeterRegistry;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.time.Duration;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

class ExactlyOnceIT {

  private static final Duration TIMEOUT = Duration.ofSeconds(60);
  private static final BigDecimal PRICE = new BigDecimal("5.00");

  @BeforeAll
  static void start() {
    Stack.start();
  }

  @Test
  void repeatingARequestWithTheSameKeyPlacesOneOrderAndReturnsTheFirstAnswer() {
    Stack.putStock("E2E-KEY", 40);
    String key = UUID.randomUUID().toString();

    JsonNode first = Stack.createOrder(key, "cust-key", "E2E-KEY", 2, PRICE);
    JsonNode second = Stack.createOrder(key, "cust-key", "E2E-KEY", 2, PRICE);

    assertThat(second).isEqualTo(first);
    String id = first.get("id").asText();
    await().atMost(TIMEOUT).until(() -> Stack.orderStatus(id).equals("CONFIRMED"));
    assertThat(Stack.stock("E2E-KEY")).isEqualTo(38);
    assertThat(payment(id).getAmount()).isEqualByComparingTo("10.00");
  }

  @Test
  void twoRequestsRacingOnOneKeyPlaceOneOrder() throws Exception {
    Stack.putStock("E2E-RACE", 40);
    String key = UUID.randomUUID().toString();
    ExecutorService pool = Executors.newFixedThreadPool(2);
    try {
      List<Future<JsonNode>> answers = pool.invokeAll(List.of(place(key), place(key)));
      JsonNode first = answers.get(0).get();
      JsonNode second = answers.get(1).get();

      assertThat(second).isEqualTo(first);
      String id = first.get("id").asText();
      await().atMost(TIMEOUT).until(() -> Stack.orderStatus(id).equals("CONFIRMED"));
      assertThat(Stack.stock("E2E-RACE")).isEqualTo(38);
    } finally {
      pool.shutdownNow();
    }
  }

  @Test
  void anOutboxSendThatNeverConfirmedIsRepeatedAndTheConsumerIgnoresIt() {
    Stack.putStock("E2E-CRASH", 30);
    MeterRegistry orderMeters = Stack.order.getBean(MeterRegistry.class);
    MeterRegistry inventoryMeters = Stack.inventory.getBean(MeterRegistry.class);
    String id = Stack.createOrder("cust-crash", "E2E-CRASH", 3, PRICE);
    await().atMost(TIMEOUT).until(() -> Stack.orderStatus(id).equals("CONFIRMED"));

    int[] ledgerBefore = ledger("E2E-CRASH");
    double resendsBefore = orderMeters.counter("ledgermesh.outbox.resends").count();
    double duplicatesBefore = inventoryMeters.counter("ledgermesh.consumer.duplicates").count();

    // Exactly what a crash between the send and the ack leaves behind: attempted, not published.
    assertThat(unpublish(id)).isEqualTo(1);

    await()
        .atMost(TIMEOUT)
        .until(() -> orderMeters.counter("ledgermesh.outbox.resends").count() > resendsBefore);
    await()
        .atMost(TIMEOUT)
        .until(
            () ->
                inventoryMeters.counter("ledgermesh.consumer.duplicates").count()
                    > duplicatesBefore);

    assertThat(Stack.orderStatus(id)).isEqualTo("CONFIRMED");
    assertThat(ledger("E2E-CRASH")).isEqualTo(ledgerBefore);
    assertThat(ledgerBefore[0] + ledgerBefore[1]).isEqualTo(30);
    assertThat(payment(id).getAmount()).isEqualByComparingTo("15.00");
  }

  private static Callable<JsonNode> place(String key) {
    return () -> Stack.createOrder(key, "cust-race", "E2E-RACE", 2, PRICE);
  }

  /** Available and reserved units of a sku, which together may never change on a redelivery. */
  private static int[] ledger(String sku) {
    StockItem item = Stack.inventory.getBean(StockRepository.class).findById(sku).orElseThrow();
    return new int[] {item.getAvailable(), item.getReserved()};
  }

  private static Payment payment(String orderId) {
    return Stack.payment.getBean(PaymentRepository.class).findById(orderId).orElseThrow();
  }

  private static int unpublish(String orderId) {
    try (Connection connection =
            DriverManager.getConnection(
                Stack.jdbcUrl("orders"),
                Stack.POSTGRES.getUsername(),
                Stack.POSTGRES.getPassword());
        PreparedStatement statement =
            connection.prepareStatement(
                "update outbox_event set published_at = null"
                    + " where message_key = ? and event_type = 'OrderCreated'")) {
      statement.setString(1, orderId);
      return statement.executeUpdate();
    } catch (java.sql.SQLException e) {
      throw new IllegalStateException("could not reopen the outbox row", e);
    }
  }
}
