package io.ledgermesh.e2e;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.ledgermesh.common.idempotency.RequestDeduplicator;
import io.ledgermesh.common.idempotency.RequestDeduplicator.Outcome;
import io.ledgermesh.e2e.Stack.Reply;
import io.ledgermesh.inventory.stock.StockItem;
import io.ledgermesh.inventory.stock.StockRepository;
import io.ledgermesh.order.api.OrderController;
import io.ledgermesh.order.api.OrderResponse;
import io.ledgermesh.order.domain.OrderItem;
import io.ledgermesh.order.saga.OrderSagaService;
import io.ledgermesh.payment.domain.Payment;
import io.ledgermesh.payment.domain.PaymentRepository;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.transaction.support.TransactionTemplate;

class ExactlyOnceIT {

  private static final Duration TIMEOUT = Duration.ofSeconds(60);
  private static final BigDecimal PRICE = new BigDecimal("5.00");
  private static final String REPLAY = OrderController.IDEMPOTENT_REPLAY;

  @BeforeAll
  static void start() {
    Stack.start();
  }

  @Test
  void repeatingARequestWithTheSameKeyPlacesOneOrderAndReturnsTheFirstAnswer() {
    Stack.putStock("E2E-KEY", 40);
    String key = UUID.randomUUID().toString();

    Reply first = Stack.createOrder(key, "cust-key", "E2E-KEY", 2, PRICE);
    Reply second = Stack.createOrder(key, "cust-key", "E2E-KEY", 2, PRICE);

    assertThat(second.body()).isEqualTo(first.body());
    assertThat(first.header(REPLAY)).isEqualTo("false");
    assertThat(second.header(REPLAY)).isEqualTo("true");
    String id = first.body().get("id").asText();
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
      List<Future<Reply>> answers = pool.invokeAll(List.of(place(key), place(key)));
      Reply first = answers.get(0).get();
      Reply second = answers.get(1).get();

      // whichever call lost, by finding the answer or by failing to store its own, says so
      assertThat(second.body()).isEqualTo(first.body());
      assertThat(List.of(first.header(REPLAY), second.header(REPLAY)))
          .containsExactlyInAnyOrder("false", "true");
      assertThat(ordersOf("cust-race")).isEqualTo(1);
      String id = first.body().get("id").asText();
      await().atMost(TIMEOUT).until(() -> Stack.orderStatus(id).equals("CONFIRMED"));
      assertThat(Stack.stock("E2E-RACE")).isEqualTo(38);
    } finally {
      pool.shutdownNow();
    }
  }

  @Test
  void aRequestThatStoresItsAnswerAfterTheFirstCommittedPlacesNoSecondOrder() throws Exception {
    Stack.putStock("E2E-RACE-LATE", 40);
    String key = UUID.randomUUID().toString();
    RequestDeduplicator requests = Stack.order.getBean(RequestDeduplicator.class);
    OrderSagaService saga = Stack.order.getBean(OrderSagaService.class);
    Supplier<OrderResponse> place =
        () ->
            OrderResponse.from(
                saga.create("cust-race-late", List.of(new OrderItem("E2E-RACE-LATE", 2, PRICE))));
    CountDownLatch lookedUp = new CountDownLatch(1);
    CountDownLatch firstCommitted = new CountDownLatch(1);
    ExecutorService pool = Executors.newSingleThreadExecutor();
    try {
      // the interleaving the race above only sometimes hits: this request found no answer for the
      // key and is held in its work until the first one has committed its order and its answer
      Future<Outcome<OrderResponse>> late =
          pool.submit(
              () ->
                  requests.once(
                      key,
                      OrderResponse.class,
                      () -> {
                        lookedUp.countDown();
                        waitFor(firstCommitted);
                        return place.get();
                      }));
      assertThat(lookedUp.await(10, TimeUnit.SECONDS)).isTrue();
      OrderResponse first = requests.once(key, OrderResponse.class, place).body();
      firstCommitted.countDown();

      assertThatThrownBy(() -> late.get(10, TimeUnit.SECONDS))
          .hasCauseInstanceOf(DataIntegrityViolationException.class);
      assertThat(requests.stored(key, OrderResponse.class)).contains(first);
      assertThat(ordersOf("cust-race-late")).isEqualTo(1);
      await().atMost(TIMEOUT).until(() -> Stack.orderStatus(first.id()).equals("CONFIRMED"));
      assertThat(Stack.stock("E2E-RACE-LATE")).isEqualTo(38);
    } finally {
      pool.shutdownNow();
    }
  }

  @Test
  void aCallWhoseAnswerWaitsOnTheKeyUntilTheFirstCommitsReturnsTheFirstAnswer() throws Exception {
    Stack.putStock("E2E-RACE-WAIT", 40);
    String key = UUID.randomUUID().toString();
    RequestDeduplicator requests = Stack.order.getBean(RequestDeduplicator.class);
    OrderSagaService saga = Stack.order.getBean(OrderSagaService.class);
    TransactionTemplate tx = Stack.order.getBean(TransactionTemplate.class);
    ObjectMapper json = Stack.order.getBean(ObjectMapper.class);
    Counter replays =
        Stack.order.getBean(MeterRegistry.class).counter("ledgermesh.requests.replayed");
    double replaysBefore = replays.count();
    AtomicReference<Future<Reply>> second = new AtomicReference<>();
    ExecutorService pool = Executors.newSingleThreadExecutor();
    try {
      // the first call has stored its answer without committing it, so the second one finds no
      // answer, places an order of its own and then waits on the key until the first commits
      OrderResponse first =
          tx.execute(
              status -> {
                OrderResponse body =
                    requests
                        .once(
                            key,
                            OrderResponse.class,
                            () ->
                                OrderResponse.from(
                                    saga.create(
                                        "cust-race-wait",
                                        List.of(new OrderItem("E2E-RACE-WAIT", 2, PRICE)))))
                        .body();
                second.set(
                    pool.submit(
                        () -> Stack.createOrder(key, "cust-race-wait", "E2E-RACE-WAIT", 2, PRICE)));
                await().atMost(TIMEOUT).until(() -> sessionsWaitingToStoreAnAnswer() > 0);
                return body;
              });

      Reply reply = second.get().get(10, TimeUnit.SECONDS);
      assertThat(reply.status()).isEqualTo(202);
      assertThat(reply.header(REPLAY)).isEqualTo("true");
      assertThat(reply.body()).isEqualTo(Stack.JSON.readTree(json.writeValueAsString(first)));
      // answered by the controller once its insert failed, not by the lookup that counts replays
      assertThat(replays.count()).isEqualTo(replaysBefore);
      assertThat(ordersOf("cust-race-wait")).isEqualTo(1);
      await().atMost(TIMEOUT).until(() -> Stack.orderStatus(first.id()).equals("CONFIRMED"));
      assertThat(Stack.stock("E2E-RACE-WAIT")).isEqualTo(38);
    } finally {
      pool.shutdownNow();
    }
  }

  @Test
  void aKeyRetriedAcrossAnOrderServiceRestartPlacesOneOrder() {
    Stack.putStock("E2E-KEY-RESTART", 40);
    String key = UUID.randomUUID().toString();
    Reply first = Stack.createOrder(key, "cust-key-restart", "E2E-KEY-RESTART", 2, PRICE);

    // the client never saw the answer and the service went away in between: the store is in
    // Postgres, so the restarted service answers the same key with the same body
    Stack.order.close();
    Stack.order = Stack.bootOrder();
    Reply second = Stack.createOrder(key, "cust-key-restart", "E2E-KEY-RESTART", 2, PRICE);

    assertThat(second.body()).isEqualTo(first.body());
    assertThat(second.header(REPLAY)).isEqualTo("true");
    String id = first.body().get("id").asText();
    await().atMost(TIMEOUT).until(() -> Stack.orderStatus(id).equals("CONFIRMED"));
    assertThat(Stack.stock("E2E-KEY-RESTART")).isEqualTo(38);
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

  private static Callable<Reply> place(String key) {
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

  private static int ordersOf(String customer) {
    try (Connection connection =
            DriverManager.getConnection(
                Stack.jdbcUrl("orders"),
                Stack.POSTGRES.getUsername(),
                Stack.POSTGRES.getPassword());
        PreparedStatement statement =
            connection.prepareStatement("select count(*) from orders where customer_id = ?")) {
      statement.setString(1, customer);
      try (ResultSet rows = statement.executeQuery()) {
        rows.next();
        return rows.getInt(1);
      }
    } catch (SQLException e) {
      throw new IllegalStateException("could not count the orders of " + customer, e);
    }
  }

  /** Order service sessions blocked on a lock while they insert an idempotency answer. */
  private static int sessionsWaitingToStoreAnAnswer() {
    try (Connection connection =
            DriverManager.getConnection(
                Stack.jdbcUrl("orders"),
                Stack.POSTGRES.getUsername(),
                Stack.POSTGRES.getPassword());
        PreparedStatement statement =
            connection.prepareStatement(
                "select count(*) from pg_stat_activity where datname = 'orders'"
                    + " and wait_event_type = 'Lock'"
                    + " and query ilike 'insert into idempotent_request%'")) {
      try (ResultSet rows = statement.executeQuery()) {
        rows.next();
        return rows.getInt(1);
      }
    } catch (SQLException e) {
      throw new IllegalStateException("could not read the lock waits of the orders database", e);
    }
  }

  private static void waitFor(CountDownLatch latch) {
    try {
      assertThat(latch.await(10, TimeUnit.SECONDS)).isTrue();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException(e);
    }
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
