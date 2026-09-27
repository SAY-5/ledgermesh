package io.ledgermesh.e2e;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import io.ledgermesh.common.events.EventCodec;
import io.ledgermesh.common.events.OrderCancelled;
import io.ledgermesh.common.events.OrderCreated;
import io.ledgermesh.common.events.OrderLine;
import io.ledgermesh.common.events.Topics;
import io.ledgermesh.inventory.stock.Reservation;
import io.ledgermesh.inventory.stock.ReservationRepository;
import io.micrometer.core.instrument.MeterRegistry;
import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

class ReservationLedgerIT {

  private static final Duration TIMEOUT = Duration.ofSeconds(30);
  private static final String SKU = "E2E-LEDGER";

  @BeforeAll
  static void start() {
    Stack.start();
  }

  /**
   * The compensation and the reservation travel on independent topics. When the release lands first
   * (the reaper cancelled a pending order, or a dead lettered order.created is replayed later), it
   * must not credit units the order never took, and the late reservation must not take them either.
   */
  @Test
  void aReleaseThatArrivesBeforeTheReservationLeavesStockAloneAndBlocksTheLateReservation()
      throws Exception {
    Stack.putStock(SKU, 10);
    EventCodec codec = Stack.inventory.getBean(EventCodec.class);
    MeterRegistry meters = Stack.inventory.getBean(MeterRegistry.class);
    ReservationRepository ledger = Stack.inventory.getBean(ReservationRepository.class);
    String orderId = UUID.randomUUID().toString();
    List<OrderLine> lines = List.of(new OrderLine(SKU, 3));
    double noopsBefore = releases(meters, "noop");
    double rejectedBefore = reservations(meters, "rejected");

    publish(
        Topics.ORDER_CANCELLED,
        orderId,
        codec.encode(
            new OrderCancelled(
                UUID.randomUUID().toString(),
                orderId,
                "corr-ledger",
                Instant.now(),
                "RESERVATION_TIMEOUT",
                lines)));
    await().atMost(TIMEOUT).until(() -> releases(meters, "noop") > noopsBefore);
    assertThat(Stack.stock(SKU)).isEqualTo(10);
    assertThat(ledger.findByOrderIdOrderBySkuAsc(orderId))
        .extracting(Reservation::getState, Reservation::getQuantity)
        .containsExactly(org.assertj.core.groups.Tuple.tuple(Reservation.State.RELEASED, 0));

    publish(
        Topics.ORDER_CREATED,
        orderId,
        codec.encode(
            new OrderCreated(
                UUID.randomUUID().toString(),
                orderId,
                "corr-ledger",
                Instant.now(),
                "cust-ledger",
                lines,
                new BigDecimal("3.00"))));
    await().atMost(TIMEOUT).until(() -> reservations(meters, "rejected") > rejectedBefore);
    assertThat(Stack.stock(SKU)).isEqualTo(10);
    assertThat(ledger.findByOrderIdOrderBySkuAsc(orderId))
        .allMatch(row -> row.getState() == Reservation.State.RELEASED && row.getQuantity() == 0);
  }

  private static double releases(MeterRegistry meters, String result) {
    return meters.counter("ledgermesh.inventory.releases", "result", result).count();
  }

  private static double reservations(MeterRegistry meters, String result) {
    return meters.counter("ledgermesh.inventory.reservations", "result", result).count();
  }

  private static void publish(String topic, String key, String payload) throws Exception {
    try (KafkaProducer<String, String> producer =
        new KafkaProducer<>(
            Map.of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
                Stack.REDPANDA.getBootstrapServers(),
                ProducerConfig.ACKS_CONFIG,
                "all"),
            new StringSerializer(),
            new StringSerializer())) {
      producer.send(new ProducerRecord<>(topic, key, payload)).get();
    }
  }
}
