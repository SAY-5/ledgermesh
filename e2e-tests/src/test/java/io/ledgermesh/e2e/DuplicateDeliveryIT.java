package io.ledgermesh.e2e;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import io.ledgermesh.common.events.EventCodec;
import io.ledgermesh.common.events.OrderCreated;
import io.ledgermesh.common.events.OrderLine;
import io.ledgermesh.common.events.Topics;
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

class DuplicateDeliveryIT {

  @BeforeAll
  static void start() {
    Stack.start();
  }

  @Test
  void redeliveredOrderCreatedReservesStockExactlyOnce() throws Exception {
    Stack.putStock("E2E-DUP", 50);
    EventCodec codec = Stack.inventory.getBean(EventCodec.class);
    MeterRegistry meters = Stack.inventory.getBean(MeterRegistry.class);
    double duplicatesBefore = meters.counter("ledgermesh.consumer.duplicates").count();

    OrderCreated event =
        new OrderCreated(
            UUID.randomUUID().toString(),
            UUID.randomUUID().toString(),
            "corr-dup",
            Instant.now(),
            "cust-dup",
            List.of(new OrderLine("E2E-DUP", 4)),
            new BigDecimal("4.00"));
    String payload = codec.encode(event);

    try (KafkaProducer<String, String> producer =
        new KafkaProducer<>(
            Map.of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, Stack.REDPANDA.getBootstrapServers(),
                ProducerConfig.ACKS_CONFIG, "all"),
            new StringSerializer(),
            new StringSerializer())) {
      for (int i = 0; i < 3; i++) {
        producer
            .send(new ProducerRecord<>(Topics.ORDER_CREATED, event.orderId(), payload))
            .get();
      }
    }

    await().atMost(Duration.ofSeconds(30)).until(() -> Stack.stock("E2E-DUP") == 46);
    await()
        .atMost(Duration.ofSeconds(30))
        .until(() -> meters.counter("ledgermesh.consumer.duplicates").count() >= duplicatesBefore + 2);
    Thread.sleep(1500);
    assertThat(Stack.stock("E2E-DUP")).isEqualTo(46);
  }
}
