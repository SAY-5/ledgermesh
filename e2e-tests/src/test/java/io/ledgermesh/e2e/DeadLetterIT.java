package io.ledgermesh.e2e;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.fasterxml.jackson.databind.JsonNode;
import io.ledgermesh.common.events.EventCodec;
import io.ledgermesh.common.events.OrderCreated;
import io.ledgermesh.common.events.OrderLine;
import io.ledgermesh.common.events.Topics;
import io.ledgermesh.common.kafka.KafkaSupportConfiguration;
import io.ledgermesh.common.metrics.KafkaLagMetrics;
import io.micrometer.core.instrument.MeterRegistry;
import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

class DeadLetterIT {

  private static final Duration TIMEOUT = Duration.ofSeconds(60);
  private static final String SKU = "E2E-DLQ";
  private static final String KEY = "e2e-dlq-partition";

  @BeforeAll
  static void start() {
    Stack.start();
  }

  @Test
  void poisonRecordIsDeadLetteredThenReplayedOnceAndParked() throws Exception {
    Stack.putStock(SKU, 10);
    MeterRegistry meters = Stack.inventory.getBean(MeterRegistry.class);
    KafkaLagMetrics lag = Stack.inventory.getBean(KafkaLagMetrics.class);
    EventCodec codec = Stack.inventory.getBean(EventCodec.class);
    double failuresBefore = deliveryFailures(meters);

    // An order id longer than the outbox message key column: the reservation rolls back on every
    // attempt, so this record can never succeed.
    publish(codec.encode(orderCreated("o".repeat(120))), codec.encode(orderCreated(id())));

    await().atMost(TIMEOUT).until(() -> Stack.stock(SKU) == 9);
    assertThat(deliveryFailures(meters) - failuresBefore).isEqualTo(Stack.DLQ_ATTEMPTS);
    assertThat(deadLetters()).hasSize(1);
    assertThat(depth(lag)).isEqualTo(1);

    JsonNode first = replay(1);
    assertThat(first.get("replayed").asInt()).isEqualTo(1);
    assertThat(first.get("parked").asInt()).isZero();

    await().atMost(TIMEOUT).until(() -> deadLetters().size() == 2);
    assertThat(depth(lag)).isEqualTo(1);

    JsonNode second = replay(10);
    assertThat(second.get("replayed").asInt()).isZero();
    assertThat(second.get("parked").asInt()).isEqualTo(1);

    assertThat(deadLetters()).hasSize(2);
    assertThat(depth(lag)).isZero();
    // parked is not gone: the record is retained on the parked topic and listed by the admin api
    JsonNode parked = parked().get(Topics.ORDER_CREATED);
    assertThat(parked.size()).isEqualTo(1);
    assertThat(parked.get(0).get("key").asText()).isEqualTo(KEY);
    assertThat(parked.get(0).get("replays").asInt()).isEqualTo(Stack.DLQ_MAX_REPLAYS);
    assertThat(parked.get(0).get("originalTopic").asText()).isEqualTo(Topics.ORDER_CREATED);
    assertThat(parkedDepth(lag)).isEqualTo(1);
    assertThat(Stack.stock(SKU)).isEqualTo(9);
  }

  private static long depth(KafkaLagMetrics lag) {
    lag.refresh();
    return lag.dlqDepth().get(Topics.ORDER_CREATED);
  }

  private static long parkedDepth(KafkaLagMetrics lag) {
    lag.refresh();
    return lag.parkedDepth().get(Topics.ORDER_CREATED);
  }

  private static JsonNode parked() {
    return Stack.send("GET", "http://localhost:" + Stack.inventoryPort + "/admin/dlq/parked", null);
  }

  private static JsonNode replay(int max) {
    return Stack.send(
        "POST",
        "http://localhost:"
            + Stack.inventoryPort
            + "/admin/dlq/"
            + Topics.ORDER_CREATED
            + "/replay?max="
            + max,
        null);
  }

  private static double deliveryFailures(MeterRegistry meters) {
    return meters
        .counter(KafkaSupportConfiguration.DELIVERY_FAILURES, "topic", Topics.ORDER_CREATED)
        .count();
  }

  private static OrderCreated orderCreated(String orderId) {
    return new OrderCreated(
        id(),
        orderId,
        "corr-dlq",
        Instant.now(),
        "cust-dlq",
        List.of(new OrderLine(SKU, 1)),
        new BigDecimal("1.00"));
  }

  private static String id() {
    return UUID.randomUUID().toString();
  }

  private static void publish(String... payloads) throws Exception {
    try (KafkaProducer<String, String> producer =
        new KafkaProducer<>(
            Map.of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
                Stack.REDPANDA.getBootstrapServers(),
                ProducerConfig.ACKS_CONFIG,
                "all"),
            new StringSerializer(),
            new StringSerializer())) {
      for (String payload : payloads) {
        producer.send(new ProducerRecord<>(Topics.ORDER_CREATED, KEY, payload)).get();
      }
    }
  }

  private static List<ConsumerRecord<String, String>> deadLetters() {
    List<ConsumerRecord<String, String>> found = new ArrayList<>();
    try (KafkaConsumer<String, String> consumer =
        new KafkaConsumer<>(
            Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
                Stack.REDPANDA.getBootstrapServers(),
                ConsumerConfig.GROUP_ID_CONFIG,
                "e2e-dlq-reader-" + id(),
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,
                "earliest",
                ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,
                "false"),
            new StringDeserializer(),
            new StringDeserializer())) {
      consumer.subscribe(List.of(Topics.dlq(Topics.ORDER_CREATED)));
      Instant deadline = Instant.now().plusSeconds(10);
      while (Instant.now().isBefore(deadline)) {
        ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(500));
        records.forEach(found::add);
        if (!found.isEmpty() && records.isEmpty()) {
          break;
        }
      }
    }
    return found;
  }
}
