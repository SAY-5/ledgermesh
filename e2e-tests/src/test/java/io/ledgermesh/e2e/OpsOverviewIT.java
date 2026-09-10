package io.ledgermesh.e2e;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.fasterxml.jackson.databind.JsonNode;
import io.ledgermesh.common.events.Topics;
import java.math.BigDecimal;
import java.time.Duration;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.listener.MessageListenerContainer;

class OpsOverviewIT {

  private static final Duration TIMEOUT = Duration.ofSeconds(60);

  @BeforeAll
  static void start() {
    Stack.start();
  }

  @Test
  void overviewFollowsAnOrderThroughAStoppedListenerAndBack() {
    Stack.putStock("E2E-OPS", 20);
    MessageListenerContainer listener =
        Stack.inventory
            .getBean(KafkaListenerEndpointRegistry.class)
            .getListenerContainer("inventory-orders");
    long inFlightBefore = inFlight();

    listener.stop();
    String id = Stack.createOrder("cust-ops", "E2E-OPS", 1, new BigDecimal("4.00"));
    await().atMost(TIMEOUT).until(() -> inFlight() == inFlightBefore + 1);

    JsonNode open = overview(Stack.orderPort);
    assertThat(open.get("service").asText()).isNotBlank();
    assertThat(open.get("health").asText()).isEqualTo("UP");
    assertThat(open.at("/breakers/inventory").asText()).isEqualTo("CLOSED");
    assertThat(open.at("/sagas/stuck").asLong()).isZero();
    assertThat(open.at("/sagas/byState/PENDING").asLong()).isPositive();
    assertThat(open.get("deadLetterDepth").has(Topics.ORDER_CREATED)).isTrue();
    assertThat(open.get("consumerLag").size()).isPositive();
    await().atMost(TIMEOUT).until(() -> lag("inventory-service|" + Topics.ORDER_CREATED) >= 1);

    listener.start();
    await().atMost(TIMEOUT).until(() -> Stack.orderStatus(id).equals("CONFIRMED"));
    await().atMost(TIMEOUT).until(() -> inFlight() == inFlightBefore);
    await().atMost(TIMEOUT).until(() -> lag("inventory-service|" + Topics.ORDER_CREATED) == 0);
  }

  @Test
  void onlyTheServiceThatOwnsTheSagaReportsSagaNumbers() {
    JsonNode inventory = overview(Stack.inventoryPort);

    assertThat(inventory.get("health").asText()).isEqualTo("UP");
    assertThat(inventory.get("sagas").isNull()).isTrue();
    assertThat(inventory.get("deadLetterDepth").has(Topics.ORDER_CREATED)).isTrue();
    assertThat(overview(Stack.orderPort).get("sagas").isNull()).isFalse();
  }

  private static long inFlight() {
    return overview(Stack.orderPort).at("/sagas/inFlight").asLong();
  }

  private static long lag(String groupAndTopic) {
    return overview(Stack.inventoryPort).at("/consumerLag/" + groupAndTopic).asLong();
  }

  private static JsonNode overview(int port) {
    return Stack.send("GET", "http://localhost:" + port + "/ops/overview", null);
  }
}
