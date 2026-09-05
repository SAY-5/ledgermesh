package io.ledgermesh.e2e;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.math.BigDecimal;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.listener.MessageListenerContainer;

class ConsumerRestartIT {

  private static final Duration SAGA_TIMEOUT = Duration.ofSeconds(60);

  @BeforeAll
  static void start() {
    Stack.start();
  }

  @Test
  void paymentListenerStoppedMidFlightStillConfirmsAfterRestart() throws Exception {
    Stack.putStock("E2E-RESTART-A", 100);
    MessageListenerContainer listener =
        Stack.payment
            .getBean(KafkaListenerEndpointRegistry.class)
            .getListenerContainer("payment-reserved");
    listener.stop();

    String id = Stack.createOrder("cust-r", "E2E-RESTART-A", 1, new BigDecimal("3.00"));
    await().atMost(SAGA_TIMEOUT).until(() -> Stack.orderStatus(id).equals("RESERVED"));
    Thread.sleep(2000);
    assertThat(Stack.orderStatus(id)).isEqualTo("RESERVED");

    listener.start();
    await().atMost(SAGA_TIMEOUT).until(() -> Stack.orderStatus(id).equals("CONFIRMED"));
  }

  @Test
  void inventoryServiceRestartedWithOrdersInFlightConfirmsEveryOrder() {
    Stack.putStock("E2E-RESTART-B", 1000);
    List<String> ids = new ArrayList<>();
    for (int i = 0; i < 12; i++) {
      ids.add(Stack.createOrder("cust-" + i, "E2E-RESTART-B", 1, new BigDecimal("1.00")));
    }

    Stack.inventory.close();
    Stack.inventory = Stack.bootInventory();

    for (String id : ids) {
      await().atMost(SAGA_TIMEOUT).until(() -> Stack.orderStatus(id).equals("CONFIRMED"));
    }
    assertThat(Stack.stock("E2E-RESTART-B")).isEqualTo(988);
  }
}
