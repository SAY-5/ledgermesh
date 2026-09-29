package io.ledgermesh.order.admin;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import io.ledgermesh.common.events.Topics;
import io.ledgermesh.common.outbox.OutboxEvent;
import io.ledgermesh.common.outbox.OutboxEventRepository;
import io.ledgermesh.order.domain.Order;
import io.ledgermesh.order.domain.OrderEventRepository;
import io.ledgermesh.order.domain.OrderItem;
import io.ledgermesh.order.domain.OrderRepository;
import io.ledgermesh.order.domain.SagaEvent;
import io.ledgermesh.order.saga.OrderSagaService;
import java.math.BigDecimal;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.web.servlet.MockMvc;

@SpringBootTest
@AutoConfigureMockMvc
class CompensationAdminControllerTest {

  @Autowired private MockMvc mvc;
  @Autowired private OrderSagaService saga;
  @Autowired private OrderRepository orders;
  @Autowired private OrderEventRepository timeline;
  @Autowired private OutboxEventRepository outbox;
  @Autowired private JdbcTemplate jdbc;

  @BeforeEach
  void clean() {
    outbox.deleteAll();
    timeline.deleteAll();
    orders.deleteAll();
  }

  /**
   * After an upgrade, orders cancelled before the order service waited for the payment service's
   * answer are not waiting for one; the repair sends their cancellation again and makes them wait,
   * and leaves answered, out of stock and confirmed orders alone.
   */
  @Test
  void resendSendsTheCancellationOfEveryUnansweredCancelledOrderAgain() throws Exception {
    String answered = cancelled("cust-a");
    saga.compensated(answered, "AUTHORIZED", "c");
    String beforeUpgrade = cancelled("cust-b");
    jdbc.update("update orders set compensation_due_at = null where id = ?", beforeUpgrade);
    Order outOfStock = order("cust-c");
    saga.apply(outOfStock.getId(), SagaEvent.INVENTORY_REJECTED, "c");
    Order confirmed = order("cust-d");
    saga.apply(confirmed.getId(), SagaEvent.INVENTORY_RESERVED, "c");
    saga.apply(confirmed.getId(), SagaEvent.PAYMENT_COMPLETED, "c");
    outbox.deleteAll();

    mvc.perform(post("/admin/compensations/resend"))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.resent").value(1))
        .andExpect(jsonPath("$.orders[0]").value(beforeUpgrade));

    List<OutboxEvent> sent = outbox.findAll();
    assertThat(sent).extracting(OutboxEvent::getTopic).containsExactly(Topics.ORDER_CANCELLED);
    assertThat(sent.get(0).getMessageKey()).isEqualTo(beforeUpgrade);
    assertThat(orders.findById(beforeUpgrade).orElseThrow().getCompensationDueAt()).isNotNull();
  }

  private String cancelled(String customer) {
    Order order = order(customer);
    saga.apply(order.getId(), SagaEvent.INVENTORY_RESERVED, "c");
    saga.apply(order.getId(), SagaEvent.PAYMENT_FAILED, "c");
    return order.getId();
  }

  private Order order(String customer) {
    return saga.create(customer, List.of(new OrderItem("sku-1", 1, new BigDecimal("5.00"))));
  }
}
