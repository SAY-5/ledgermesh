package io.ledgermesh.order.admin;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import com.fasterxml.jackson.databind.ObjectMapper;
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
import java.util.ArrayList;
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
  @Autowired private ObjectMapper json;

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

  /** Each call takes the next unanswered orders, so repeated calls reach every one exactly once. */
  @Test
  void repeatedCallsSendEveryUnansweredCancellationOnceAndThenNothing() throws Exception {
    List<String> ids = new ArrayList<>();
    for (int i = 0; i < 3; i++) {
      String id = cancelled("cust-" + i);
      jdbc.update("update orders set compensation_due_at = null where id = ?", id);
      ids.add(id);
    }
    outbox.deleteAll();

    List<String> resent = new ArrayList<>();
    resent.addAll(resend(2, 2));
    resent.addAll(resend(2, 1));
    resent.addAll(resend(2, 0));

    assertThat(resent).containsExactlyInAnyOrderElementsOf(ids);
    assertThat(outbox.findAll())
        .extracting(OutboxEvent::getMessageKey)
        .containsExactlyInAnyOrderElementsOf(ids);
  }

  @Test
  void aBatchSizeOutOfRangeIsClampedInsteadOfFailing() throws Exception {
    String id = cancelled("cust-clamp");
    jdbc.update("update orders set compensation_due_at = null where id = ?", id);

    assertThat(resend(0, 1)).containsExactly(id);
    assertThat(resend(-1, 0)).isEmpty();
  }

  private List<String> resend(int max, int expected) throws Exception {
    String body =
        mvc.perform(post("/admin/compensations/resend").param("max", Integer.toString(max)))
            .andExpect(status().isOk())
            .andExpect(jsonPath("$.resent").value(expected))
            .andReturn()
            .getResponse()
            .getContentAsString();
    List<String> orders = new ArrayList<>();
    json.readTree(body).get("orders").forEach(node -> orders.add(node.asText()));
    return orders;
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
