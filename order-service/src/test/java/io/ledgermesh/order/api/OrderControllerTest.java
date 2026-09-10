package io.ledgermesh.order.api;

import static org.assertj.core.api.Assertions.assertThat;
import static org.hamcrest.Matchers.hasSize;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.header;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;

@SpringBootTest
@AutoConfigureMockMvc
class OrderControllerTest {

  @Autowired private MockMvc mvc;
  @Autowired private ObjectMapper json;

  @Test
  void timelineListsTheCreationStepWithATimestamp() throws Exception {
    String body =
        mvc.perform(
                post("/orders")
                    .contentType(MediaType.APPLICATION_JSON)
                    .content(
                        """
                        {"customerId":"cust-t","items":[{"sku":"sku-1","quantity":1,"unitPrice":"2.00"}]}
                        """))
            .andExpect(status().isAccepted())
            .andExpect(jsonPath("$.deadlineAt").exists())
            .andReturn()
            .getResponse()
            .getContentAsString();
    JsonNode order = json.readTree(body);

    mvc.perform(get("/orders/{id}/timeline", order.get("id").asText()))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$", hasSize(1)))
        .andExpect(jsonPath("$[0].type").value("CREATED"))
        .andExpect(jsonPath("$[0].to").value("PENDING"))
        .andExpect(jsonPath("$[0].at").exists());
  }

  @Test
  void sameIdempotencyKeyReturnsTheFirstAnswerAndPlacesOneOrder() throws Exception {
    String key = UUID.randomUUID().toString();

    String first = submit(key, "false");
    String second = submit(key, "true");

    assertThat(second).isEqualTo(first);
    mvc.perform(get("/orders/{id}/timeline", json.readTree(first).get("id").asText()))
        .andExpect(jsonPath("$", hasSize(1)));
  }

  private String submit(String key, String replayed) throws Exception {
    return mvc.perform(
            post("/orders")
                .header(OrderController.IDEMPOTENCY_KEY, key)
                .contentType(MediaType.APPLICATION_JSON)
                .content(
                    """
                    {"customerId":"cust-i","items":[{"sku":"sku-9","quantity":1,"unitPrice":"3.00"}]}
                    """))
        .andExpect(status().isAccepted())
        .andExpect(header().string(OrderController.IDEMPOTENT_REPLAY, replayed))
        .andReturn()
        .getResponse()
        .getContentAsString();
  }

  @Test
  void timelineOfAnUnknownOrderIs404() throws Exception {
    mvc.perform(get("/orders/nope/timeline")).andExpect(status().isNotFound());
  }
}
