package io.ledgermesh.order.api;

import static org.hamcrest.Matchers.hasSize;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
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
  void timelineOfAnUnknownOrderIs404() throws Exception {
    mvc.perform(get("/orders/nope/timeline")).andExpect(status().isNotFound());
  }
}
