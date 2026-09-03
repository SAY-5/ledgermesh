package io.ledgermesh.order.stock;

import java.time.Duration;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.web.client.ClientHttpRequestFactories;
import org.springframework.boot.web.client.ClientHttpRequestFactorySettings;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestClient;

@Component
public class HttpInventoryGateway implements InventoryGateway {

  private record StockPayload(String sku, int available) {}

  private final RestClient client;

  public HttpInventoryGateway(@Value("${ledgermesh.inventory.url}") String baseUrl) {
    this.client =
        RestClient.builder()
            .baseUrl(baseUrl)
            .requestFactory(
                ClientHttpRequestFactories.get(
                    ClientHttpRequestFactorySettings.DEFAULTS
                        .withConnectTimeout(Duration.ofMillis(500))
                        .withReadTimeout(Duration.ofSeconds(2))))
            .build();
  }

  @Override
  public StockView fetch(String sku) {
    StockPayload payload =
        client.get().uri("/stock/{sku}", sku).retrieve().body(StockPayload.class);
    if (payload == null) {
      throw new IllegalStateException("empty stock response for " + sku);
    }
    return StockView.live(payload.sku(), payload.available());
  }
}
