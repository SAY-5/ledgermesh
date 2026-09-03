package io.ledgermesh.order.stock;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import java.time.Duration;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.bean.override.mockito.MockitoBean;

@SpringBootTest
class StockCheckClientTest {

  @Autowired private StockCheckClient client;
  @Autowired private CircuitBreakerRegistry breakers;
  @MockitoBean private InventoryGateway gateway;

  @BeforeEach
  void resetBreaker() {
    reset(gateway);
    breakers.circuitBreaker(StockCheckClient.BREAKER).reset();
  }

  @Test
  void servesLiveValueWhenInventoryAnswers() {
    when(gateway.fetch("sku-1")).thenReturn(StockView.live("sku-1", 42));

    StockView view = client.stock("sku-1").join();

    assertThat(view.available()).isEqualTo(42);
    assertThat(view.source()).isEqualTo(StockView.LIVE);
  }

  @Test
  void fallsBackToLastKnownValueWhenInventoryFails() {
    when(gateway.fetch("sku-2")).thenReturn(StockView.live("sku-2", 7));
    client.stock("sku-2").join();
    when(gateway.fetch("sku-2")).thenThrow(new IllegalStateException("connection refused"));

    StockView view = client.stock("sku-2").join();

    assertThat(view.available()).isEqualTo(7);
    assertThat(view.source()).isEqualTo(StockView.CACHE);
  }

  @Test
  void reportsUnknownWhenNothingWasEverCached() {
    when(gateway.fetch("sku-3")).thenThrow(new IllegalStateException("connection refused"));

    StockView view = client.stock("sku-3").join();

    assertThat(view.available()).isNull();
    assertThat(view.source()).isEqualTo(StockView.UNKNOWN);
  }

  @Test
  void breakerOpensAfterRepeatedFailuresAndStopsCallingInventory() {
    when(gateway.fetch(anyString())).thenThrow(new IllegalStateException("down"));

    for (int i = 0; i < 4; i++) {
      client.stock("sku-4").join();
    }
    CircuitBreaker breaker = breakers.circuitBreaker(StockCheckClient.BREAKER);
    assertThat(breaker.getState()).isEqualTo(CircuitBreaker.State.OPEN);

    client.stock("sku-4").join();
    client.stock("sku-4").join();
    verify(gateway, times(4)).fetch(anyString());
  }

  @Test
  void slowInventoryIsCutOffByTheTimeLimiter() {
    when(gateway.fetch("sku-5"))
        .thenAnswer(
            inv -> {
              Thread.sleep(2000);
              return StockView.live("sku-5", 1);
            });

    long start = System.nanoTime();
    StockView view = client.stock("sku-5").join();
    Duration elapsed = Duration.ofNanos(System.nanoTime() - start);

    assertThat(view.source()).isEqualTo(StockView.UNKNOWN);
    assertThat(elapsed).isLessThan(Duration.ofMillis(1500));
  }
}
