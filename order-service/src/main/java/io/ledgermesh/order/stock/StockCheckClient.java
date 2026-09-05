package io.ledgermesh.order.stock;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import io.github.resilience4j.circuitbreaker.annotation.CircuitBreaker;
import io.github.resilience4j.timelimiter.annotation.TimeLimiter;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/**
 * Read-your-writes stock lookup used by the UI right after placing an order. The saga itself never
 * depends on this call: when inventory is slow or down the breaker opens, the time limiter cuts the
 * wait, and the last value seen for the sku is served from a local cache.
 */
@Component
public class StockCheckClient {

  public static final String BREAKER = "inventory";

  private static final Logger log = LoggerFactory.getLogger(StockCheckClient.class);

  private final InventoryGateway gateway;
  private final Cache<String, StockView> lastKnown;
  private final Executor executor = Executors.newVirtualThreadPerTaskExecutor();

  public StockCheckClient(InventoryGateway gateway) {
    this.gateway = gateway;
    this.lastKnown = Caffeine.newBuilder().expireAfterWrite(Duration.ofMinutes(10)).build();
  }

  @CircuitBreaker(name = BREAKER, fallbackMethod = "fromCache")
  @TimeLimiter(name = BREAKER)
  public CompletableFuture<StockView> stock(String sku) {
    return CompletableFuture.supplyAsync(
        () -> {
          StockView view = gateway.fetch(sku);
          lastKnown.put(sku, view);
          return view;
        },
        executor);
  }

  @SuppressWarnings("unused")
  private CompletableFuture<StockView> fromCache(String sku, Throwable cause) {
    log.debug("stock lookup for {} fell back to cache: {}", sku, cause.toString());
    StockView cached = lastKnown.getIfPresent(sku);
    return CompletableFuture.completedFuture(
        cached == null ? StockView.unknown(sku) : cached.asCached());
  }
}
