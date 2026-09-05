package io.ledgermesh.inventory.cache;

import io.github.resilience4j.circuitbreaker.annotation.CircuitBreaker;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import java.time.Duration;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;

/**
 * Redis view of stock levels. Writes go through after the owning database transaction commits, so
 * the cache never shows a value that was rolled back; every entry carries a TTL so a missed write
 * heals itself. Reads sit behind a breaker: when Redis is unhealthy the service answers from the
 * database instead of waiting on it.
 */
@Component
public class StockCache {

  public static final String BREAKER = "redis";
  static final String PREFIX = "stock:";

  private static final Logger log = LoggerFactory.getLogger(StockCache.class);

  private final StringRedisTemplate redis;
  private final Duration ttl;
  private final Counter hits;
  private final Counter misses;

  public StockCache(
      StringRedisTemplate redis,
      @Value("${ledgermesh.cache.ttl:60s}") Duration ttl,
      MeterRegistry meters) {
    this.redis = redis;
    this.ttl = ttl;
    this.hits = meters.counter("ledgermesh.cache.reads", "result", "hit");
    this.misses = meters.counter("ledgermesh.cache.reads", "result", "miss");
  }

  @CircuitBreaker(name = BREAKER, fallbackMethod = "readUnavailable")
  public Optional<Integer> read(String sku) {
    String value = redis.opsForValue().get(PREFIX + sku);
    if (value == null) {
      misses.increment();
      return Optional.empty();
    }
    hits.increment();
    return Optional.of(Integer.parseInt(value));
  }

  @SuppressWarnings("unused")
  private Optional<Integer> readUnavailable(String sku, Throwable cause) {
    misses.increment();
    return Optional.empty();
  }

  public void writeThrough(String sku, int available) {
    if (TransactionSynchronizationManager.isSynchronizationActive()) {
      TransactionSynchronizationManager.registerSynchronization(
          new TransactionSynchronization() {
            @Override
            public void afterCommit() {
              write(sku, available);
            }
          });
    } else {
      write(sku, available);
    }
  }

  private void write(String sku, int available) {
    try {
      redis.opsForValue().set(PREFIX + sku, Integer.toString(available), ttl);
    } catch (RuntimeException e) {
      log.warn("cache write for {} skipped: {}", sku, e.getMessage());
    }
  }
}
