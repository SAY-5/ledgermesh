package io.ledgermesh.inventory.cache;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.never;
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
import org.springframework.data.redis.RedisConnectionFailureException;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ValueOperations;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.transaction.support.TransactionTemplate;

@SpringBootTest
class StockCacheTest {

  @Autowired private StockCache cache;
  @Autowired private TransactionTemplate tx;
  @Autowired private CircuitBreakerRegistry breakers;
  @MockitoBean private StringRedisTemplate redis;
  @MockitoBean private ValueOperations<String, String> values;

  @BeforeEach
  void wire() {
    reset(redis, values);
    when(redis.opsForValue()).thenReturn(values);
    breakers.circuitBreaker(StockCache.BREAKER).reset();
  }

  @Test
  void writeThroughSetsValueWithConfiguredTtl() {
    cache.writeThrough("A", 12);

    verify(values).set("stock:A", "12", Duration.ofSeconds(45));
  }

  @Test
  void writeInsideTransactionIsDeferredUntilCommit() {
    tx.executeWithoutResult(
        status -> {
          cache.writeThrough("A", 5);
          verify(values, never())
              .set(anyString(), anyString(), org.mockito.ArgumentMatchers.any(Duration.class));
        });

    verify(values).set("stock:A", "5", Duration.ofSeconds(45));
  }

  @Test
  void rolledBackTransactionNeverTouchesTheCache() {
    tx.executeWithoutResult(
        status -> {
          cache.writeThrough("A", 5);
          status.setRollbackOnly();
        });

    verify(values, never())
        .set(anyString(), anyString(), org.mockito.ArgumentMatchers.any(Duration.class));
  }

  @Test
  void readReturnsCachedValueOrEmpty() {
    when(values.get("stock:A")).thenReturn("9");
    when(values.get("stock:B")).thenReturn(null);

    assertThat(cache.read("A")).contains(9);
    assertThat(cache.read("B")).isEmpty();
  }

  @Test
  void redisOutageOpensBreakerAndReadsFallBackToEmpty() {
    when(values.get(anyString())).thenThrow(new RedisConnectionFailureException("down"));

    for (int i = 0; i < 3; i++) {
      assertThat(cache.read("A")).isEmpty();
    }
    assertThat(breakers.circuitBreaker(StockCache.BREAKER).getState())
        .isEqualTo(CircuitBreaker.State.OPEN);

    assertThat(cache.read("A")).isEmpty();
    verify(values, times(3)).get(anyString());
  }

  @Test
  void writeFailureIsSwallowedSoTheReservationStillCommits() {
    org.mockito.Mockito.doThrow(new RedisConnectionFailureException("down"))
        .when(values)
        .set(anyString(), anyString(), org.mockito.ArgumentMatchers.any(Duration.class));

    cache.writeThrough("A", 1);
  }
}
