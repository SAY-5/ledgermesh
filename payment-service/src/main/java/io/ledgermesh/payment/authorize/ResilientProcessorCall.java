package io.ledgermesh.payment.authorize;

import io.github.resilience4j.circuitbreaker.annotation.CircuitBreaker;
import io.github.resilience4j.retry.annotation.Retry;
import io.github.resilience4j.timelimiter.annotation.TimeLimiter;
import io.ledgermesh.payment.processor.PaymentProcessor;
import io.ledgermesh.payment.processor.PaymentProcessor.Approved;
import io.ledgermesh.payment.processor.PaymentProcessor.Declined;
import io.ledgermesh.payment.processor.PaymentProcessor.Result;
import java.math.BigDecimal;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/**
 * The single decorated call to the processor. Order of decoration is
 * Retry(CircuitBreaker(TimeLimiter(call))): a hung call is cut by the time limiter, counted by the
 * breaker and retried; once the breaker is open calls fail fast without retrying; when retries are
 * exhausted the fallback on the outermost decorator defers the payment instead of failing it. Lives
 * in its own bean so the annotations are always applied through the proxy.
 */
@Component
public class ResilientProcessorCall {

  public static final String RESILIENCE_NAME = "processor";

  private static final Logger log = LoggerFactory.getLogger(ResilientProcessorCall.class);

  private final PaymentProcessor processor;
  private final Executor executor = Executors.newVirtualThreadPerTaskExecutor();

  public ResilientProcessorCall(PaymentProcessor processor) {
    this.processor = processor;
  }

  @Retry(name = RESILIENCE_NAME, fallbackMethod = "defer")
  @CircuitBreaker(name = RESILIENCE_NAME)
  @TimeLimiter(name = RESILIENCE_NAME)
  public CompletableFuture<AuthorizationOutcome> authorize(
      String orderId, String customerId, BigDecimal amount, AtomicInteger attempts) {
    return CompletableFuture.supplyAsync(
        () -> {
          int attempt = attempts.incrementAndGet();
          Result result = processor.authorize(orderId, customerId, amount, attempt);
          return switch (result) {
            case Approved a -> new AuthorizationOutcome.Authorized(a.authorizationCode());
            case Declined d -> new AuthorizationOutcome.Declined(d.reason());
          };
        },
        executor);
  }

  @SuppressWarnings("unused")
  private CompletableFuture<AuthorizationOutcome> defer(
      String orderId,
      String customerId,
      BigDecimal amount,
      AtomicInteger attempts,
      Throwable cause) {
    log.warn(
        "payment for order {} deferred after {} attempt(s): {}",
        orderId,
        attempts.get(),
        cause.toString());
    return CompletableFuture.completedFuture(
        new AuthorizationOutcome.Deferred(cause.getClass().getSimpleName()));
  }
}
