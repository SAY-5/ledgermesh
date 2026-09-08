package io.ledgermesh.payment.authorize;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import io.ledgermesh.payment.processor.PaymentProcessor;
import io.ledgermesh.payment.processor.PaymentProcessor.Approved;
import io.ledgermesh.payment.processor.PaymentProcessor.Declined;
import io.ledgermesh.payment.processor.ProcessorUnavailableException;
import java.math.BigDecimal;
import java.time.Duration;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.bean.override.mockito.MockitoBean;

@SpringBootTest
class PaymentAuthorizerTest {

  @Autowired private PaymentAuthorizer authorizer;
  @Autowired private CircuitBreakerRegistry breakers;
  @MockitoBean private PaymentProcessor processor;

  @BeforeEach
  void resetState() {
    reset(processor);
    breakers.circuitBreaker(PaymentAuthorizer.RESILIENCE_NAME).reset();
  }

  @Test
  void transientFaultIsRetriedAndThenAuthorized() {
    when(processor.authorize(anyString(), anyString(), any(), anyInt()))
        .thenThrow(new ProcessorUnavailableException("busy"))
        .thenReturn(new Approved("AUTH-1"));

    AuthorizationOutcome outcome = authorizer.authorize("o1", "cust", BigDecimal.TEN);

    assertThat(outcome).isEqualTo(new AuthorizationOutcome.Authorized("AUTH-1"));
    verify(processor, times(2)).authorize(anyString(), anyString(), any(), anyInt());
  }

  @Test
  void declineIsFinalAndNotRetried() {
    when(processor.authorize(anyString(), anyString(), any(), anyInt()))
        .thenReturn(new Declined("CARD_DECLINED"));

    assertThat(authorizer.authorize("o2", "cust", BigDecimal.TEN))
        .isEqualTo(new AuthorizationOutcome.Declined("CARD_DECLINED"));
    verify(processor, times(1)).authorize(anyString(), anyString(), any(), anyInt());
  }

  @Test
  void exhaustedRetriesDeferInsteadOfFailing() {
    when(processor.authorize(anyString(), anyString(), any(), anyInt()))
        .thenThrow(new ProcessorUnavailableException("busy"));

    AuthorizationOutcome outcome = authorizer.authorize("o3", "cust", BigDecimal.TEN);

    assertThat(outcome).isInstanceOf(AuthorizationOutcome.Deferred.class);
    verify(processor, times(3)).authorize(anyString(), anyString(), any(), anyInt());
  }

  @Test
  void hungProcessorIsCutByTheTimeLimiter() {
    when(processor.authorize(anyString(), anyString(), any(), anyInt()))
        .thenAnswer(
            inv -> {
              Thread.sleep(5000);
              return new Approved("late");
            });

    long start = System.nanoTime();
    AuthorizationOutcome outcome = authorizer.authorize("o4", "cust", BigDecimal.TEN);

    assertThat(outcome).isInstanceOf(AuthorizationOutcome.Deferred.class);
    assertThat(Duration.ofNanos(System.nanoTime() - start)).isLessThan(Duration.ofSeconds(3));
  }

  @Test
  void openBreakerFailsFastIntoTheDeferredQueue() {
    when(processor.authorize(anyString(), anyString(), any(), anyInt()))
        .thenThrow(new ProcessorUnavailableException("busy"));
    for (int i = 0; i < 2; i++) {
      authorizer.authorize("o5-" + i, "cust", BigDecimal.TEN);
    }
    CircuitBreaker breaker = breakers.circuitBreaker(PaymentAuthorizer.RESILIENCE_NAME);
    assertThat(breaker.getState()).isEqualTo(CircuitBreaker.State.OPEN);

    reset(processor);
    AuthorizationOutcome outcome = authorizer.authorize("o6", "cust", BigDecimal.TEN);

    assertThat(outcome).isInstanceOf(AuthorizationOutcome.Deferred.class);
    verify(processor, times(0)).authorize(anyString(), anyString(), any(), anyInt());
  }

  @Test
  void slowProcessorIsCutOnTheListenerPathButCompletesOnTheDeferredPath() {
    when(processor.authorize(anyString(), anyString(), any(), anyInt()))
        .thenAnswer(
            inv -> {
              Thread.sleep(2000);
              return new Approved("AUTH-SLOW");
            });
    assertThat(authorizer.authorize("o7", "cust", BigDecimal.TEN))
        .isInstanceOf(AuthorizationOutcome.Deferred.class);
    breakers.circuitBreaker(PaymentAuthorizer.RESILIENCE_NAME).reset();
    assertThat(authorizer.authorizeDeferred("o7", "cust", BigDecimal.TEN, 3))
        .isEqualTo(new AuthorizationOutcome.Authorized("AUTH-SLOW"));
  }
}
