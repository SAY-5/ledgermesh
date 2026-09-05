package io.ledgermesh.payment.authorize;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.when;

import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import io.ledgermesh.common.events.InventoryReserved;
import io.ledgermesh.common.events.OrderLine;
import io.ledgermesh.common.events.Topics;
import io.ledgermesh.common.outbox.OutboxEvent;
import io.ledgermesh.common.outbox.OutboxEventRepository;
import io.ledgermesh.payment.domain.Payment;
import io.ledgermesh.payment.domain.PaymentRepository;
import io.ledgermesh.payment.domain.PaymentStatus;
import io.ledgermesh.payment.processor.PaymentProcessor;
import io.ledgermesh.payment.processor.PaymentProcessor.Approved;
import io.ledgermesh.payment.processor.ProcessorUnavailableException;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.bean.override.mockito.MockitoBean;

@SpringBootTest
class PaymentServiceTest {

  @Autowired private PaymentService service;
  @Autowired private PaymentRepository payments;
  @Autowired private OutboxEventRepository outbox;
  @Autowired private CircuitBreakerRegistry breakers;
  @MockitoBean private PaymentProcessor processor;

  @BeforeEach
  void clean() {
    reset(processor);
    breakers.circuitBreaker(PaymentAuthorizer.RESILIENCE_NAME).reset();
    outbox.deleteAll();
    payments.deleteAll();
  }

  @Test
  void recordIsIdempotentPerOrder() {
    InventoryReserved event = reserved("o1");

    Payment first = service.record(event, "c");
    Payment again = service.record(event, "c");

    assertThat(first.getOrderId()).isEqualTo(again.getOrderId());
    assertThat(payments.count()).isEqualTo(1);
    assertThat(first.getStatus()).isEqualTo(PaymentStatus.NEW);
  }

  @Test
  void successfulAttemptCommitsOutcomeAndOutboxEventTogether() {
    when(processor.authorize(anyString(), anyString(), any(), anyInt())).thenReturn(new Approved("AUTH-9"));
    service.record(reserved("o2"), "c");

    assertThat(service.attempt("o2")).contains(PaymentStatus.AUTHORIZED);

    assertThat(payments.findById("o2").orElseThrow().getAuthorizationCode()).isEqualTo("AUTH-9");
    assertThat(outbox.findAll()).extracting(OutboxEvent::getTopic).containsExactly(Topics.PAYMENT_COMPLETED);
  }

  @Test
  void processorOutageDefersAndTheSweepFinishesLater() {
    when(processor.authorize(anyString(), anyString(), any(), anyInt()))
        .thenThrow(new ProcessorUnavailableException("outage"));
    service.record(reserved("o3"), "c");

    assertThat(service.attempt("o3")).contains(PaymentStatus.DEFERRED);
    assertThat(outbox.count()).isZero();
    assertThat(payments.findById("o3").orElseThrow().getNextAttemptAt()).isNotNull();

    reset(processor);
    when(processor.authorize(anyString(), anyString(), any(), anyInt())).thenReturn(new Approved("AUTH-3"));
    assertThat(service.sweep()).isEqualTo(1);

    assertThat(payments.findById("o3").orElseThrow().getStatus()).isEqualTo(PaymentStatus.AUTHORIZED);
    assertThat(outbox.findAll()).extracting(OutboxEvent::getTopic).containsExactly(Topics.PAYMENT_COMPLETED);
  }

  @Test
  void sweepPicksUpPaymentsThatWereRecordedButNeverAttempted() {
    when(processor.authorize(anyString(), anyString(), any(), anyInt())).thenReturn(new Approved("AUTH-4"));
    service.record(reserved("o4"), "c");

    assertThat(service.sweep()).isEqualTo(1);
    assertThat(service.sweep()).isZero();
    assertThat(payments.findById("o4").orElseThrow().getStatus()).isEqualTo(PaymentStatus.AUTHORIZED);
  }

  @Test
  void settledPaymentsAreNeverAttemptedAgain() {
    when(processor.authorize(anyString(), anyString(), any(), anyInt())).thenReturn(new Approved("AUTH-5"));
    service.record(reserved("o5"), "c");
    service.attempt("o5");

    assertThat(service.attempt("o5")).isEmpty();
    assertThat(outbox.count()).isEqualTo(1);
  }

  private static InventoryReserved reserved(String orderId) {
    return new InventoryReserved(
        "evt-" + orderId,
        orderId,
        "c",
        Instant.now(),
        "cust",
        List.of(new OrderLine("SKU", 1)),
        new BigDecimal("12.50"));
  }
}
