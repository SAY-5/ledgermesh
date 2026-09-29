package io.ledgermesh.payment.processor;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import io.ledgermesh.common.events.DomainEvent;
import io.ledgermesh.common.events.EventCodec;
import io.ledgermesh.common.events.InventoryReserved;
import io.ledgermesh.common.events.OrderCancelled;
import io.ledgermesh.common.events.OrderLine;
import io.ledgermesh.payment.authorize.PaymentAuthorizer;
import io.ledgermesh.payment.authorize.PaymentService;
import io.ledgermesh.payment.domain.PaymentRepository;
import io.ledgermesh.payment.domain.PaymentStatus;
import io.ledgermesh.payment.messaging.PaymentEventListener;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

/**
 * The synthetic processor and the real decorator: a first attempt that is slow is cut off by the
 * time limiter, but the call is not stopped, and it approves after the retry already authorized the
 * payment and the cancellation voided it and released the card. That late approval is a live
 * authorization all the same, and has to be released like any other.
 */
@SpringBootTest(
    properties = {
      "spring.datasource.url=jdbc:h2:mem:late-approvals;DB_CLOSE_DELAY=-1",
      "ledgermesh.processor.transient-percent=0",
      "ledgermesh.processor.slow-percent=50",
      "ledgermesh.processor.slow-millis=" + LateApprovalTest.SLOW_MILLIS
    })
class LateApprovalTest {

  static final long SLOW_MILLIS = 1500;

  @Autowired private PaymentService service;
  @Autowired private PaymentRepository payments;
  @Autowired private PaymentEventListener listener;
  @Autowired private AuthorizationHolds holds;
  @Autowired private EventCodec codec;
  @Autowired private CircuitBreakerRegistry breakers;

  @BeforeEach
  void clean() {
    breakers.circuitBreaker(PaymentAuthorizer.RESILIENCE_NAME).reset();
    payments.deleteAll();
  }

  @Test
  void anApprovalThatLandsAfterTheVoidAndItsReleaseIsReleasedToo() throws Exception {
    // an order whose first attempt takes the slow path and whose second is answered at once
    String orderId =
        IntStream.range(0, 1000)
            .mapToObj(i -> "late-" + i)
            .filter(id -> SyntheticProcessor.bucket(id + ":1") < 50)
            .filter(id -> SyntheticProcessor.bucket(id + ":2") >= 50)
            .findFirst()
            .orElseThrow();
    service.record(reserved(orderId), "c");

    long started = System.nanoTime();
    ExecutorService pool = Executors.newSingleThreadExecutor();
    try {
      Future<?> attempt = pool.submit(() -> service.attempt(orderId));
      attempt.get(10, TimeUnit.SECONDS);
    } finally {
      pool.shutdownNow();
    }
    assertThat(payments.findById(orderId).orElseThrow().getStatus())
        .isEqualTo(PaymentStatus.AUTHORIZED);

    listener.onOrderCancelled(consumed(cancelled(orderId)));
    service.sweepReleases();
    assertThat(payments.findById(orderId).orElseThrow().getStatus())
        .isEqualTo(PaymentStatus.VOIDED);

    // the slow first call is still running; wait it out, then let the releases that are due run
    long elapsed = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - started);
    Thread.sleep(Math.max(0, SLOW_MILLIS + 500 - elapsed));
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
    while (holds.outstanding(orderId) > 0 && System.nanoTime() < deadline) {
      service.sweepReleases();
      Thread.sleep(100);
    }

    assertThat(holds.outstanding(orderId)).isZero();
    assertThat(payments.findById(orderId).orElseThrow().getStatus())
        .isEqualTo(PaymentStatus.VOIDED);
  }

  private ConsumerRecord<String, String> consumed(DomainEvent event) {
    return new ConsumerRecord<>(event.topic(), 0, 0L, event.orderId(), codec.encode(event));
  }

  private static OrderCancelled cancelled(String orderId) {
    return new OrderCancelled(
        "cancel-" + orderId,
        orderId,
        "c",
        Instant.now(),
        "RESERVATION_TIMEOUT",
        List.of(new OrderLine("SKU", 1)));
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
