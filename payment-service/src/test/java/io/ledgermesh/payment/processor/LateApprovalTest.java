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
import java.util.stream.IntStream;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;

/**
 * The synthetic processor behind the real decorator, with the test configuration's 300 ms time
 * limit: a first attempt on the slow path is cut off by the time limiter, but the call is not
 * stopped, and it approves after the retry already authorized the payment and the cancellation
 * voided it and released the card. That late approval is a live authorization all the same, and has
 * to be released like any other.
 */
@SpringBootTest(properties = "spring.datasource.url=jdbc:h2:mem:late-approvals;DB_CLOSE_DELAY=-1")
class LateApprovalTest {

  @Autowired private PaymentService service;
  @Autowired private PaymentRepository payments;
  @Autowired private PaymentEventListener listener;
  @Autowired private AuthorizationHolds holds;
  @Autowired private EventCodec codec;
  @Autowired private CircuitBreakerRegistry breakers;

  @Value("${ledgermesh.processor.transient-percent}")
  private int transientPercent;

  @Value("${ledgermesh.processor.slow-percent}")
  private int slowPercent;

  @Value("${ledgermesh.processor.slow-millis}")
  private long slowMillis;

  @BeforeEach
  void clean() {
    breakers.circuitBreaker(PaymentAuthorizer.RESILIENCE_NAME).reset();
    payments.deleteAll();
  }

  @Test
  void anApprovalCutOffByTheTimeLimiterThatLandsAfterTheReleaseIsReleasedToo() throws Exception {
    // attempt 1 takes the slow path (cut off by the time limiter, still running for slow-millis),
    // attempt 2 is approved at once
    String id =
        IntStream.range(0, 200000)
            .mapToObj(i -> "late-" + i)
            .filter(s -> slow(SyntheticProcessor.bucket(s + ":1")))
            .filter(s -> SyntheticProcessor.bucket(s + ":2") >= transientPercent + slowPercent)
            .findFirst()
            .orElseThrow();

    listener.onInventoryReserved(consumed(reserved(id)));
    assertThat(payments.findById(id).orElseThrow().getStatus()).isEqualTo(PaymentStatus.AUTHORIZED);

    listener.onOrderCancelled(consumed(cancelled(id)));
    service.sweepReleases();
    assertThat(payments.findById(id).orElseThrow().getStatus()).isEqualTo(PaymentStatus.VOIDED);
    assertThat(holds.outstanding(id)).isZero();

    // the first call approves once its slow path is over; then the releases that are due run
    Thread.sleep(slowMillis + 1000);
    service.sweepReleases();

    assertThat(holds.outstanding(id))
        .as("authorizations outstanding at the processor for a voided payment")
        .isZero();
    assertThat(payments.findById(id).orElseThrow().getStatus()).isEqualTo(PaymentStatus.VOIDED);
  }

  private boolean slow(int bucket) {
    return bucket >= transientPercent && bucket < transientPercent + slowPercent;
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
