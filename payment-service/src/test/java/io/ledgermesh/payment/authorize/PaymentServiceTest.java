package io.ledgermesh.payment.authorize;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.AdditionalAnswers.delegatesTo;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import io.ledgermesh.common.events.DomainEvent;
import io.ledgermesh.common.events.EventCodec;
import io.ledgermesh.common.events.InventoryReserved;
import io.ledgermesh.common.events.OrderLine;
import io.ledgermesh.common.events.PaymentRequested;
import io.ledgermesh.common.events.Topics;
import io.ledgermesh.common.idempotency.IdempotentConsumer;
import io.ledgermesh.common.outbox.OutboxEvent;
import io.ledgermesh.common.outbox.OutboxEventRepository;
import io.ledgermesh.common.outbox.OutboxWriter;
import io.ledgermesh.payment.domain.Payment;
import io.ledgermesh.payment.domain.PaymentRepository;
import io.ledgermesh.payment.domain.PaymentStatus;
import io.ledgermesh.payment.messaging.PaymentEventListener;
import io.ledgermesh.payment.processor.PaymentProcessor;
import io.ledgermesh.payment.processor.PaymentProcessor.Approved;
import io.ledgermesh.payment.processor.PaymentProcessor.Declined;
import io.ledgermesh.payment.processor.ProcessorUnavailableException;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.math.BigDecimal;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.transaction.support.TransactionTemplate;

@SpringBootTest
class PaymentServiceTest {

  @Autowired private PaymentService service;
  @Autowired private PaymentRepository payments;
  @Autowired private OutboxEventRepository outbox;
  @Autowired private CircuitBreakerRegistry breakers;
  @Autowired private PaymentAuthorizer authorizer;
  @Autowired private OutboxWriter outboxWriter;
  @Autowired private TransactionTemplate tx;
  @Autowired private Clock clock;
  @Autowired private EventCodec codec;
  @Autowired private IdempotentConsumer idempotent;
  @Autowired private PaymentEventListener listener;
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
    when(processor.authorize(anyString(), anyString(), any(), anyInt()))
        .thenReturn(new Approved("AUTH-9"));
    service.record(reserved("o2"), "c");

    assertThat(service.attempt("o2")).contains(PaymentStatus.AUTHORIZED);

    assertThat(payments.findById("o2").orElseThrow().getAuthorizationCode()).isEqualTo("AUTH-9");
    assertThat(outbox.findAll())
        .extracting(OutboxEvent::getTopic)
        .containsExactly(Topics.PAYMENT_COMPLETED);
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
    when(processor.authorize(anyString(), anyString(), any(), anyInt()))
        .thenReturn(new Approved("AUTH-3"));
    assertThat(service.sweep()).isEqualTo(1);

    assertThat(payments.findById("o3").orElseThrow().getStatus())
        .isEqualTo(PaymentStatus.AUTHORIZED);
    assertThat(outbox.findAll())
        .extracting(OutboxEvent::getTopic)
        .containsExactly(Topics.PAYMENT_COMPLETED);
  }

  @Test
  void sweepPicksUpPaymentsThatWereRecordedButNeverAttempted() {
    when(processor.authorize(anyString(), anyString(), any(), anyInt()))
        .thenReturn(new Approved("AUTH-4"));
    service.record(reserved("o4"), "c");

    assertThat(service.sweep()).isEqualTo(1);
    assertThat(service.sweep()).isZero();
    assertThat(payments.findById("o4").orElseThrow().getStatus())
        .isEqualTo(PaymentStatus.AUTHORIZED);
  }

  @Test
  void settledPaymentsAreNeverAttemptedAgain() {
    when(processor.authorize(anyString(), anyString(), any(), anyInt()))
        .thenReturn(new Approved("AUTH-5"));
    service.record(reserved("o5"), "c");
    service.attempt("o5");

    assertThat(service.attempt("o5")).isEmpty();
    assertThat(outbox.count()).isEqualTo(1);
  }

  @Test
  void redriveOfASettledPaymentReEmitsTheOutcomeWithAFreshEventId() {
    when(processor.authorize(anyString(), anyString(), any(), anyInt()))
        .thenReturn(new Approved("AUTH-6"));
    service.record(reserved("o6"), "c");
    service.attempt("o6");

    assertThat(service.requestAgain(requested("o6"), "c")).isEqualTo(PaymentStatus.AUTHORIZED);

    List<OutboxEvent> rows = outbox.findAll();
    assertThat(rows)
        .extracting(OutboxEvent::getTopic)
        .containsExactly(Topics.PAYMENT_COMPLETED, Topics.PAYMENT_COMPLETED);
    assertThat(rows.get(1).getEventId()).isNotEqualTo(rows.get(0).getEventId());
    assertThat(rows.get(1).getPayload()).contains("AUTH-6");
    assertThat(payments.findById("o6").orElseThrow().getAttempts()).isEqualTo(1);
  }

  @Test
  void redriveOfAnOpenPaymentAttemptsItNow() {
    when(processor.authorize(anyString(), anyString(), any(), anyInt()))
        .thenThrow(new ProcessorUnavailableException("outage"));
    service.record(reserved("o7"), "c");
    service.attempt("o7");
    assertThat(payments.findById("o7").orElseThrow().getStatus()).isEqualTo(PaymentStatus.DEFERRED);

    reset(processor);
    when(processor.authorize(anyString(), anyString(), any(), anyInt()))
        .thenReturn(new Approved("AUTH-7"));
    assertThat(service.requestAgain(requested("o7"), "c")).isEqualTo(PaymentStatus.AUTHORIZED);
    assertThat(outbox.findAll())
        .extracting(OutboxEvent::getTopic)
        .containsExactly(Topics.PAYMENT_COMPLETED);
  }

  @Test
  void redriveOfAnUnknownPaymentRecordsItFromTheRequestAndAttempts() {
    when(processor.authorize(anyString(), anyString(), any(), anyInt()))
        .thenReturn(new Declined("CARD_DECLINED"));

    assertThat(service.requestAgain(requested("o8"), "c")).isEqualTo(PaymentStatus.DECLINED);

    assertThat(payments.findById("o8").orElseThrow().getAmount()).isEqualByComparingTo("12.50");
    assertThat(outbox.findAll())
        .extracting(OutboxEvent::getTopic)
        .containsExactly(Topics.PAYMENT_FAILED);
  }

  @Test
  void aReDriveThatCreatesThePaymentAfterTheReservationDidFailsInsteadOfOverwritingIt()
      throws Exception {
    when(processor.authorize(anyString(), anyString(), any(), anyInt()))
        .thenReturn(new Approved("AUTH-10"));
    CountDownLatch lookedUp = new CountDownLatch(1);
    CountDownLatch recorded = new CountDownLatch(1);
    PaymentEventListener late = listenerHeldAfterLookup("o10", lookedUp, recorded);
    ConsumerRecord<String, String> redrive =
        consumed(
            new PaymentRequested(
                "req-o10",
                "o10",
                "c-late",
                Instant.now(),
                "cust-late",
                new BigDecimal("99.00"),
                1));
    ExecutorService pool = Executors.newSingleThreadExecutor();
    try {
      // the re-drive found no payment for the order and is held until the reservation's record
      // step has committed one, before that payment has been attempted
      Future<?> first = pool.submit(() -> late.onPaymentRequested(redrive));
      assertThat(lookedUp.await(10, TimeUnit.SECONDS)).isTrue();
      idempotent.once(
          PaymentEventListener.CONSUMER, "evt-o10", () -> service.record(reserved("o10"), "c"));
      recorded.countDown();

      assertThatThrownBy(() -> first.get(10, TimeUnit.SECONDS))
          .hasCauseInstanceOf(DataIntegrityViolationException.class);
      Payment stored = payments.findById("o10").orElseThrow();
      assertThat(stored.getCustomerId()).isEqualTo("cust");
      assertThat(stored.getAmount()).isEqualByComparingTo("12.50");
      assertThat(stored.getStatus()).isEqualTo(PaymentStatus.NEW);

      // the error handler delivers the re-drive again, and it attempts the payment on file
      listener.onPaymentRequested(redrive);
      Payment attempted = payments.findById("o10").orElseThrow();
      assertThat(attempted.getStatus()).isEqualTo(PaymentStatus.AUTHORIZED);
      assertThat(attempted.getCustomerId()).isEqualTo("cust");
      assertThat(attempted.getCorrelationId()).isEqualTo("c");
      verify(processor, times(1)).authorize(anyString(), anyString(), any(), anyInt());
    } finally {
      pool.shutdownNow();
    }
  }

  @Test
  void aReservationThatRecordsAfterAReDriveAuthorizedThePaymentStillFailsAndLeavesItAlone()
      throws Exception {
    when(processor.authorize(anyString(), anyString(), any(), anyInt()))
        .thenReturn(new Approved("AUTH-11"));
    CountDownLatch lookedUp = new CountDownLatch(1);
    CountDownLatch redriven = new CountDownLatch(1);
    PaymentEventListener late = listenerHeldAfterLookup("o11", lookedUp, redriven);
    ConsumerRecord<String, String> reservation =
        consumed(
            new InventoryReserved(
                "evt-o11",
                "o11",
                "c-late",
                Instant.now(),
                "cust-late",
                List.of(new OrderLine("SKU", 1)),
                new BigDecimal("99.00")));
    ExecutorService pool = Executors.newSingleThreadExecutor();
    try {
      // the reservation found no payment for the order and is held until a re-drive has
      // recorded and authorized one. The authorization moved the payment's version on, so a merge
      // failed here as well, on the optimistic lock, and never overwrote it; this guards the
      // insert failing on the primary key and the redelivery leaving the settled payment alone
      Future<?> first = pool.submit(() -> late.onInventoryReserved(reservation));
      assertThat(lookedUp.await(10, TimeUnit.SECONDS)).isTrue();
      listener.onPaymentRequested(consumed(requested("o11")));
      redriven.countDown();

      assertThatThrownBy(() -> first.get(10, TimeUnit.SECONDS))
          .hasCauseInstanceOf(DataIntegrityViolationException.class);

      // delivered again, the reservation finds the settled payment and does not attempt it
      listener.onInventoryReserved(reservation);
      Payment stored = payments.findById("o11").orElseThrow();
      assertThat(stored.getStatus()).isEqualTo(PaymentStatus.AUTHORIZED);
      assertThat(stored.getCustomerId()).isEqualTo("cust");
      assertThat(stored.getAmount()).isEqualByComparingTo("12.50");
      verify(processor, times(1)).authorize(anyString(), anyString(), any(), anyInt());
      assertThat(outbox.findAll())
          .extracting(OutboxEvent::getTopic)
          .containsExactly(Topics.PAYMENT_COMPLETED);
    } finally {
      pool.shutdownNow();
    }
  }

  @Test
  void aReservationThatRecordsWhileAReDriveIsAtTheProcessorFailsSoThePaymentIsAuthorizedOnce()
      throws Exception {
    CountDownLatch lookedUp = new CountDownLatch(1);
    CountDownLatch atProcessor = new CountDownLatch(1);
    when(processor.authorize(anyString(), anyString(), any(), anyInt()))
        .thenAnswer(
            call -> {
              if (atProcessor.getCount() > 0) {
                // the re-drive's payment is not committed yet; the held reservation saves now
                atProcessor.countDown();
                TimeUnit.MILLISECONDS.sleep(300);
              }
              return new Approved("AUTH-12");
            });
    PaymentEventListener late = listenerHeldAfterLookup("o12", lookedUp, atProcessor);
    ConsumerRecord<String, String> reservation =
        consumed(
            new InventoryReserved(
                "evt-o12",
                "o12",
                "c-late",
                Instant.now(),
                "cust-late",
                List.of(new OrderLine("SKU", 1)),
                new BigDecimal("99.00")));
    ExecutorService pool = Executors.newFixedThreadPool(2);
    try {
      // the reservation found no payment for the order and is held until a re-drive, which
      // records and attempts its payment in one transaction, is calling the processor
      Future<?> first = pool.submit(() -> late.onInventoryReserved(reservation));
      assertThat(lookedUp.await(10, TimeUnit.SECONDS)).isTrue();
      Future<?> redrive =
          pool.submit(() -> listener.onPaymentRequested(consumed(requested("o12"))));

      assertThatThrownBy(() -> first.get(10, TimeUnit.SECONDS))
          .hasCauseInstanceOf(DataIntegrityViolationException.class);
      redrive.get(10, TimeUnit.SECONDS);

      // delivered again, the reservation finds the payment the re-drive authorized
      listener.onInventoryReserved(reservation);
      Payment stored = payments.findById("o12").orElseThrow();
      assertThat(stored.getStatus()).isEqualTo(PaymentStatus.AUTHORIZED);
      assertThat(stored.getCustomerId()).isEqualTo("cust");
      verify(processor, times(1)).authorize(anyString(), anyString(), any(), anyInt());
      assertThat(outbox.findAll())
          .extracting(OutboxEvent::getTopic)
          .containsExactly(Topics.PAYMENT_COMPLETED);
    } finally {
      pool.shutdownNow();
    }
  }

  /**
   * A listener whose first lookup of the order's payment returns what it found and then waits for
   * {@code release}, which puts another creator's commit between that lookup and the save.
   */
  private PaymentEventListener listenerHeldAfterLookup(
      String orderId, CountDownLatch lookedUp, CountDownLatch release) {
    PaymentRepository held = mock(PaymentRepository.class, delegatesTo(payments));
    doAnswer(
            call -> {
              Optional<Payment> found = payments.findById(orderId);
              lookedUp.countDown();
              waitFor(release);
              return found;
            })
        .when(held)
        .findById(orderId);
    PaymentService heldService =
        new PaymentService(
            held,
            authorizer,
            outboxWriter,
            tx,
            clock,
            new SimpleMeterRegistry(),
            Duration.ZERO,
            Duration.ZERO);
    return new PaymentEventListener(codec, idempotent, heldService);
  }

  private ConsumerRecord<String, String> consumed(DomainEvent event) {
    return new ConsumerRecord<>(event.topic(), 0, 0L, event.orderId(), codec.encode(event));
  }

  private static void waitFor(CountDownLatch latch) {
    try {
      assertThat(latch.await(10, TimeUnit.SECONDS)).isTrue();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException(e);
    }
  }

  private static PaymentRequested requested(String orderId) {
    return new PaymentRequested(
        "req-" + orderId, orderId, "c", Instant.now(), "cust", new BigDecimal("12.50"), 1);
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
