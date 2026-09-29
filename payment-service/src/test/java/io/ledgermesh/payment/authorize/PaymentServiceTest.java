package io.ledgermesh.payment.authorize;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.AdditionalAnswers.delegatesTo;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import io.ledgermesh.common.events.DomainEvent;
import io.ledgermesh.common.events.EventCodec;
import io.ledgermesh.common.events.InventoryReserved;
import io.ledgermesh.common.events.OrderCancelled;
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
import org.springframework.dao.OptimisticLockingFailureException;
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
  @Autowired private AuthorizationDecisions decisions;
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
                // The re-drive committed its payment before this call; the stale reservation
                // now tries to save the NEW row it constructed before that commit.
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
      // The reservation found no payment and is held until the re-drive has recorded its
      // payment, committed it, and begun its separate processor call.
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

  @Test
  void aCancelledOrderHasItsAuthorizedPaymentVoidedAndReleasedOnce() {
    when(processor.authorize(anyString(), anyString(), any(), anyInt()))
        .thenReturn(new Approved("AUTH-20"));
    when(processor.release("o20", null)).thenReturn(1);
    listener.onInventoryReserved(consumed(reserved("o20")));
    assertThat(payments.findById("o20").orElseThrow().getStatus())
        .isEqualTo(PaymentStatus.AUTHORIZED);

    ConsumerRecord<String, String> cancel = consumed(cancelled("o20"));
    listener.onOrderCancelled(cancel);
    listener.onOrderCancelled(cancel);
    assertThat(service.sweepReleases()).isEqualTo(1);

    Payment voided = payments.findById("o20").orElseThrow();
    assertThat(voided.getStatus()).isEqualTo(PaymentStatus.VOIDED);
    assertThat(voided.getReason()).isEqualTo("RESERVATION_TIMEOUT");
    assertThat(voided.getAuthorizationCode()).isEqualTo("AUTH-20");
    assertThat(voided.getReleaseDueAt()).isNull();
    assertThat(voided.getReleasedAt()).isNotNull();
    verify(processor, times(1)).release("o20", null);
    assertThat(topics()).containsExactly(Topics.PAYMENT_COMPLETED, Topics.PAYMENT_VOIDED);
    assertThat(outbox.findAll().get(1).getPayload()).contains("\"previous\":\"AUTHORIZED\"");
  }

  @Test
  void aCancelThatArrivesBeforeThePaymentExistsKeepsItFromBeingMade() {
    when(processor.authorize(anyString(), anyString(), any(), anyInt()))
        .thenReturn(new Approved("AUTH-21"));

    listener.onOrderCancelled(consumed(cancelled("o21")));
    listener.onInventoryReserved(consumed(reserved("o21")));
    listener.onPaymentRequested(consumed(requested("o21")));

    Payment marker = payments.findById("o21").orElseThrow();
    assertThat(marker.getStatus()).isEqualTo(PaymentStatus.VOIDED);
    assertThat(marker.getAmount()).isEqualByComparingTo("0");
    assertThat(marker.getAttempts()).isZero();
    assertThat(marker.getReleaseDueAt()).isNull();
    verify(processor, never()).authorize(anyString(), anyString(), any(), anyInt());
    assertThat(service.sweepReleases()).isZero();
    verify(processor, never()).release(anyString(), any());
    assertThat(topics()).containsExactly(Topics.PAYMENT_VOIDED);
    assertThat(outbox.findAll().get(0).getPayload()).contains("\"previous\":\"NONE\"");
  }

  @Test
  void anOpenPaymentIsVoidedNeverAttemptedAgainAndReleasedAtTheProcessor() {
    when(processor.authorize(anyString(), anyString(), any(), anyInt()))
        .thenThrow(new ProcessorUnavailableException("outage"));
    service.record(reserved("o22"), "c");
    assertThat(service.attempt("o22")).contains(PaymentStatus.DEFERRED);

    reset(processor);
    listener.onOrderCancelled(consumed(cancelled("o22")));

    assertThat(service.sweep()).isZero();
    assertThat(service.attempt("o22")).isEmpty();
    assertThat(service.sweepReleases()).isEqualTo(1);
    assertThat(payments.findById("o22").orElseThrow().getStatus()).isEqualTo(PaymentStatus.VOIDED);
    verify(processor, never()).authorize(anyString(), anyString(), any(), anyInt());
    // an attempt that timed out may still have been approved, so an open payment is released too
    verify(processor, times(1)).release("o22", null);
    assertThat(topics()).containsExactly(Topics.PAYMENT_VOIDED);
  }

  @Test
  void aReleaseTheProcessorCouldNotTakeStaysDueUntilTheSweepGetsItThrough() {
    when(processor.release("o25", null))
        .thenThrow(new ProcessorUnavailableException("processor away"))
        .thenReturn(1);
    service.record(reserved("o25"), "c");

    listener.onOrderCancelled(consumed(cancelled("o25")));
    assertThat(payments.findById("o25").orElseThrow().getReleaseDueAt()).isNotNull();

    assertThat(service.sweepReleases()).isZero();
    Payment refused = payments.findById("o25").orElseThrow();
    assertThat(refused.getReleaseDueAt()).isNotNull();
    assertThat(refused.getReleaseAttempts()).isEqualTo(1);
    assertThat(service.sweepReleases()).isEqualTo(1);
    assertThat(service.sweepReleases()).isZero();
    assertThat(payments.findById("o25").orElseThrow().getReleaseDueAt()).isNull();
    assertThat(payments.findById("o25").orElseThrow().getReleaseAttempts()).isZero();
    verify(processor, times(2)).release("o25", null);
  }

  /**
   * A release the processor refuses waits longer before it is asked again, and releases due after
   * it are not held up behind it.
   */
  @Test
  void aRefusedReleaseBacksOffWithoutHoldingUpTheOthers() {
    PaymentService backingOff =
        new PaymentService(
            payments,
            authorizer,
            outboxWriter,
            tx,
            decisions,
            clock,
            new SimpleMeterRegistry(),
            Duration.ZERO,
            Duration.ofSeconds(30));
    when(processor.release("o28", null)).thenThrow(new ProcessorUnavailableException("away"));
    service.record(reserved("o28"), "c");
    service.record(reserved("o29"), "c");
    listener.onOrderCancelled(consumed(cancelled("o28")));
    listener.onOrderCancelled(consumed(cancelled("o29")));

    assertThat(backingOff.sweepReleases()).isEqualTo(1);
    assertThat(backingOff.sweepReleases()).isZero();

    Payment refused = payments.findById("o28").orElseThrow();
    assertThat(refused.getReleaseAttempts()).isEqualTo(1);
    assertThat(refused.getReleaseDueAt()).isAfter(Instant.now().plusSeconds(20));
    assertThat(payments.findById("o29").orElseThrow().getReleaseDueAt()).isNull();
    verify(processor, times(1)).release("o28", null);
    verify(processor, times(1)).release("o29", null);
  }

  @Test
  void anAuthorizedPaymentKeepsItsAuthorizationAndReleasesAnyOther() {
    when(processor.authorize(anyString(), anyString(), any(), anyInt()))
        .thenReturn(new Approved("AUTH-34"));
    when(processor.release("o34", "AUTH-34")).thenReturn(1);
    service.record(reserved("o34"), "c");
    assertThat(service.attempt("o34")).contains(PaymentStatus.AUTHORIZED);
    assertThat(payments.findById("o34").orElseThrow().getReleaseDueAt()).isNotNull();

    assertThat(service.sweepReleases()).isEqualTo(1);

    // a second approval for the order, a retry's or a late one, is a hold the payment does not keep
    assertThat(service.commit("o34", new AuthorizationOutcome.Authorized("AUTH-34-B")))
        .isEqualTo(PaymentStatus.AUTHORIZED);
    Payment kept = payments.findById("o34").orElseThrow();
    assertThat(kept.getAuthorizationCode()).isEqualTo("AUTH-34");
    assertThat(kept.getReleaseDueAt()).isNotNull();
    assertThat(service.sweepReleases()).isEqualTo(1);
    verify(processor, times(2)).release("o34", "AUTH-34");
    assertThat(topics()).containsExactly(Topics.PAYMENT_COMPLETED);

    // the same approval committed twice changes nothing
    assertThat(service.commit("o34", new AuthorizationOutcome.Authorized("AUTH-34")))
        .isEqualTo(PaymentStatus.AUTHORIZED);
    assertThat(payments.findById("o34").orElseThrow().getReleaseDueAt()).isNull();
  }

  @Test
  void anApprovalThatLandsDuringAReleaseLeavesTheNextReleaseDue() {
    service.record(reserved("o26"), "c");
    when(processor.release("o26", null))
        .thenAnswer(
            call -> {
              // the processor approved an attempt that started before the void, while this
              // release was on its way: the release read the row before that approval landed
              service.commit("o26", new AuthorizationOutcome.Authorized("AUTH-26"));
              return 0;
            })
        .thenReturn(1);

    listener.onOrderCancelled(consumed(cancelled("o26")));

    assertThat(service.sweepReleases()).isZero();
    assertThat(payments.findById("o26").orElseThrow().getReleaseDueAt()).isNotNull();
    assertThat(service.sweepReleases()).isEqualTo(1);
    assertThat(payments.findById("o26").orElseThrow().getReleaseDueAt()).isNull();
    assertThat(payments.findById("o26").orElseThrow().getStatus()).isEqualTo(PaymentStatus.VOIDED);
    verify(processor, times(2)).release("o26", null);
    assertThat(topics()).containsExactly(Topics.PAYMENT_VOIDED);
  }

  @Test
  void aDeclinedPaymentIsLeftAsItIsButTheCancelIsStillAnswered() {
    when(processor.authorize(anyString(), anyString(), any(), anyInt()))
        .thenReturn(new Declined("CARD_DECLINED"));
    service.record(reserved("o24"), "c");
    service.attempt("o24");

    assertThat(service.cancel(cancelled("o24"))).isEqualTo(PaymentStatus.DECLINED);
    assertThat(payments.findById("o24").orElseThrow().getReason()).isEqualTo("CARD_DECLINED");
    assertThat(payments.findById("o24").orElseThrow().getReleaseDueAt()).isNull();
    assertThat(topics()).containsExactly(Topics.PAYMENT_FAILED, Topics.PAYMENT_VOIDED);
    assertThat(outbox.findAll().get(1).getPayload()).contains("\"previous\":\"DECLINED\"");
  }

  @Test
  void aCancelSentAgainIsAnsweredAgainWithoutVoidingTwice() {
    listener.onOrderCancelled(consumed(cancelled("o27")));
    OrderCancelled again =
        new OrderCancelled(
            "cancel-again-o27",
            "o27",
            "c",
            Instant.now(),
            "RESERVATION_TIMEOUT",
            List.of(new OrderLine("SKU", 1)));

    listener.onOrderCancelled(consumed(again));

    assertThat(topics()).containsExactly(Topics.PAYMENT_VOIDED, Topics.PAYMENT_VOIDED);
    assertThat(outbox.findAll().get(1).getPayload()).contains("\"previous\":\"VOIDED\"");
    // sent again, the cancellation has the processor checked once more
    assertThat(payments.findById("o27").orElseThrow().getReleaseDueAt()).isNotNull();
    assertThat(service.sweepReleases()).isEqualTo(1);
    verify(processor, times(1)).release("o27", null);
    assertThat(payments.findById("o27").orElseThrow().getStatus()).isEqualTo(PaymentStatus.VOIDED);
  }

  /**
   * A cancellation that looked for the payment before the reservation recorded and authorized it
   * inserts a marker on a key that is now taken, and fails; the redelivery the error handler makes
   * voids the authorized payment and releases it.
   */
  @Test
  void aCancelThatLosesTheInsertRaceToAReservationVoidsTheAuthorizedPaymentOnRedelivery()
      throws Exception {
    when(processor.authorize(anyString(), anyString(), any(), anyInt()))
        .thenReturn(new Approved("AUTH-30"));
    CountDownLatch lookedUp = new CountDownLatch(1);
    CountDownLatch reserved = new CountDownLatch(1);
    PaymentEventListener late = listenerHeldAfterLookup("o30", lookedUp, reserved);
    ConsumerRecord<String, String> cancel = consumed(cancelled("o30"));
    ExecutorService pool = Executors.newSingleThreadExecutor();
    try {
      Future<?> first = pool.submit(() -> late.onOrderCancelled(cancel));
      assertThat(lookedUp.await(10, TimeUnit.SECONDS)).isTrue();
      listener.onInventoryReserved(consumed(reserved("o30")));
      assertThat(payments.findById("o30").orElseThrow().getStatus())
          .isEqualTo(PaymentStatus.AUTHORIZED);
      reserved.countDown();

      assertThatThrownBy(() -> first.get(10, TimeUnit.SECONDS))
          .hasCauseInstanceOf(DataIntegrityViolationException.class);
      assertThat(topics()).containsExactly(Topics.PAYMENT_COMPLETED);

      listener.onOrderCancelled(cancel);
      assertThat(service.sweepReleases()).isEqualTo(1);
      Payment voided = payments.findById("o30").orElseThrow();
      assertThat(voided.getStatus()).isEqualTo(PaymentStatus.VOIDED);
      assertThat(voided.getAuthorizationCode()).isEqualTo("AUTH-30");
      verify(processor, times(1)).release("o30", null);
      assertThat(topics()).containsExactly(Topics.PAYMENT_COMPLETED, Topics.PAYMENT_VOIDED);
    } finally {
      pool.shutdownNow();
    }
  }

  /**
   * A reservation that looked for the payment before the cancellation inserted its marker fails on
   * the key; its redelivery finds the marker and never calls the processor.
   */
  @Test
  void aReservationThatLosesTheInsertRaceToTheMarkerNeverCallsTheProcessor() throws Exception {
    when(processor.authorize(anyString(), anyString(), any(), anyInt()))
        .thenReturn(new Approved("AUTH-31"));
    CountDownLatch lookedUp = new CountDownLatch(1);
    CountDownLatch marked = new CountDownLatch(1);
    PaymentEventListener late = listenerHeldAfterLookup("o31", lookedUp, marked);
    ConsumerRecord<String, String> reservation = consumed(reserved("o31"));
    ExecutorService pool = Executors.newSingleThreadExecutor();
    try {
      Future<?> first = pool.submit(() -> late.onInventoryReserved(reservation));
      assertThat(lookedUp.await(10, TimeUnit.SECONDS)).isTrue();
      listener.onOrderCancelled(consumed(cancelled("o31")));
      marked.countDown();

      assertThatThrownBy(() -> first.get(10, TimeUnit.SECONDS))
          .hasCauseInstanceOf(DataIntegrityViolationException.class);

      listener.onInventoryReserved(reservation);
      assertThat(payments.findById("o31").orElseThrow().getStatus())
          .isEqualTo(PaymentStatus.VOIDED);
      verify(processor, never()).authorize(anyString(), anyString(), any(), anyInt());
      assertThat(topics()).containsExactly(Topics.PAYMENT_VOIDED);
    } finally {
      pool.shutdownNow();
    }
  }

  /**
   * An outcome whose commit read the payment before the void committed writes over a row that has
   * moved on, fails on the version, and rolls back with its event: the payment stays voided.
   */
  @Test
  void anOutcomeThatReadThePaymentBeforeTheVoidFailsOnTheVersionAndLeavesItVoided()
      throws Exception {
    service.record(reserved("o32"), "c");
    CountDownLatch lookedUp = new CountDownLatch(1);
    CountDownLatch voided = new CountDownLatch(1);
    PaymentService held = serviceHeldAfterLookup("o32", lookedUp, voided);
    ExecutorService pool = Executors.newSingleThreadExecutor();
    try {
      Future<PaymentStatus> outcome =
          pool.submit(() -> held.commit("o32", new AuthorizationOutcome.Authorized("AUTH-32")));
      assertThat(lookedUp.await(10, TimeUnit.SECONDS)).isTrue();
      listener.onOrderCancelled(consumed(cancelled("o32")));
      voided.countDown();

      assertThatThrownBy(() -> outcome.get(10, TimeUnit.SECONDS))
          .hasCauseInstanceOf(OptimisticLockingFailureException.class);
      Payment stored = payments.findById("o32").orElseThrow();
      assertThat(stored.getStatus()).isEqualTo(PaymentStatus.VOIDED);
      assertThat(stored.getAuthorizationCode()).isNull();
      assertThat(service.sweepReleases()).isEqualTo(1);
      verify(processor, times(1)).release("o32", null);
      assertThat(topics()).containsExactly(Topics.PAYMENT_VOIDED);
    } finally {
      pool.shutdownNow();
    }
  }

  /**
   * An attempt that read the payment open before the void calls the processor after it: the
   * approval is dropped from the ledger and what the processor granted is released again.
   */
  @Test
  void theProcessorCalledAfterTheVoidHasItsApprovalDroppedAndReleased() throws Exception {
    when(processor.authorize(anyString(), anyString(), any(), anyInt()))
        .thenReturn(new Approved("AUTH-33"));
    service.record(reserved("o33"), "c");
    CountDownLatch lookedUp = new CountDownLatch(1);
    CountDownLatch voided = new CountDownLatch(1);
    PaymentService held = serviceHeldAfterLookup("o33", lookedUp, voided);
    ExecutorService pool = Executors.newSingleThreadExecutor();
    try {
      Future<Optional<PaymentStatus>> attempt = pool.submit(() -> held.attempt("o33"));
      assertThat(lookedUp.await(10, TimeUnit.SECONDS)).isTrue();
      listener.onOrderCancelled(consumed(cancelled("o33")));
      assertThat(service.sweepReleases()).isEqualTo(1);
      verify(processor, times(1)).release("o33", null);
      voided.countDown();

      assertThat(attempt.get(10, TimeUnit.SECONDS)).contains(PaymentStatus.VOIDED);
      verify(processor, times(1)).authorize(anyString(), anyString(), any(), anyInt());
      assertThat(service.sweepReleases()).isEqualTo(1);
      verify(processor, times(2)).release("o33", null);
      Payment stored = payments.findById("o33").orElseThrow();
      assertThat(stored.getStatus()).isEqualTo(PaymentStatus.VOIDED);
      assertThat(stored.getAuthorizationCode()).isNull();
      assertThat(stored.getReleaseDueAt()).isNull();
      assertThat(topics()).containsExactly(Topics.PAYMENT_VOIDED);
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
    return new PaymentEventListener(
        codec, idempotent, serviceHeldAfterLookup(orderId, lookedUp, release));
  }

  /** A payment service whose lookups of the order's payment wait for {@code release}. */
  private PaymentService serviceHeldAfterLookup(
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
    return new PaymentService(
        held,
        authorizer,
        outboxWriter,
        tx,
        decisions,
        clock,
        new SimpleMeterRegistry(),
        Duration.ZERO,
        Duration.ZERO);
  }

  private List<String> topics() {
    return outbox.findAll().stream().map(OutboxEvent::getTopic).toList();
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
