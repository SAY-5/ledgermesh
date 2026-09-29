package io.ledgermesh.payment.authorize;

import io.ledgermesh.common.events.InventoryReserved;
import io.ledgermesh.common.events.OrderCancelled;
import io.ledgermesh.common.events.PaymentCompleted;
import io.ledgermesh.common.events.PaymentFailed;
import io.ledgermesh.common.events.PaymentRequested;
import io.ledgermesh.common.events.PaymentVoided;
import io.ledgermesh.common.outbox.OutboxWriter;
import io.ledgermesh.payment.domain.Payment;
import io.ledgermesh.payment.domain.PaymentRepository;
import io.ledgermesh.payment.domain.PaymentStatus;
import io.ledgermesh.payment.processor.PaymentProcessor;
import io.micrometer.core.instrument.MeterRegistry;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.support.TransactionTemplate;

/**
 * Two step flow. {@link #record} durably stores the payment inside the consumer's idempotent
 * transaction. {@link #attempt} runs the authorization outside any transaction and then commits the
 * outcome plus its outbox event. A process killed between the two leaves an open payment which the
 * deferred queue picks up, so a payment is only ever authorized, declined or voided, never
 * forgotten.
 *
 * <p>A payment has two creators: {@link #record}, fed by {@code inventory.reserved}, and {@link
 * #requestAgain}, fed by {@code order.payment_requested} when no payment is on file. They consume
 * different topics on different listener threads, so both can find no payment for one order and
 * create it. The payment is inserted and flushed right away, so whichever creator comes second
 * fails on the primary key before it calls the processor, its transaction rolls back, and the
 * redelivery the error handler makes finds the payment on file.
 *
 * <p>{@link #cancel}, fed by {@code order.cancelled}, voids the payment of an order that will never
 * be fulfilled, or inserts a voided marker when it comes before both creators, the same way, so a
 * race with a creator ends with one of the two delivered again onto the other's row. It answers
 * every cancellation with {@code payment.voided}, which is how the order service knows the payment
 * side of the compensation is done. A void that may leave money held on the card makes a release at
 * the processor due; {@link #releaseHold} asks for it right away and {@link #sweepReleases} again
 * until the processor confirms, and an approval that lands after the void makes it due again.
 */
@Service
public class PaymentService {

  private static final Logger log = LoggerFactory.getLogger(PaymentService.class);

  private final PaymentRepository payments;
  private final PaymentAuthorizer authorizer;
  private final PaymentProcessor processor;
  private final OutboxWriter outbox;
  private final TransactionTemplate tx;
  private final Clock clock;
  private final MeterRegistry meters;
  private final Duration graceBeforeSweep;
  private final Duration retryDelay;

  public PaymentService(
      PaymentRepository payments,
      PaymentAuthorizer authorizer,
      PaymentProcessor processor,
      OutboxWriter outbox,
      TransactionTemplate tx,
      Clock clock,
      MeterRegistry meters,
      @Value("${ledgermesh.payment.sweep-grace:3s}") Duration graceBeforeSweep,
      @Value("${ledgermesh.payment.retry-delay:2s}") Duration retryDelay) {
    this.payments = payments;
    this.authorizer = authorizer;
    this.processor = processor;
    this.outbox = outbox;
    this.tx = tx;
    this.clock = clock;
    this.meters = meters;
    this.graceBeforeSweep = graceBeforeSweep;
    this.retryDelay = retryDelay;
    for (PaymentStatus status : PaymentStatus.values()) {
      meters.gauge(
          "ledgermesh.payments.by_state",
          List.of(io.micrometer.core.instrument.Tag.of("state", status.name())),
          payments,
          r -> r.countByStatus(status));
    }
    meters.gauge(
        "ledgermesh.payments.releases.due", payments, r -> r.countByReleaseDueAtIsNotNull());
  }

  @Transactional
  public Payment record(InventoryReserved event, String correlationId) {
    return payments
        .findById(event.orderId())
        .orElseGet(
            () ->
                payments.saveAndFlush(
                    new Payment(
                        event.orderId(),
                        event.customerId(),
                        event.amount(),
                        correlationId,
                        clock.instant(),
                        clock.instant().plus(graceBeforeSweep))));
  }

  /**
   * Answers a re-drive from the order service. A settled payment has its outcome event emitted
   * again under a fresh event id, since the first copy evidently never applied; an open payment is
   * attempted right away; an unknown one is recorded from the request and attempted. A voided
   * payment belongs to an order that is already cancelled, which no outcome can move, so it is
   * answered with nothing rather than with a decline it never had.
   */
  public PaymentStatus requestAgain(PaymentRequested event, String correlationId) {
    Payment payment =
        tx.execute(
            status ->
                payments
                    .findById(event.orderId())
                    .orElseGet(
                        () ->
                            payments.saveAndFlush(
                                new Payment(
                                    event.orderId(),
                                    event.customerId(),
                                    event.amount(),
                                    correlationId,
                                    clock.instant(),
                                    clock.instant()))));
    if (payment.getStatus().isOpen()) {
      meters.counter("ledgermesh.payments.redriven", "state", "open").increment();
      return attempt(event.orderId(), true).orElse(payment.getStatus());
    }
    if (payment.getStatus() == PaymentStatus.VOIDED) {
      meters.counter("ledgermesh.payments.redriven", "state", "voided").increment();
      log.info("payment for order {} was voided, re-drive left unanswered", event.orderId());
      return PaymentStatus.VOIDED;
    }
    tx.executeWithoutResult(status -> reemit(payment));
    meters.counter("ledgermesh.payments.redriven", "state", "settled").increment();
    log.info(
        "payment for order {} already {}, outcome re-emitted",
        event.orderId(),
        payment.getStatus());
    return payment.getStatus();
  }

  /**
   * Answers a cancelled order. Payments are authorized off {@code inventory.reserved} whatever the
   * order has become since, so a cancellation can find its payment authorized (the order passed its
   * deadline while the answers waited for the order service to read them), open (the processor was
   * away), or not on file yet (the reservation has not reached this service). An authorized or open
   * payment is voided, with a release at the processor due, and a missing one is replaced by a
   * voided marker, so neither a late reservation nor a re-drive charges for the order; a declined
   * or voided payment stays as it is. Whatever it found, the cancellation is answered with {@code
   * payment.voided} in the same transaction, so the order service can stop sending it. The release
   * itself is asked for by {@link #releaseHold}, after this commits.
   */
  @Transactional
  public PaymentStatus cancel(OrderCancelled event) {
    Instant now = clock.instant();
    Payment payment = payments.findById(event.orderId()).orElse(null);
    String before = payment == null ? "NONE" : payment.getStatus().name();
    PaymentStatus after = PaymentStatus.VOIDED;
    if (payment == null) {
      payments.saveAndFlush(
          Payment.voidedMarker(event.orderId(), event.correlationId(), event.reason(), now));
    } else if (payment.getStatus() == PaymentStatus.AUTHORIZED || payment.getStatus().isOpen()) {
      payment.voided(event.reason(), now);
    } else {
      after = payment.getStatus();
    }
    outbox.append(
        new PaymentVoided(
            UUID.randomUUID().toString(), event.orderId(), event.correlationId(), now, before));
    if (after == PaymentStatus.VOIDED && !"VOIDED".equals(before)) {
      meters.counter("ledgermesh.payments.voided", "was", before).increment();
      log.info(
          "payment for order {} voided, it was {} (order {})",
          event.orderId(),
          before,
          event.reason());
    }
    return after;
  }

  /**
   * Asks the processor to release what it holds for a voided payment's order, when a release is
   * due. Runs outside any transaction, and records the release only on the version of the row it
   * read, so an approval that landed meanwhile leaves the release due for the next sweep. A
   * processor that cannot be reached leaves it due as well. Returns whether the release was
   * recorded.
   */
  public boolean releaseHold(String orderId) {
    Payment payment = payments.findById(orderId).orElse(null);
    if (payment == null || payment.getReleaseDueAt() == null) {
      return false;
    }
    int released;
    try {
      released = processor.release(orderId);
    } catch (RuntimeException e) {
      meters.counter("ledgermesh.payments.releases", "result", "failed").increment();
      log.warn("release for order {} failed, left due: {}", orderId, e.toString());
      return false;
    }
    if (payments.released(orderId, payment.getVersion(), clock.instant()) == 0) {
      meters.counter("ledgermesh.payments.releases", "result", "superseded").increment();
      return false;
    }
    meters
        .counter("ledgermesh.payments.releases", "result", released > 0 ? "released" : "nothing")
        .increment();
    log.info("processor released {} authorization(s) for order {}", released, orderId);
    return true;
  }

  /** Asks for every release that is due. Returns how many were recorded. */
  public int sweepReleases() {
    int recorded = 0;
    for (Payment payment :
        payments.findTop100ByReleaseDueAtLessThanEqualOrderByReleaseDueAtAsc(clock.instant())) {
      if (releaseHold(payment.getOrderId())) {
        recorded++;
      }
    }
    return recorded;
  }

  private void reemit(Payment payment) {
    if (payment.getStatus() == PaymentStatus.AUTHORIZED) {
      outbox.append(
          new PaymentCompleted(
              UUID.randomUUID().toString(),
              payment.getOrderId(),
              payment.getCorrelationId(),
              clock.instant(),
              payment.getAuthorizationCode()));
    } else {
      outbox.append(
          new PaymentFailed(
              UUID.randomUUID().toString(),
              payment.getOrderId(),
              payment.getCorrelationId(),
              clock.instant(),
              payment.getReason()));
    }
  }

  /** Authorizes an open payment and commits the outcome. Returns the final status if any. */
  public Optional<PaymentStatus> attempt(String orderId) {
    return attempt(orderId, false);
  }

  /** Background attempts (from the sweeper) get the longer deferred time budget. */
  public Optional<PaymentStatus> attempt(String orderId, boolean background) {
    Payment payment = payments.findById(orderId).orElse(null);
    if (payment == null || !payment.getStatus().isOpen()) {
      return Optional.empty();
    }
    AuthorizationOutcome outcome =
        background
            ? authorizer.authorizeDeferred(
                orderId, payment.getCustomerId(), payment.getAmount(), payment.getAttempts())
            : authorizer.authorize(orderId, payment.getCustomerId(), payment.getAmount());
    PaymentStatus status = commit(orderId, outcome);
    if (status == PaymentStatus.VOIDED && outcome instanceof AuthorizationOutcome.Authorized) {
      releaseHold(orderId);
    }
    return Optional.of(status);
  }

  /** Persists the outcome and its event in one transaction. Safe to call from any thread. */
  public PaymentStatus commit(String orderId, AuthorizationOutcome outcome) {
    return tx.execute(status -> applyOutcome(orderId, outcome));
  }

  private PaymentStatus applyOutcome(String orderId, AuthorizationOutcome outcome) {
    Payment payment = payments.findById(orderId).orElseThrow();
    if (payment.getStatus() == PaymentStatus.VOIDED
        && outcome instanceof AuthorizationOutcome.Authorized) {
      // the processor approved an attempt that started before the void: that hold goes back too
      payment.approvedAfterVoid(clock.instant());
      meters.counter("ledgermesh.payments.approvals_after_void").increment();
      log.warn("payment for order {} approved after its void, release due again", orderId);
      return PaymentStatus.VOIDED;
    }
    if (!payment.getStatus().isOpen()) {
      return payment.getStatus();
    }
    payment.attempted();
    switch (outcome) {
      case AuthorizationOutcome.Authorized a -> {
        payment.authorized(a.code(), clock.instant());
        outbox.append(
            new PaymentCompleted(
                UUID.randomUUID().toString(),
                orderId,
                payment.getCorrelationId(),
                clock.instant(),
                a.code()));
      }
      case AuthorizationOutcome.Declined d -> {
        payment.declined(d.reason(), clock.instant());
        outbox.append(
            new PaymentFailed(
                UUID.randomUUID().toString(),
                orderId,
                payment.getCorrelationId(),
                clock.instant(),
                d.reason()));
      }
      case AuthorizationOutcome.Deferred d -> {
        Duration backoff = retryDelay.multipliedBy(Math.min(payment.getAttempts(), 5));
        payment.deferred(d.reason(), clock.instant().plus(backoff), clock.instant());
        meters.counter("ledgermesh.payments.deferred").increment();
      }
    }
    meters
        .counter("ledgermesh.payments.outcomes", "status", payment.getStatus().name())
        .increment();
    log.info(
        "payment for order {} is {} after {} attempt(s)",
        orderId,
        payment.getStatus(),
        payment.getAttempts());
    return payment.getStatus();
  }

  /** Attempts every open payment whose retry time has come. Returns how many were attempted. */
  public int sweep() {
    List<Payment> due =
        payments.findTop100ByStatusInAndNextAttemptAtLessThanEqualOrderByNextAttemptAtAsc(
            List.of(PaymentStatus.NEW, PaymentStatus.DEFERRED), clock.instant());
    int attempted = 0;
    for (Payment payment : due) {
      if (attempt(payment.getOrderId(), true).isPresent()) {
        attempted++;
      }
    }
    return attempted;
  }
}
