package io.ledgermesh.payment.authorize;

import io.ledgermesh.common.events.InventoryReserved;
import io.ledgermesh.common.events.PaymentCompleted;
import io.ledgermesh.common.events.PaymentFailed;
import io.ledgermesh.common.outbox.OutboxWriter;
import io.ledgermesh.payment.domain.Payment;
import io.ledgermesh.payment.domain.PaymentRepository;
import io.ledgermesh.payment.domain.PaymentStatus;
import io.micrometer.core.instrument.MeterRegistry;
import java.time.Clock;
import java.time.Duration;
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
 * deferred queue picks up, so an order can only ever be completed or declined, never forgotten.
 */
@Service
public class PaymentService {

  private static final Logger log = LoggerFactory.getLogger(PaymentService.class);

  private final PaymentRepository payments;
  private final PaymentAuthorizer authorizer;
  private final OutboxWriter outbox;
  private final TransactionTemplate tx;
  private final Clock clock;
  private final MeterRegistry meters;
  private final Duration graceBeforeSweep;
  private final Duration retryDelay;

  public PaymentService(
      PaymentRepository payments,
      PaymentAuthorizer authorizer,
      OutboxWriter outbox,
      TransactionTemplate tx,
      Clock clock,
      MeterRegistry meters,
      @Value("${ledgermesh.payment.sweep-grace:3s}") Duration graceBeforeSweep,
      @Value("${ledgermesh.payment.retry-delay:2s}") Duration retryDelay) {
    this.payments = payments;
    this.authorizer = authorizer;
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
  }

  @Transactional
  public Payment record(InventoryReserved event, String correlationId) {
    return payments
        .findById(event.orderId())
        .orElseGet(
            () ->
                payments.save(
                    new Payment(
                        event.orderId(),
                        event.customerId(),
                        event.amount(),
                        correlationId,
                        clock.instant(),
                        clock.instant().plus(graceBeforeSweep))));
  }

  /** Authorizes an open payment and commits the outcome. Returns the final status if any. */
  public Optional<PaymentStatus> attempt(String orderId) {
    Payment payment = payments.findById(orderId).orElse(null);
    if (payment == null || !payment.getStatus().isOpen()) {
      return Optional.empty();
    }
    AuthorizationOutcome outcome =
        authorizer.authorize(orderId, payment.getCustomerId(), payment.getAmount());
    return Optional.of(commit(orderId, outcome));
  }

  /** Persists the outcome and its event in one transaction. Safe to call from any thread. */
  public PaymentStatus commit(String orderId, AuthorizationOutcome outcome) {
    return tx.execute(status -> applyOutcome(orderId, outcome));
  }

  private PaymentStatus applyOutcome(String orderId, AuthorizationOutcome outcome) {
    Payment payment = payments.findById(orderId).orElseThrow();
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
      if (attempt(payment.getOrderId()).isPresent()) {
        attempted++;
      }
    }
    return attempted;
  }
}
