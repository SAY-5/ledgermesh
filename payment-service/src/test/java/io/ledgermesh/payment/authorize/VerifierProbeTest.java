package io.ledgermesh.payment.authorize;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.AdditionalAnswers.delegatesTo;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.when;

import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import io.ledgermesh.common.events.DomainEvent;
import io.ledgermesh.common.events.EventCodec;
import io.ledgermesh.common.events.InventoryReserved;
import io.ledgermesh.common.events.OrderCancelled;
import io.ledgermesh.common.events.OrderLine;
import io.ledgermesh.common.events.PaymentRequested;
import io.ledgermesh.common.outbox.OutboxEventRepository;
import io.ledgermesh.common.outbox.OutboxWriter;
import io.ledgermesh.payment.domain.Payment;
import io.ledgermesh.payment.domain.PaymentRepository;
import io.ledgermesh.payment.domain.PaymentStatus;
import io.ledgermesh.payment.messaging.PaymentEventListener;
import io.ledgermesh.payment.processor.AuthorizationHolds;
import io.ledgermesh.payment.processor.PaymentProcessor;
import io.ledgermesh.payment.processor.PaymentProcessor.Approved;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.math.BigDecimal;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.dao.OptimisticLockingFailureException;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.transaction.support.TransactionTemplate;

/**
 * The four races a verifier found in which a processor answer's commit does not land, each of which
 * left an authorization no payment keeps outstanding at the processor, or released the one a
 * payment keeps. Whatever each path does, {@link HoldReconciler} has to leave the card holding
 * exactly what the payment keeps once the grace period is over.
 */
@SpringBootTest(properties = "spring.datasource.url=jdbc:h2:mem:verifier-probe;DB_CLOSE_DELAY=-1")
class VerifierProbeTest {

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
  @Autowired private PaymentEventListener listener;
  @Autowired private AuthorizationHolds holds;
  @Autowired private ObjectProvider<HoldReconciler> reconciler;
  @MockitoBean private PaymentProcessor processor;

  @BeforeEach
  void clean() {
    reset(processor);
    breakers.circuitBreaker(PaymentAuthorizer.RESILIENCE_NAME).reset();
    outbox.deleteAll();
    payments.deleteAll();
    when(processor.release(anyString(), any()))
        .thenAnswer(call -> holds.release(call.getArgument(0), call.getArgument(1)));
    when(processor.releaseAuthorization(anyString(), anyString()))
        .thenAnswer(call -> holds.releaseCode(call.getArgument(0), call.getArgument(1)));
    when(processor.outstanding(any(), anyInt()))
        .thenAnswer(call -> holds.outstanding(call.getArgument(0), (int) call.getArgument(1)));
  }

  /**
   * What the scheduled reconciliation does once every hold of the probe is past the grace period.
   */
  private void reconcileAfterTheGracePeriod() {
    reconciler.ifAvailable(
        r -> r.reconcile(clock.instant().plus(r.grace()).plus(Duration.ofSeconds(1))));
  }

  /**
   * R: a re-drive of an open payment reads it inside the idempotent consumer's transaction, calls
   * the processor, and commits on the entity it read before the call. A void and a sweep that both
   * land while the processor works, and an approval granted after that sweep's release.
   */
  @Test
  void probeR_redriveApprovalGrantedAfterVoidAndSweep() throws Exception {
    String id = "r-" + UUID.randomUUID().toString().substring(0, 8);
    service.record(reserved(id), "c");
    CountDownLatch calling = new CountDownLatch(1);
    CountDownLatch go = new CountDownLatch(1);
    when(processor.authorize(eq(id), anyString(), any(), anyInt()))
        .thenAnswer(
            call -> {
              calling.countDown();
              assertThat(go.await(10, TimeUnit.SECONDS)).isTrue();
              String code = "AUTH-R-" + UUID.randomUUID().toString().substring(0, 8);
              holds.grant(id, code, new BigDecimal("12.50"));
              return new Approved(code);
            });
    ConsumerRecord<String, String> redrive = consumed(requested(id));
    ExecutorService pool = Executors.newSingleThreadExecutor();
    try {
      Future<?> first = pool.submit(() -> listener.onPaymentRequested(redrive));
      assertThat(calling.await(10, TimeUnit.SECONDS)).isTrue();
      listener.onOrderCancelled(consumed(cancelled(id)));
      service.sweepReleases();
      go.countDown();
      try {
        first.get(10, TimeUnit.SECONDS);
      } catch (Exception e) {
        // the commit may lose to the void or the sweep; the reconciler has to cope
      }
      // the error handler delivers the re-drive again
      try {
        listener.onPaymentRequested(redrive);
      } catch (RuntimeException e) {
        // the redelivery may fail the same way
      }
      service.sweepReleases();
      service.sweepReleases();
      reconcileAfterTheGracePeriod();
      Payment stored = payments.findById(id).orElseThrow();
      assertThat(stored.getStatus()).isEqualTo(PaymentStatus.VOIDED);
      assertThat(holds.outstanding(id))
          .as("authorizations outstanding for a voided payment")
          .isZero();
    } finally {
      pool.shutdownNow();
    }
  }

  @Test
  void probeR2_redriveApprovalAfterAnotherAuthorizationAndSweep() throws Exception {
    String id = "q-" + UUID.randomUUID().toString().substring(0, 8);
    service.record(reserved(id), "c");
    CountDownLatch calling = new CountDownLatch(1);
    CountDownLatch go = new CountDownLatch(1);
    when(processor.authorize(eq(id), anyString(), any(), anyInt()))
        .thenAnswer(
            call -> {
              calling.countDown();
              assertThat(go.await(10, TimeUnit.SECONDS)).isTrue();
              String code = "AUTH-Q-" + UUID.randomUUID().toString().substring(0, 8);
              holds.grant(id, code, new BigDecimal("12.50"));
              return new Approved(code);
            });
    ConsumerRecord<String, String> redrive = consumed(requested(id));
    ExecutorService pool = Executors.newSingleThreadExecutor();
    try {
      Future<?> first = pool.submit(() -> listener.onPaymentRequested(redrive));
      assertThat(calling.await(10, TimeUnit.SECONDS)).isTrue();
      // another attempt of the same open payment (the reservation listener, or a late answer) is
      // approved and committed first, and the sweep releases all but its code
      holds.grant(id, "AUTH-FIRST", new BigDecimal("12.50"));
      service.commit(id, new AuthorizationOutcome.Authorized("AUTH-FIRST"));
      service.sweepReleases();
      go.countDown();
      try {
        first.get(10, TimeUnit.SECONDS);
      } catch (Exception e) {
        // the commit may lose to the void or the sweep; the reconciler has to cope
      }
      try {
        listener.onPaymentRequested(redrive);
      } catch (RuntimeException e) {
        // the redelivery may fail the same way
      }
      service.sweepReleases();
      service.sweepReleases();
      reconcileAfterTheGracePeriod();
      Payment stored = payments.findById(id).orElseThrow();
      assertThat(holds.outstanding(id)).as("holds for an authorized payment").isEqualTo(1);
    } finally {
      pool.shutdownNow();
    }
  }

  /**
   * A: the listener's attempt reads the payment open, the order is cancelled and the sweep starts;
   * the approval is granted after the sweep's release call, its commit reads the voided payment,
   * and the sweep records its release before that commit flushes.
   */
  @Test
  void probeA_listenerApprovalCommitLosesToReleased() throws Exception {
    String id = "a-" + UUID.randomUUID().toString().substring(0, 8);
    service.record(reserved(id), "c");
    CountDownLatch calling = new CountDownLatch(1);
    CountDownLatch grant = new CountDownLatch(1);
    CountDownLatch committing = new CountDownLatch(1);
    CountDownLatch flush = new CountDownLatch(1);
    AtomicInteger lookups = new AtomicInteger();
    PaymentRepository held = mock(PaymentRepository.class, delegatesTo(payments));
    doAnswer(
            call -> {
              Optional<Payment> found = payments.findById(id);
              if (lookups.incrementAndGet() == 2) {
                committing.countDown();
                assertThat(flush.await(10, TimeUnit.SECONDS)).isTrue();
              }
              return found;
            })
        .when(held)
        .findById(id);
    PaymentService attempting =
        new PaymentService(
            held,
            authorizer,
            outboxWriter,
            tx,
            decisions,
            clock,
            new SimpleMeterRegistry(),
            Duration.ZERO,
            Duration.ZERO);
    when(processor.authorize(eq(id), anyString(), any(), anyInt()))
        .thenAnswer(
            call -> {
              calling.countDown();
              assertThat(grant.await(10, TimeUnit.SECONDS)).isTrue();
              String code = "AUTH-A-" + UUID.randomUUID().toString().substring(0, 8);
              holds.grant(id, code, new BigDecimal("12.50"));
              return new Approved(code);
            });
    when(processor.release(eq(id), any()))
        .thenAnswer(
            call -> {
              int n = holds.release(id, call.getArgument(1));
              grant.countDown();
              assertThat(committing.await(10, TimeUnit.SECONDS)).isTrue();
              return n;
            })
        .thenAnswer(call -> holds.release(id, call.getArgument(1)));
    ExecutorService pool = Executors.newSingleThreadExecutor();
    try {
      Future<Optional<PaymentStatus>> attempt = pool.submit(() -> attempting.attempt(id, true));
      assertThat(calling.await(10, TimeUnit.SECONDS)).isTrue();
      listener.onOrderCancelled(consumed(cancelled(id)));
      service.sweepReleases();
      flush.countDown();
      try {
        attempt.get(10, TimeUnit.SECONDS);
      } catch (Exception e) {
        // the commit may lose to the sweep; the reconciler has to cope
      }
      service.sweepReleases();
      reconcileAfterTheGracePeriod();
      Payment stored = payments.findById(id).orElseThrow();
      assertThat(holds.outstanding(id))
          .as("authorizations outstanding for a voided payment")
          .isZero();
    } finally {
      pool.shutdownNow();
    }
  }

  /**
   * B: the unversioned fallback makes a release due on a payment that is still open, so the sweep
   * releases with nothing kept; an approval of the open payment already granted at the processor is
   * released, and then committed as the payment's kept authorization.
   */
  @Test
  void probeB_fallbackOnOpenPaymentReleasesTheApprovalItThenKeeps() {
    String id = "b-" + UUID.randomUUID().toString().substring(0, 8);
    service.record(reserved(id), "c");
    PaymentRepository contended = mock(PaymentRepository.class, delegatesTo(payments));
    doThrow(new OptimisticLockingFailureException("another write won"))
        .when(contended)
        .findById(id);
    PaymentService losing =
        new PaymentService(
            contended,
            authorizer,
            outboxWriter,
            tx,
            decisions,
            clock,
            new SimpleMeterRegistry(),
            Duration.ZERO,
            Duration.ZERO);
    holds.grant(id, "AUTH-B-LATE", new BigDecimal("12.50"));
    losing.onLateOutcome(new LateOutcome(id, new AuthorizationOutcome.Authorized("AUTH-B-LATE")));
    // another attempt of the still open payment is approved; its answer is on its way back
    holds.grant(id, "AUTH-B-KEPT", new BigDecimal("12.50"));
    service.sweepReleases();
    assertThat(service.commit(id, new AuthorizationOutcome.Authorized("AUTH-B-KEPT")))
        .isEqualTo(PaymentStatus.AUTHORIZED);
    service.sweepReleases();
    reconcileAfterTheGracePeriod();
    Payment stored = payments.findById(id).orElseThrow();
    assertThat(holds.outstanding(id))
        .as("the authorized payment's own authorization at the processor")
        .isEqualTo(1);
  }

  private ConsumerRecord<String, String> consumed(DomainEvent event) {
    return new ConsumerRecord<>(event.topic(), 0, 0L, event.orderId(), codec.encode(event));
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
