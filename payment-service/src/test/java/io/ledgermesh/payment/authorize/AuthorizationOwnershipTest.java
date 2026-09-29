package io.ledgermesh.payment.authorize;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.AdditionalAnswers.delegatesTo;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;

import io.github.resilience4j.timelimiter.TimeLimiterRegistry;
import io.ledgermesh.common.events.InventoryReserved;
import io.ledgermesh.common.events.OrderCancelled;
import io.ledgermesh.common.events.OrderLine;
import io.ledgermesh.common.outbox.OutboxEventRepository;
import io.ledgermesh.common.outbox.OutboxWriter;
import io.ledgermesh.payment.domain.AuthorizationDecisionRepository;
import io.ledgermesh.payment.domain.Payment;
import io.ledgermesh.payment.domain.PaymentRepository;
import io.ledgermesh.payment.domain.PaymentStatus;
import io.ledgermesh.payment.processor.AuthorizationHolds;
import io.ledgermesh.payment.processor.PaymentProcessor;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.math.BigDecimal;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.dao.OptimisticLockingFailureException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.support.TransactionSynchronizationManager;
import org.springframework.transaction.support.TransactionTemplate;

/** Safety at the reconciliation/approval boundary, with real database and processor holds. */
@SpringBootTest(properties = "spring.datasource.url=jdbc:h2:mem:ownership;DB_CLOSE_DELAY=-1")
class AuthorizationOwnershipTest {
  @Autowired PaymentService service;
  @Autowired PaymentRepository payments;
  @Autowired OutboxEventRepository outbox;
  @Autowired AuthorizationHolds holds;
  @Autowired PaymentProcessor processor;
  @Autowired HoldReconciler reconciler;
  @Autowired TimeLimiterRegistry limiters;
  @Autowired Clock clock;
  @Autowired AuthorizationDecisions decisions;
  @Autowired AuthorizationDecisionRepository decisionRows;
  @Autowired PaymentAuthorizer authorizer;
  @Autowired OutboxWriter writer;
  @Autowired TransactionTemplate tx;
  @Autowired JdbcTemplate jdbc;

  @BeforeEach
  void clean() {
    outbox.deleteAll();
    payments.deleteAll();
    decisionRows.deleteAll();
    jdbc.update("delete from processor_hold");
  }

  @Test
  void approvalDeliveredAfterRetirementCannotCompleteThePayment() {
    String id = newPayment();
    String code = "AUTH-" + id;
    holds.grant(id, code, new BigDecimal("12.50"));
    reconciler.reconcile(clock.instant().plus(reconciler.grace()).plusSeconds(1));
    assertThat(holds.outstanding(id)).isZero();
    long before = outbox.count();

    service.commit(id, new AuthorizationOutcome.Authorized(code));

    assertThat(payments.findById(id).orElseThrow().getStatus().isOpen()).isTrue();
    assertThat(outbox.count()).isEqualTo(before);
  }

  @Test
  void approvalRacingReconciliationLookupNeverKeepsAReleasedCode() throws Exception {
    String id = newPayment();
    String code = "AUTH-" + id;
    holds.grant(id, code, new BigDecimal("12.50"));
    // An existing undecided row exercises SELECT FOR UPDATE, separately from first-insert races.
    decisions.execute(code, id, decision -> null);
    CountDownLatch lookedUp = new CountDownLatch(1);
    CountDownLatch resume = new CountDownLatch(1);
    PaymentRepository held = mock(PaymentRepository.class, delegatesTo(payments));
    doAnswer(
            call -> {
              var rows = payments.findAllById(call.getArgument(0));
              lookedUp.countDown();
              assertThat(resume.await(10, TimeUnit.SECONDS)).isTrue();
              return rows;
            })
        .when(held)
        .findAllById(any());
    doAnswer(
            call -> {
              var row = payments.findById(id);
              lookedUp.countDown();
              assertThat(resume.await(10, TimeUnit.SECONDS)).isTrue();
              return row;
            })
        .when(held)
        .findById(id);
    HoldReconciler racing =
        new HoldReconciler(
            processor,
            held,
            decisions,
            limiters,
            clock,
            new SimpleMeterRegistry(),
            Duration.ZERO,
            5000,
            1000);
    try (var pool = Executors.newFixedThreadPool(2)) {
      var retirement = pool.submit(() -> racing.reconcile(clock.instant().plusSeconds(60)));
      try {
        assertThat(lookedUp.await(10, TimeUnit.SECONDS)).isTrue();
        var approval =
            pool.submit(() -> service.commit(id, new AuthorizationOutcome.Authorized(code)));
        // The old implementation commits immediately against the detached NEW snapshot. A
        // serialized decision instead waits here until the retirement transaction has committed.
        try {
          approval.get(300, TimeUnit.MILLISECONDS);
        } catch (java.util.concurrent.TimeoutException expected) {
          // Database lock is still held by reconciliation.
        }
        resume.countDown();
        retirement.get(10, TimeUnit.SECONDS);
        approval.get(10, TimeUnit.SECONDS);
        Payment stored = payments.findById(id).orElseThrow();
        assertThat(holds.outstanding(id))
            .isEqualTo(stored.getStatus() == PaymentStatus.AUTHORIZED ? 1 : 0);
      } finally {
        resume.countDown();
        pool.shutdownNow();
      }
    }
  }

  @Test
  void reconciliationWaitsForAClaimThenReadsTheCommittedPayment() throws Exception {
    String id = newPayment();
    String code = "AUTH-" + id;
    holds.grant(id, code, new BigDecimal("12.50"));
    decisions.execute(code, id, decision -> null);
    CountDownLatch read = new CountDownLatch(1);
    CountDownLatch listed = new CountDownLatch(1);
    CountDownLatch resume = new CountDownLatch(1);
    PaymentRepository held = mock(PaymentRepository.class, delegatesTo(payments));
    doAnswer(
            call -> {
              var result = payments.findById(id);
              read.countDown();
              assertThat(resume.await(10, TimeUnit.SECONDS)).isTrue();
              return result;
            })
        .when(held)
        .findById(id);
    PaymentProcessor listing = mock(PaymentProcessor.class, delegatesTo(processor));
    doAnswer(
            call -> {
              var result = processor.outstanding(call.getArgument(0), call.getArgument(1));
              listed.countDown();
              return result;
            })
        .when(listing)
        .outstanding(any(), org.mockito.ArgumentMatchers.anyInt());
    HoldReconciler racing =
        new HoldReconciler(
            listing,
            payments,
            decisions,
            limiters,
            clock,
            new SimpleMeterRegistry(),
            Duration.ZERO,
            5000,
            1000);
    try (var pool = Executors.newFixedThreadPool(2)) {
      var approval =
          pool.submit(
              () ->
                  serviceUsing(held, decisions)
                      .commit(id, new AuthorizationOutcome.Authorized(code)));
      try {
        assertThat(read.await(10, TimeUnit.SECONDS)).isTrue();
        var retirement = pool.submit(() -> racing.reconcile(clock.instant().plusSeconds(60)));
        assertThat(listed.await(10, TimeUnit.SECONDS)).isTrue();
        assertThatThrownBy(() -> retirement.get(300, TimeUnit.MILLISECONDS))
            .isInstanceOf(java.util.concurrent.TimeoutException.class);
        resume.countDown();
        assertThat(approval.get(10, TimeUnit.SECONDS)).isEqualTo(PaymentStatus.AUTHORIZED);
        assertThat(retirement.get(10, TimeUnit.SECONDS)).isZero();
      } finally {
        resume.countDown();
        pool.shutdownNow();
      }
    }
    assertThat(holds.outstanding(id)).isEqualTo(1);
    assertThat(outbox.count()).isEqualTo(1);
  }

  @Test
  void concurrentFirstInsertRetriesOnlyAfterRollbackAndCompletesOnce() throws Exception {
    String id = newPayment();
    String code = "AUTH-" + id;
    holds.grant(id, code, new BigDecimal("12.50"));
    var rows = mock(AuthorizationDecisionRepository.class, delegatesTo(decisionRows));
    var bothAbsent = new CyclicBarrier(2);
    AtomicInteger reads = new AtomicInteger();
    doAnswer(
            call -> {
              var result = decisionRows.lockCode(code);
              if (reads.incrementAndGet() <= 2) {
                assertThat(result).isEmpty();
                bothAbsent.await(10, TimeUnit.SECONDS);
              }
              return result;
            })
        .when(rows)
        .lockCode(code);
    PaymentService racing = serviceUsing(payments, new AuthorizationDecisions(rows, tx));
    try (var pool = Executors.newFixedThreadPool(2)) {
      var first = pool.submit(() -> racing.commit(id, new AuthorizationOutcome.Authorized(code)));
      var second = pool.submit(() -> racing.commit(id, new AuthorizationOutcome.Authorized(code)));
      assertThat(first.get(10, TimeUnit.SECONDS)).isEqualTo(PaymentStatus.AUTHORIZED);
      assertThat(second.get(10, TimeUnit.SECONDS)).isEqualTo(PaymentStatus.AUTHORIZED);
    }
    assertThat(reads.get()).isGreaterThanOrEqualTo(3);
    assertThat(decisionRows.count()).isEqualTo(1);
    assertThat(outbox.count()).isEqualTo(1);
    assertThat(holds.outstanding(id)).isEqualTo(1);
  }

  @Test
  void competingCodesRollBackTheLosingClaimAndCompletion() throws Exception {
    String id = newPayment();
    String firstCode = "A-" + id;
    String secondCode = "B-" + id;
    holds.grant(id, firstCode, new BigDecimal("12.50"));
    holds.grant(id, secondCode, new BigDecimal("12.50"));
    var bothRead = new CyclicBarrier(2);
    PaymentRepository held = mock(PaymentRepository.class, delegatesTo(payments));
    doAnswer(
            call -> {
              var result = payments.findById(id);
              bothRead.await(10, TimeUnit.SECONDS);
              return result;
            })
        .when(held)
        .findById(id);
    PaymentService racing = serviceUsing(held, decisions);
    AtomicInteger conflicts = new AtomicInteger();
    try (var pool = Executors.newFixedThreadPool(2)) {
      var futures =
          List.of(firstCode, secondCode).stream()
              .map(
                  code ->
                      pool.submit(
                          () -> {
                            try {
                              racing.commit(id, new AuthorizationOutcome.Authorized(code));
                            } catch (OptimisticLockingFailureException expected) {
                              conflicts.incrementAndGet();
                            }
                          }))
              .toList();
      for (var future : futures) {
        future.get(10, TimeUnit.SECONDS);
      }
    }
    assertThat(conflicts.get()).isEqualTo(1);
    assertThat(outbox.count()).isEqualTo(1);
    assertThat(decisionRows.count()).isEqualTo(1);
    String kept = payments.findById(id).orElseThrow().getAuthorizationCode();
    assertThat(decisionRows.findById(kept)).isPresent();
    assertThat(decisionRows.findById(kept.equals(firstCode) ? secondCode : firstCode)).isEmpty();
    reconciler.reconcile(clock.instant().plusSeconds(60));
    assertThat(holds.outstanding(id)).isEqualTo(1);
  }

  @Test
  void cancellationWinsAnInFlightClaimWithoutLeavingAClaimOrCompletion() throws Exception {
    String id = newPayment();
    String code = "AUTH-" + id;
    holds.grant(id, code, new BigDecimal("12.50"));
    CountDownLatch read = new CountDownLatch(1);
    CountDownLatch resume = new CountDownLatch(1);
    PaymentRepository held = mock(PaymentRepository.class, delegatesTo(payments));
    doAnswer(
            call -> {
              var result = payments.findById(id);
              read.countDown();
              assertThat(resume.await(10, TimeUnit.SECONDS)).isTrue();
              return result;
            })
        .when(held)
        .findById(id);
    try (var pool = Executors.newSingleThreadExecutor()) {
      var approval =
          pool.submit(
              () ->
                  serviceUsing(held, decisions)
                      .commit(id, new AuthorizationOutcome.Authorized(code)));
      try {
        assertThat(read.await(10, TimeUnit.SECONDS)).isTrue();
        service.cancel(
            new OrderCancelled(
                "cancel-" + id,
                id,
                "c",
                Instant.now(),
                "PAYMENT_TIMEOUT",
                List.of(new OrderLine("SKU", 1))));
        resume.countDown();
        assertThatThrownBy(() -> approval.get(10, TimeUnit.SECONDS))
            .hasCauseInstanceOf(OptimisticLockingFailureException.class);
      } finally {
        resume.countDown();
        pool.shutdownNow();
      }
    }
    assertThat(decisionRows.findById(code)).isEmpty();
    assertThat(outbox.findAll())
        .allSatisfy(event -> assertThat(event.getTopic()).isEqualTo("payment.voided"));
    service.onLateOutcome(new LateOutcome(id, new AuthorizationOutcome.Authorized(code)));
    reconciler.reconcile(clock.instant().plusSeconds(60));
    assertThat(payments.findById(id).orElseThrow().getStatus()).isEqualTo(PaymentStatus.VOIDED);
    assertThat(holds.outstanding(id)).isZero();
    assertThat(decisionRows.findById(code).orElseThrow().isRetired()).isTrue();
  }

  @Test
  void failedReleaseLeavesCommittedRetirementForARecreatedReconciler() {
    String id = newPayment();
    String code = "AUTH-" + id;
    holds.grant(id, code, new BigDecimal("12.50"));
    PaymentProcessor unavailable = mock(PaymentProcessor.class, delegatesTo(processor));
    doAnswer(
            call -> {
              assertThat(TransactionSynchronizationManager.isActualTransactionActive()).isFalse();
              assertThat(decisionRows.findById(code).orElseThrow().isRetired()).isTrue();
              throw new IllegalStateException("provider unavailable after retirement commit");
            })
        .when(unavailable)
        .releaseAuthorization(id, code);
    HoldReconciler first =
        new HoldReconciler(
            unavailable,
            payments,
            decisions,
            limiters,
            clock,
            new SimpleMeterRegistry(),
            Duration.ZERO,
            5000,
            1000);
    assertThat(first.reconcile(clock.instant().plusSeconds(60))).isZero();
    assertThat(holds.outstanding(id)).isEqualTo(1);
    service.onLateOutcome(new LateOutcome(id, new AuthorizationOutcome.Authorized(code)));
    assertThat(payments.findById(id).orElseThrow().getStatus().isOpen()).isTrue();
    assertThat(outbox.count()).isZero();
    HoldReconciler restarted =
        new HoldReconciler(
            processor,
            payments,
            new AuthorizationDecisions(decisionRows, tx),
            limiters,
            clock,
            new SimpleMeterRegistry(),
            Duration.ZERO,
            5000,
            1000);
    assertThat(restarted.reconcile(clock.instant().plusSeconds(60))).isEqualTo(1);
    assertThat(holds.outstanding(id)).isZero();
    service.commit(id, new AuthorizationOutcome.Authorized(code));
    assertThat(outbox.count()).isZero();
  }

  @Test
  void legacyAuthorizedPaymentIsRecognizedWithoutAProviderRelease() {
    String id = newPayment();
    String code = "AUTH-" + id;
    holds.grant(id, code, new BigDecimal("12.50"));
    tx.executeWithoutResult(
        status -> payments.findById(id).orElseThrow().authorized(code, clock.instant()));
    assertThat(decisionRows.findById(code)).isEmpty();

    assertThat(reconciler.reconcile(clock.instant().plusSeconds(60))).isZero();

    assertThat(holds.outstanding(id)).isEqualTo(1);
    assertThat(decisionRows.findById(code).orElseThrow().isRetired()).isFalse();
    assertThat(outbox.count()).isZero();
  }

  @Test
  void oneAuthorizationCodeCannotBeReboundToAnotherOrder() {
    String first = newPayment();
    String second = newPayment();
    String code = "AUTH-" + first;
    service.commit(first, new AuthorizationOutcome.Authorized(code));

    assertThatThrownBy(() -> service.commit(second, new AuthorizationOutcome.Authorized(code)))
        .isInstanceOf(IllegalArgumentException.class);

    assertThat(payments.findById(second).orElseThrow().getStatus()).isEqualTo(PaymentStatus.NEW);
    assertThat(outbox.count()).isEqualTo(1);
    assertThat(decisionRows.count()).isEqualTo(1);
  }

  @Test
  void claimRefusesAnAmbientTransactionRatherThanReusingItsStaleEntity() {
    String id = newPayment();
    assertThatThrownBy(
            () ->
                tx.executeWithoutResult(
                    status -> {
                      payments.findById(id).orElseThrow();
                      service.commit(id, new AuthorizationOutcome.Authorized("AUTH-" + id));
                    }))
        .isInstanceOf(IllegalStateException.class);
    assertThat(outbox.count()).isZero();
    assertThat(decisionRows.count()).isZero();
  }

  private PaymentService serviceUsing(
      PaymentRepository repository, AuthorizationDecisions coordinator) {
    return new PaymentService(
        repository,
        authorizer,
        writer,
        tx,
        coordinator,
        clock,
        new SimpleMeterRegistry(),
        Duration.ZERO,
        Duration.ZERO);
  }

  private String newPayment() {
    String id = UUID.randomUUID().toString();
    service.record(
        new InventoryReserved(
            "evt-" + id,
            id,
            "c",
            Instant.now(),
            "cust",
            List.of(new OrderLine("SKU", 1)),
            new BigDecimal("12.50")),
        "c");
    return id;
  }
}
