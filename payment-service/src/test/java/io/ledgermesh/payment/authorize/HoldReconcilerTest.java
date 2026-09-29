package io.ledgermesh.payment.authorize;

import static org.assertj.core.api.Assertions.assertThat;

import io.ledgermesh.common.events.InventoryReserved;
import io.ledgermesh.common.events.OrderCancelled;
import io.ledgermesh.common.events.OrderLine;
import io.ledgermesh.common.outbox.OutboxEventRepository;
import io.ledgermesh.payment.domain.PaymentRepository;
import io.ledgermesh.payment.domain.PaymentStatus;
import io.ledgermesh.payment.processor.AuthorizationHolds;
import io.micrometer.core.instrument.MeterRegistry;
import java.math.BigDecimal;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

/**
 * The reconciler against the synthetic processor's own table: each kind of payment an authorization
 * can belong to, with the authorization past the grace period or still inside it. Nothing here asks
 * the sweeper for a release, so whatever is released, the reconciler released.
 */
@SpringBootTest(
    properties = {
      "spring.datasource.url=jdbc:h2:mem:reconciler;DB_CLOSE_DELAY=-1",
      "ledgermesh.payment.reconcile.page=2"
    })
class HoldReconcilerTest {

  @Autowired private PaymentService service;
  @Autowired private PaymentRepository payments;
  @Autowired private OutboxEventRepository outbox;
  @Autowired private AuthorizationHolds holds;
  @Autowired private ObjectProvider<HoldReconciler> reconciler;
  @Autowired private MeterRegistry meters;
  @Autowired private Clock clock;

  @BeforeEach
  void clean() {
    outbox.deleteAll();
    payments.deleteAll();
    // what earlier tests left outstanding is not this test's
    reconcile(clock.instant().plus(Duration.ofDays(1)));
  }

  @Test
  void aVoidedPaymentKeepsNothing() {
    String id = order();
    service.record(reserved(id), "c");
    grant(id, "V1");
    grant(id, "V2");
    service.cancel(cancelled(id));

    assertThat(reconcile(pastGrace())).isEqualTo(2);
    assertThat(holds.outstanding(id)).isZero();
  }

  @Test
  void aDeclinedPaymentKeepsNothing() {
    String id = order();
    service.record(reserved(id), "c");
    assertThat(service.commit(id, new AuthorizationOutcome.Declined("CARD_DECLINED")))
        .isEqualTo(PaymentStatus.DECLINED);
    grant(id, "D1");

    assertThat(reconcile(pastGrace())).isEqualTo(1);
    assertThat(holds.outstanding(id)).isZero();
  }

  @Test
  void anOrderWithNoPaymentKeepsNothing() {
    String id = order();
    grant(id, "N1");

    assertThat(reconcile(pastGrace())).isEqualTo(1);
    assertThat(holds.outstanding(id)).isZero();
  }

  @Test
  void anAuthorizedPaymentKeepsItsOwnCodeAndNothingElse() {
    String id = order();
    service.record(reserved(id), "c");
    grant(id, "K1");
    grant(id, "S1");
    assertThat(service.commit(id, new AuthorizationOutcome.Authorized(code(id, "K1"))))
        .isEqualTo(PaymentStatus.AUTHORIZED);

    assertThat(reconcile(pastGrace())).isEqualTo(1);
    assertThat(holds.outstanding(id)).isEqualTo(1);
    // the kept code is never released, however old
    assertThat(reconcile(pastGrace().plus(Duration.ofDays(30)))).isZero();
    assertThat(holds.outstanding(id)).isEqualTo(1);
  }

  @Test
  void anOpenPaymentKeepsNoApprovalItNeverCommitted() {
    String id = order();
    service.record(reserved(id), "c");
    grant(id, "O1");

    assertThat(reconcile(pastGrace())).isEqualTo(1);
    assertThat(holds.outstanding(id)).isZero();
    assertThat(payments.findById(id).orElseThrow().getStatus()).isEqualTo(PaymentStatus.NEW);
  }

  @Test
  void anApprovalYoungerThanTheGracePeriodIsLeftAloneUntilItIsNot() {
    String open = order();
    service.record(reserved(open), "c");
    grant(open, "Y1");
    String voided = order();
    service.record(reserved(voided), "c");
    grant(voided, "Y2");
    service.cancel(cancelled(voided));

    // still inside the grace period: an open payment's approval may be on its way to its commit
    assertThat(reconcile(clock.instant().plusSeconds(3))).isZero();
    assertThat(holds.outstanding(open)).isEqualTo(1);
    assertThat(holds.outstanding(voided)).isEqualTo(1);
    assertThat(meters.get("ledgermesh.payments.unkept.oldest.seconds").gauge().value())
        .isGreaterThanOrEqualTo(2);

    assertThat(reconcile(pastGrace())).isEqualTo(2);
    assertThat(holds.outstanding(open)).isZero();
    assertThat(holds.outstanding(voided)).isZero();
  }

  /** With a page of two, the kept authorizations of earlier orders fill the first pages. */
  @Test
  void anUnkeptAuthorizationBehindPagesOfKeptOnesIsReachedToo() {
    for (int i = 0; i < 5; i++) {
      String kept = order();
      service.record(reserved(kept), "c");
      grant(kept, "P" + i);
      service.commit(kept, new AuthorizationOutcome.Authorized(code(kept, "P" + i)));
    }
    String voided = order();
    service.record(reserved(voided), "c");
    grant(voided, "LAST");
    service.cancel(cancelled(voided));

    assertThat(reconcile(pastGrace())).isEqualTo(1);
    assertThat(holds.outstanding(voided)).isZero();
  }

  @Test
  void reconcilingAgainReleasesNothingMore() {
    String id = order();
    service.record(reserved(id), "c");
    grant(id, "I1");
    service.cancel(cancelled(id));
    double before = meters.counter("ledgermesh.payments.reconciled.released").count();

    assertThat(reconcile(pastGrace())).isEqualTo(1);
    assertThat(reconcile(pastGrace())).isZero();
    assertThat(meters.counter("ledgermesh.payments.reconciled.released").count() - before)
        .isEqualTo(1);
  }

  private int reconcile(Instant now) {
    HoldReconciler r = reconciler.getIfAvailable();
    return r == null ? 0 : r.reconcile(now);
  }

  private Instant pastGrace() {
    HoldReconciler r = reconciler.getIfAvailable();
    Duration grace = r == null ? Duration.ZERO : r.grace();
    return clock.instant().plus(grace).plus(Duration.ofSeconds(1));
  }

  private void grant(String orderId, String suffix) {
    holds.grant(orderId, code(orderId, suffix), new BigDecimal("12.50"));
  }

  private static String code(String orderId, String suffix) {
    return "AUTH-" + suffix + "-" + orderId;
  }

  private static String order() {
    return "rec-" + UUID.randomUUID().toString().substring(0, 8);
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
