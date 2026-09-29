package io.ledgermesh.payment.authorize;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/**
 * Drains the deferred queue (payments the listener could not finish, or that were deferred) and
 * asks the processor for every release that is due. It is the only place releases are asked for, so
 * {@code ledgermesh.payment.sweeper=false}, which the unit tests set to drive it by hand, would
 * leave every authorization a payment does not keep on the card; no deployment should set it.
 */
@Component
@ConditionalOnProperty(
    name = "ledgermesh.payment.sweeper",
    havingValue = "true",
    matchIfMissing = true)
public class DeferredPaymentSweeper {

  private final PaymentService payments;

  public DeferredPaymentSweeper(PaymentService payments) {
    this.payments = payments;
  }

  @Scheduled(fixedDelayString = "${ledgermesh.payment.sweep-ms:1000}")
  public void tick() {
    payments.sweep();
    payments.sweepReleases();
  }
}
